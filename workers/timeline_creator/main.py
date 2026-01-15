import json
import os
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytz
import requests
from supabase import ClientOptions, create_client

from workers.lib.json_utils import safe_parse_model_json
from workers.lib.simple_queue import ack, receive

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for timeline_creator")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "timeline.plan"
DEFAULT_TZ = "America/New_York"
ACTIVE_DAYS = int(os.getenv("TIMELINE_ACTIVE_DAYS", "3"))
SELF_SCHEDULE = os.getenv("TIMELINE_SELF_SCHEDULE", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SCHEDULE_TZ = os.getenv("TIMELINE_SCHEDULE_TZ", DEFAULT_TZ)
SCHEDULE_HOUR = int(os.getenv("TIMELINE_SCHEDULE_HOUR", "2"))
SCHEDULE_MINUTE = int(os.getenv("TIMELINE_SCHEDULE_MINUTE", "0"))
SCHEDULE_WINDOW_MINUTES = int(os.getenv("TIMELINE_SCHEDULE_WINDOW_MINUTES", "10"))

RUNPOD_URL = (os.getenv("RUNPOD_URL") or "").rstrip("/")
RUNPOD_MODEL = os.getenv("TIMELINE_MODEL") or os.getenv(
    "RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored"
)

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "timeline_plan.txt"


def _load_prompt_template() -> str:
    if not PROMPT_PATH.exists():
        raise FileNotFoundError(f"Missing prompt template: {PROMPT_PATH}")
    return PROMPT_PATH.read_text(encoding="utf-8")


def _render_prompt(input_json: str) -> str:
    template = _load_prompt_template()
    return template.replace("{TIMELINE_INPUT}", input_json)


def _now_local(tz_name: str) -> datetime:
    tz = pytz.timezone(tz_name)
    return datetime.now(tz)


def _plan_date_from_payload(payload: Dict[str, Any]) -> Tuple[str, str, datetime]:
    tz_name = payload.get("tz") or DEFAULT_TZ
    now_local = _now_local(tz_name)
    plan_date = payload.get("plan_date")
    if plan_date:
        return str(plan_date), tz_name, now_local
    days_ahead = int(payload.get("days_ahead", 1))
    plan_date = (now_local.date() + timedelta(days=days_ahead)).isoformat()
    return plan_date, tz_name, now_local


def _in_schedule_window(now_local: datetime) -> bool:
    if now_local.hour != SCHEDULE_HOUR:
        return False
    minute = now_local.minute
    window_end = SCHEDULE_MINUTE + max(1, SCHEDULE_WINDOW_MINUTES)
    return SCHEDULE_MINUTE <= minute < window_end


def _fetch_active_thread_ids(days: int, page_size: int = 1000) -> List[int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_iso = cutoff.isoformat()
    thread_ids: set[int] = set()
    offset = 0

    while True:
        rows = (
            SB.table("messages")
            .select("thread_id")
            .gte("created_at", cutoff_iso)
            .order("thread_id")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break
        for row in rows:
            thread_id = row.get("thread_id")
            if thread_id is None:
                continue
            try:
                thread_ids.add(int(thread_id))
            except Exception:
                continue
        if len(rows) < page_size:
            break
        offset += page_size

    return sorted(thread_ids)


def _fetch_thread_context(thread_id: int) -> Dict[str, Any] | None:
    thread = (
        SB.table("threads")
        .select("id,creator_id,fan_identity_card,creator_identity_card")
        .eq("id", thread_id)
        .single()
        .execute()
        .data
        or {}
    )
    if not thread:
        return None

    creator_card = thread.get("creator_identity_card")
    creator_id = thread.get("creator_id")
    if not creator_card and creator_id:
        try:
            creator_row = (
                SB.table("creators")
                .select("creator_identity_card")
                .eq("id", creator_id)
                .single()
                .execute()
                .data
                or {}
            )
            creator_card = creator_row.get("creator_identity_card")
        except Exception:
            creator_card = None

    return {
        "thread_id": thread_id,
        "creator_id": creator_id,
        "fan_identity_card": thread.get("fan_identity_card") or "Identity card: empty",
        "creator_identity_card": creator_card or "Identity card: empty",
    }


def _fetch_turns_window(
    thread_id: int, start_local: datetime, end_local: datetime
) -> List[Dict[str, Any]]:
    start_utc = start_local.astimezone(timezone.utc).isoformat()
    end_utc = end_local.astimezone(timezone.utc).isoformat()
    rows = (
        SB.table("messages")
        .select("turn_index,sender,message_text,created_at")
        .eq("thread_id", thread_id)
        .gte("created_at", start_utc)
        .lte("created_at", end_utc)
        .order("turn_index")
        .execute()
        .data
        or []
    )
    return rows


def _plan_exists(thread_id: int, plan_date: str) -> bool:
    rows = (
        SB.table("daily_plans")
        .select("id")
        .eq("thread_id", thread_id)
        .eq("plan_date", plan_date)
        .limit(1)
        .execute()
        .data
        or []
    )
    return bool(rows)


def _call_llm(prompt: str) -> str:
    if not RUNPOD_URL:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": RUNPOD_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": int(os.getenv("TIMELINE_MAX_TOKENS", "1200")),
        "temperature": float(os.getenv("TIMELINE_TEMPERATURE", "0.2")),
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=180)
    resp.raise_for_status()
    data = resp.json()

    raw_text = ""
    try:
        if data.get("choices"):
            choice = data["choices"][0]
            message = choice.get("message") or {}
            raw_text = message.get("content") or message.get("reasoning") or ""
            if not raw_text:
                raw_text = choice.get("text") or ""
    except Exception:
        pass

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text.strip()


def _upsert_plan(
    *,
    thread_id: int,
    plan_date: str,
    plan_json: dict | None,
    raw_text: str,
    status: str,
    error: str | None,
    tz_name: str,
) -> None:
    payload = {
        "thread_id": thread_id,
        "plan_date": plan_date,
        "plan_json": plan_json,
        "raw_text": raw_text,
        "status": status,
        "error": error,
        "time_zone": tz_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    SB.table("daily_plans").upsert(
        payload, on_conflict="thread_id,plan_date"
    ).execute()


def _build_plan_for_thread(
    thread_id: int,
    plan_date: str,
    tz_name: str,
    now_local: datetime,
    force: bool = False,
) -> None:
    if not force and _plan_exists(thread_id, plan_date):
        print(
            f"[timeline_creator] plan exists for thread {thread_id} on {plan_date}; skipping",
            flush=True,
        )
        return

    context = _fetch_thread_context(thread_id)
    if not context:
        print(
            f"[timeline_creator] thread {thread_id} missing context; skipping",
            flush=True,
        )
        return

    window_end_local = now_local
    window_start_local = now_local - timedelta(hours=24)
    day_turns = _fetch_turns_window(thread_id, window_start_local, window_end_local)
    input_payload = {
        "thread_id": thread_id,
        "plan_date": plan_date,
        "time_zone": tz_name,
        "now_local": now_local.isoformat(),
        "creator_identity_card": context["creator_identity_card"],
        "fan_identity_card": context["fan_identity_card"],
        "turns_for_day": day_turns,
    }
    prompt = _render_prompt(json.dumps(input_payload))

    raw_text = ""
    status = "failed"
    error = None
    plan_json = None

    try:
        raw_text = _call_llm(prompt)
        plan_json, error = safe_parse_model_json(raw_text)
        if error or not isinstance(plan_json, dict):
            status = "failed"
            if not error:
                error = "model_output_not_object"
        else:
            status = "ok"
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = str(exc)

    _upsert_plan(
        thread_id=thread_id,
        plan_date=plan_date,
        plan_json=plan_json if status == "ok" else None,
        raw_text=raw_text,
        status=status,
        error=error,
        tz_name=tz_name,
    )


def process_job(payload: Dict[str, Any]) -> bool:
    plan_date, tz_name, now_local = _plan_date_from_payload(payload or {})
    force = bool(payload.get("force")) if isinstance(payload, dict) else False
    active_threads = _fetch_active_thread_ids(ACTIVE_DAYS)

    print(
        f"[timeline_creator] planning for {plan_date} in {tz_name} "
        f"({len(active_threads)} active threads)",
        flush=True,
    )

    for thread_id in active_threads:
        try:
            _build_plan_for_thread(
                thread_id,
                plan_date,
                tz_name,
                now_local,
                force=force,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[timeline_creator] error for thread {thread_id}: {exc}")

    return True


def _maybe_run_scheduled(last_run_date: date | None) -> date | None:
    now_local = _now_local(SCHEDULE_TZ)
    today = now_local.date()
    if last_run_date == today:
        return last_run_date
    if not _in_schedule_window(now_local):
        return last_run_date

    print(
        f"[timeline_creator] self-schedule triggered at {now_local.isoformat()}",
        flush=True,
    )
    payload = {"tz": SCHEDULE_TZ, "days_ahead": 1}
    try:
        process_job(payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[timeline_creator] self-schedule error: {exc}", flush=True)
    return today


if __name__ == "__main__":
    print("[timeline_creator] started - waiting for jobs", flush=True)
    last_run_date = None
    while True:
        if SELF_SCHEDULE:
            last_run_date = _maybe_run_scheduled(last_run_date)

        job = receive(QUEUE, 1800)
        if job:
            row_id = job["row_id"]
            try:
                payload = job["payload"]
                if process_job(payload):
                    ack(row_id)
            except Exception as exc:  # noqa: BLE001
                print("[timeline_creator] error:", exc)
                traceback.print_exc()
                time.sleep(2)
            continue

        time.sleep(1)
