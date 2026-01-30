import datetime
import os
import time
import uuid

import requests
from postgrest.exceptions import APIError
from supabase import ClientOptions, create_client

from workers.lib.job_utils import job_exists
from workers.lib.reply_run_tracking import upsert_step
from workers.lib.simple_queue import send


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for fan sim")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

THREAD_IDS_RAW = os.getenv("FAN_SIM_THREAD_IDS", "").strip()
if not THREAD_IDS_RAW:
    raise RuntimeError("FAN_SIM_THREAD_IDS is required (comma-separated thread ids)")

THREAD_IDS = [
    int(part)
    for part in [p.strip() for p in THREAD_IDS_RAW.replace(";", ",").split(",")]
    if part.strip()
]
if not THREAD_IDS:
    raise RuntimeError("FAN_SIM_THREAD_IDS did not contain any ids")

POLL_SECONDS = float(os.getenv("FAN_SIM_POLL_SECONDS", "2"))
CURSOR_TABLE = os.getenv("FAN_SIM_CURSOR_TABLE", "fan_sim_cursors").strip() or "fan_sim_cursors"
HISTORY_LIMIT = int(os.getenv("FAN_SIM_HISTORY_LIMIT", "0"))
MAX_TURNS = int(os.getenv("FAN_SIM_MAX_TURNS", "0"))
SIM_ENABLED = os.getenv("FAN_SIM_ENABLED", "true").lower() in {"1", "true", "yes", "on"}

BOOTSTRAP_MODE = os.getenv("FAN_SIM_BOOTSTRAP", "latest").lower().strip()
# BOOTSTRAP_MODE:
#   "latest" (default): set cursor to latest creator message and wait for new ones
#   "reply_latest": reply once to the latest creator message if no cursor yet

RAW_SOURCE_CHANNEL = (os.getenv("FAN_SIM_SOURCE_CHANNEL") or "live").strip().lower()
SOURCE_CHANNEL = RAW_SOURCE_CHANNEL if RAW_SOURCE_CHANNEL in {"live", "scheduler", "backfill"} else "live"


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_ts(value) -> datetime.datetime | None:
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            return datetime.datetime.fromisoformat(raw)
        except Exception:
            return None
    return None


def _run_has_jobs(run_id: str) -> bool:
    try:
        rows = (
            SB.table("job_queue")
            .select("id")
            .filter("payload->>run_id", "eq", str(run_id))
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        return True
    return bool(rows)


def _cancel_stale_run(run_id: str, *, reason: str) -> None:
    now = _utcnow().isoformat()
    try:
        SB.table("reply_runs").update(
            {
                "status": "canceled",
                "canceled_at": now,
                "cancel_reason": str(reason or "stale"),
                "current_step": "canceled",
            }
        ).eq("run_id", str(run_id)).execute()
    except Exception:
        return


def _active_reply_run(thread_id: int) -> dict | None:
    try:
        rows = (
            SB.table("reply_runs")
            .select("run_id,root_fan_message_id,created_at")
            .eq("thread_id", int(thread_id))
            .eq("status", "active")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        return None
    active = rows[0] if rows and isinstance(rows[0], dict) else None
    if not active:
        return None

    run_id = str(active.get("run_id") or "")
    created_at = _parse_ts(active.get("created_at"))
    if run_id and created_at:
        try:
            age = (_utcnow() - created_at).total_seconds()
        except Exception:
            age = None
        if age is not None and age >= 15 and not _run_has_jobs(run_id):
            _cancel_stale_run(run_id, reason="stale_active_run_no_jobs")
            return None

    return active


def _create_reply_run(*, thread_id: int, message_id: int, snapshot_end_turn_index: int) -> str | None:
    payload = {
        "thread_id": int(thread_id),
        "trigger_source": "fan",
        "root_fan_message_id": int(message_id),
        "snapshot_end_turn_index": int(snapshot_end_turn_index),
        "status": "active",
        "current_step": "queued",
        "metadata": {"source": "fan_sim"},
    }
    try:
        row = SB.table("reply_runs").insert(payload).execute().data
    except Exception:
        return None
    if not row or not isinstance(row, list) or not isinstance(row[0], dict):
        return None
    run_id = str(row[0].get("run_id") or "")
    return run_id or None


def _enqueue_reply_pipeline(*, message_id: int, run_id: str | None, has_media: bool) -> None:
    payload = {"message_id": int(message_id)}
    if run_id:
        payload["run_id"] = str(run_id)

    if has_media:
        if not job_exists("argus.analyse", message_id, client=SB):
            send("argus.analyse", payload)
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="argus",
                attempt=0,
                status="pending",
                client=SB,
                message_id=int(message_id),
            )
    else:
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="argus",
                attempt=0,
                status="skipped",
                client=SB,
                message_id=int(message_id),
                mode="skip",
                meta={"reason": "fan_sim_no_media"},
            )

    if not job_exists("iris.decide", message_id, client=SB):
        send("iris.decide", payload)


def _kick_reply_flow(*, message_id: int, thread_id: int, turn_index: int, has_media: bool) -> None:
    active_run = _active_reply_run(int(thread_id))
    if active_run:
        active_root = active_run.get("root_fan_message_id")
        if active_root is None or int(active_root) != int(message_id):
            if not job_exists("reply.supervise", message_id, client=SB):
                send("reply.supervise", {"message_id": int(message_id)})
        else:
            _enqueue_reply_pipeline(
                message_id=int(message_id),
                run_id=str(active_run.get("run_id") or "") or None,
                has_media=has_media,
            )
        return

    run_id = _create_reply_run(
        thread_id=int(thread_id),
        message_id=int(message_id),
        snapshot_end_turn_index=int(turn_index),
    )
    _enqueue_reply_pipeline(message_id=int(message_id), run_id=run_id, has_media=has_media)


def _latest_summary_end(thread_id: int, tier: str) -> int:
    row = (
        SB.table("summaries")
        .select("end_turn")
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not row:
        return 0
    return int(row[0].get("end_turn") or 0)


def _summary_exists(thread_id: int, tier: str, start_turn: int, end_turn: int) -> bool:
    existing = (
        SB.table("summaries")
        .select("id")
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .eq("start_turn", start_turn)
        .eq("end_turn", end_turn)
        .limit(1)
        .execute()
        .data
    )
    return bool(existing)


def _pending_summary_job(queue: str, thread_id: int, start_turn: int, end_turn: int) -> bool:
    jobs = (
        SB.table("job_queue")
        .select("id")
        .eq("queue", queue)
        .filter("payload->>thread_id", "eq", str(thread_id))
        .filter("payload->>start_turn", "eq", str(start_turn))
        .filter("payload->>end_turn", "eq", str(end_turn))
        .limit(1)
        .execute()
        .data
    )
    return bool(jobs)


def _maybe_enqueue_episode_summary(thread_id: int, latest_turn_index: int) -> None:
    last_episode_end = _latest_summary_end(thread_id, "episode")
    unsummarized = latest_turn_index - last_episode_end
    if unsummarized < 40:
        return
    start_turn = last_episode_end + 1
    end_turn = start_turn + 19
    if _summary_exists(thread_id, "episode", start_turn, end_turn):
        return
    if _pending_summary_job("episode.abstract", thread_id, start_turn, end_turn):
        return
    send(
        "episode.abstract",
        {"thread_id": thread_id, "start_turn": start_turn, "end_turn": end_turn},
    )


def _next_turn_index(thread_id: int) -> int:
    latest = (
        SB.table("messages")
        .select("turn_index")
        .eq("thread_id", thread_id)
        .order("turn_index", desc=True)
        .limit(1)
        .execute()
        .data
    )
    latest_turn = latest[0]["turn_index"] if latest else 0
    return int(latest_turn or 0) + 1


def _get_cursor(thread_id: int) -> int | None:
    rows = (
        SB.table(CURSOR_TABLE)
        .select("last_creator_message_id")
        .eq("thread_id", int(thread_id))
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return None
    value = rows[0].get("last_creator_message_id")
    return int(value) if value is not None else None


def _set_cursor(thread_id: int, message_id: int) -> None:
    payload = {
        "thread_id": int(thread_id),
        "last_creator_message_id": int(message_id),
        "updated_at": _utcnow().isoformat(),
    }
    SB.table(CURSOR_TABLE).upsert(payload).execute()


def _latest_creator_message_id(thread_id: int) -> int | None:
    rows = (
        SB.table("messages")
        .select("id")
        .eq("thread_id", int(thread_id))
        .eq("sender", "creator")
        .order("id", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return None
    return int(rows[0].get("id") or 0) or None


def _bootstrap_cursor(thread_id: int) -> int | None:
    latest_id = _latest_creator_message_id(thread_id)
    if latest_id is None:
        return None
    _set_cursor(thread_id, latest_id)
    return latest_id


def _fetch_creator_messages(thread_id: int, after_id: int, limit: int = 10) -> list[dict]:
    return (
        SB.table("messages")
        .select("id,thread_id,message_text,created_at")
        .eq("thread_id", int(thread_id))
        .eq("sender", "creator")
        .gt("id", int(after_id))
        .order("id", desc=False)
        .limit(limit)
        .execute()
        .data
        or []
    )


def _fetch_history(thread_id: int, limit: int) -> list[dict]:
    if limit and int(limit) > 0:
        rows = (
            SB.table("messages")
            .select("id,sender,message_text,created_at")
            .eq("thread_id", int(thread_id))
            .order("id", desc=True)
            .limit(int(limit))
            .execute()
            .data
            or []
        )
        return list(reversed(rows))
    return (
        SB.table("messages")
        .select("id,sender,message_text,created_at")
        .eq("thread_id", int(thread_id))
        .order("id", desc=False)
        .execute()
        .data
        or []
    )


def _call_fan_model(history: list[dict]) -> str:
    base = (os.getenv("FAN_SIM_URL") or os.getenv("RUNPOD_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("FAN_SIM_URL or RUNPOD_URL not set for fan simulator")
    key = os.getenv("FAN_SIM_API_KEY") or os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("FAN_SIM_API_KEY or RUNPOD_API_KEY is not set")
    url = f"{base}/v1/chat/completions"
    model = os.getenv("FAN_SIM_MODEL") or os.getenv("RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored")
    system_prompt = os.getenv(
        "FAN_SIM_SYSTEM_PROMPT",
        "You are the fan in a private chat. Respond with the next fan message only.",
    )

    history_lines = []
    for idx, row in enumerate(history, 1):
        speaker = "Fan" if row.get("sender") == "fan" else "Creator"
        text = (row.get("message_text") or "").strip()
        created_at = row.get("created_at") or ""
        line = f"{idx:02d}. [{created_at}] {speaker}: {text}"
        history_lines.append(line)

    user_prompt = (
        "Conversation (all turns, oldest -> newest):\n"
        + "\n".join(history_lines)
        + "\n\nNow produce the next FAN message."
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": float(os.getenv("FAN_SIM_TEMPERATURE", "0.7")),
        "max_tokens": int(os.getenv("FAN_SIM_MAX_TOKENS", "20000")),
    }

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    raw_text = ""
    try:
        if data.get("choices"):
            msg = data["choices"][0].get("message") or {}
            raw_text = msg.get("content") or msg.get("reasoning") or ""
            if not raw_text:
                raw_text = data["choices"][0].get("text") or ""
    except Exception:
        raw_text = ""
    return (raw_text or "").strip()


def _insert_fan_message(thread_id: int, reply: str) -> int | None:
    if not reply:
        return None
    turn_index = _next_turn_index(int(thread_id))
    payload = {
        "thread_id": int(thread_id),
        "ext_message_id": f"sim-{uuid.uuid4()}",
        "sender": "fan",
        "message_text": reply,
        "source_channel": SOURCE_CHANNEL,
        "created_at": _utcnow().isoformat(),
        "turn_index": int(turn_index),
    }
    try:
        res = SB.table("messages").insert(payload, upsert=False).execute().data
        if not res:
            return None
        msg_id = res[0]["id"]
    except APIError as exc:
        err = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {}
        if err.get("code") == "23505":
            return None
        raise

    SB.table("threads").update({"turn_count": turn_index}).eq("id", int(thread_id)).execute()
    _kick_reply_flow(
        message_id=int(msg_id),
        thread_id=int(thread_id),
        turn_index=int(turn_index),
        has_media=False,
    )
    _maybe_enqueue_episode_summary(int(thread_id), int(turn_index))
    return int(msg_id)


def _should_stop_for_thread(thread_id: int) -> bool:
    if MAX_TURNS and int(MAX_TURNS) > 0:
        rows = (
            SB.table("messages")
            .select("id", count="exact")
            .eq("thread_id", int(thread_id))
            .execute()
        )
        count = rows.count if hasattr(rows, "count") else None
        if count is not None and count >= int(MAX_TURNS):
            return True
    return False


def _process_thread(thread_id: int) -> None:
    if _should_stop_for_thread(thread_id):
        return

    cursor = _get_cursor(thread_id)
    if cursor is None:
        latest = _bootstrap_cursor(thread_id)
        if BOOTSTRAP_MODE == "reply_latest" and latest:
            cursor = latest - 1
        else:
            return

    creator_rows = _fetch_creator_messages(thread_id, cursor, limit=10)
    if not creator_rows:
        return

    for row in creator_rows:
        creator_id = int(row.get("id") or 0)
        if creator_id <= 0:
            continue
        history = _fetch_history(thread_id, HISTORY_LIMIT)
        if not history:
            _set_cursor(thread_id, creator_id)
            continue
        try:
            reply = _call_fan_model(history)
        except Exception as exc:
            print(f"[fan_sim] model error thread {thread_id}: {exc}", flush=True)
            return
        if not reply:
            _set_cursor(thread_id, creator_id)
            continue
        msg_id = _insert_fan_message(thread_id, reply)
        if msg_id:
            _set_cursor(thread_id, creator_id)


def run_loop() -> None:
    print("[fan_sim] started", flush=True)
    while True:
        if SIM_ENABLED:
            for thread_id in THREAD_IDS:
                try:
                    _process_thread(int(thread_id))
                except Exception as exc:
                    print(f"[fan_sim] error thread {thread_id}: {exc}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run_loop()

