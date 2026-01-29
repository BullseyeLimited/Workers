import datetime
import json
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
    raise RuntimeError("Missing Supabase configuration for test harness")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

TEST_TABLE = os.getenv("TEST_CONVERSATIONS_TABLE", "test_conversations")
POLL_SECONDS = float(os.getenv("TEST_HARNESS_POLL_SECONDS", "2"))
FAN_BATCH_SIZE = int(os.getenv("TEST_HARNESS_FAN_BATCH", "10"))
CREATOR_BATCH_SIZE = int(os.getenv("TEST_HARNESS_CREATOR_BATCH", "10"))
TEST_SOURCE_CHANNEL = os.getenv("TEST_SOURCE_CHANNEL", "live").strip().lower() or "live"

FAN_SIM_ENABLED = os.getenv("TEST_FAN_SIM_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FAN_SIM_MAX_TURNS = int(os.getenv("TEST_FAN_SIM_MAX_TURNS", "30"))
FAN_SIM_HISTORY_LIMIT = int(os.getenv("TEST_FAN_SIM_HISTORY_LIMIT", "0"))


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


def _create_reply_run(
    *, thread_id: int, message_id: int, snapshot_end_turn_index: int
) -> str | None:
    payload = {
        "thread_id": int(thread_id),
        "trigger_source": "fan",
        "root_fan_message_id": int(message_id),
        "snapshot_end_turn_index": int(snapshot_end_turn_index),
        "status": "active",
        "current_step": "queued",
        "metadata": {"source": "test_harness"},
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
                meta={"reason": "test_harness_no_media"},
            )

    if not job_exists("iris.decide", message_id, client=SB):
        send("iris.decide", payload)


def _kick_reply_flow(
    *, message_id: int, thread_id: int, turn_index: int, has_media: bool
) -> None:
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


def _insert_fan_message(row: dict) -> int | None:
    thread_id = row.get("thread_id")
    if not thread_id:
        return None
    message_text = row.get("message_text") or ""
    created_at = row.get("created_at") or _utcnow().isoformat()
    ext_message_id = f"test-{row.get('id') or uuid.uuid4()}"
    media_payload = row.get("media_payload")
    has_media = False
    if isinstance(media_payload, dict):
        items = media_payload.get("items")
        has_media = isinstance(items, list) and len(items) > 0

    turn_index = _next_turn_index(int(thread_id))
    payload = {
        "thread_id": int(thread_id),
        "ext_message_id": ext_message_id,
        "sender": "fan",
        "message_text": message_text,
        "source_channel": TEST_SOURCE_CHANNEL,
        "created_at": created_at,
        "turn_index": turn_index,
        "media_status": "pending" if has_media else None,
        "media_payload": media_payload if has_media else None,
    }
    try:
        res = SB.table("messages").insert(payload, upsert=False).execute().data
        if not res:
            return None
        msg_id = res[0]["id"]
    except APIError as exc:
        err = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {}
        if err.get("code") == "23505":
            existing = (
                SB.table("messages")
                .select("id")
                .eq("thread_id", int(thread_id))
                .eq("ext_message_id", ext_message_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if existing:
                msg_id = existing[0]["id"]
            else:
                return None
        else:
            raise
    SB.table("threads").update({"turn_count": turn_index}).eq(
        "id", int(thread_id)
    ).execute()
    _kick_reply_flow(
        message_id=int(msg_id),
        thread_id=int(thread_id),
        turn_index=int(turn_index),
        has_media=has_media,
    )
    _maybe_enqueue_episode_summary(int(thread_id), int(turn_index))
    return int(msg_id)


def _fetch_pending_fan_rows(limit: int) -> list[dict]:
    return (
        SB.table(TEST_TABLE)
        .select(
            "id,thread_id,message_text,media_payload,created_at,status,meta,source_message_id"
        )
        .eq("sender", "fan")
        .eq("status", "pending")
        .order("id", desc=False)
        .limit(limit)
        .execute()
        .data
        or []
    )


def _mark_fan_row(row_id: int, *, status: str, message_id: int | None = None, error: str | None = None) -> None:
    payload = {
        "status": status,
        "processed_at": _utcnow().isoformat(),
    }
    if message_id:
        payload["source_message_id"] = int(message_id)
    if error:
        payload["meta"] = {"error": str(error)}
    SB.table(TEST_TABLE).update(payload).eq("id", int(row_id)).execute()


def _mirror_creator_messages(thread_id: int, limit: int) -> int:
    last_rows = (
        SB.table(TEST_TABLE)
        .select("source_message_id")
        .eq("thread_id", int(thread_id))
        .eq("sender", "creator")
        .order("source_message_id", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    last_id = 0
    if last_rows and last_rows[0].get("source_message_id"):
        last_id = int(last_rows[0]["source_message_id"])

    rows = (
        SB.table("messages")
        .select("id,thread_id,message_text,created_at,ext_message_id")
        .eq("thread_id", int(thread_id))
        .eq("sender", "creator")
        .gt("id", last_id)
        .order("id", desc=False)
        .limit(limit)
        .execute()
        .data
        or []
    )
    count = 0
    for row in rows:
        payload = {
            "thread_id": int(thread_id),
            "sender": "creator",
            "message_text": row.get("message_text") or "",
            "created_at": row.get("created_at") or _utcnow().isoformat(),
            "status": "mirrored",
            "source_message_id": int(row.get("id")),
            "meta": {"ext_message_id": row.get("ext_message_id")},
        }
        try:
            SB.table(TEST_TABLE).insert(payload, upsert=False).execute()
            count += 1
        except APIError as exc:
            err = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {}
            if err.get("code") == "23505":
                continue
            if "duplicate key" in str(exc).lower():
                continue
            raise
    return count


def _latest_test_row(thread_id: int) -> dict | None:
    rows = (
        SB.table(TEST_TABLE)
        .select("id,sender,created_at")
        .eq("thread_id", int(thread_id))
        .order("id", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return rows[0] if rows else None


def _pending_fan_exists(thread_id: int) -> bool:
    rows = (
        SB.table(TEST_TABLE)
        .select("id")
        .eq("thread_id", int(thread_id))
        .eq("sender", "fan")
        .eq("status", "pending")
        .limit(1)
        .execute()
        .data
        or []
    )
    return bool(rows)


def _fetch_history(thread_id: int, limit: int) -> list[dict]:
    query = (
        SB.table(TEST_TABLE)
        .select("sender,message_text,created_at,id")
        .eq("thread_id", int(thread_id))
        .order("id", desc=False)
    )
    if limit and int(limit) > 0:
        query = query.limit(int(limit))
    return query.execute().data or []


def _call_fan_model(history: list[dict]) -> str:
    base = (os.getenv("TEST_FAN_URL") or os.getenv("RUNPOD_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("TEST_FAN_URL or RUNPOD_URL not set for fan simulator")
    key = os.getenv("TEST_FAN_API_KEY") or os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("TEST_FAN_API_KEY or RUNPOD_API_KEY is not set")
    url = f"{base}/v1/chat/completions"
    model = os.getenv("TEST_FAN_MODEL") or os.getenv("RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored")
    system_prompt = os.getenv(
        "TEST_FAN_SYSTEM_PROMPT",
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
        "temperature": float(os.getenv("TEST_FAN_TEMPERATURE", "0.7")),
        "max_tokens": int(os.getenv("TEST_FAN_MAX_TOKENS", "200")),
    }
    resp = requests.post(url, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, json=payload, timeout=120)
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


def _maybe_generate_fan_reply(thread_id: int) -> None:
    if not FAN_SIM_ENABLED:
        return
    if _pending_fan_exists(thread_id):
        return
    latest = _latest_test_row(thread_id)
    if not latest or latest.get("sender") != "creator":
        return
    total_rows = (
        SB.table(TEST_TABLE)
        .select("id", count="exact")
        .eq("thread_id", int(thread_id))
        .execute()
    )
    count = total_rows.count if hasattr(total_rows, "count") else None
    if count is not None and count >= FAN_SIM_MAX_TURNS:
        return
    history = _fetch_history(thread_id, FAN_SIM_HISTORY_LIMIT)
    if not history:
        return
    try:
        reply = _call_fan_model(history)
    except Exception as exc:
        print(f"[test_harness] fan sim error: {exc}", flush=True)
        return
    if not reply:
        return
    payload = {
        "thread_id": int(thread_id),
        "sender": "fan",
        "message_text": reply,
        "status": "pending",
        "created_at": _utcnow().isoformat(),
    }
    SB.table(TEST_TABLE).insert(payload).execute()


def _fetch_test_thread_ids() -> list[int]:
    rows = (
        SB.table(TEST_TABLE)
        .select("thread_id")
        .order("thread_id", desc=False)
        .execute()
        .data
        or []
    )
    seen = set()
    ids: list[int] = []
    for row in rows:
        tid = row.get("thread_id")
        if tid is None or tid in seen:
            continue
        seen.add(tid)
        ids.append(int(tid))
    return ids


def run_loop() -> None:
    print("[test_harness] started", flush=True)
    while True:
        try:
            pending = _fetch_pending_fan_rows(FAN_BATCH_SIZE)
            for row in pending:
                row_id = row.get("id")
                if not row_id:
                    continue
                try:
                    msg_id = _insert_fan_message(row)
                    if msg_id:
                        _mark_fan_row(int(row_id), status="sent", message_id=int(msg_id))
                    else:
                        _mark_fan_row(int(row_id), status="error", error="insert_failed")
                except Exception as exc:
                    _mark_fan_row(int(row_id), status="error", error=str(exc))
                    print(f"[test_harness] fan ingest error: {exc}", flush=True)

            thread_ids = _fetch_test_thread_ids()
            for thread_id in thread_ids:
                try:
                    _mirror_creator_messages(int(thread_id), CREATOR_BATCH_SIZE)
                except Exception as exc:
                    print(f"[test_harness] mirror error thread {thread_id}: {exc}", flush=True)

            if FAN_SIM_ENABLED:
                for thread_id in thread_ids:
                    _maybe_generate_fan_reply(int(thread_id))
        except Exception as exc:
            print(f"[test_harness] loop error: {exc}", flush=True)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run_loop()
