import os, json, hashlib, uuid, datetime

import pytz
from fastapi import FastAPI, HTTPException, Request
from postgrest.exceptions import APIError
from supabase import create_client, ClientOptions
from workers.lib.cards import new_base_card
from workers.lib.job_utils import job_exists
from workers.lib.link_utils import extract_urls
from workers.lib.reply_run_tracking import upsert_step
from workers.lib.simple_queue import send

# connect to Supabase using secrets that Fly will provide
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SB = create_client(
    supabase_url=os.environ["SUPABASE_URL"],
    supabase_key=SERVICE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SERVICE_KEY}",
        }
    ),
)

app = FastAPI()

# If a run is marked active but has no queued jobs for a short grace period,
# treat it as stale so new messages don't get misclassified as "interrupts".
REPLY_RUN_STALE_SECONDS = int(os.getenv("REPLY_RUN_STALE_SECONDS", "15"))

# -------------- helpers --------------
def get_creator_id(handle: str) -> int:
    data = (
        SB.table("creators")
        .select("id")
        .eq("of_handle", handle)
        .execute()
        .data
    )
    if not data:
        raise HTTPException(404, f"unknown creator_handle {handle}")
    return data[0]["id"]

def enqueue_hermes(mid: int):
    # route every fan turn through Hermes for routing decisions
    send("hermes.route", {"message_id": mid})

def enqueue_argus(mid: int):
    # media-aware worker that will eventually wake Kairos
    send("argus.analyse", {"message_id": mid})

def enqueue_reply_supervisor(mid: int):
    # reply supervisor only handles mid-run interrupt/followup decisions
    send("reply.supervise", {"message_id": mid})


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
    """
    Best-effort check: if a run is truly in-flight, there should be at least one job_queue row
    referencing its run_id (claimed jobs still exist; only ack deletes).
    """
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
        if age is not None and age >= REPLY_RUN_STALE_SECONDS and not _run_has_jobs(run_id):
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
        "metadata": {},
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
                meta={"reason": "no_media"},
            )

    if not job_exists("hermes.route", message_id, client=SB):
        send("hermes.route", payload)


def _replied_boundary_turn(thread_id: int) -> int:
    """
    Best-effort "already replied" boundary for a thread.
    Prefer the latest committed reply_run snapshot; fall back to the latest creator turn.
    """
    try:
        committed = (
            SB.table("reply_runs")
            .select("snapshot_end_turn_index")
            .eq("thread_id", int(thread_id))
            .eq("status", "committed")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        committed = []

    if committed and committed[0].get("snapshot_end_turn_index") is not None:
        try:
            return int(committed[0]["snapshot_end_turn_index"])
        except Exception:
            return 0

    try:
        latest_creator = (
            SB.table("messages")
            .select("turn_index")
            .eq("thread_id", int(thread_id))
            .eq("sender", "creator")
            .order("turn_index", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        latest_creator = []

    if latest_creator:
        try:
            return int(latest_creator[0].get("turn_index") or 0)
        except Exception:
            return 0
    return 0


def _kick_reply_flow(
    *,
    message_id: int,
    thread_id: int,
    turn_index: int,
    has_media: bool,
    is_retry: bool = False,
) -> None:
    """
    Ensure the reply pipeline is started (when idle) or the interrupt decider runs (when busy).
    """
    if is_retry:
        boundary = _replied_boundary_turn(int(thread_id))
        if int(turn_index or 0) <= int(boundary):
            return

    active_run = _active_reply_run(int(thread_id))
    if active_run:
        active_root = active_run.get("root_fan_message_id")
        # If this message is already the root of the active run, don't treat it as an interrupt.
        if active_root is None or int(active_root) != int(message_id):
            if not job_exists("reply.supervise", message_id, client=SB):
                enqueue_reply_supervisor(int(message_id))
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


def _kick_existing_message(message_id: int) -> None:
    try:
        rows = (
            SB.table("messages")
            .select("id,thread_id,turn_index,sender,media_payload")
            .eq("id", int(message_id))
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        return

    if not rows:
        return
    row = rows[0] if isinstance(rows[0], dict) else {}
    if (row.get("sender") or "").strip().lower() != "fan":
        return

    thread_id = int(row.get("thread_id") or 0)
    turn_index = int(row.get("turn_index") or 0)
    media_payload = row.get("media_payload") or {}
    items = media_payload.get("items") if isinstance(media_payload, dict) else None
    has_media = isinstance(items, list) and len(items) > 0

    _kick_reply_flow(
        message_id=int(message_id),
        thread_id=int(thread_id),
        turn_index=int(turn_index),
        has_media=bool(has_media),
        is_retry=True,
    )


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

def _pending_summary_job(
    queue: str, thread_id: int, start_turn: int, end_turn: int
) -> bool:
    """
    Check if a matching job is already in-flight to avoid duplicate episode summaries.
    """
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


def _maybe_enqueue_episode_summary(thread_id: int, latest_turn_index: int):
    """
    Maintain the rolling 20-40 turn window by summarizing the oldest 20
    once 40 unsummarized turns accumulate.
    """
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
        {
            "thread_id": thread_id,
            "start_turn": start_turn,
            "end_turn": end_turn,
        },
    )

# -------------- HTTP endpoint --------------
@app.post("/turn")
async def receive(request: Request):
    payload = await request.json()
    try:
        text = payload["text"]
        creator = payload["creator_handle"]
    except KeyError as exc:
        raise HTTPException(422, f"Missing field: {exc.args[0]}") from exc

    fan_id = payload.get("fan_ext_id")

    ext_id = payload.get("ext_message_id") or str(uuid.uuid4())
    ts = payload.get("timestamp") or datetime.datetime.utcnow().replace(
        tzinfo=pytz.UTC
    ).isoformat()
    media_items = payload.get("media") or []
    if not isinstance(media_items, list):
        media_items = []

    link_items = []
    for url in extract_urls(text):
        link_items.append({"type": "link", "url": url})

    all_media_items = media_items + link_items
    has_media = bool(all_media_items)

    creator_id = get_creator_id(creator)
    fan_hash = hashlib.sha256(fan_id.encode()).hexdigest()

    thread_rows = (
        SB.table("threads")
        .select("id,turn_count")
        .eq("creator_id", creator_id)
        .eq("fan_ext_id_hash", fan_hash)
        .execute()
        .data
    )

    if thread_rows:
        thread = thread_rows[0]
    else:
        thread = (
            SB.table("threads")
            .insert(
                {
                    "creator_id": creator_id,
                    "fan_ext_id_hash": fan_hash,
                    "turn_count": 0,
                    "fan_psychic_card": new_base_card(),
                }
            )
            .execute()
            .data[0]
        )

    thread_id = thread["id"]

    # Derive next turn index from messages to avoid stale turn_count
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
    turn_index = (latest_turn or 0) + 1

    try:
        res = (
            SB.table("messages")
            .insert(
                {
                    "thread_id": thread_id,
                    "ext_message_id": ext_id,
                    "sender": "fan",
                    "message_text": text,
                    "source_channel": payload.get("source_channel") or "live",
                    "created_at": ts,
                    "turn_index": turn_index,
                    # Media fields are optional; populated only when attachments exist.
                    "media_status": "pending" if has_media else None,
                    "media_payload": {"items": all_media_items} if has_media else None,
                },
                upsert=False,
            )
            .execute()
            .data
        )
    except APIError as exc:
        # Idempotency: if the caller retries the same ext_message_id, return the
        # existing message instead of 500.
        err = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {}
        if err.get("code") == "23505":  # unique constraint violation
            existing = (
                SB.table("messages")
                .select("id,thread_id")
                .eq("thread_id", thread_id)
                .eq("ext_message_id", ext_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if existing:
                # Best-effort: ensure work isn't dropped on retries.
                try:
                    _kick_existing_message(existing[0]["id"])
                except Exception:
                    pass
                return {"message_id": existing[0]["id"], "thread_id": existing[0]["thread_id"]}
            raise HTTPException(409, "Duplicate message") from exc
        raise

    if not res:
        raise HTTPException(500, "Message insert failed")

    msg_id = res[0]["id"]
    SB.table("threads").update({"turn_count": turn_index}).eq("id", thread_id).execute()

    _kick_reply_flow(
        message_id=int(msg_id),
        thread_id=int(thread_id),
        turn_index=int(turn_index),
        has_media=has_media,
    )

    _maybe_enqueue_episode_summary(thread_id, turn_index)

    return {"message_id": msg_id, "thread_id": thread_id}

# Fly runs uvicorn via fly.toml command
