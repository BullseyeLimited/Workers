import os, json, hashlib, uuid, datetime

import pytz
from fastapi import FastAPI, HTTPException, Request
from postgrest.exceptions import APIError
from supabase import create_client, ClientOptions
from workers.lib.cards import new_base_card
from workers.lib.link_utils import extract_urls
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
                return {"message_id": existing[0]["id"], "thread_id": existing[0]["thread_id"]}
            raise HTTPException(409, "Duplicate message") from exc
        raise

    if not res:
        raise HTTPException(500, "Message insert failed")

    msg_id = res[0]["id"]
    SB.table("threads").update({"turn_count": turn_index}).eq("id", thread_id).execute()
    if has_media:
        # Run Argus (media describer) and Hermes (router) in parallel.
        # Hermes Join will gate Napoleon until upstream results are ready.
        enqueue_argus(msg_id)
        enqueue_hermes(msg_id)
    else:
        enqueue_hermes(msg_id)

    _maybe_enqueue_episode_summary(thread_id, turn_index)

    return {"message_id": msg_id, "thread_id": thread_id}

# Fly runs uvicorn via fly.toml command
