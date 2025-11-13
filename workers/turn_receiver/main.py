import os, json, hashlib, uuid, datetime

import pytz
from fastapi import FastAPI, HTTPException, Request
from supabase import create_client, ClientOptions
from workers.lib.simple_queue import send

# connect to Supabase using secrets that Fly will provide
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SB = create_client(
    supabase_url=os.environ["SUPABASE_URL"],
    supabase_key=SERVICE_KEY,
    options=ClientOptions(
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
        }
    ),
)

app = FastAPI()

# -------------- helpers --------------
def get_creator_id(handle: str) -> int:
    row = (
        SB.table("creators")
        .select("id")
        .eq("of_handle", handle)
        .single()
        .execute()
        .data
    )
    if not row:
        raise ValueError(f"creator_handle {handle} not found")
    return row["id"]

def enqueue_kairos(mid:int):
    # put the message id on the queue so kairos-worker processes it
    send("kairos.analyse", {"message_id": mid})

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

    creator_id = get_creator_id(creator)
    fan_hash = hashlib.sha256(fan_id.encode()).hexdigest()

    thread = (
        SB.table("threads")
        .select("id,turn_count")
        .eq("creator_id", creator_id)
        .eq("fan_ext_id_hash", fan_hash)
        .single()
        .execute()
        .data
    )

    if not thread:
        thread = (
            SB.table("threads")
            .insert(
                {
                    "creator_id": creator_id,
                    "fan_ext_id_hash": fan_hash,
                    "turn_count": 0,
                }
            )
            .execute()
            .data[0]
        )

    thread_id = thread["id"]
    turn_index = (thread.get("turn_count") or 0) + 1

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
                "creator_id": creator_id,
                "turn_index": turn_index,
            },
            upsert=False,
        )
        .execute()
        .data
    )
    if not res:
        raise HTTPException(409, "Duplicate message")

    msg_id = res[0]["id"]
    enqueue_kairos(msg_id)

    return {"message_id": msg_id, "thread_id": thread_id}

# Fly runs uvicorn via fly.toml command
