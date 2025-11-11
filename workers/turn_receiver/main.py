import os, json, hashlib, uuid, datetime

import pytz
from fastapi import FastAPI, HTTPException, Request
from supabase import create_client
from workers.lib.simple_queue import send

# connect to Supabase using secrets that Fly will provide
SB = create_client(os.getenv("SUPABASE_URL"),
                   os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

app = FastAPI()

# -------------- helpers --------------
def hash_fan(ext_id: str) -> str:
    return hashlib.sha256(ext_id.encode()).hexdigest()

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

    thread_id = payload.get("thread_id")
    fan_id = payload.get("fan_ext_id")

    ext_id = payload.get("ext_message_id") or str(uuid.uuid4())
    ts = payload.get("timestamp") or datetime.datetime.utcnow().replace(
        tzinfo=pytz.UTC
    ).isoformat()

    if not thread_id:
        if not fan_id:
            raise HTTPException(
                400, "fan_ext_id is required when thread_id is not provided"
            )

        thread = (
            SB.table("threads")
            .select("id")
            .eq("creator_handle", creator)
            .eq("fan_ext_id_hash", hash_fan(fan_id))
            .single()
            .execute()
            .data
        )
        if thread:
            thread_id = thread["id"]
        else:
            thread_id = (
                SB.table("threads")
                .insert(
                    {
                        "creator_handle": creator,
                        "fan_ext_id_hash": hash_fan(fan_id),
                    }
                )
                .execute()
                .data[0]["id"]
            )

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
