import os, json, hashlib, uvicorn
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from supabase import create_client

# connect to Supabase using secrets that Fly will provide
SB = create_client(os.getenv("SUPABASE_URL"),
                   os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

app = FastAPI()

# -------------- helpers --------------
def sha256(x:str) -> str:
    return hashlib.sha256(x.encode()).hexdigest()

def enqueue_kairos(mid:int):
    # put the message id on the queue so kairos-worker processes it
    SB.rpc("pgmq_send", params={
        "q_name": "kairos.analyse",
        "message": json.dumps({"message_id": mid})
    })

# -------------- payload schemas --------------
class InMsg(BaseModel):
    ext_message_id : str
    sender         : str = Field(regex="^(fan|creator|system)$")
    timestamp      : datetime
    text           : str
    content_id     : int | None = None
    channel        : str = "live"

class Payload(BaseModel):
    creator_handle : str
    fan_ext_id     : str
    messages       : list[InMsg]

# -------------- HTTP endpoint --------------
@app.post("/turn")
def receive(p: Payload):
    # 1. look up creator by handle
    c = SB.table("creators").select("id") \
          .eq("of_handle", p.creator_handle) \
          .single().execute().data
    if not c:
        raise HTTPException(404, "unknown creator_handle")
    cid = c["id"]
    fan_hash = sha256(p.fan_ext_id)

    # 2. get or create the thread
    thr = SB.table("threads").select("id,turn_count") \
            .eq("creator_id", cid)\
            .eq("fan_ext_id_hash", fan_hash)\
            .single().execute().data
    if not thr:
        thr = SB.table("threads").insert({
              "creator_id": cid,
              "fan_ext_id_hash": fan_hash
        }).execute().data[0]

    tid, turn = thr["id"], thr["turn_count"]
    stored = queued = 0

    # 3. insert each message
    for m in p.messages:
        turn += 1
        res = SB.table("messages").insert({
              "thread_id": tid,
              "turn_index": turn,
              "ext_message_id": m.ext_message_id,
              "sender": m.sender,
              "message_text": m.text,
              "content_id": m.content_id,
              "source_channel": m.channel,
              "created_at": m.timestamp.isoformat()
        }, upsert=False).execute().data
        if res:                              # not duplicate
            stored += 1
            if m.sender == "fan":
                enqueue_kairos(res[0]["id"])
                queued += 1

    return {"stored": stored, "queued_kairos": queued, "thread_id": tid}

# When Fly (or local dev) runs `python main.py`, start the server
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
