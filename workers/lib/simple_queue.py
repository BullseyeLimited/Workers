import os
import json
import datetime
import time

from supabase import create_client
from postgrest.exceptions import APIError


SB = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)


def send(queue: str, payload: dict):
    SB.table("job_queue").insert(
        {"queue": queue, "payload": json.dumps(payload, ensure_ascii=False)}
    ).execute()


def receive(queue: str, vt_seconds: int = 30):
    vt_until = (
        datetime.datetime.utcnow()
        + datetime.timedelta(seconds=vt_seconds)
    ).isoformat()

    # 1. Pick one due job. If Supabase/PostgREST is temporarily failing,
    # treat that as "no job" so the worker loop does not crash.
    try:
        rows = (
            SB.table("job_queue")
            .select("id,payload")
            .eq("queue", queue)
            .lte("available_at", datetime.datetime.utcnow().isoformat())
            .order("id")
            .limit(1)
            .execute()
            .data
        )
    except APIError as exc:
        print(
            "[simple_queue] receive error for queue",
            queue,
            "->",
            exc,
            flush=True,
        )
        time.sleep(1)
        return None

    if not rows:
        return None

    row_id = rows[0]["id"]
    payload = rows[0]["payload"]
    try:
        # Supabase can return JSON columns as dicts; only decode if still a string.
        payload = json.loads(payload) if isinstance(payload, str) else payload
    except Exception:
        # Fall through with the raw payload if decoding fails.
        pass

    # 2. Claim it by pushing its visibility timeout into the future.
    SB.table("job_queue").update({"available_at": vt_until}).eq("id", row_id).execute()

    return {"row_id": row_id, "payload": payload}


def ack(row_id: int):
    SB.table("job_queue").delete().eq("id", row_id).execute()

