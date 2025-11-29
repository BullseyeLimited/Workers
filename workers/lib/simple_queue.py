import os, json, datetime, time
from supabase import create_client

SB = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)


# ------------------------------------------------------------------
def send(queue: str, payload: dict):
    SB.table("job_queue").insert(
        {"queue": queue, "payload": json.dumps(payload, ensure_ascii=False)}
    ).execute()


# ------------------------------------------------------------------
def receive(queue: str, vt_seconds: int = 30):
    vt_until = (
        datetime.datetime.utcnow()
        + datetime.timedelta(seconds=vt_seconds)
    ).isoformat()

    # 1 · pick one due job
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
    if not rows:
        return None

    row_id = rows[0]["id"]

    # 2 · claim it by pushing its visibility timeout into the future
    SB.table("job_queue").update({"available_at": vt_until}).eq("id", row_id).execute()

    return {"row_id": row_id, "payload": json.loads(rows[0]["payload"])}


# ------------------------------------------------------------------
def ack(row_id: int):
    SB.table("job_queue").delete().eq("id", row_id).execute()
