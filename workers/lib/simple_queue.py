import os, json, datetime, time
from supabase import create_client

SB = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)

USE_RPC = os.getenv("QUEUE_USE_RPC", "true").lower() in {"1", "true", "yes", "on"}
ALLOW_FALLBACK = os.getenv("QUEUE_CLAIM_FALLBACK", "").lower() in {"1", "true", "yes", "on"}
WORKER_ID = (
    os.getenv("WORKER_ID")
    or os.getenv("FLY_ALLOC_ID")
    or os.getenv("FLY_MACHINE_ID")
    or os.getenv("HOSTNAME")
)

# ------------------------------------------------------------------
def send(queue: str, payload: dict):
    SB.table("job_queue").insert(
        {"queue": queue, "payload": json.dumps(payload, ensure_ascii=False)}
    ).execute()


# ------------------------------------------------------------------
def receive(queue: str, vt_seconds: int = 30):
    if USE_RPC:
        job = _receive_via_rpc(queue, vt_seconds)
        if job:
            return job
        if not ALLOW_FALLBACK:
            return None

    vt_until = (
        datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(seconds=vt_seconds)
    ).isoformat()

    # 1 · pick one due job
    rows = (
        SB.table("job_queue")
        .select("id,payload")
        .eq("queue", queue)
        .lte("available_at", datetime.datetime.now(datetime.timezone.utc).isoformat())
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

    payload = rows[0]["payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return {"row_id": row_id, "queue": queue, "payload": payload}


# ------------------------------------------------------------------
def ack(row_id: int):
    SB.table("job_queue").delete().eq("id", row_id).execute()


def _receive_via_rpc(queue: str, vt_seconds: int):
    try:
        res = SB.rpc(
            "claim_job",
            {
                "queue_name": queue,
                "vt_seconds": int(vt_seconds),
                "worker_id": WORKER_ID,
            },
        ).execute()
    except Exception:
        return None

    data = getattr(res, "data", None)
    if not data:
        return None

    row = data[0] if isinstance(data, list) else data
    if not isinstance(row, dict):
        return None

    row_id = row.get("id")
    payload = row.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return {"row_id": row_id, "queue": row.get("queue"), "payload": payload}
