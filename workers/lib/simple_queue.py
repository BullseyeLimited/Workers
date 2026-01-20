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
_LAST_RPC_ERROR_LOG_AT = 0.0

# ------------------------------------------------------------------
def send(queue: str, payload: dict):
    # Store payload as JSON (dict) so Postgres JSON operators (payload->>'x') work.
    # Older deployments stored payload as a JSON *string*; receive() still supports that.
    try:
        SB.table("job_queue").insert({"queue": queue, "payload": payload}).execute()
    except Exception as exc:
        msg = str(exc)
        # If the DB has a unique index for idempotency, concurrent inserts can race.
        # Treat duplicates as success.
        if "duplicate key value violates unique constraint" in msg or "23505" in msg:
            return
        raise


# ------------------------------------------------------------------
def receive(queue: str, vt_seconds: int = 30):
    if USE_RPC:
        rpc_error = None
        try:
            job = _receive_via_rpc(queue, vt_seconds)
        except Exception as exc:
            job = None
            rpc_error = exc
        if job:
            return job
        if rpc_error is not None:
            _maybe_log_rpc_error(rpc_error)
        if not ALLOW_FALLBACK and rpc_error is None:
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
    res = SB.rpc(
        "claim_job",
        {
            "queue_name": queue,
            "vt_seconds": int(vt_seconds),
            "worker_id": WORKER_ID,
        },
    ).execute()

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


def _maybe_log_rpc_error(exc: Exception) -> None:
    global _LAST_RPC_ERROR_LOG_AT
    now = time.time()
    # Avoid log spam in tight polling loops.
    if now - _LAST_RPC_ERROR_LOG_AT < 60:
        return
    _LAST_RPC_ERROR_LOG_AT = now
    print(
        f"[simple_queue] claim_job RPC failed ({type(exc).__name__}: {exc}); falling back to table polling",
        flush=True,
    )
