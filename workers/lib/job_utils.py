"""Job queue idempotency helpers."""

from __future__ import annotations

import os
from typing import Any

from supabase import create_client

_SB = None


def _client(client=None):
    """Return a Supabase client, creating a singleton when none is provided."""
    global _SB
    if client is not None:
        return client
    if _SB is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise RuntimeError("Supabase credentials missing in environment")
        _SB = create_client(url, key)
    return _SB


def job_exists(queue: str, message_id: Any, *, client=None, field: str = "message_id") -> bool:
    """
    Return True if a job already exists in the queue with payload->{field} == message_id.
    Handles string/integer ids by comparing as text.
    """
    sb = _client(client)
    try:
        jobs = (
            sb.table("job_queue")
            .select("id")
            .eq("queue", queue)
            .filter(f"payload->>{field}", "eq", str(message_id))
            .limit(1)
            .execute()
            .data
        )
        return bool(jobs)
    except Exception:
        # If the DB has legacy rows where payload is a JSON string, the JSON operator
        # query may not match. We treat that as "doesn't exist" and rely on DB-side
        # cleanup/migration + uniqueness constraints to prevent explosions.
        return False


__all__ = ["job_exists"]
