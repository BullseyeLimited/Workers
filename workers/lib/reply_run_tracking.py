"""Helpers for recording reply run + step telemetry.

These helpers are best-effort: failures to write telemetry should not break the
core pipeline. All functions accept a Supabase client to avoid global state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_run_status(run_id: str, *, client) -> str | None:
    try:
        rows = (
            client.table("reply_runs")
            .select("status")
            .eq("run_id", str(run_id))
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        return None
    if not rows:
        return None
    status = rows[0].get("status")
    return str(status) if status is not None else None


def is_run_active(run_id: str, *, client) -> bool:
    return get_run_status(run_id, client=client) == "active"


def set_run_current_step(run_id: str, step: str, *, client) -> None:
    try:
        client.table("reply_runs").update({"current_step": step}).eq(
            "run_id", str(run_id)
        ).execute()
    except Exception:
        return


def mark_run_committed(run_id: str, *, creator_message_id: int, client) -> None:
    try:
        client.table("reply_runs").update(
            {
                "status": "committed",
                "committed_creator_message_id": int(creator_message_id),
                "committed_at": _utcnow_iso(),
                "current_step": "committed",
            }
        ).eq("run_id", str(run_id)).execute()
    except Exception:
        return


def upsert_step(
    *,
    run_id: str,
    step: str,
    attempt: int = 0,
    status: str,
    client,
    message_id: int | None = None,
    mode: str | None = None,
    error: str | None = None,
    meta: dict | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "run_id": str(run_id),
        "step": step,
        "attempt": int(attempt),
        "status": status,
        "message_id": int(message_id) if message_id is not None else None,
        "mode": mode,
        "error": error,
        "meta": meta or {},
    }
    if started_at:
        payload["started_at"] = started_at
    if ended_at:
        payload["ended_at"] = ended_at

    try:
        client.table("reply_run_steps").upsert(
            payload, on_conflict="run_id,step,attempt"
        ).execute()
    except Exception:
        return


def step_started_at(
    *,
    run_id: str,
    step: str,
    attempt: int = 0,
    client,
) -> str | None:
    try:
        rows = (
            client.table("reply_run_steps")
            .select("started_at,status")
            .eq("run_id", str(run_id))
            .eq("step", step)
            .eq("attempt", int(attempt))
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        return None
    if not rows:
        return None
    return rows[0].get("started_at")


__all__ = [
    "get_run_status",
    "is_run_active",
    "mark_run_committed",
    "set_run_current_step",
    "step_started_at",
    "upsert_step",
]

