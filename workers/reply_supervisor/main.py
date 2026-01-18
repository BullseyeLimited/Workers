"""
Reply supervisor worker.

Responsibilities:
- Start a reply_run when a new fan message arrives.
- Cancel/supersede an active reply_run based on timing rules.
- Defer or schedule follow-ups when we choose not to interrupt.

This worker intentionally does not call LLMs yet; it only implements the
time-window policy skeleton. A later "interrupt decider" model can be added
behind the same decision points.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from supabase import ClientOptions, create_client

from workers.lib.simple_queue import ack, receive, send
from workers.lib.reply_run_tracking import upsert_step

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for reply supervisor")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(headers={"Authorization": f"Bearer {SUPABASE_KEY}"}),
)

QUEUE = "reply.supervise"
HERMES_QUEUE = "hermes.route"
ARGUS_QUEUE = "argus.analyse"

AUTO_ABORT_SECONDS = float(os.getenv("REPLY_AUTO_ABORT_SECONDS", "10"))
NAPOLEON_PROTECT_SECONDS = float(os.getenv("REPLY_NAPOLEON_PROTECT_SECONDS", "10"))
FOLLOWUP_RECHECK_SECONDS = float(os.getenv("REPLY_FOLLOWUP_RECHECK_SECONDS", "10"))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(raw)
        except Exception:
            return None
    return None


def _json_dump(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _enqueue_delayed(payload: dict, *, delay_seconds: float) -> None:
    available_at = (_utcnow() + timedelta(seconds=delay_seconds)).isoformat()
    SB.table("job_queue").insert(
        {
            "queue": QUEUE,
            "payload": _json_dump(payload),
            "available_at": available_at,
        }
    ).execute()


def _record_event(
    *,
    run_id: str,
    event_type: str,
    triggering_message_id: int | None = None,
    decision: str | None = None,
    reason: str | None = None,
    payload: dict | None = None,
) -> None:
    row = {
        "run_id": run_id,
        "event_type": event_type,
        "event_at": _utcnow().isoformat(),
        "triggering_message_id": triggering_message_id,
        "decision": decision,
        "reason": reason,
        "payload": payload or {},
    }
    try:
        SB.table("reply_run_events").insert(row).execute()
    except Exception:
        # Fail-open: monitoring should not block core routing.
        pass


def _active_run(thread_id: int) -> dict | None:
    rows = (
        SB.table("reply_runs")
        .select("run_id,thread_id,status,created_at,root_fan_message_id,snapshot_end_turn_index,current_step")
        .eq("thread_id", thread_id)
        .eq("status", "active")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return rows[0] if rows else None


def _latest_step(run_id: str, step: str) -> dict | None:
    rows = (
        SB.table("reply_run_steps")
        .select("status,mode,attempt,started_at,ended_at")
        .eq("run_id", run_id)
        .eq("step", step)
        .order("attempt", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return rows[0] if rows else None


def _cancel_run(run_id: str, *, reason: str, superseded_by: str | None = None) -> None:
    now_iso = _utcnow().isoformat()
    update = {
        "status": "canceled",
        "canceled_at": now_iso,
        "cancel_reason": reason,
        "current_step": "canceled",
    }
    if superseded_by:
        update["superseded_by"] = superseded_by
    SB.table("reply_runs").update(update).eq("run_id", run_id).execute()

    # Mark any in-flight steps as canceled (best-effort).
    try:
        SB.table("reply_run_steps").update(
            {"status": "canceled", "ended_at": now_iso, "error": reason}
        ).eq("run_id", run_id).in_("status", ["pending", "running"]).execute()
    except Exception:
        pass

    # Remove queued jobs for this run (workers may already be running; they will no-op).
    try:
        SB.table("job_queue").delete().filter("payload->>run_id", "eq", str(run_id)).execute()
    except Exception:
        pass


def _mark_superseded(old_run_id: str, new_run_id: str) -> None:
    try:
        SB.table("reply_runs").update({"superseded_by": str(new_run_id)}).eq(
            "run_id", str(old_run_id)
        ).execute()
    except Exception:
        return


def _create_run(
    *,
    thread_id: int,
    root_fan_message_id: int | None,
    snapshot_end_turn_index: int | None,
    trigger_source: str = "fan",
) -> str | None:
    payload = {
        "thread_id": thread_id,
        "trigger_source": trigger_source,
        "root_fan_message_id": root_fan_message_id,
        "snapshot_end_turn_index": snapshot_end_turn_index,
        "status": "active",
        "current_step": "supervisor",
        "metadata": {},
    }
    try:
        row = SB.table("reply_runs").insert(payload).execute().data
        if row and isinstance(row, list) and row and isinstance(row[0], dict):
            run_id = str(row[0].get("run_id") or "")
            if run_id:
                msg_id = int(root_fan_message_id) if root_fan_message_id else None
                # Seed step rows so dashboards show pending work immediately.
                for step_name in ("hermes", "kairos", "web", "join", "napoleon", "writer"):
                    upsert_step(
                        run_id=run_id,
                        step=step_name,
                        attempt=0,
                        status="pending",
                        client=SB,
                        message_id=msg_id,
                        meta=(
                            {"reason": "awaiting_hermes"}
                            if step_name in {"kairos", "web"}
                            else {}
                        ),
                    )
            return run_id
    except Exception:
        return None
    return None


def _has_media(row: dict) -> bool:
    media_payload = row.get("media_payload") or {}
    if not isinstance(media_payload, dict):
        return False
    items = media_payload.get("items")
    return isinstance(items, list) and len(items) > 0


def _enqueue_pipeline(*, run_id: str, message_id: int) -> None:
    msg_rows = (
        SB.table("messages")
        .select("id,media_payload,media_status")
        .eq("id", message_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    msg_row = msg_rows[0] if msg_rows else {}
    if _has_media(msg_row):
        upsert_step(
            run_id=str(run_id),
            step="argus",
            attempt=0,
            status="pending",
            client=SB,
            message_id=int(message_id),
        )
        send(ARGUS_QUEUE, {"message_id": message_id, "run_id": run_id})
    else:
        upsert_step(
            run_id=str(run_id),
            step="argus",
            attempt=0,
            status="skipped",
            client=SB,
            message_id=int(message_id),
            mode="skip",
            meta={"reason": "no_media"},
        )
    send(HERMES_QUEUE, {"message_id": message_id, "run_id": run_id})


def _latest_unreplied_fan_turn(thread_id: int) -> dict | None:
    # A "replied" boundary is the latest creator message.
    latest_creator = (
        SB.table("messages")
        .select("turn_index")
        .eq("thread_id", thread_id)
        .eq("sender", "creator")
        .order("turn_index", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    last_creator_turn = int(latest_creator[0].get("turn_index") or 0) if latest_creator else 0

    fan_rows = (
        SB.table("messages")
        .select("id,turn_index")
        .eq("thread_id", thread_id)
        .eq("sender", "fan")
        .gt("turn_index", last_creator_turn)
        .order("turn_index", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return fan_rows[0] if fan_rows else None


def _middle_window_decision(message_text: str) -> str:
    """
    Placeholder "importance" logic until you add the model prompt.

    Returns one of:
    - ABORT_REDO
    - DEFER
    """
    text = (message_text or "").lower()
    hard_keywords = [
        "died",
        "dead",
        "passed away",
        "funeral",
        "hospital",
        "emergency",
        "911",
        "suicide",
        "kill myself",
    ]
    if any(k in text for k in hard_keywords):
        return "ABORT_REDO"
    return "DEFER"


def _handle_new_fan_message(message_id: int) -> bool:
    rows = (
        SB.table("messages")
        .select("id,thread_id,turn_index,created_at,sender,message_text")
        .eq("id", message_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return True
    msg = rows[0]
    if (msg.get("sender") or "").lower() != "fan":
        return True

    thread_id = int(msg["thread_id"])
    active = _active_run(thread_id)
    now = _utcnow()

    if not active:
        run_id = _create_run(
            thread_id=thread_id,
            root_fan_message_id=message_id,
            snapshot_end_turn_index=int(msg.get("turn_index") or 0) or None,
            trigger_source="fan",
        )
        if run_id:
            _record_event(
                run_id=run_id,
                event_type="run_started",
                triggering_message_id=message_id,
                decision="START",
            )
            _enqueue_pipeline(run_id=run_id, message_id=message_id)
        return True

    run_id = str(active.get("run_id"))
    run_started_at = _parse_ts(active.get("created_at")) or now
    run_age = (now - run_started_at).total_seconds()

    # Rule 1: auto-abort if new message arrives quickly after starting.
    if run_age < AUTO_ABORT_SECONDS:
        _record_event(
            run_id=run_id,
            event_type="interrupt",
            triggering_message_id=message_id,
            decision="AUTO_ABORT_REDO",
            reason=f"new_message_within_{AUTO_ABORT_SECONDS}s",
        )
        _cancel_run(run_id, reason="auto_abort_new_message")

        new_run_id = _create_run(
            thread_id=thread_id,
            root_fan_message_id=message_id,
            snapshot_end_turn_index=int(msg.get("turn_index") or 0) or None,
            trigger_source="fan",
        )
        if not new_run_id:
            # Another supervisor instance may have created a replacement run already.
            active_now = _active_run(thread_id)
            new_run_id = str(active_now.get("run_id")) if active_now else None

        if new_run_id:
            _mark_superseded(run_id, new_run_id)
            _record_event(
                run_id=new_run_id,
                event_type="run_started",
                triggering_message_id=message_id,
                decision="START",
                reason="auto_abort_restart",
            )
            _enqueue_pipeline(run_id=new_run_id, message_id=message_id)
        return True

    # Rule 2: never abort once Napoleon has been running long enough.
    napoleon = _latest_step(run_id, "napoleon")
    napoleon_started = _parse_ts((napoleon or {}).get("started_at"))
    if napoleon_started and (now - napoleon_started).total_seconds() >= NAPOLEON_PROTECT_SECONDS:
        _record_event(
            run_id=run_id,
            event_type="interrupt",
            triggering_message_id=message_id,
            decision="DEFER",
            reason="napoleon_protected_window",
            payload={"napoleon_started_at": napoleon_started.isoformat()},
        )
        _enqueue_delayed(
            {"thread_id": thread_id, "reason": "followup_after_protected_run"},
            delay_seconds=FOLLOWUP_RECHECK_SECONDS,
        )
        return True

    # Middle window: decide whether to abort or defer.
    decision = _middle_window_decision(msg.get("message_text") or "")
    _record_event(
        run_id=run_id,
        event_type="interrupt",
        triggering_message_id=message_id,
        decision=decision,
        reason="middle_window_placeholder",
        payload={"run_age_seconds": run_age},
    )

    if decision == "ABORT_REDO":
        _cancel_run(run_id, reason="middle_window_abort_redo")
        new_run_id = _create_run(
            thread_id=thread_id,
            root_fan_message_id=message_id,
            snapshot_end_turn_index=int(msg.get("turn_index") or 0) or None,
            trigger_source="fan",
        )
        if not new_run_id:
            active_now = _active_run(thread_id)
            new_run_id = str(active_now.get("run_id")) if active_now else None
        if new_run_id:
            _mark_superseded(run_id, new_run_id)
            _enqueue_pipeline(run_id=new_run_id, message_id=message_id)
        return True

    # Default: defer and schedule a follow-up.
    _enqueue_delayed(
        {"thread_id": thread_id, "reason": "followup_after_defer"},
        delay_seconds=FOLLOWUP_RECHECK_SECONDS,
    )
    return True


def _handle_followup(thread_id: int) -> bool:
    active = _active_run(thread_id)
    if active:
        _enqueue_delayed(
            {"thread_id": thread_id, "reason": "followup_still_active"},
            delay_seconds=FOLLOWUP_RECHECK_SECONDS,
        )
        return True

    latest_fan = _latest_unreplied_fan_turn(thread_id)
    if not latest_fan:
        return True

    message_id = int(latest_fan["id"])
    run_id = _create_run(
        thread_id=thread_id,
        root_fan_message_id=message_id,
        snapshot_end_turn_index=int(latest_fan.get("turn_index") or 0) or None,
        trigger_source="fan",
    )
    if run_id:
        _record_event(
            run_id=run_id,
            event_type="run_started",
            triggering_message_id=message_id,
            decision="START",
            reason="followup_start",
        )
        _enqueue_pipeline(run_id=run_id, message_id=message_id)
    return True


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload:
        return True
    if payload.get("message_id"):
        try:
            return _handle_new_fan_message(int(payload["message_id"]))
        except Exception:
            return True
    if payload.get("thread_id"):
        try:
            return _handle_followup(int(payload["thread_id"]))
        except Exception:
            return True
    return True


if __name__ == "__main__":
    print("[reply_supervisor] started - waiting for jobs", flush=True)
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        row_id = job["row_id"]
        try:
            payload = job.get("payload") or {}
            if process_job(payload):
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("[reply_supervisor] error:", exc, flush=True)
            traceback.print_exc()
            time.sleep(2)
