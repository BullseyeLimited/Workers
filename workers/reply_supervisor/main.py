"""
Reply supervisor worker.

Responsibilities:
- Start a reply_run when a new fan message arrives.
- Cancel/supersede an active reply_run based on timing rules.
- Defer or schedule follow-ups when we choose not to interrupt.

This worker implements a hybrid policy:
- hard rules (auto-abort window, Napoleon protection window)
- model-driven "interrupt decider" for the middle window that chooses
  between aborting, fast follow-up, or deferring to a natural next turn.
"""

from __future__ import annotations

import json
import os
import time
import traceback
import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import requests
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
FOLLOWUP_FAST_RECHECK_SECONDS = float(os.getenv("REPLY_FOLLOWUP_FAST_RECHECK_SECONDS", "1"))
REPLY_DECIDER_TIMEOUT_SECONDS = float(os.getenv("REPLY_DECIDER_TIMEOUT_SECONDS", "8"))
ENABLE_DELAYED_FOLLOWUP = os.getenv("REPLY_SUPERVISOR_ENABLE_DELAYED_FOLLOWUP", "").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
DECIDER_PROMPT_PATH = PROMPTS_DIR / "reply_interrupt_decider.txt"
DECIDER_PROMPT_NO_ABORT_PATH = PROMPTS_DIR / "reply_interrupt_decider_no_abort.txt"

RUNPOD_URL = (os.getenv("REPLY_DECIDER_RUNPOD_URL") or os.getenv("RUNPOD_URL") or "").rstrip("/")
RUNPOD_API_KEY = os.getenv("REPLY_DECIDER_RUNPOD_API_KEY") or os.getenv("RUNPOD_API_KEY") or ""
RUNPOD_MODEL_NAME = (
    os.getenv("REPLY_DECIDER_RUNPOD_MODEL_NAME")
    or os.getenv("RUNPOD_MODEL_NAME")
    or "gpt-oss-20b-uncensored"
)

DECISION_ABORT_REDO = "ABORT_REDO"
DECISION_FOLLOWUP_FAST = "FOLLOWUP_FAST"
DECISION_DEFER = "DEFER"

ALLOWED_DECISIONS = {DECISION_ABORT_REDO, DECISION_FOLLOWUP_FAST, DECISION_DEFER}


def _decode_jwt_claims(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8")).decode("utf-8")
        data = json.loads(decoded)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _log_supabase_identity() -> None:
    claims = _decode_jwt_claims(SUPABASE_KEY or "")
    ref = claims.get("ref")
    role = claims.get("role")
    iss = claims.get("iss")
    print(
        f"[reply_supervisor] supabase_url={SUPABASE_URL} jwt_iss={iss} jwt_ref={ref} jwt_role={role}",
        flush=True,
    )


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


def _load_decider_prompt(*, abort_allowed: bool) -> str:
    prompt_path = DECIDER_PROMPT_PATH if abort_allowed else DECIDER_PROMPT_NO_ABORT_PATH
    try:
        return prompt_path.read_text(encoding="utf-8")
    except Exception:
        # Minimal default prompt if the file is missing.
        if not abort_allowed:
            return (
                "You decide what to do when a new fan message arrives while a reply pipeline is running.\n"
                "Aborting the current run is NOT allowed.\n"
                "Return STRICT JSON only.\n"
                "Schema:\n"
                '{ "decision": "FOLLOWUP_FAST|DEFER", "reason": "..." }\n'
            )
        return (
            "You decide how to handle a new fan message while a reply pipeline is running.\n"
            "Return STRICT JSON only.\n"
            "Schema:\n"
            '{ "decision": "ABORT_REDO|FOLLOWUP_FAST|DEFER", "reason": "..." }\n'
            "Rules:\n"
            "- Prefer DEFER unless the message is urgent.\n"
            "- Use ABORT_REDO only for emergencies or major corrections.\n"
            "- If abort_allowed=false, do NOT output ABORT_REDO.\n"
        )


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


def _mark_followup_requested(
    run_id: str, *, mode: str, triggering_message_id: int | None = None
) -> None:
    now_iso = _utcnow().isoformat()
    try:
        rows = (
            SB.table("reply_runs")
            .select("metadata")
            .eq("run_id", str(run_id))
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        rows = []

    metadata = rows[0].get("metadata") if rows and isinstance(rows[0], dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}

    followup = metadata.get("followup") or {}
    if not isinstance(followup, dict):
        followup = {}

    followup.update(
        {
            "requested": True,
            "mode": str(mode or "fast"),
            "requested_at": now_iso,
            "triggering_message_id": int(triggering_message_id)
            if triggering_message_id is not None
            else None,
        }
    )
    metadata["followup"] = followup

    try:
        SB.table("reply_runs").update({"metadata": metadata}).eq("run_id", str(run_id)).execute()
    except Exception:
        return


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


def _format_turn_text(row: dict) -> str:
    parts: list[str] = []
    text = (row.get("message_text") or "").strip()
    media_analysis = (row.get("media_analysis_text") or "").strip()

    if text:
        parts.append(text)
    if media_analysis:
        parts.append(f"(media): {media_analysis}")

    return "\n".join(parts).strip()


def _recent_turns_before(thread_id: int, *, before_turn_index: int | None, limit: int = 20) -> list[dict]:
    """
    Return recent turns for context, ordered oldest -> newest.
    """
    query = (
        SB.table("messages")
        .select("turn_index,sender,message_text,media_analysis_text")
        .eq("thread_id", int(thread_id))
    )
    if before_turn_index is not None:
        query = query.lt("turn_index", int(before_turn_index))

    rows = (
        query.order("turn_index", desc=True)
        .limit(int(limit))
        .execute()
        .data
        or []
    )

    turns: list[dict] = []
    for row in reversed(rows):
        sender = (row.get("sender") or "").strip() or "unknown"
        turns.append(
            {
                "turn_index": int(row.get("turn_index") or 0),
                "sender": sender,
                "text": _format_turn_text(row),
            }
        )
    return turns


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
    # IMPORTANT: do not use "latest creator message" as the replied boundary.
    # A creator message can be inserted AFTER a new fan message arrives mid-run,
    # without actually responding to that new fan message. We therefore use the
    # latest committed run's snapshot boundary as the canonical "replied" marker.
    committed = (
        SB.table("reply_runs")
        .select("snapshot_end_turn_index")
        .eq("thread_id", thread_id)
        .eq("status", "committed")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if committed and committed[0].get("snapshot_end_turn_index") is not None:
        replied_boundary_turn = int(committed[0]["snapshot_end_turn_index"])
    else:
        # Fallback for threads that have never used reply_runs yet.
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
        replied_boundary_turn = int(latest_creator[0].get("turn_index") or 0) if latest_creator else 0

    fan_rows = (
        SB.table("messages")
        .select("id,turn_index")
        .eq("thread_id", thread_id)
        .eq("sender", "fan")
        .gt("turn_index", replied_boundary_turn)
        .order("turn_index", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return fan_rows[0] if fan_rows else None


def _middle_window_decision(message_text: str) -> str:
    """
    Deprecated: the supervisor now uses the model-driven decider.
    """
    return DECISION_DEFER


def _runpod_chat(system_prompt: str, user_prompt: str) -> tuple[str, dict]:
    if not RUNPOD_URL:
        raise RuntimeError("RUNPOD_URL is not set for reply decider")
    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
    }
    payload = {
        "model": RUNPOD_MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 250,
        "temperature": 0,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=REPLY_DECIDER_TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()

    raw_text = ""
    try:
        if data.get("choices"):
            msg = data["choices"][0].get("message") or {}
            raw_text = (
                msg.get("content")
                or msg.get("reasoning")
                or msg.get("reasoning_content")
                or ""
            )
        if not raw_text:
            raw_text = data.get("choices", [{}])[0].get("text") or ""
    except Exception:
        pass

    return raw_text, payload


def _extract_json_object(text: str) -> dict | None:
    if not text:
        return None
    raw = text.strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    # Best-effort: strip leading/trailing chatter and parse the first {...} blob.
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(raw[start : end + 1])
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _decide_interrupt(
    *,
    message_turn_index: int | None,
    message_text: str,
    abort_allowed: bool,
    run_id: str,
    thread_id: int,
    run_age_seconds: float,
    current_step: str | None,
    napoleon_started_at: datetime | None,
) -> tuple[str, str, dict]:
    """
    Decide how to handle an in-flight run when a new fan message arrives.

    Returns (decision, reason, debug_payload). Decision is one of:
    - ABORT_REDO
    - FOLLOWUP_FAST
    - DEFER
    """
    debug: dict = {
        "provider": "runpod",
        "model": RUNPOD_MODEL_NAME,
        "abort_allowed": bool(abort_allowed),
    }

    system_prompt = _load_decider_prompt(abort_allowed=abort_allowed)
    nap_elapsed: float | None = None
    if napoleon_started_at:
        try:
            nap_elapsed = (_utcnow() - napoleon_started_at).total_seconds()
        except Exception:
            nap_elapsed = None

    recent_turns = _recent_turns_before(thread_id, before_turn_index=message_turn_index, limit=20)

    decisions: dict[str, str] = {
        "FOLLOWUP_FAST": "Let current run finish; then start a new run soon after to answer this message.",
        "DEFER": "Let current run finish; do NOT start a follow-up automatically (answer naturally later).",
    }
    if abort_allowed:
        decisions["ABORT_REDO"] = "Cancel current run and restart a new run using this message as root."

    context = {
        "recent_turns": recent_turns,
        "new_fan_message": {"turn_index": message_turn_index, "text": message_text or ""},
        "run": {
            "run_age_seconds": float(run_age_seconds),
            "current_step": current_step or "",
            "napoleon_elapsed_seconds": nap_elapsed,
        },
        "policy": {
            "abort_allowed": bool(abort_allowed),
        },
        "decisions": decisions,
    }
    user_prompt = (
        "<INTERRUPT_CONTEXT>\n"
        f"{json.dumps(context, ensure_ascii=False, indent=2)}\n"
        "</INTERRUPT_CONTEXT>"
    )

    try:
        content, request_payload = _runpod_chat(system_prompt, user_prompt)
        debug["request"] = request_payload
        debug["raw"] = (content or "")[:2000]
    except Exception as exc:  # noqa: BLE001
        # If the model is temporarily unavailable, fail-safe:
        # do NOT abort the current run; queue a fast follow-up.
        debug["error"] = str(exc)
        return DECISION_FOLLOWUP_FAST, "decider_unavailable_followup_fast", debug

    parsed = _extract_json_object(content or "")
    if not parsed:
        debug["parse_error"] = "invalid_json"
        return DECISION_FOLLOWUP_FAST, "decider_invalid_json_followup_fast", debug

    decision = str(parsed.get("decision") or "").strip().upper()
    reason = str(parsed.get("reason") or "").strip() or "model_decision"

    if decision not in ALLOWED_DECISIONS:
        debug["parse_error"] = "unknown_decision"
        debug["parsed"] = parsed
        return DECISION_FOLLOWUP_FAST, "decider_unknown_decision_followup_fast", debug

    if decision == DECISION_ABORT_REDO and not abort_allowed:
        # Enforce hard rule.
        return DECISION_FOLLOWUP_FAST, "model_requested_abort_but_abort_not_allowed", debug

    return decision, reason, debug


def _handle_new_fan_message(message_id: int) -> bool:
    rows = (
        SB.table("messages")
        .select("id,thread_id,turn_index,created_at,sender,message_text,media_payload,media_analysis_text")
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
    abort_allowed = True
    if napoleon_started and (now - napoleon_started).total_seconds() >= NAPOLEON_PROTECT_SECONDS:
        abort_allowed = False

    message_text_for_decider = _format_turn_text(msg)
    if not message_text_for_decider and _has_media(msg):
        message_text_for_decider = "media attachment"

    # Decide what to do in the middle/protected window.
    decision, decision_reason, debug_payload = _decide_interrupt(
        message_turn_index=int(msg.get("turn_index") or 0) or None,
        message_text=message_text_for_decider,
        abort_allowed=abort_allowed,
        run_id=run_id,
        thread_id=thread_id,
        run_age_seconds=run_age,
        current_step=str(active.get("current_step") or ""),
        napoleon_started_at=napoleon_started,
    )
    _record_event(
        run_id=run_id,
        event_type="interrupt",
        triggering_message_id=message_id,
        decision=decision,
        reason=decision_reason,
        payload={"run_age_seconds": run_age, "debug": debug_payload},
    )

    if decision == DECISION_ABORT_REDO and abort_allowed:
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

    if decision == DECISION_FOLLOWUP_FAST:
        _mark_followup_requested(run_id, mode="fast", triggering_message_id=message_id)
        if ENABLE_DELAYED_FOLLOWUP:
            _enqueue_delayed(
                {"thread_id": thread_id, "reason": "followup_fast", "mode": "fast"},
                delay_seconds=FOLLOWUP_FAST_RECHECK_SECONDS,
            )
        return True

    # DEFER: do not follow up automatically (answer naturally on the next trigger).
    return True


def _handle_followup(thread_id: int, *, mode: str = "normal") -> bool:
    delay = FOLLOWUP_FAST_RECHECK_SECONDS if mode == "fast" else FOLLOWUP_RECHECK_SECONDS

    active = _active_run(thread_id)
    if active:
        # Optional safety net: poll until the current run is done, then start follow-up.
        # Default is off to keep `job_queue` clean and avoid "waiting around" rows.
        if ENABLE_DELAYED_FOLLOWUP:
            _enqueue_delayed(
                {"thread_id": thread_id, "reason": "followup_still_active", "mode": mode},
                delay_seconds=delay,
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
            reason=f"followup_start:{mode}",
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
            return _handle_followup(int(payload["thread_id"]), mode=str(payload.get("mode") or "normal"))
        except Exception:
            return True
    return True


if __name__ == "__main__":
    _log_supabase_identity()
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
