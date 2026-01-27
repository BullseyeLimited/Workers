"""
Iris decider worker — logs a fast "depth decision" for each fan turn.

For now Iris is NON-BINDING: we do not change routing based on Iris output.
We only record decisions (for iteration) and then pass the job to Hermes.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
from supabase import ClientOptions, create_client

from workers.lib.ai_response_store import record_ai_response
from workers.lib.job_utils import job_exists
from workers.lib.json_utils import safe_parse_model_json
from workers.lib.prompt_builder import live_turn_window
from workers.lib.reply_run_tracking import (
    is_run_active,
    set_run_current_step,
    step_started_at,
    upsert_step,
)
from workers.lib.simple_queue import ack, receive, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for Iris")


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
        f"[Iris] supabase_url={SUPABASE_URL} jwt_iss={iss} jwt_ref={ref} jwt_role={role}",
        flush=True,
    )


SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "iris.decide"
HERMES_QUEUE = "hermes.route"

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"

DEFAULT_PROMPT = """
You are Iris, the fast depth decider.

Output format (STRICT, no extra text):
HERMES_MODE: FULL|LITE|SKIP
HERMES_REASON: <very short reason>
KAIROS_MODE: FULL|LITE|SKIP
KAIROS_REASON: <very short reason>
NAPOLEON_MODE: FULL|LITE|SKIP
NAPOLEON_REASON: <very short reason>
"""

MODE_PATTERN = re.compile(
    r"(?P<key>HERMES|KAIROS|NAPOLEON)(?:_MODE)?\s*:\s*(?P<mode>SKIP|LITE|FULL)\b",
    re.IGNORECASE,
)

REASON_PATTERN = re.compile(
    r"(?P<key>HERMES|KAIROS|NAPOLEON)[_ ]*REASON\s*:\s*(?P<reason>.+)",
    re.IGNORECASE,
)


def _load_prompt() -> str:
    path = PROMPTS_DIR / "iris.txt"
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return DEFAULT_PROMPT


def _merge_extras(existing: dict, patch: dict) -> dict:
    merged = {}
    merged.update(existing or {})
    merged.update(patch or {})
    return merged


def _normalize_mode(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"skip", "skipped", "none", "no", "off", "0"}:
        return "skip"
    if text in {"lite", "light", "fast", "quick", "low"}:
        return "lite"
    if text in {"full", "deep", "high"}:
        return "full"
    return None


HEADER_PATTERNS = {
    "hermes": re.compile(r"HERMES[_ ]*MODE\s*:\s*(FULL|LITE|SKIP)", re.IGNORECASE),
    "kairos": re.compile(r"KAIROS[_ ]*MODE\s*:\s*(FULL|LITE|SKIP)", re.IGNORECASE),
    "napoleon": re.compile(r"NAPOLEON[_ ]*MODE\s*:\s*(FULL|LITE|SKIP)", re.IGNORECASE),
}


def parse_iris_output(raw_text: str) -> Tuple[dict | None, str | None]:
    if not raw_text or not raw_text.strip():
        return None, "empty_output"

    parsed: dict[str, str] = {}
    for key, pattern in HEADER_PATTERNS.items():
        match = pattern.search(raw_text)
        if not match:
            continue
        parsed[key] = (match.group(1) or "").strip().lower()

    reasons: dict[str, str] = {}
    for match in REASON_PATTERN.finditer(raw_text):
        key = (match.group("key") or "").strip().lower()
        reason = (match.group("reason") or "").strip()
        if key and reason:
            reasons[key] = reason

    if len(parsed) == 3:
        # Reasons are optional (but preferred); keep output stable.
        out = dict(parsed)
        out["hermes_reason"] = reasons.get("hermes", "")
        out["kairos_reason"] = reasons.get("kairos", "")
        out["napoleon_reason"] = reasons.get("napoleon", "")
        return out, None

    matches = list(MODE_PATTERN.finditer(raw_text))
    if matches:
        parsed = {}
        for match in matches:
            key = (match.group("key") or "").strip().lower()
            mode = _normalize_mode(match.group("mode"))
            if key and mode:
                parsed[key] = mode
        missing = [k for k in ("hermes", "kairos", "napoleon") if k not in parsed]
        if not missing:
            return {
                "hermes": parsed["hermes"],
                "kairos": parsed["kairos"],
                "napoleon": parsed["napoleon"],
                "hermes_reason": reasons.get("hermes", ""),
                "kairos_reason": reasons.get("kairos", ""),
                "napoleon_reason": reasons.get("napoleon", ""),
            }, None

    # Back-compat: accept JSON outputs too (we still parse them, but the prompt asks for headers).
    data, error = safe_parse_model_json(raw_text)
    if not error and isinstance(data, dict):
        hermes = _normalize_mode(data.get("hermes") or data.get("hermes_mode"))
        kairos = _normalize_mode(data.get("kairos") or data.get("kairos_mode"))
        napoleon = _normalize_mode(data.get("napoleon") or data.get("napoleon_mode"))
        missing = [k for k, v in (("hermes", hermes), ("kairos", kairos), ("napoleon", napoleon)) if not v]
        if missing:
            return None, f"missing_fields: {', '.join(missing)}"
        return {
            "hermes": hermes,
            "kairos": kairos,
            "napoleon": napoleon,
            "hermes_reason": str(data.get("hermes_reason") or ""),
            "kairos_reason": str(data.get("kairos_reason") or ""),
            "napoleon_reason": str(data.get("napoleon_reason") or ""),
        }, None

    return None, error or "unparseable_output"


def _fail_closed_defaults() -> dict:
    # Default bias: lite (fast but still thoughtful).
    return {
        "hermes": "lite",
        "kairos": "lite",
        "napoleon": "lite",
        "hermes_reason": "",
        "kairos_reason": "",
        "napoleon_reason": "",
    }


def _format_fan_turn(row: dict) -> str:
    parts: list[str] = []
    text = (row.get("message_text") or "").strip()
    media_analysis = (row.get("media_analysis_text") or "").strip()
    media_payload = row.get("media_payload") or {}
    items = []
    if isinstance(media_payload, dict):
        maybe_items = media_payload.get("items")
        if isinstance(maybe_items, list):
            items = maybe_items

    if text:
        parts.append(f"(text): {text}")

    if items:
        for item in items:
            if not isinstance(item, dict):
                continue
            kind = (item.get("type") or "media").lower()
            desc = (item.get("argus_text") or "").strip()
            if not desc:
                desc = (item.get("argus_preview") or "").strip()
            if not desc:
                desc = media_analysis
            if not desc:
                desc = item.get("argus_error") or "media attachment"
            parts.append(f"({kind}): {desc}")
    elif media_analysis:
        parts.append(f"(media): {media_analysis}")

    return "\n".join(parts) if parts else text


def _runpod_call(system_prompt: str, user_message: str) -> tuple[str, dict, dict]:
    base = (os.getenv("IRIS_RUNPOD_URL") or os.getenv("RUNPOD_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")
    key = os.getenv("IRIS_RUNPOD_API_KEY") or os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("RUNPOD_API_KEY or OPENAI_API_KEY is not set")

    url = f"{base}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
    }
    payload = {
        "model": os.getenv("IRIS_RUNPOD_MODEL_NAME") or os.getenv("RUNPOD_MODEL_NAME") or "gpt-oss-20b-uncensored",
        "messages": [
            {"role": "system", "content": "Reasoning: high"},
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": float(os.getenv("IRIS_TEMPERATURE", "0.2")),
        "max_tokens": int(os.getenv("IRIS_MAX_TOKENS", "200")),
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    response_payload = data

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
                raw_text = data["choices"][0].get("text") or ""
    except Exception:
        raw_text = ""

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text or "", payload, response_payload


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = int(payload["message_id"])
    attempt = int(payload.get("iris_retry", 0))
    run_id = payload.get("run_id")

    if run_id:
        set_run_current_step(str(run_id), "iris", client=SB)
        started_at = step_started_at(run_id=str(run_id), step="iris", attempt=attempt, client=SB) or datetime.now(
            timezone.utc
        ).isoformat()
        upsert_step(
            run_id=str(run_id),
            step="iris",
            attempt=attempt,
            status="running",
            client=SB,
            message_id=int(fan_msg_id),
            started_at=started_at,
            meta={"queue": QUEUE},
        )
        if not is_run_active(str(run_id), client=SB):
            upsert_step(
                run_id=str(run_id),
                step="iris",
                attempt=attempt,
                status="canceled",
                client=SB,
                message_id=int(fan_msg_id),
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="run_canceled",
            )
            return True

    msg_rows = (
        SB.table("messages")
        .select("id,thread_id,turn_index,message_text,media_payload,media_analysis_text")
        .eq("id", fan_msg_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not msg_rows:
        return True
    msg_row = msg_rows[0] if isinstance(msg_rows[0], dict) else {}
    thread_id = int(msg_row.get("thread_id") or 0)
    turn_index = msg_row.get("turn_index")

    details_rows = (
        SB.table("message_ai_details")
        .select("extras")
        .eq("message_id", fan_msg_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    existing_extras = (details_rows[0].get("extras") if details_rows else None) or {}
    if not isinstance(existing_extras, dict):
        existing_extras = {}

    # Ensure message_ai_details row exists so downstream joiners can proceed even if we skip workers.
    if not details_rows:
        try:
            SB.table("message_ai_details").insert(
                {
                    "message_id": fan_msg_id,
                    "thread_id": thread_id,
                    "sender": "fan",
                    "raw_hash": hashlib.sha256(f"iris_seed:{thread_id}:{fan_msg_id}".encode("utf-8")).hexdigest(),
                    "kairos_status": "pending",
                    "extras": existing_extras,
                }
            ).execute()
        except Exception:
            pass

    raw_turns = live_turn_window(
        thread_id,
        boundary_turn=turn_index,
        limit=20,
        exclude_message_id=fan_msg_id,
        client=SB,
    )
    raw_turns = raw_turns or "[No raw turns provided]"
    latest_fan_text = _format_fan_turn(msg_row)

    user_block = (
        "<IRIS_INPUT>\n"
        f"{raw_turns}\n\n"
        "LATEST_FAN_MESSAGE:\n"
        f"{latest_fan_text}\n"
        "</IRIS_INPUT>"
    )
    system_prompt = _load_prompt()

    request_payload = None
    response_payload = None
    raw_text = ""
    try:
        raw_text, request_payload, response_payload = _runpod_call(system_prompt, user_block)
        parsed, error = parse_iris_output(raw_text)
    except Exception as exc:  # noqa: BLE001
        parsed, error = None, f"runpod_error: {exc}"
        status = "failed"
    else:
        status = "ok" if parsed else "failed"

    record_ai_response(
        SB,
        worker="iris",
        thread_id=thread_id,
        message_id=fan_msg_id,
        run_id=str(run_id) if run_id else None,
        model=(request_payload or {}).get("model") if request_payload else None,
        request_payload=request_payload,
        response_payload=response_payload,
        status=status,
        error=error,
    )

    if not parsed:
        parsed = _fail_closed_defaults()

    now_iso = datetime.now(timezone.utc).isoformat()
    iris_blob = {
        "status": status,
        "raw_output": raw_text,
        "parsed": parsed,
        "error": error,
        "created_at": now_iso,
    }
    merged_extras = _merge_extras(existing_extras, {"iris": iris_blob})
    try:
        SB.table("message_ai_details").update({"extras": merged_extras}).eq("message_id", fan_msg_id).execute()
    except Exception:
        pass

    # Best-effort: also write into dedicated columns when present (non-binding phase).
    try:
        SB.table("message_ai_details").update(
            {
                "iris_status": status,
                "iris_error": error,
                "iris_created_at": now_iso,
                "iris_output_raw": raw_text,
                "iris_hermes_mode": (parsed.get("hermes") or "").strip().lower(),
                "iris_hermes_reason": (parsed.get("hermes_reason") or "").strip() or None,
                "iris_kairos_mode": (parsed.get("kairos") or "").strip().lower(),
                "iris_kairos_reason": (parsed.get("kairos_reason") or "").strip() or None,
                "iris_napoleon_mode": (parsed.get("napoleon") or "").strip().lower(),
                "iris_napoleon_reason": (parsed.get("napoleon_reason") or "").strip() or None,
            }
        ).eq("message_id", fan_msg_id).execute()
    except Exception:
        pass

    # If this run was canceled while Iris was working, do not enqueue downstream jobs.
    if run_id and not is_run_active(str(run_id), client=SB):
        upsert_step(
            run_id=str(run_id),
            step="iris",
            attempt=attempt,
            status="canceled",
            client=SB,
            message_id=int(fan_msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error="run_canceled_post_compute",
        )
        return True

    # Non-binding phase: always continue the existing pipeline by enqueuing Hermes.
    if not job_exists(HERMES_QUEUE, fan_msg_id, client=SB):
        hermes_payload = {"message_id": fan_msg_id}
        if run_id:
            hermes_payload["run_id"] = str(run_id)
        send(HERMES_QUEUE, hermes_payload)

    if run_id:
        upsert_step(
            run_id=str(run_id),
            step="iris",
            attempt=attempt,
            status="ok" if status == "ok" else "failed",
            client=SB,
            message_id=int(fan_msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error=error,
            meta={"queue": QUEUE},
        )
    return True


if __name__ == "__main__":
    _log_supabase_identity()
    print("[Iris] started - waiting for jobs", flush=True)
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        row_id = job["row_id"]
        try:
            if process_job(job["payload"]):
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("[Iris] error:", exc, flush=True)
            traceback.print_exc()
            time.sleep(2)
