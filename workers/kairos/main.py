"""Kairos worker – builds prompts and records analytical outputs."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Tuple

import requests
from openai import OpenAI
from supabase import create_client, ClientOptions

from workers.lib.ai_response_store import record_ai_response
from workers.lib.daily_plan_utils import (
    fetch_daily_plan_row as _fetch_daily_plan_row_shared,
    format_daily_plan_for_prompt as _format_daily_plan_for_prompt_shared,
)
from workers.lib.prompt_builder import (
    build_prompt,
    build_prompt_sections,
    live_turn_window,
)
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
    raise RuntimeError("Missing Supabase configuration for Kairos")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)
QUEUE = "kairos.analyse"
JOIN_QUEUE = "hermes.join"

TRANSLATOR_MODEL = os.getenv("TRANSLATOR_MODEL", "gpt-4o-mini")
TRANSLATOR_BASE_URL = os.getenv("TRANSLATOR_BASE_URL", "https://api.openai.com/v1")
TRANSLATOR_API_KEY = os.getenv("TRANSLATOR_API_KEY") or os.getenv("OPENAI_API_KEY")

if not TRANSLATOR_API_KEY:
    raise RuntimeError("Missing TRANSLATOR_API_KEY or OPENAI_API_KEY for Kairos translator")

TRANSLATOR_CLIENT = OpenAI(
    base_url=TRANSLATOR_BASE_URL,
    api_key=TRANSLATOR_API_KEY,
)

KAIROS_TRANSLATOR_SCHEMA = """
Return a JSON object with exactly these keys:
- STRATEGIC_NARRATIVE: string
- TACTICAL_SIGNALS: string
- PSYCHOLOGICAL_LEVERS: string
- RISKS: string
- TURN_MICRO_NOTE: object with key SUMMARY (string).
- MOMENT_COMPASS: string
- SCHEDULE_RETHINK: string
Output ONLY valid JSON matching this shape. Do not include markdown or prose.
Do not paraphrase, summarize, or inject new content; preserve the original wording in each field.
"""

HEADER_MAP = {
    "STRATEGIC_NARRATIVE": "STRATEGIC_NARRATIVE",
    "STRATEGIC NARRATIVE": "STRATEGIC_NARRATIVE",
    "TACTICAL_SIGNALS": "TACTICAL_SIGNALS",
    "TACTICAL SIGNALS": "TACTICAL_SIGNALS",
    "PSYCHOLOGICAL_LEVERS": "PSYCHOLOGICAL_LEVERS",
    "PSYCHOLOGICAL LEVERS": "PSYCHOLOGICAL_LEVERS",
    "RISKS": "RISKS",
    "TURN_MICRO_NOTE": "TURN_MICRO_NOTE",
    "TURN MICRO NOTE": "TURN_MICRO_NOTE",
    "MOMENT_COMPASS": "MOMENT_COMPASS",
    "MOMENT COMPASS": "MOMENT_COMPASS",
    "SCHEDULE_RETHINK": "SCHEDULE_RETHINK",
    "SCHEDULE RETHINK": "SCHEDULE_RETHINK",
}

HEADER_PATTERN = re.compile(
    r"^[\s>*-]*\**\s*(?P<header>"
    r"STRATEGIC[_ ]NARRATIVE|"
    r"TACTICAL[_ ]SIGNALS|PSYCHOLOGICAL[_ ]LEVERS|RISKS|TURN[_ ]MICRO[_ ]NOTE|"
    r"MOMENT[_ ]COMPASS|SCHEDULE[_ ]RETHINK)"
    r"\s*\**\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _format_fan_turn(row: dict) -> str:
    """
    Render the latest fan turn as a sequence of text/media parts, so media
    descriptions produced by Argus appear inline in Kairos/Napoleon prompts.
    """
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
            if not desc and media_analysis:
                # Fallback: reuse the aggregate analysis if no per-item text.
                desc = media_analysis
            if not desc:
                desc = item.get("argus_error") or "media attachment"
            parts.append(f"({kind}): {desc}")
    elif media_analysis:
        # No structured items, but Argus still produced text.
        parts.append(f"(media): {media_analysis}")

    return "\n".join(parts) if parts else text


def runpod_call(system_prompt: str, user_message: str) -> tuple[str, dict, dict]:
    """
    Call the RunPod vLLM OpenAI-compatible server using chat completions.
    Returns (raw_text, request_payload) for logging.
    """
    base = os.getenv("RUNPOD_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{base}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
    }
    payload = {
        "model": os.getenv("RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored"),
        "messages": [
            {"role": "system", "content": "Reasoning: high"},
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 4000,
        "temperature": 0.6,
        "top_p": 0.95,
        "repetition_penalty": 1.0,  # effectively off to allow headers to repeat
        "presence_penalty": 0.0,
        "frequency_penalty": 0.1,   # gentle nudge against excessive repetition
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=600)
    resp.raise_for_status()
    data = resp.json()
    response_payload = data

    raw_text = ""
    try:
        if "choices" in data and len(data["choices"]) > 0:
            choice = data["choices"][0]
            message = choice.get("message") or {}

            # 1. Try standard content first
            content = message.get("content")

            # 2. Get reasoning (this model uses it heavily)
            reasoning = message.get("reasoning") or message.get("reasoning_content")

            if content:
                raw_text = content
            elif reasoning:
                # Fallback: if content is null, use the reasoning as the output
                # (This happens often with this specific model)
                raw_text = reasoning
            else:
                raw_text = choice.get("text") or ""
    except Exception:
        pass

    # DEBUG FALLBACK: If we extracted nothing, dump the whole JSON object
    # so it appears in the database logs for debugging.
    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text, payload, response_payload


def translate_kairos_output(raw_text: str) -> Tuple[dict | None, str | None]:
    """
    Convert Kairos' freeform text into structured JSON using a translator model.

    Returns (parsed_json, error_message). On success error_message is None.
    """
    if not raw_text or not raw_text.strip():
        return None, "empty_raw_text"

    try:
        resp = TRANSLATOR_CLIENT.chat.completions.create(
            model=TRANSLATOR_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You convert Kairos analysis text into STRICT JSON. "
                        "DO NOT paraphrase, summarize, invent, or omit any content. "
                        "Copy every header/value exactly as given into the JSON fields. "
                        "If a field is missing, use an empty string. "
                        "All fields are free text; do not enforce enums. "
                        "Output ONLY valid JSON."
                    ),
                },
                {
                    "role": "user",
                    "content": f"{KAIROS_TRANSLATOR_SCHEMA.strip()}\n\nRaw Kairos analysis:\n{raw_text}",
                },
            ],
            temperature=0,
            max_tokens=1200,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"Translator API error: {exc}"

    message = resp.choices[0].message
    content = (message.content or "").strip()
    if not content:
        return None, "translator_empty_response"

    try:
        parsed = json.loads(content)
    except Exception as exc:  # noqa: BLE001
        return None, f"Translator JSON decode error: {exc}"

    return parsed, None


def _merge_extras(existing: dict, patch: dict) -> dict:
    merged: dict = {}
    merged.update(existing or {})
    merged.update(patch or {})
    return merged


def _load_existing_extras(message_id: int) -> dict:
    rows = (
        SB.table("message_ai_details")
        .select("extras")
        .eq("message_id", message_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return {}
    return rows[0].get("extras") or {}


def _missing_required_fields(analysis: dict | None) -> list[str]:
    """
    Check for empty critical fields in Kairos output.
    """
    if analysis is None:
        return ["analysis_none"]

    missing: list[str] = []

    def check_non_empty(container: dict, key: str, label: str):
        val = container.get(key) if isinstance(container, dict) else None
        if val is None or not str(val).strip():
            missing.append(label)

    check_non_empty(analysis, "STRATEGIC_NARRATIVE", "STRATEGIC_NARRATIVE")

    check_non_empty(analysis, "TACTICAL_SIGNALS", "TACTICAL_SIGNALS")
    check_non_empty(analysis, "PSYCHOLOGICAL_LEVERS", "PSYCHOLOGICAL_LEVERS")
    check_non_empty(analysis, "RISKS", "RISKS")
    check_non_empty(analysis, "MOMENT_COMPASS", "MOMENT_COMPASS")
    check_non_empty(analysis, "SCHEDULE_RETHINK", "SCHEDULE_RETHINK")

    micro = analysis.get("TURN_MICRO_NOTE") or {}
    check_non_empty(micro, "SUMMARY", "TURN_MICRO_NOTE.SUMMARY")

    return missing


def run_repair_call(
    original_output: str,
    missing_fields: list[str],
    original_system_prompt: str,
    original_user_prompt: str,
) -> tuple[str, dict, dict]:
    """
    Ask the model to repair an incomplete Kairos output by filling only missing sections.
    Returns (raw_text, request_payload).
    """
    system_prompt = (
        "You are the Kairos Repair Assistant. You fix incomplete Kairos outputs.\n"
        "Output ONLY the missing headers/sections listed. Use the same header format.\n"
        "Do NOT rewrite sections that were already present. Keep style and intent."
    )

    user_message = (
        "You are fixing an incomplete Kairos output.\n\n"
        "Missing or incomplete fields:\n"
        f"- {', '.join(missing_fields) if missing_fields else '(unspecified)'}\n\n"
        "Original input (system + user):\n"
        f"{original_system_prompt}\n\n{original_user_prompt}\n\n"
        "Previous output to repair (incomplete):\n"
        f"{original_output}\n\n"
        "Instructions:\n"
        "- Read the input briefly to recall context.\n"
        "- Read the previous output and continue/repair it.\n"
        "- Output ONLY the missing headers/sections listed above. Use the same header format.\n"
        "- Do NOT rewrite sections that were already present.\n"
        "- Keep the same style and intent; do not invent new headers."
    )

    return runpod_call(system_prompt, user_message)


def parse_kairos_partial(raw_text: str) -> dict:
    """
    Best-effort partial parser: returns whatever headers are present.
    """
    parsed, _err = parse_kairos_headers(raw_text)
    return parsed or {}


def merge_kairos_analysis(base: dict, patch: dict) -> dict:
    """
    Merge partial Kairos analysis into a base analysis.
    Only overwrite fields if patch provides a non-empty value.
    """
    merged = {
        "STRATEGIC_NARRATIVE": base.get("STRATEGIC_NARRATIVE") or "",
        "TACTICAL_SIGNALS": base.get("TACTICAL_SIGNALS") or "",
        "PSYCHOLOGICAL_LEVERS": base.get("PSYCHOLOGICAL_LEVERS") or "",
        "RISKS": base.get("RISKS") or "",
        "TURN_MICRO_NOTE": base.get("TURN_MICRO_NOTE") or {"SUMMARY": ""},
        "MOMENT_COMPASS": base.get("MOMENT_COMPASS") or "",
        "SCHEDULE_RETHINK": base.get("SCHEDULE_RETHINK") or "",
    }

    if patch.get("STRATEGIC_NARRATIVE") and str(patch["STRATEGIC_NARRATIVE"]).strip():
        merged["STRATEGIC_NARRATIVE"] = patch["STRATEGIC_NARRATIVE"]

    for key in (
        "TACTICAL_SIGNALS",
        "PSYCHOLOGICAL_LEVERS",
        "RISKS",
        "MOMENT_COMPASS",
        "SCHEDULE_RETHINK",
    ):
        val = patch.get(key)
        if val and str(val).strip():
            merged[key] = val

    if patch.get("TURN_MICRO_NOTE") and isinstance(patch["TURN_MICRO_NOTE"], dict):
        summary = patch["TURN_MICRO_NOTE"].get("SUMMARY")
        if summary and str(summary).strip():
            merged["TURN_MICRO_NOTE"]["SUMMARY"] = summary

    return merged


def parse_kairos_headers(raw_text: str) -> Tuple[dict | None, str | None]:
    """
    Parse the RunPod (Kairos) freeform text by header labels.
    Returns (dict, None) on success; (None, error) on failure.
    """
    if not raw_text or not raw_text.strip():
        return None, "empty_text"

    lines = raw_text.splitlines()
    matches: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        m = HEADER_PATTERN.match(line)
        if not m:
            continue
        raw_header = m.group("header")
        normalized = HEADER_MAP.get(raw_header.upper().replace("_", " "), None)
        if normalized:
            matches.append((idx, normalized))

    if not matches:
        return None, "no_headers_found"

    matches.sort(key=lambda x: x[0])
    sections: dict[str, str] = {}

    def _strip_end_marker(text: str) -> str:
        if not text:
            return ""
        lines = text.splitlines()
        while lines and lines[-1].strip() in {"### END", "END"}:
            lines.pop()
        return "\n".join(lines).strip()

    for i, (idx, key) in enumerate(matches):
        start = idx + 1
        end = matches[i + 1][0] if i + 1 < len(matches) else len(lines)
        segments = lines[start:end]
        sections[key] = _strip_end_marker("\n".join(segments).strip())

    # Build analysis dict with defaults
    analysis: dict[str, Any] = {
        "STRATEGIC_NARRATIVE": sections.get("STRATEGIC_NARRATIVE", ""),
        "TACTICAL_SIGNALS": sections.get("TACTICAL_SIGNALS", ""),
        "PSYCHOLOGICAL_LEVERS": sections.get("PSYCHOLOGICAL_LEVERS", ""),
        "RISKS": sections.get("RISKS", ""),
        "TURN_MICRO_NOTE": {"SUMMARY": ""},
        "MOMENT_COMPASS": sections.get("MOMENT_COMPASS", ""),
        "SCHEDULE_RETHINK": sections.get("SCHEDULE_RETHINK", ""),
    }

    tmn_block = sections.get("TURN_MICRO_NOTE", "")
    if tmn_block:
        # Try to pull after "summary:" if present, else whole block
        summary = tmn_block
        for line in tmn_block.splitlines():
            if "summary" in line.lower():
                summary = line.split(":", 1)[-1].strip() or summary
                break
        analysis["TURN_MICRO_NOTE"]["SUMMARY"] = summary.strip()

    return analysis, None


def _validated_analysis(fragments: dict) -> dict:
    """
    Ensure required keys exist and are NOT empty strings.
    """
    if not isinstance(fragments, dict):
        raise ValueError("analysis is not an object")

    # Helper: fail if missing OR empty
    def _require_text(key: str) -> str:
        val = fragments.get(key)
        if val is None:
            raise ValueError(f"missing field: {key}")
        s_val = str(val).strip()
        if not s_val:
            raise ValueError(f"field empty: {key}")
        return s_val

    strategic_narrative = _require_text("STRATEGIC_NARRATIVE")
    tactical = _require_text("TACTICAL_SIGNALS")
    levers = _require_text("PSYCHOLOGICAL_LEVERS")
    risks = _require_text("RISKS")
    moment_compass = _require_text("MOMENT_COMPASS")
    schedule_rethink = _require_text("SCHEDULE_RETHINK")

    micro = fragments.get("TURN_MICRO_NOTE")
    if not isinstance(micro, dict):
        raise ValueError("TURN_MICRO_NOTE must be an object")
    micro_summary = str(micro.get("SUMMARY") or "").strip()
    if not micro_summary:
        raise ValueError("TURN_MICRO_NOTE.SUMMARY is empty")

    return {
        "STRATEGIC_NARRATIVE": strategic_narrative,
        "TACTICAL_SIGNALS": tactical,
        "PSYCHOLOGICAL_LEVERS": levers,
        "RISKS": risks,
        "TURN_MICRO_NOTE": {"SUMMARY": micro_summary},
        "MOMENT_COMPASS": moment_compass,
        "SCHEDULE_RETHINK": schedule_rethink,
    }


def _safe_now_local_iso(tz_name: str | None) -> tuple[str, str]:
    """
    Return (tz_name, now_local_iso). Fail-open to UTC on invalid/missing tz.
    """
    fallback_tz = "UTC"
    tz = (tz_name or "").strip() or fallback_tz
    try:
        now_local = datetime.now(ZoneInfo(tz))
        return tz, now_local.replace(second=0, microsecond=0).isoformat()
    except Exception:
        now_utc = datetime.now(timezone.utc)
        return fallback_tz, now_utc.replace(second=0, microsecond=0).isoformat()


def _current_plan_date_for_now(now_local_iso: str) -> str | None:
    """
    Timeline day windows are 02:00 -> next day 02:00.
    If it's before 02:00 local, "today's" plan is yesterday's plan_date.
    """
    try:
        now_local = datetime.fromisoformat(now_local_iso)
    except Exception:
        return None
    plan_day = now_local.date()
    if now_local.hour < 2:
        from datetime import timedelta

        plan_day = plan_day - timedelta(days=1)
    return plan_day.isoformat()


def _fetch_daily_plan_row(thread_id: int) -> dict | None:
    return _fetch_daily_plan_row_shared(SB, thread_id)


def _format_daily_plan_for_prompt(plan_row: dict | None) -> str:
    return _format_daily_plan_for_prompt_shared(plan_row)


def record_kairos_failure(
    message_id: int,
    thread_id: int,
    prompt: str,
    raw_text: str,
    error_message: str,
    translator_error: str | None = None,
    partial: dict | None = None,
) -> None:
    """
    Upsert a message_ai_details row that marks this analysis as failed,
    and keep a snippet of the raw model output for debugging.
    """
    existing_extras = _load_existing_extras(message_id)
    extras_patch = {
        "kairos_raw_text_preview": (raw_text or "")[:2000],
    }

    row = {
        "message_id": message_id,
        "thread_id": thread_id,
        "sender": "fan",
        "raw_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "kairos_status": "failed",
        "extract_error": error_message,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "kairos_prompt_raw": prompt,
        "kairos_output_raw": raw_text,
        "translator_error": translator_error,
        "extras": _merge_extras(existing_extras, extras_patch),
    }

    # If we have partial analysis, keep what we have (without marking ok)
    if partial:
        extras_patch = {
            "kairos_raw_json": partial,
            "kairos_raw_text_preview": (raw_text or "")[:2000],
            "schedule_rethink": partial.get("SCHEDULE_RETHINK"),
        }
        row.update(
            {
                "strategic_narrative": partial.get("STRATEGIC_NARRATIVE"),
                "tactical_signals": partial.get("TACTICAL_SIGNALS"),
                "psychological_levers": partial.get("PSYCHOLOGICAL_LEVERS"),
                "risks": partial.get("RISKS"),
                "moment_compass": partial.get("MOMENT_COMPASS"),
                "kairos_summary": (partial.get("TURN_MICRO_NOTE") or {}).get("SUMMARY"),
                "extras": _merge_extras(existing_extras, extras_patch),
            }
        )

    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
        .execute()
    )
    # Best-effort: persist schedule_rethink into a dedicated column when present.
    try:
        if partial and partial.get("SCHEDULE_RETHINK") is not None:
            SB.table("message_ai_details").update(
                {"schedule_rethink": partial.get("SCHEDULE_RETHINK")}
            ).eq("message_id", int(message_id)).execute()
    except Exception:
        pass


def upsert_kairos_details(
    message_id: int,
    thread_id: int,
    prompt: str,
    raw_text: str,
    analysis: dict,
) -> None:
    """
    Persist a successful Kairos analysis into message_ai_details.
    Assumes `analysis` is the dict parsed from the model JSON.
    """
    existing_extras = _load_existing_extras(message_id)
    extras_patch = {
        "kairos_raw_json": analysis,
        "kairos_raw_text_preview": (raw_text or "")[:2000],
        "schedule_rethink": analysis.get("SCHEDULE_RETHINK"),
    }

    row = {
        "message_id": message_id,
        "thread_id": thread_id,
        "sender": "fan",
        "raw_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        # Kairos is now authoritative for its own status.
        "kairos_status": "ok",
        "extract_error": None,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "kairos_prompt_raw": prompt,
        "kairos_output_raw": raw_text,
        "translator_error": None,
        "strategic_narrative": analysis["STRATEGIC_NARRATIVE"],
        "tactical_signals": analysis["TACTICAL_SIGNALS"],
        "psychological_levers": analysis["PSYCHOLOGICAL_LEVERS"],
        "risks": analysis["RISKS"],
        "moment_compass": analysis["MOMENT_COMPASS"],
        # Store schedule rethink in its own column when available (best-effort; safe on older schemas).
        "kairos_summary": analysis["TURN_MICRO_NOTE"]["SUMMARY"],
        "turn_micro_note": analysis["TURN_MICRO_NOTE"],
        "extras": _merge_extras(existing_extras, extras_patch),
    }
    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
        .execute()
    )
    # Best-effort: persist schedule_rethink into a dedicated column when present.
    try:
        SB.table("message_ai_details").update(
            {"schedule_rethink": analysis.get("SCHEDULE_RETHINK")}
        ).eq("message_id", int(message_id)).execute()
    except Exception:
        pass


def enqueue_join_job(message_id: int, run_id: str | None = None) -> None:
    payload = {"message_id": message_id}
    if run_id:
        payload["run_id"] = str(run_id)
    send(JOIN_QUEUE, payload)


def process_job(payload: Dict[str, Any], row_id: int) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = payload["message_id"]
    attempt = int(payload.get("kairos_retry", 0))
    run_id = payload.get("run_id")
    is_repair = bool(payload.get("repair_mode"))
    previous_raw_text = payload.get("orig_raw_text") or ""
    root_raw_text = payload.get("root_raw_text") or previous_raw_text or ""
    previous_missing = payload.get("missing_fields") or []
    orig_system_prompt = payload.get("orig_system_prompt") or ""
    orig_user_prompt = payload.get("orig_user_prompt") or ""
    root_analysis = payload.get("root_analysis") or {}
    kairos_mode = (payload.get("kairos_mode") or "full").lower()

    if run_id:
        set_run_current_step(str(run_id), "kairos", client=SB)
        started_at = step_started_at(
            run_id=str(run_id), step="kairos", attempt=attempt, client=SB
        ) or datetime.now(timezone.utc).isoformat()
        upsert_step(
            run_id=str(run_id),
            step="kairos",
            attempt=attempt,
            status="running",
            client=SB,
            message_id=int(fan_msg_id),
            mode=kairos_mode,
            started_at=started_at,
            meta={"queue": QUEUE, "repair_mode": bool(is_repair)},
        )
        if not is_run_active(str(run_id), client=SB):
            upsert_step(
                run_id=str(run_id),
                step="kairos",
                attempt=attempt,
                status="canceled",
                client=SB,
                message_id=int(fan_msg_id),
                mode=kairos_mode,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="run_canceled",
            )
            return True
    message_row = (
        SB.table("messages")
        .select("id,thread_id,sender,turn_index,message_text,media_analysis_text,media_payload")
        .eq("id", fan_msg_id)
        .limit(1)
        .execute()
        .data
    )
    if not message_row:
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="kairos",
                attempt=attempt,
                status="failed",
                client=SB,
                message_id=int(fan_msg_id),
                mode=kairos_mode,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="message_missing",
            )
        raise ValueError(f"Message {fan_msg_id} missing")
    message_row = message_row[0]

    thread_id = message_row["thread_id"]
    turn_index = message_row.get("turn_index")
    latest_fan_text = _format_fan_turn(message_row)
    daily_plan_row = None
    daily_plan_text = "NO_DAILY_PLAN: true"
    try:
        daily_plan_row = _fetch_daily_plan_row(thread_id)
        daily_plan_text = _format_daily_plan_for_prompt(daily_plan_row)
    except Exception:
        daily_plan_text = "NO_DAILY_PLAN: true"
    if kairos_mode == "lite":
        raw_turns = live_turn_window(
            thread_id,
            boundary_turn=turn_index,
            limit=10,
            client=SB,
            exclude_message_id=fan_msg_id,
        )
    else:
        raw_turns = live_turn_window(
            thread_id,
            boundary_turn=turn_index,
            client=SB,
            exclude_message_id=fan_msg_id,
        )

    template_name = "kairos_lite" if kairos_mode == "lite" else "kairos"
    try:
        system_prompt, user_prompt = build_prompt_sections(
            template_name,
            thread_id,
            raw_turns,
            latest_fan_text=latest_fan_text,
            client=SB,
            boundary_turn=turn_index,
            extra_context={
                "DAILY_PLAN_TODAY": daily_plan_text,
            },
        )
    except FileNotFoundError:
        system_prompt, user_prompt = build_prompt_sections(
            "kairos",
            thread_id,
            raw_turns,
            latest_fan_text=latest_fan_text,
            client=SB,
            boundary_turn=turn_index,
            extra_context={
                "DAILY_PLAN_TODAY": daily_plan_text,
            },
        )

    try:
        if is_repair:
            raw_text, request_payload, response_payload = run_repair_call(
                root_raw_text or previous_raw_text or "",
                previous_missing,
                orig_system_prompt or system_prompt,
                orig_user_prompt or user_prompt,
            )
            record_ai_response(
                SB,
                worker="kairos",
                thread_id=thread_id,
                message_id=fan_msg_id,
                run_id=str(run_id) if run_id else None,
                model=(request_payload or {}).get("model"),
                request_payload=request_payload,
                response_payload=response_payload,
                status="repair",
            )
        else:
            raw_text, request_payload, response_payload = runpod_call(
                system_prompt, user_prompt
            )
            record_ai_response(
                SB,
                worker="kairos",
                thread_id=thread_id,
                message_id=fan_msg_id,
                run_id=str(run_id) if run_id else None,
                model=(request_payload or {}).get("model"),
                request_payload=request_payload,
                response_payload=response_payload,
                status="ok",
            )
    except Exception as exc:  # noqa: BLE001
        record_kairos_failure(
            message_id=fan_msg_id,
            thread_id=thread_id,
            prompt=json.dumps(
                {
                    "system": system_prompt,
                    "user": user_prompt,
                },
                ensure_ascii=False,
            ),
            raw_text="",
            error_message=f"RunPod error: {exc}",
        )
        print(f"[Kairos] RunPod error for message {fan_msg_id}: {exc}")
        # Even if Kairos fails due to infrastructure/network issues, wake the joiner
        # so the pipeline can continue (Hermes Join treats kairos_status=failed as done).
        enqueue_join_job(fan_msg_id, str(run_id) if run_id else None)
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="kairos",
                attempt=attempt,
                status="failed",
                client=SB,
                message_id=int(fan_msg_id),
                mode=kairos_mode,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error=f"runpod_error: {exc}",
            )
        return True

    # Try code parser first
    parsed, parse_error = parse_kairos_headers(raw_text)
    analysis = None
    if parsed is not None and parse_error is None:
        try:
            analysis = _validated_analysis(parsed)
        except ValueError as exc:  # noqa: BLE001
            print(f"[Kairos] Parser validation error for message {fan_msg_id}: {exc}")
            analysis = None

    if analysis is None:
        translated, translator_error = translate_kairos_output(raw_text)
        if translator_error is not None or translated is None:
            # LOG ONLY: Do not return True. Let it fall through to retry.
            print(f"[Kairos] Translator failure: {translator_error}")
            analysis = None
        else:
            try:
                analysis = _validated_analysis(translated)
            except ValueError as exc:  # noqa: BLE001
                # LOG ONLY: Do not return True. Let it fall through to retry.
                print(f"[Kairos] Validation error after translation: {exc}")
                analysis = None

    # Basic validation for missing required fields
    missing_fields = _missing_required_fields(analysis)

    # Establish root analysis from the first attempt (best-effort partial)
    if not root_analysis:
        root_analysis = parse_kairos_partial(raw_text)

    # If parsing failed or critical fields are missing, try repair up to 2 attempts.
    if (parse_error is not None or analysis is None or missing_fields) and attempt < 2:
        retry_payload = {
            "message_id": fan_msg_id,
            "kairos_retry": attempt + 1,
            "repair_mode": True,
            "orig_raw_text": raw_text,
            "root_raw_text": root_raw_text or raw_text,
            "missing_fields": missing_fields,
            "orig_system_prompt": orig_system_prompt or system_prompt,
            "orig_user_prompt": orig_user_prompt or user_prompt,
            "root_analysis": root_analysis,
            "run_id": str(run_id) if run_id else None,
            "kairos_mode": kairos_mode,
        }
        send(QUEUE, retry_payload)
        print(
            f"[Kairos] Enqueued repair attempt {attempt + 1} for message {fan_msg_id} "
            f"(parse_error={parse_error}, missing={missing_fields})"
        )
        return True

    if is_repair:
        patch = parse_kairos_partial(raw_text)
        merged = merge_kairos_analysis(root_analysis, patch)
        analysis = merged
        parse_error = None
        missing_fields = _missing_required_fields(analysis)

    if parse_error is not None or analysis is None or missing_fields:
        # Final failure after retries; save partial if present
        record_kairos_failure(
            message_id=fan_msg_id,
            thread_id=thread_id,
            prompt=json.dumps(
                {
                    "system": system_prompt,
                    "user": user_prompt,
                },
                ensure_ascii=False,
            ),
            raw_text=raw_text,
            error_message=f"Parse/validation error after retries (missing={missing_fields}, parse_error={parse_error})",
            partial=analysis,
        )
        print(
            f"[Kairos] Parse/validation error after retries for message {fan_msg_id}: "
            f"missing={missing_fields}, parse_error={parse_error}"
        )
        # Even if Kairos failed, hand off to Hermes Join so the pipeline can continue.
        enqueue_join_job(fan_msg_id, str(run_id) if run_id else None)
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="kairos",
                attempt=attempt,
                status="failed",
                client=SB,
                message_id=int(fan_msg_id),
                mode=kairos_mode,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="parse_or_validation_failed",
            )
        return True

    upsert_kairos_details(
        message_id=fan_msg_id,
        thread_id=thread_id,
        prompt=json.dumps(
                {
                    "system": system_prompt,
                    "user": user_prompt,
                },
                ensure_ascii=False,
            ),
        raw_text=raw_text,
        analysis=analysis,
    )
    SB.table("messages").update({"kairos_output": analysis}).eq(
        "id", fan_msg_id
    ).execute()

    if run_id and not is_run_active(str(run_id), client=SB):
        upsert_step(
            run_id=str(run_id),
            step="kairos",
            attempt=attempt,
            status="canceled",
            client=SB,
            message_id=int(fan_msg_id),
            mode=kairos_mode,
            ended_at=datetime.now(timezone.utc).isoformat(),
            error="run_canceled_post_compute",
        )
        return True

    enqueue_join_job(fan_msg_id, str(run_id) if run_id else None)
    if run_id:
        upsert_step(
            run_id=str(run_id),
            step="kairos",
            attempt=attempt,
            status="ok",
            client=SB,
            message_id=int(fan_msg_id),
            mode=kairos_mode,
            ended_at=datetime.now(timezone.utc).isoformat(),
        )
    return True


if __name__ == "__main__":
    print("Kairos started - waiting for jobs")
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1); continue
        row_id = job["row_id"]
        try:
            payload = job["payload"]
            if process_job(payload, row_id):
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("Kairos error:", exc)
            traceback.print_exc()
            time.sleep(2)
