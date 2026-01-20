import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone

import requests
from supabase import create_client, ClientOptions

from workers.lib.content_pack import format_content_pack
from workers.lib.json_utils import safe_parse_model_json
from workers.lib.prompt_builder import build_prompt_sections, live_turn_window
from workers.lib.reply_run_tracking import (
    is_run_active,
    set_run_current_step,
    step_started_at,
    upsert_step,
)
from workers.lib.simple_queue import receive, ack, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CONTENT_PACK_ENABLED = os.getenv("CONTENT_PACK_ENABLED", "").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
        "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)
QUEUE = "napoleon.reply"
WRITER_QUEUE = "napoleon.compose"
HORIZONS = ["EPISODE", "CHAPTER", "SEASON", "YEAR", "LIFETIME"]
HEADER_PATTERN = re.compile(
    r"^[\s>*#-]*\**\s*(?:SECTION\s*\d+\s*[:\-–—]?\s*)?(?P<header>"
    r"TACTICAL[\s_-]?PLAN[\s_-]?3[\s_-]?TURNS|"
    r"RETHINK[\s_-]?HORIZONS|"
    r"VOICE[\s_-]?ENGINEERING[\s_-]?LOGIC|"
    r"FINAL[\s_-]?MESSAGE"
    r")\s*\**\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
HORIZON_LINE_PATTERN = re.compile(
    r"^[\s>*-]*\**\s*(?P<horizon>EPISODE|CHAPTER|SEASON|YEAR|LIFETIME)\s*:\s*(?P<body>.+)$",
    re.IGNORECASE,
)
FIELD_PATTERN = re.compile(
    r"\b(?P<field>PLAN|PROGRESS|STATE|NOTES)\s*:\s*(?P<value>.*?)(?=(?:\bPLAN|\bPROGRESS|\bSTATE|\bNOTES)\s*:|$)",
    re.IGNORECASE,
)
TACTICAL_LINE_PATTERN = re.compile(
    r"^[\s>*-]*\s*(?P<label>TURN\s*\d+[AB]?(?:_[A-Z]+)?)\s*:\s*(?P<value>.+)$",
    re.IGNORECASE,
)
ANALYST_BLOCK_PATTERN = re.compile(
    r"<ANALYST_ANALYSIS_JSON>.*?</ANALYST_ANALYSIS_JSON>",
    re.IGNORECASE | re.DOTALL,
)
CONTENT_ACTIONS_PATTERN = re.compile(
    r"<CONTENT_ACTIONS>(.*?)</CONTENT_ACTIONS>",
    re.IGNORECASE | re.DOTALL,
)
PROGRESS_WINDOWS = {
    "EPISODE": 20,
    "CHAPTER": 60,
    "SEASON": 180,
    "YEAR": 540,
    "LIFETIME": 1620,
}
PLACEHOLDER_TOKENS = {
    "n/a",
    "na",
    "none",
    "[none]",
    "null",
    "nil",
    "tbd",
    "todo",
    "unknown",
}
RETHINK_UPDATE_PATTERN = re.compile(
    r"^\s*UPDATE[_\s-]*(?P<idx>\d+)[_\s-]+(?P<field>"
    r"HORIZON|END_STATE|END_EVIDENCE|HISTORIAN_NOTE|"
    r"NEW_PLAN_OBJECTIVE|NEW_PLAN_METHOD|NEW_PLAN_SUCCESS_SIGNAL|NEW_PLAN_GUARDRAILS"
    r")\s*:\s*(?P<value>.+)$",
    re.IGNORECASE,
)


def _format_fan_turn(row: dict) -> str:
    """
    Render the latest fan turn as a sequence of text/media parts, so media
    descriptions produced by Argus appear inline in Napoleon/Kairos prompts.
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
                desc = media_analysis
            if not desc:
                desc = item.get("argus_error") or "media attachment"
            parts.append(f"({kind}): {desc}")
    elif media_analysis:
        parts.append(f"(media): {media_analysis}")

    return "\n".join(parts) if parts else text


def _build_content_pack_block(content_pack: dict | None) -> str:
    if not content_pack:
        return ""
    packed = format_content_pack(content_pack)
    if not packed:
        return ""
    return f"  <CONTENT_PACK>\n    {packed}\n  </CONTENT_PACK>"


def _blank_horizon_plan(state: str = "") -> dict:
    """
    Return a fresh horizon plan container with all expected fields.
    """
    return {
        "PLAN": "",
        "PROGRESS": "",
        "STATE": state or "",
        "NOTES": [],
        "OBJECTIVE": "",
        "METHOD": "",
        "SUCCESS_SIGNAL": "",
        "GUARDRAILS": "",
        "END_STATE": "",
        "END_EVIDENCE": "",
        "HISTORIAN_NOTE": "",
    }


def _is_placeholder(text: str | None) -> bool:
    """
    Return True if the string is an obvious placeholder ("n/a", "none", etc.).
    """
    if text is None:
        return True
    if not isinstance(text, str):
        return False
    stripped = text.strip()
    if not stripped:
        return True
    normalized = re.sub(r"[\s\[\]\(\)\{\}\.,;:_\-]+", "", stripped.lower())
    return normalized in PLACEHOLDER_TOKENS


def _should_use_model_value(val) -> bool:
    """
    Decide whether a model-provided value should overwrite canonical state.
    Rejects None, empty/whitespace, and placeholder tokens.
    """
    if val is None:
        return False
    if isinstance(val, str):
        return not _is_placeholder(val)
    return True


def _normalize_state(state: str | None) -> str:
    """
    Normalize plan states to the canonical set.
    """
    if not state:
        return ""
    cleaned = (state or "").strip().lower()
    if cleaned in {"completed"}:
        return "achieved"
    if cleaned in {"complete", "done", "finished"}:
        return "achieved"
    if cleaned in {"altered", "adjusted", "changed"}:
        return "altered"
    if cleaned in {"diverted", "abandoned", "pivot"}:
        return "diverted"
    if cleaned in {"ongoing", "active"}:
        return "ongoing"
    return cleaned


def _is_complete_update(update: dict) -> bool:
    """
    Determine if an UPDATE block is actionable.
    Requires: HORIZON + END_STATE + at least one NEW_PLAN_* field with a usable value.
    """
    if not isinstance(update, dict):
        return False
    horizon = (update.get("HORIZON") or "").upper()
    if not horizon or horizon not in HORIZONS:
        return False
    end_state = _normalize_state(update.get("END_STATE"))
    if not end_state:
        return False
    plan_fields = [
        update.get("NEW_PLAN_OBJECTIVE"),
        update.get("NEW_PLAN_METHOD"),
        update.get("NEW_PLAN_SUCCESS_SIGNAL"),
        update.get("NEW_PLAN_GUARDRAILS"),
        update.get("PLAN"),
    ]
    usable = any(_should_use_model_value(val) for val in plan_fields)
    return usable


def _extract_plan_text(plan_value) -> str:
    """
    Return a string plan body from either a string or dict.
    """
    if plan_value is None:
        return ""
    if isinstance(plan_value, dict):
        return str(plan_value.get("PLAN") or plan_value.get("plan") or "")
    return str(plan_value)


def _already_archived(
    thread_id: int,
    horizon: str,
    plan_status: str,
    previous_plan,
    reason_for_change: str,
) -> bool:
    """
    Idempotency guard to avoid duplicate plan_history rows on retries.
    """
    try:
        existing = (
            SB.table("plan_history")
            .select("id")
            .eq("thread_id", thread_id)
            .eq("horizon", horizon)
            .eq("plan_status", plan_status)
            .eq("previous_plan", previous_plan)
            .eq("reason_for_change", reason_for_change)
            .limit(1)
            .execute()
            .data
        )
        return bool(existing)
    except Exception:
        return False


def record_napoleon_failure(
    fan_message_id: int,
    thread_id: int,
    prompt: str,
    raw_text: str,
    raw_hash: str,
    error_message: str,
) -> None:
    """
    Mark a Napoleon job as failed on the message_ai_details row
    attached to the *fan* message, without clobbering Kairos fields.
    """
    existing_row = (
        SB.table("message_ai_details")
        .select("*")
        .eq("message_id", fan_message_id)
        .single()
        .execute()
        .data
    ) or {}

    merged_extras = existing_row.get("extras") or {}
    merged_extras.update(
        {
            "napoleon_raw_text_preview": (raw_text or "")[:2000],
        }
    )

    update_fields = {
        "napoleon_status": "failed",
        "extract_error": error_message,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "raw_hash": raw_hash,
        "napoleon_prompt_raw": prompt,
        "napoleon_output_raw": raw_text,
        "extras": merged_extras,
    }

    if existing_row:
        (
            SB.table("message_ai_details")
            .update(update_fields)
            .eq("message_id", fan_message_id)
            .execute()
        )
    else:
        # Fallback: create minimal row
        update_fields.update(
            {
                "message_id": fan_message_id,
                "thread_id": thread_id,
                "sender": "fan",
                "kairos_status": "pending",
            }
        )
        (
            SB.table("message_ai_details")
            .upsert(update_fields, on_conflict="message_id")
            .execute()
        )


def upsert_napoleon_details(
    fan_message_id: int,
    thread_id: int,
    prompt: str,
    raw_text: str,
    raw_hash: str,
    analysis: dict,
    content_actions: dict | None = None,
    content_actions_error: str | None = None,
    creator_message_id: int | None = None,
) -> None:
    """
    Persist Napoleon planning fields for the fan turn.
    The creator reply message will be produced by a downstream writer worker.
    """
    rethink = analysis.get("RETHINK_HORIZONS") or {}
    voice_logic = analysis.get("VOICE_ENGINEERING_LOGIC") or {}
    final_message = analysis.get("FINAL_MESSAGE") or ""

    # Fetch existing row so we can preserve all Kairos fields and satisfy constraints.
    existing_row = (
        SB.table("message_ai_details")
        .select("*")
        .eq("message_id", fan_message_id)
        .single()
        .execute()
        .data
    )

    if not existing_row:
        # Without Kairos we cannot set status ok; surface loudly.
        raise ValueError(f"message_ai_details missing for fan message {fan_message_id}")

    merged_extras = existing_row.get("extras") or {}
    merged_extras.update(
        {
            "napoleon_raw_json": analysis,
            "napoleon_raw_text_preview": (raw_text or "")[:2000],
            "napoleon_prompt_preview": (prompt or "")[:2000],
            "napoleon_rethink_horizons": rethink,
            "napoleon_save_note": "Merged with Kairos",
            "kairos_check": "found" if existing_row.get("strategic_narrative") else "missing",
        }
    )
    if content_actions is not None:
        merged_extras["content_actions"] = content_actions
    if content_actions_error:
        merged_extras["content_actions_error"] = content_actions_error

    update_fields = {
        # Do not overwrite Kairos columns; only add Napoleon fields + status flip.
        "napoleon_status": "ok",
        "extract_error": None,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "raw_hash": raw_hash,
        "napoleon_prompt_raw": prompt,
        "napoleon_output_raw": raw_text,
        "tactical_plan_3turn": analysis["TACTICAL_PLAN_3TURNS"],
        "plan_episode": analysis["MULTI_HORIZON_PLAN"]["EPISODE"],
        "plan_chapter": analysis["MULTI_HORIZON_PLAN"]["CHAPTER"],
        "plan_season": analysis["MULTI_HORIZON_PLAN"]["SEASON"],
        "plan_year": analysis["MULTI_HORIZON_PLAN"]["YEAR"],
        "plan_lifetime": analysis["MULTI_HORIZON_PLAN"]["LIFETIME"],
        "rethink_horizons": rethink,
        "napoleon_final_message": final_message,
        "napoleon_voice_engine": voice_logic,
        "extras": merged_extras,
        "historian_entry": analysis.get("HISTORIAN_ENTRY", {}),
    }

    (
        SB.table("message_ai_details")
        .update(update_fields)
        .eq("message_id", fan_message_id)
        .execute()
    )


def insert_creator_reply(thread_id: int, final_text: str) -> int:
    # Fetch next turn index based on existing messages to avoid drift
    latest = (
        SB.table("messages")
        .select("turn_index")
        .eq("thread_id", thread_id)
        .order("turn_index", desc=True)
        .limit(1)
        .execute()
        .data
    )
    latest_turn = latest[0]["turn_index"] if latest else 0
    next_turn = (latest_turn or 0) + 1
    row = {
        "thread_id": thread_id,
        "turn_index": next_turn,
        "sender": "creator",
        "message_text": final_text,
        "ext_message_id": f"auto-{int(time.time()*1000)}",
        "source_channel": "scheduler",
    }
    msg = SB.table("messages").insert(row).execute().data[0]
    return msg["id"]


def runpod_call(system_prompt: str, user_message: str) -> tuple[str, dict]:
    """
    Call the RunPod vLLM OpenAI-compatible server using chat completions.
    Returns (raw_text, request_payload) for logging/debugging.
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
        pass

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text, payload


def _redact_analyst_block(text: str) -> str:
    """
    Remove the verbose Kairos JSON block from logged prompts.
    """
    if not text:
        return text
    return ANALYST_BLOCK_PATTERN.sub(
        "<ANALYST_ANALYSIS_JSON>[redacted]</ANALYST_ANALYSIS_JSON>",
        text,
    )


def redact_prompt_log(prompt_log: str) -> str:
    """
    Strip the Kairos blob from logged Napoleon prompts while keeping structure.
    """
    if not prompt_log:
        return prompt_log
    try:
        data = json.loads(prompt_log)
        for key in ("system", "user"):
            if isinstance(data.get(key), str):
                data[key] = _redact_analyst_block(data[key])
        return json.dumps(data, ensure_ascii=False)
    except Exception:
        return _redact_analyst_block(prompt_log)


def _normalize_header(header: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9_]+", "_", header.upper()).strip("_")
    cleaned = cleaned.replace("PLAN_3_TURNS", "PLAN_3TURNS")
    return cleaned


def _strip_quotes(text: str) -> str:
    s = text.strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1].strip()
    return s


def _parse_notes_field(raw: str) -> list[str]:
    if not raw:
        return []
    cleaned = raw.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].strip()
    parts = re.split(r"[•;\n]| - ", cleaned)
    notes = []
    for part in parts:
        item = part.strip(" -,")
        if item:
            notes.append(item)
    return notes


TACTICAL_KEY_MAP = {
    "TURN1": "TURN1_DIRECTIVE",
    "TURN1DIRECTIVE": "TURN1_DIRECTIVE",
    "TURN1_DIRECTIVE": "TURN1_DIRECTIVE",
    "TURN2A": "TURN2A_FAN_PATH",
    "TURN2AFANPATH": "TURN2A_FAN_PATH",
    "TURN2A_FAN_PATH": "TURN2A_FAN_PATH",
    "TURN2B": "TURN2B_FAN_PATH",
    "TURN2BFANPATH": "TURN2B_FAN_PATH",
    "TURN2B_FAN_PATH": "TURN2B_FAN_PATH",
    "TURN3A": "TURN3A_DIRECTIVE",
    "TURN3ADIRECTIVE": "TURN3A_DIRECTIVE",
    "TURN3A_DIRECTIVE": "TURN3A_DIRECTIVE",
    "TURN3B": "TURN3B_DIRECTIVE",
    "TURN3BDIRECTIVE": "TURN3B_DIRECTIVE",
    "TURN3B_DIRECTIVE": "TURN3B_DIRECTIVE",
}


def _parse_tactical_section(section_text: str) -> dict:
    result = {
        "TURN1_DIRECTIVE": "",
        "TURN2A_FAN_PATH": "",
        "TURN2B_FAN_PATH": "",
        "TURN3A_DIRECTIVE": "",
        "TURN3B_DIRECTIVE": "",
    }
    if not section_text:
        return result

    for line in section_text.splitlines():
        match = TACTICAL_LINE_PATTERN.match(line)
        if not match:
            continue
        label = match.group("label") or ""
        normalized = re.sub(r"[^A-Z0-9_]", "", label.upper())
        key = TACTICAL_KEY_MAP.get(normalized)
        if key:
            result[key] = match.group("value").strip()
    return result


def _parse_multi_horizon_section(section_text: str) -> dict:
    plans = {hz: _blank_horizon_plan() for hz in HORIZONS}
    if not section_text:
        raise ValueError("no_multi_horizon_section")

    found_any = False
    horizon_lines_found = False
    updates: dict[int, dict[str, str]] = {}
    for line in section_text.splitlines():
        update_match = RETHINK_UPDATE_PATTERN.match(line)
        if update_match:
            idx = int(update_match.group("idx") or 0)
            field = (update_match.group("field") or "").upper()
            value = update_match.group("value") or ""
            updates.setdefault(idx, {})[field] = value.strip()
            found_any = True
            continue

        match = HORIZON_LINE_PATTERN.match(line)
        if not match:
            continue
        horizon_lines_found = True
        found_any = True
        horizon = (match.group("horizon") or "").upper()
        body = match.group("body") or ""
        if horizon not in plans:
            continue
        fields = plans[horizon]

        for fmatch in FIELD_PATTERN.finditer(body):
            field = (fmatch.group("field") or "").upper()
            value = fmatch.group("value") or ""
            if field == "PLAN":
                fields["PLAN"] = _strip_quotes(value)
            elif field == "PROGRESS":
                fields["PROGRESS"] = value.strip()
            elif field == "STATE":
                fields["STATE"] = value.strip()
            elif field == "NOTES":
                fields["NOTES"] = _parse_notes_field(value)

    if updates:
        for idx in sorted(updates):
            data = updates[idx]
            hz = (data.get("HORIZON") or "").upper()
            if hz not in plans:
                continue
            fields = plans[hz]
            fields["STATE"] = "ongoing"
            fields["END_STATE"] = data.get("END_STATE", "").strip()
            fields["END_EVIDENCE"] = data.get("END_EVIDENCE", "").strip()
            fields["HISTORIAN_NOTE"] = data.get("HISTORIAN_NOTE", "").strip()
            objective = data.get("NEW_PLAN_OBJECTIVE", "").strip()
            method = data.get("NEW_PLAN_METHOD", "").strip()
            success = data.get("NEW_PLAN_SUCCESS_SIGNAL", "").strip()
            guardrails = data.get("NEW_PLAN_GUARDRAILS", "").strip()
            fields["OBJECTIVE"] = objective
            fields["METHOD"] = method
            fields["SUCCESS_SIGNAL"] = success
            fields["GUARDRAILS"] = guardrails
            parts = []
            if not _is_placeholder(objective):
                parts.append(f"OBJECTIVE: {objective}")
            if not _is_placeholder(method):
                parts.append(f"METHOD: {method}")
            if not _is_placeholder(success):
                parts.append(f"SUCCESS_SIGNAL: {success}")
            if not _is_placeholder(guardrails):
                parts.append(f"GUARDRAILS: {guardrails}")
            fields["PLAN"] = " | ".join(parts)
            notes: list[str] = []
            if fields["HISTORIAN_NOTE"] and not _is_placeholder(fields["HISTORIAN_NOTE"]):
                notes.append(fields["HISTORIAN_NOTE"])
            if fields["END_EVIDENCE"] and not _is_placeholder(fields["END_EVIDENCE"]):
                notes.append(fields["END_EVIDENCE"])
            fields["NOTES"] = notes

    if not found_any and not horizon_lines_found:
        raise ValueError("no_horizon_lines")

    return plans


def _parse_rethink_section(section_text: str) -> dict:
    """
    Parse the rethink block, supporting both legacy "REASON" lines and the new
    UPDATE_X_* schema with changed horizons.
    """
    if not section_text or not section_text.strip():
        return {
            "STATUS": "",
            "SUMMARY": "",
            "REASON": "",
            "CHANGED_HORIZONS": [],
            "UPDATES": [],
            "HAS_UPDATES": False,
        }

    status = ""
    summary = ""
    reason = ""
    changed: list[str] = []
    updates: dict[int, dict[str, str]] = {}

    has_updates = False

    for line in section_text.splitlines():
        header_match = re.match(
            r"^\s*(STATUS|SUMMARY|CHANGED_HORIZONS|REASON)\s*:\s*(.*)$",
            line,
            re.IGNORECASE,
        )
        if header_match:
            key = (header_match.group(1) or "").upper()
            value = (header_match.group(2) or "").strip()
            if key == "STATUS":
                status = value.lower()
            elif key == "SUMMARY":
                summary = value
            elif key == "REASON":
                reason = value
            elif key == "CHANGED_HORIZONS":
                changed = [
                    h.strip().upper()
                    for h in re.split(r"[,\s]+", value)
                    if h.strip()
                ]
            continue

        update_match = RETHINK_UPDATE_PATTERN.match(line)
        if update_match:
            idx = int(update_match.group("idx") or 0)
            field = (update_match.group("field") or "").upper()
            value = update_match.group("value") or ""
            updates.setdefault(idx, {})[field] = value.strip()
            has_updates = True

    ordered_updates = []
    for idx in sorted(updates):
        data = updates[idx]
        ordered_updates.append(
            {
                "HORIZON": (data.get("HORIZON") or "").upper(),
                "END_STATE": (data.get("END_STATE") or "").lower(),
                "END_EVIDENCE": data.get("END_EVIDENCE", "").strip(),
                "HISTORIAN_NOTE": data.get("HISTORIAN_NOTE", "").strip(),
                "NEW_PLAN_OBJECTIVE": data.get("NEW_PLAN_OBJECTIVE", "").strip(),
                "NEW_PLAN_METHOD": data.get("NEW_PLAN_METHOD", "").strip(),
                "NEW_PLAN_SUCCESS_SIGNAL": data.get("NEW_PLAN_SUCCESS_SIGNAL", "").strip(),
                "NEW_PLAN_GUARDRAILS": data.get("NEW_PLAN_GUARDRAILS", "").strip(),
            }
        )

    if not changed:
        changed = [u["HORIZON"] for u in ordered_updates if u.get("HORIZON")]

    if has_updates and status != "yes":
        status = "yes"

    if summary and not reason:
        reason = summary
    if reason and status != "yes":
        status = "yes"
    if not status:
        status = "no"

    return {
        "STATUS": status,
        "SUMMARY": summary,
        "REASON": reason,
        "CHANGED_HORIZONS": changed,
        "UPDATES": ordered_updates,
        "HAS_UPDATES": has_updates,
    }


def _parse_voice_section(section_text: str) -> dict:
    voice = {"INTENT": "", "MECHANISM": "", "DRAFTING": ""}
    if not section_text:
        return voice

    for line in section_text.splitlines():
        match = re.match(
            r"^\s*(INTENT|MECHANISM|DRAFTING)\s*:\s*(.+)$",
            line,
            re.IGNORECASE,
        )
        if not match:
            continue
        key = (match.group(1) or "").upper()
        voice[key] = match.group(2).strip()
    return voice


def parse_napoleon_headers(raw_text: str) -> tuple[dict | None, str | None]:
    """
    Parse header-based Napoleon output into structured dicts.
    Returns (analysis, error_message).
    """
    if not raw_text or not raw_text.strip():
        return None, "empty_text"

    lines = raw_text.splitlines()
    matches: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        match = HEADER_PATTERN.match(line)
        if not match:
            continue
        header = _normalize_header(match.group("header") or "")
        matches.append((idx, header))

    if not matches:
        return None, "no_headers_found"

    sections: dict[str, str] = {}
    for i, (idx, header) in enumerate(matches):
        start = idx + 1
        next_idx = matches[i + 1][0] if i + 1 < len(matches) else len(lines)
        sections[header] = "\n".join(lines[start:next_idx]).strip()

    required = [
        "TACTICAL_PLAN_3TURNS",
        "RETHINK_HORIZONS",
    ]
    missing = [hdr for hdr in required if hdr not in sections]
    if missing:
        return None, f"missing_sections: {', '.join(missing)}"

    try:
        tactical = _parse_tactical_section(sections["TACTICAL_PLAN_3TURNS"])
        # MULTI_HORIZON_PLAN header is deprecated; attempt to parse horizon lines only from the rethink section.
        try:
            multi_plan = _parse_multi_horizon_section(sections["RETHINK_HORIZONS"])
        except Exception:
            multi_plan = {}
        rethink = _parse_rethink_section(sections["RETHINK_HORIZONS"])
    except ValueError as exc:  # noqa: BLE001
        return None, str(exc)

    analysis = {
        "TACTICAL_PLAN_3TURNS": tactical,
        "MULTI_HORIZON_PLAN": multi_plan,
        "RETHINK_HORIZONS": rethink,
        # Voice/final are optional in the new contract; seed empty for compatibility.
        "VOICE_ENGINEERING_LOGIC": {},
        "FINAL_MESSAGE": "",
    }
    return analysis, None


def parse_content_actions(raw_text: str) -> tuple[dict | None, str | None]:
    if not raw_text:
        return None, None
    match = CONTENT_ACTIONS_PATTERN.search(raw_text)
    if not match:
        return None, None
    payload = match.group(1).strip()
    if not payload:
        return {}, None
    if _is_placeholder(payload):
        return {}, None

    def _dedupe_ints(values: list[int]) -> list[int]:
        seen: set[int] = set()
        out: list[int] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    def _strip_fences(text: str) -> str:
        s = (text or "").strip()
        if not s:
            return s
        if s.startswith("```"):
            newline = s.find("\n")
            if newline != -1:
                s = s[newline + 1 :]
            s = s.strip()
            if s.endswith("```"):
                s = s[:-3]
            s = s.strip()
        return s

    # Preferred contract: header lines inside the tag (regex-friendly; not JSON).
    sends: list[int] = []
    offers: list[dict] = []
    saw_action_line = False
    send_keys = r"(?:SENDS?|SEND|DELIVERIES?|DELIVERY|DELIVER|SENT)"
    offer_keys = r"(?:OFFERS?|OFFER|PPV[_\s-]*OFFERS?|PPV[_\s-]*OFFER)"

    for raw_line in payload.splitlines():
        line = (raw_line or "").strip()
        if not line:
            continue
        if _is_placeholder(line):
            continue
        # tolerate bullets/numbering/noise
        line = re.sub(r"^[\s>*#-]+", "", line).strip()
        if not line:
            continue

        send_match = re.match(
            rf"^(?P<key>{send_keys})\s*[:=-]\s*(?P<body>.*)$",
            line,
            re.IGNORECASE,
        )
        if send_match:
            saw_action_line = True
            body = (send_match.group("body") or "").strip()
            if body and not _is_placeholder(body):
                for num in re.findall(r"\d+", body):
                    try:
                        sends.append(int(num))
                    except Exception:
                        continue
            continue

        offer_match = re.match(
            rf"^(?P<key>{offer_keys})\s*[:=-]\s*(?P<body>.*)$",
            line,
            re.IGNORECASE,
        )
        if offer_match:
            saw_action_line = True
            body = (offer_match.group("body") or "").strip()
            if not body or _is_placeholder(body):
                continue
            chunks = [
                chunk.strip()
                for chunk in re.split(r"[;,]+", body)
                if chunk.strip()
            ]
            for chunk in chunks:
                id_match = re.match(r"^(?P<id>\d+)(?P<rest>.*)$", chunk)
                if not id_match:
                    continue
                try:
                    content_id = int(id_match.group("id"))
                except Exception:
                    continue
                offer: dict = {"content_id": content_id}
                rest = (id_match.group("rest") or "").strip()
                if rest:
                    price_match = re.search(
                        r"(?:\boffered_price\b|\bprice\b|\bamount\b)?\s*=?\s*\$?\s*(?P<price>\d+(?:\.\d+)?)",
                        rest,
                        re.IGNORECASE,
                    )
                    if price_match:
                        offer["offered_price"] = price_match.group("price")
                offers.append(offer)
            continue

    if sends or offers:
        result: dict = {}
        if sends:
            result["sends"] = _dedupe_ints(sends)
        if offers:
            # Dedupe offers by content_id while preserving order (keep first price).
            seen_offer_ids: set[int] = set()
            deduped_offers: list[dict] = []
            for offer in offers:
                try:
                    cid = int(offer.get("content_id"))
                except Exception:
                    continue
                if cid in seen_offer_ids:
                    continue
                seen_offer_ids.add(cid)
                deduped_offers.append(offer)
            result["offers"] = deduped_offers
        return result, None
    if saw_action_line:
        return {}, None

    # Backwards compatibility: accept JSON payloads from older prompts/runs.
    cleaned = _strip_fences(payload)
    if cleaned:
        # Try object first (safe_parse_model_json handles noisy output).
        data, object_error = safe_parse_model_json(cleaned)
        if not object_error and isinstance(data, dict):
            return data, None

        # Try list payloads: extract between first '[' and last ']'.
        start = cleaned.find("[")
        end = cleaned.rfind("]")
        if start != -1 and end != -1 and start < end:
            try:
                parsed = json.loads(cleaned[start : end + 1])
            except Exception as exc:  # noqa: BLE001
                return None, f"{exc.__class__.__name__}: {exc}"
            if isinstance(parsed, list):
                sends = []
                for item in parsed:
                    try:
                        sends.append(int(item))
                    except Exception:
                        continue
                return {"sends": _dedupe_ints(sends)}, None
        if object_error:
            return None, object_error

    return None, "content_actions_unparseable"


def parse_napoleon_partial(raw_text: str) -> dict:
    """
    Best-effort parser that returns only the sections present, without requiring all headers.
    """
    if not raw_text or not raw_text.strip():
        return {}

    lines = raw_text.splitlines()
    matches: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        match = HEADER_PATTERN.match(line)
        if not match:
            continue
        header = _normalize_header(match.group("header") or "")
        matches.append((idx, header))

    if not matches:
        return {}

    sections: dict[str, str] = {}
    for i, (idx, header) in enumerate(matches):
        start = idx + 1
        end = matches[i + 1][0] if i + 1 < len(matches) else len(lines)
        sections[header] = "\n".join(lines[start:end]).strip()

    result: dict = {}

    if "TACTICAL_PLAN_3TURNS" in sections:
        try:
            result["TACTICAL_PLAN_3TURNS"] = _parse_tactical_section(
                sections["TACTICAL_PLAN_3TURNS"]
            )
        except Exception:
            pass

    if "RETHINK_HORIZONS" in sections:
        try:
            result["MULTI_HORIZON_PLAN"] = _parse_multi_horizon_section(
                sections["RETHINK_HORIZONS"]
            )
        except Exception:
            pass

    if "RETHINK_HORIZONS" in sections:
        try:
            result["RETHINK_HORIZONS"] = _parse_rethink_section(
                sections["RETHINK_HORIZONS"]
            )
        except Exception:
            pass

    if "VOICE_ENGINEERING_LOGIC" in sections:
        try:
            result["VOICE_ENGINEERING_LOGIC"] = _parse_voice_section(
                sections["VOICE_ENGINEERING_LOGIC"]
            )
        except Exception:
            pass

    if "FINAL_MESSAGE" in sections:
        msg = sections["FINAL_MESSAGE"].strip()
        if msg:
            result["FINAL_MESSAGE"] = msg

    return result


def merge_napoleon_analysis(base: dict, patch: dict) -> dict:
    """
    Merge partial Napoleon analysis into a base analysis.
    Only overwrite fields if patch provides a non-empty value.
    """
    merged = {
        "TACTICAL_PLAN_3TURNS": base.get("TACTICAL_PLAN_3TURNS") or {
            "TURN1_DIRECTIVE": "",
            "TURN2A_FAN_PATH": "",
            "TURN2B_FAN_PATH": "",
            "TURN3A_DIRECTIVE": "",
            "TURN3B_DIRECTIVE": "",
        },
        "MULTI_HORIZON_PLAN": base.get("MULTI_HORIZON_PLAN")
        or {hz: _blank_horizon_plan() for hz in HORIZONS},
        "RETHINK_HORIZONS": base.get("RETHINK_HORIZONS") or {
            "STATUS": "",
            "SUMMARY": "",
            "REASON": "",
            "CHANGED_HORIZONS": [],
            "UPDATES": [],
        },
        "VOICE_ENGINEERING_LOGIC": base.get("VOICE_ENGINEERING_LOGIC") or {"INTENT": "", "MECHANISM": "", "DRAFTING": ""},
        "FINAL_MESSAGE": base.get("FINAL_MESSAGE") or "",
    }

    # Merge tactical
    if "TACTICAL_PLAN_3TURNS" in patch:
        for k, v in patch["TACTICAL_PLAN_3TURNS"].items():
            if v and str(v).strip():
                merged["TACTICAL_PLAN_3TURNS"][k] = v

    # Merge multi-horizon
    if "MULTI_HORIZON_PLAN" in patch:
        for hz, hz_data in patch["MULTI_HORIZON_PLAN"].items():
            target = merged["MULTI_HORIZON_PLAN"].setdefault(hz, _blank_horizon_plan())
            if not isinstance(hz_data, dict):
                continue
            for field, val in hz_data.items():
                field_key = field.upper()
                if field_key == "NOTES":
                    if val:
                        target["NOTES"] = val
                else:
                    if not _should_use_model_value(val):
                        continue
                    target[field_key] = val

    # Merge rethink
    if "RETHINK_HORIZONS" in patch and isinstance(patch["RETHINK_HORIZONS"], dict):
        for k in ("STATUS", "SUMMARY", "REASON"):
            val = patch["RETHINK_HORIZONS"].get(k)
            if val and str(val).strip():
                merged["RETHINK_HORIZONS"][k] = val
        if patch["RETHINK_HORIZONS"].get("CHANGED_HORIZONS"):
            merged["RETHINK_HORIZONS"]["CHANGED_HORIZONS"] = patch["RETHINK_HORIZONS"]["CHANGED_HORIZONS"]
        if patch["RETHINK_HORIZONS"].get("UPDATES"):
            merged["RETHINK_HORIZONS"]["UPDATES"] = patch["RETHINK_HORIZONS"]["UPDATES"]

    # Merge voice
    if "VOICE_ENGINEERING_LOGIC" in patch and isinstance(patch["VOICE_ENGINEERING_LOGIC"], dict):
        for k in ("INTENT", "MECHANISM", "DRAFTING"):
            val = patch["VOICE_ENGINEERING_LOGIC"].get(k)
            if val and str(val).strip():
                merged["VOICE_ENGINEERING_LOGIC"][k] = val

    # Merge final message
    if patch.get("FINAL_MESSAGE") and str(patch["FINAL_MESSAGE"]).strip():
        merged["FINAL_MESSAGE"] = patch["FINAL_MESSAGE"]

    return merged


def _canonical_multi_from_threads(thread_row: dict | None) -> dict:
    """
    Build a multi-horizon plan dict from canonical thread columns.
    Falls back to empty strings when missing.
    """
    plans = {}
    if not isinstance(thread_row, dict):
        thread_row = {}
    for hz in HORIZONS:
        col = f"{hz.lower()}_plan"
        val = thread_row.get(col)
        container = _blank_horizon_plan("ongoing")
        if isinstance(val, dict):
            for key, value in val.items():
                upper_key = key.upper()
                if upper_key == "NOTES":
                    if isinstance(value, list):
                        container["NOTES"] = value
                elif upper_key in container:
                    if value is None:
                        continue
                    if isinstance(value, str):
                        if _is_placeholder(value) or not value.strip():
                            continue
                    container[upper_key] = value
            if not container["PLAN"]:
                container["PLAN"] = val.get("PLAN") or val.get("plan") or ""
        else:
            container["PLAN"] = "" if _is_placeholder(val) else (val or "")
        plans[hz] = container
    return plans


def _merge_with_canonical_plans(
    analysis: dict | None, thread_row: dict | None
) -> dict | None:
    """
    Ensure every horizon has a plan by overlaying model output on top of
    canonical thread plans. Model-provided fields take precedence.
    """
    if analysis is None:
        return None

    canonical = _canonical_multi_from_threads(thread_row)
    model_multi = analysis.get("MULTI_HORIZON_PLAN") or {}
    merged_multi: dict = {}

    for hz in HORIZONS:
        base = _blank_horizon_plan("ongoing")
        base.update(canonical.get(hz, {}))
        if isinstance(base.get("NOTES"), list):
            base["NOTES"] = list(base["NOTES"])

        hz_data = model_multi.get(hz)
        if isinstance(hz_data, dict):
            for field, val in hz_data.items():
                field_key = field.upper()
                if field_key == "NOTES":
                    if val:
                        base["NOTES"] = val
                else:
                    if not _should_use_model_value(val):
                        continue
                    base[field_key] = val

        merged_multi[hz] = base

    analysis["MULTI_HORIZON_PLAN"] = merged_multi
    return analysis


def _apply_progress_overrides(analysis: dict | None, turn_index: int | None) -> dict | None:
    """
    Override model-provided progress with server-derived counters based on turn index.
    Keeps existing plans/states/notes intact.
    """
    if analysis is None or turn_index is None:
        return analysis

    multi = analysis.get("MULTI_HORIZON_PLAN")
    if not isinstance(multi, dict):
        return analysis

    for hz, total in PROGRESS_WINDOWS.items():
        # Ensure horizon container exists
        hz_data = multi.setdefault(hz, _blank_horizon_plan())
        try:
            current = ((int(turn_index) - 1) % total) + 1
        except Exception:
            current = 1
        hz_data["PROGRESS"] = f"({current}/{total})"

    analysis["MULTI_HORIZON_PLAN"] = multi
    return analysis


def _missing_required_fields(analysis: dict | None) -> list[str]:
    """
    Flag a field only if the KEY is absent (empty strings are allowed).
    """
    if analysis is None:
        return ["analysis_none"]

    missing: list[str] = []

    def check_present(container, key, path_name):
        if not isinstance(container, dict) or key not in container:
            missing.append(path_name)

    # Check Root Fields
    tactical = analysis.get("TACTICAL_PLAN_3TURNS") or {}
    for k in ["TURN1_DIRECTIVE", "TURN2A_FAN_PATH", "TURN2B_FAN_PATH", "TURN3A_DIRECTIVE", "TURN3B_DIRECTIVE"]:
        check_present(tactical, k, f"TACTICAL_PLAN_3TURNS.{k}")

    return missing


def run_repair_call(
    original_output: str,
    missing_fields: list[str],
    original_system_prompt: str,
    original_user_prompt: str,
) -> tuple[str, dict]:
    """
    Ask the model to repair an incomplete Napoleon output by filling only missing sections.
    Returns (raw_text, request_payload).
    """
    system_prompt = (
        "You are the Napoleon Repair Assistant. You fix incomplete Napoleon outputs.\n"
        "Output ONLY the missing headers/sections listed. Use the same header format.\n"
        "Do NOT rewrite sections that were already present. Keep style and intent."
    )

    user_message = (
        "You are fixing an incomplete Napoleon output.\n\n"
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


def process_job(payload):
    fan_message_id = payload["message_id"]
    run_id = payload.get("run_id")
    attempt = 0

    if run_id:
        set_run_current_step(str(run_id), "napoleon", client=SB)
        started_at = step_started_at(
            run_id=str(run_id), step="napoleon", attempt=attempt, client=SB
        ) or datetime.now(timezone.utc).isoformat()
        upsert_step(
            run_id=str(run_id),
            step="napoleon",
            attempt=attempt,
            status="running",
            client=SB,
            message_id=int(fan_message_id),
            started_at=started_at,
            meta={"queue": QUEUE},
        )
        if not is_run_active(str(run_id), client=SB):
            upsert_step(
                run_id=str(run_id),
                step="napoleon",
                attempt=attempt,
                status="canceled",
                client=SB,
                message_id=int(fan_message_id),
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="run_canceled",
            )
            return True

    msg = (
        SB.table("messages")
        .select("thread_id,sender,message_text,media_analysis_text,media_payload,turn_index")
        .eq("id", fan_message_id)
        .single()
        .execute()
        .data
    )
    if not msg:
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="napoleon",
                attempt=attempt,
                status="failed",
                client=SB,
                message_id=int(fan_message_id),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="message_not_found",
            )
        raise ValueError(f"Message {fan_message_id} not found")

    thread_id = msg["thread_id"]
    current_turn_index = msg.get("turn_index")

    # Fetch canonical thread plans for fallback/seeding.
    thread_row = (
        SB.table("threads")
        .select(
            "episode_plan,chapter_plan,season_plan,year_plan,lifetime_plan,"
            "creator_identity_card,creator_psychic_card,"
            "fan_identity_card,fan_psychic_card,creator_id"
        )
        .eq("id", thread_id)
        .single()
        .execute()
        .data
        or {}
    )

    details_select = "extras,web_research_facts_pack,web_research_output_raw"
    if CONTENT_PACK_ENABLED:
        details_select += ",content_pack"
    details_row = (
        SB.table("message_ai_details")
        .select(details_select)
        .eq("message_id", fan_message_id)
        .single()
        .execute()
        .data
        or {}
    )
    extras = details_row.get("extras") or {}
    web_blob = extras.get("web_research") or {}
    facts_pack = (
        web_blob.get("facts_pack")
        or web_blob.get("raw_output")
        or web_blob.get("brief")
        or details_row.get("web_research_facts_pack")
        or details_row.get("web_research_output_raw")
    )
    def _format_web_research(value) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return str(value)
        return str(value)

    web_research_text = _format_web_research(facts_pack).strip()
    if web_research_text:
        web_research_section = (
            "  <WEB_RESEARCH>\n"
            f"    {web_research_text}\n"
            "  </WEB_RESEARCH>"
        )
        web_research_note = (
            "If a `<WEB_RESEARCH>` block appears inside it, treat it as optional context: "
            "it was gathered by a separate worker, and you can ignore it if it would "
            "break immersion or claim knowledge the creator should not have. If it helps "
            "and is safe, you may use it."
        )
    else:
        web_research_section = ""
        web_research_note = ""

    content_pack_block = ""
    if CONTENT_PACK_ENABLED:
        content_pack_block = _build_content_pack_block(details_row.get("content_pack"))

    raw_turns = live_turn_window(
        thread_id,
        boundary_turn=msg.get("turn_index"),
        client=SB,
        exclude_message_id=fan_message_id,
    )
    system_prompt, user_prompt = build_prompt_sections(
        "napoleon",
        thread_id,
        raw_turns,
        latest_fan_text=_format_fan_turn(msg),
        client=SB,
        boundary_turn=msg.get("turn_index"),
        analysis_message_id=fan_message_id,
        extra_context={
            "WEB_RESEARCH_SECTION": web_research_section,
            "WEB_RESEARCH_NOTE": web_research_note,
            "CONTENT_PACK": content_pack_block,
        },
    )
    full_prompt_log = json.dumps(
        {"system": system_prompt, "user": user_prompt},
        ensure_ascii=False,
    )
    prompt_log = redact_prompt_log(full_prompt_log)
    raw_hash = hashlib.sha256(full_prompt_log.encode("utf-8")).hexdigest()

    try:
        raw_text, _request_payload = runpod_call(system_prompt, user_prompt)
    except Exception as exc:  # noqa: BLE001
        record_napoleon_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=prompt_log,
            raw_text="",
            raw_hash=raw_hash,
            error_message=f"RunPod error: {exc}",
        )
        print(f"[Napoleon] RunPod error for fan message {fan_message_id}: {exc}")
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="napoleon",
                attempt=attempt,
                status="failed",
                client=SB,
                message_id=int(fan_message_id),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error=f"runpod_error: {exc}",
            )
        return True

    analysis, parse_error = parse_napoleon_headers(raw_text)

    # If the model returned nothing parsable, treat as a hard parse error.
    if analysis is None:
        parse_error = parse_error or "analysis_none"

    # Evaluate for missing fields before applying server defaults.
    missing_fields = _missing_required_fields(analysis)

    rethink_meta = (analysis or {}).get("RETHINK_HORIZONS") or {}
    updates_raw = rethink_meta.get("UPDATES") or []
    complete_updates = [u for u in updates_raw if _is_complete_update(u)]
    has_updates = bool(complete_updates)
    status_is_yes = (rethink_meta.get("STATUS") or "").lower() == "yes"

    # If STATUS is yes but no actionable updates, request repair.
    if status_is_yes and not has_updates:
        missing_fields.append("RETHINK_HORIZONS.UPDATES_MISSING")

    # If CHANGED_HORIZONS lists horizons without updates, flag mismatch.
    advisory_changed = set(
        hz.strip().upper() for hz in (rethink_meta.get("CHANGED_HORIZONS") or []) if hz
    )
    updated_horizons = set((u.get("HORIZON") or "").upper() for u in complete_updates)
    missing_update_details = advisory_changed - updated_horizons
    if status_is_yes and missing_update_details:
        missing_fields.append(
            "RETHINK_HORIZONS.UPDATE_MISMATCH:" + ",".join(sorted(missing_update_details))
        )

    # Safety valve: if the only issues are horizon-format problems, drop the rethink
    # to avoid repair loops (we'll just skip horizon updates this turn).
    horizon_only_errors = all(
        field.startswith("RETHINK_HORIZONS") for field in missing_fields
    )
    if status_is_yes and missing_fields and horizon_only_errors:
        analysis["RETHINK_HORIZONS"] = {
            "STATUS": "no",
            "SUMMARY": "",
            "REASON": "",
            "CHANGED_HORIZONS": [],
            "UPDATES": [],
        }
        missing_fields = []
        parse_error = None
        status_is_yes = False

    # Fill missing multi-horizon plan from canonical thread plans if absent/empty.
    if analysis is not None:
        analysis = _merge_with_canonical_plans(analysis, thread_row)
        # Override progress with server-derived turn counters
        analysis = _apply_progress_overrides(analysis, current_turn_index)

    # Basic validation for missing required fields (after filling defaults)
    missing_fields = _missing_required_fields(analysis)

    if parse_error is not None or analysis is None or missing_fields:
        # Record failure immediately; no repair retries.
        record_napoleon_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=prompt_log,
            raw_text=raw_text,
            raw_hash=raw_hash,
            error_message=f"Parse/validation error (missing={missing_fields}, parse_error={parse_error})",
        )
        print(
            f"[Napoleon] Parse/validation error for fan message {fan_message_id}: "
            f"missing={missing_fields}, parse_error={parse_error}"
        )
        return True

    content_actions, content_actions_error = parse_content_actions(raw_text)

    rethink = analysis.get("RETHINK_HORIZONS") or {}
    rethink_status = (
        rethink.get("STATUS")
        or rethink.get("status")
        or ""
    ).lower()
    reason_text = (
        rethink.get("SUMMARY")
        or rethink.get("REASON")
        or rethink.get("summary")
        or rethink.get("reason")
        or ""
    )
    updates_list = rethink.get("UPDATES") or []
    complete_updates = [u for u in updates_list if _is_complete_update(u)]
    updates_by_hz = {
        (item.get("HORIZON") or "").upper(): item
        for item in complete_updates
        if item.get("HORIZON")
    }

    # Safety net: if any horizon is no longer "ongoing", force a rethink even if the model forgot to say yes.
    non_ongoing_horizons: list[tuple[str, str]] = []
    multi_plan = analysis.get("MULTI_HORIZON_PLAN") or {}
    for hz in HORIZONS:
        state = (multi_plan.get(hz, {}).get("STATE") or "").lower()
        if state and state != "ongoing":
            non_ongoing_horizons.append((hz, state))

    if non_ongoing_horizons and rethink_status != "yes":
        rethink_status = "yes"
        if not reason_text:
            # Compact reason to avoid bloating the output payloads.
            reason_text = "Auto-rethink: horizons non-ongoing -> " + ", ".join(
                f"{hz}:{st}" for hz, st in non_ongoing_horizons
            )
        rethink["STATUS"] = rethink_status
        rethink["REASON"] = reason_text
        rethink.setdefault("CHANGED_HORIZONS", [])
        if not rethink.get("CHANGED_HORIZONS"):
            rethink["CHANGED_HORIZONS"] = [hz for hz, _ in non_ongoing_horizons]
        analysis["RETHINK_HORIZONS"] = rethink

    def _plan_repr(val):
        if isinstance(val, dict):
            trimmed = {
                k: v for k, v in val.items() if k.upper() != "PROGRESS"
            }
            try:
                return json.dumps(trimmed, ensure_ascii=False, sort_keys=True)
            except Exception:
                return str(trimmed)
        if isinstance(val, list):
            try:
                return json.dumps(val, ensure_ascii=False, sort_keys=True)
            except Exception:
                return str(val)
        return str(val or "")

    if rethink_status.startswith("yes"):
        multi_plan = analysis["MULTI_HORIZON_PLAN"]
        target_horizons = set(updates_by_hz.keys())

        for hz in HORIZONS:
            if target_horizons and hz not in target_horizons:
                continue

            hz_data = multi_plan.get(hz, _blank_horizon_plan("ongoing"))
            update_meta = updates_by_hz.get(hz, {})
            status_raw = (
                update_meta.get("END_STATE")
                or hz_data.get("END_STATE")
                or hz_data.get("STATE")
                or "ongoing"
            )
            plan_status_canonical = _normalize_state(status_raw) or "ongoing"
            col = f"{hz.lower()}_plan"

            old_plan = (
                SB.table("threads")
                .select(col)
                .eq("id", thread_id)
                .single()
                .execute()
                .data.get(col)
                or ""
            )
            old_plan_text = _extract_plan_text(old_plan)
            new_plan_text = hz_data.get("PLAN") or ""
            if not _should_use_model_value(new_plan_text):
                new_plan_text = old_plan_text

            changed = (new_plan_text != old_plan_text) or (
                plan_status_canonical and plan_status_canonical != "ongoing"
            )
            if changed:
                change_reason = (
                    update_meta.get("END_EVIDENCE")
                    or update_meta.get("HISTORIAN_NOTE")
                    or reason_text
                    or "Plan updated"
                )
                plan_status = plan_status_canonical or "ongoing"
                normalized_status = (
                    "completed" if plan_status == "achieved" else plan_status
                )

                if _already_archived(
                    thread_id,
                    hz.lower(),
                    normalized_status,
                    old_plan_text,
                    change_reason,
                ):
                    continue

                send(
                    "plans.archive",
                    {
                        "fan_message_id": fan_message_id,
                        "thread_id": thread_id,
                        "turn_index": current_turn_index,
                        "horizon": hz.lower(),
                        "previous_plan": old_plan_text,
                        "plan_status": normalized_status,
                        "reason_for_change": change_reason,
                    },
                )

                SB.table("threads").update({col: new_plan_text}).eq(
                    "id", thread_id
                ).execute()
                analysis["MULTI_HORIZON_PLAN"][hz]["STATE"] = "ongoing"
                analysis["MULTI_HORIZON_PLAN"][hz]["END_STATE"] = ""
                analysis["MULTI_HORIZON_PLAN"][hz]["PLAN"] = new_plan_text

    upsert_napoleon_details(
        fan_message_id=fan_message_id,
        thread_id=thread_id,
        prompt=prompt_log,
        raw_text=raw_text,
        raw_hash=raw_hash,
        analysis=analysis,
        content_actions=content_actions,
        content_actions_error=content_actions_error,
        creator_message_id=None,
    )

    if run_id and not is_run_active(str(run_id), client=SB):
        upsert_step(
            run_id=str(run_id),
            step="napoleon",
            attempt=attempt,
            status="canceled",
            client=SB,
            message_id=int(fan_message_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error="run_canceled_post_compute",
        )
        return True

    def _recent_fan_messages(limit: int = 20) -> list[str]:
        rows = (
            SB.table("messages")
            .select("id,message_text,turn_index")
            .eq("thread_id", thread_id)
            .eq("sender", "fan")
            .order("turn_index", desc=True)
            .limit(limit)
            .execute()
            .data
            or []
        )
        return [row.get("message_text") or "" for row in reversed(rows)]

    tactical_plan = analysis.get("TACTICAL_PLAN_3TURNS") or {}
    if isinstance(tactical_plan, dict):
        turn1_directive = tactical_plan.get("TURN1_DIRECTIVE") or ""
    else:
        turn1_directive = str(tactical_plan or "")

    writer_payload = {
        "fan_message_id": fan_message_id,
        "thread_id": thread_id,
        # Minimal context for the message composer (Napoleon Writer).
        "creator_identity_card": thread_row.get("creator_identity_card") or "",
        "fan_psychic_card": thread_row.get("fan_psychic_card") or {},
        "thread_history": raw_turns,
        "latest_fan_message": msg.get("message_text") or "",
        "turn_directive": turn1_directive,
        "content_actions": content_actions or {},
    }
    if run_id:
        writer_payload["run_id"] = str(run_id)
    send(WRITER_QUEUE, writer_payload)
    if run_id:
        upsert_step(
            run_id=str(run_id),
            step="napoleon",
            attempt=attempt,
            status="ok",
            client=SB,
            message_id=int(fan_message_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
        )
    return True


if __name__ == "__main__":
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1); continue
        row_id = job["row_id"]
        try:
            payload = job["payload"]
            if process_job(payload):
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("Napoleon error:", exc)
            traceback.print_exc()
            # Do not ack on unhandled errors; let the job retry.
            time.sleep(2)
