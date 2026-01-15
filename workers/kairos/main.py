"""Kairos worker – builds prompts and records analytical outputs."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import requests
from openai import OpenAI
from supabase import create_client, ClientOptions

from workers.lib.prompt_builder import (
    build_prompt,
    build_prompt_sections,
    live_turn_window,
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
- ALIGNMENT_STATUS: object with keys:
    - SHORT_TERM_PLAN: string (accept any text; do not rewrite)
    - LONG_TERM_PLANS: object with keys LIFETIME_PLAN, YEAR_PLAN, SEASON_PLAN, CHAPTER_PLAN, EPISODE_PLAN. Each value can be any text; do not rewrite.
- CONVERSATION_CRITICALITY: string (accept any text; do not rewrite)
- TACTICAL_SIGNALS: string
- PSYCHOLOGICAL_LEVERS: string
- RISKS: string
- TURN_MICRO_NOTE: object with key SUMMARY (string).
Output ONLY valid JSON matching this shape. Do not include markdown or prose.
Do not paraphrase, summarize, or inject new content; preserve the original wording in each field.
"""

HEADER_MAP = {
    "STRATEGIC_NARRATIVE": "STRATEGIC_NARRATIVE",
    "STRATEGIC NARRATIVE": "STRATEGIC_NARRATIVE",
    "ALIGNMENT_STATUS": "ALIGNMENT_STATUS",
    "ALIGNMENT STATUS": "ALIGNMENT_STATUS",
    "CONVERSATION_CRITICALITY": "CONVERSATION_CRITICALITY",
    "CONVERSATION CRITICALITY": "CONVERSATION_CRITICALITY",
    "TACTICAL_SIGNALS": "TACTICAL_SIGNALS",
    "TACTICAL SIGNALS": "TACTICAL_SIGNALS",
    "PSYCHOLOGICAL_LEVERS": "PSYCHOLOGICAL_LEVERS",
    "PSYCHOLOGICAL LEVERS": "PSYCHOLOGICAL_LEVERS",
    "RISKS": "RISKS",
    "TURN_MICRO_NOTE": "TURN_MICRO_NOTE",
    "TURN MICRO NOTE": "TURN_MICRO_NOTE",
}

HEADER_PATTERN = re.compile(
    r"^[\s>*-]*\**\s*(?P<header>"
    r"STRATEGIC[_ ]NARRATIVE|ALIGNMENT[_ ]STATUS|CONVERSATION[_ ]CRITICALITY|"
    r"TACTICAL[_ ]SIGNALS|PSYCHOLOGICAL[_ ]LEVERS|RISKS|TURN[_ ]MICRO[_ ]NOTE)"
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


def runpod_call(system_prompt: str, user_message: str) -> tuple[str, dict]:
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

    return raw_text, payload


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
                        "Alignment/status fields are free text; do not enforce enums. "
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

    align = analysis.get("ALIGNMENT_STATUS") or {}
    check_non_empty(align, "SHORT_TERM_PLAN", "SHORT_TERM_PLAN")
    # Long-term plans are allowed to be empty; only require the object to exist.

    check_non_empty(analysis, "CONVERSATION_CRITICALITY", "CONVERSATION_CRITICALITY")
    check_non_empty(analysis, "TACTICAL_SIGNALS", "TACTICAL_SIGNALS")
    check_non_empty(analysis, "PSYCHOLOGICAL_LEVERS", "PSYCHOLOGICAL_LEVERS")
    check_non_empty(analysis, "RISKS", "RISKS")

    micro = analysis.get("TURN_MICRO_NOTE") or {}
    check_non_empty(micro, "SUMMARY", "TURN_MICRO_NOTE.SUMMARY")

    return missing


def run_repair_call(
    original_output: str,
    missing_fields: list[str],
    original_system_prompt: str,
    original_user_prompt: str,
) -> tuple[str, dict]:
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
        "ALIGNMENT_STATUS": base.get("ALIGNMENT_STATUS")
        or {
            "SHORT_TERM_PLAN": "",
            "LONG_TERM_PLANS": {
                "LIFETIME_PLAN": "",
                "YEAR_PLAN": "",
                "SEASON_PLAN": "",
                "CHAPTER_PLAN": "",
                "EPISODE_PLAN": "",
            },
        },
        "CONVERSATION_CRITICALITY": base.get("CONVERSATION_CRITICALITY") or "",
        "TACTICAL_SIGNALS": base.get("TACTICAL_SIGNALS") or "",
        "PSYCHOLOGICAL_LEVERS": base.get("PSYCHOLOGICAL_LEVERS") or "",
        "RISKS": base.get("RISKS") or "",
        "TURN_MICRO_NOTE": base.get("TURN_MICRO_NOTE") or {"SUMMARY": ""},
    }

    if patch.get("STRATEGIC_NARRATIVE") and str(patch["STRATEGIC_NARRATIVE"]).strip():
        merged["STRATEGIC_NARRATIVE"] = patch["STRATEGIC_NARRATIVE"]

    if "ALIGNMENT_STATUS" in patch and isinstance(patch["ALIGNMENT_STATUS"], dict):
        align = merged["ALIGNMENT_STATUS"]
        p_align = patch["ALIGNMENT_STATUS"]
        if p_align.get("SHORT_TERM_PLAN") and str(p_align["SHORT_TERM_PLAN"]).strip():
            align["SHORT_TERM_PLAN"] = p_align["SHORT_TERM_PLAN"]
        lt_base = align.get("LONG_TERM_PLANS") or {}
        lt_patch = p_align.get("LONG_TERM_PLANS") or {}
        if isinstance(lt_patch, dict):
            for key, val in lt_patch.items():
                if val and str(val).strip():
                    lt_base[key] = val
            align["LONG_TERM_PLANS"] = lt_base
        merged["ALIGNMENT_STATUS"] = align

    for key in ("CONVERSATION_CRITICALITY", "TACTICAL_SIGNALS", "PSYCHOLOGICAL_LEVERS", "RISKS"):
        val = patch.get(key)
        if val and str(val).strip():
            merged[key] = val

    if patch.get("TURN_MICRO_NOTE") and isinstance(patch["TURN_MICRO_NOTE"], dict):
        summary = patch["TURN_MICRO_NOTE"].get("SUMMARY")
        if summary and str(summary).strip():
            merged["TURN_MICRO_NOTE"]["SUMMARY"] = summary

    return merged


def _parse_alignment_block(block: str) -> dict:
    """
    Try to extract alignment fields from a freeform block.
    Falls back to putting the whole block in SHORT_TERM_PLAN if nothing is found.
    """
    st_plan = ""
    lt_plans = {
        "LIFETIME_PLAN": "",
        "YEAR_PLAN": "",
        "SEASON_PLAN": "",
        "CHAPTER_PLAN": "",
        "EPISODE_PLAN": "",
    }

    for line in block.splitlines():
        lower = line.lower()
        if "short" in lower and "plan" in lower and not st_plan:
            if ":" in line:
                st_plan = line.split(":", 1)[1].strip()
            else:
                st_plan = line.strip()
        for key, label in (
            ("LIFETIME_PLAN", "lifetime"),
            ("YEAR_PLAN", "year"),
            ("SEASON_PLAN", "season"),
            ("CHAPTER_PLAN", "chapter"),
            ("EPISODE_PLAN", "episode"),
        ):
            if label in lower and "plan" in lower:
                if ":" in line:
                    lt_plans[key] = line.split(":", 1)[1].strip()
                else:
                    lt_plans[key] = line.strip()

    if not st_plan:
        st_plan = block.strip()
    # If no long-term items were found, keep them empty (or whole block if you prefer).
    return {
        "SHORT_TERM_PLAN": st_plan,
        "LONG_TERM_PLANS": lt_plans,
    }


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
    for i, (idx, key) in enumerate(matches):
        start = idx + 1
        end = matches[i + 1][0] if i + 1 < len(matches) else len(lines)
        segments = lines[start:end]
        sections[key] = "\n".join(segments).strip()

    # Build analysis dict with defaults
    analysis: dict[str, Any] = {
        "STRATEGIC_NARRATIVE": sections.get("STRATEGIC_NARRATIVE", ""),
        "ALIGNMENT_STATUS": _parse_alignment_block(
            sections.get("ALIGNMENT_STATUS", "")
        ),
        "CONVERSATION_CRITICALITY": sections.get(
            "CONVERSATION_CRITICALITY", ""
        ),
        "TACTICAL_SIGNALS": sections.get("TACTICAL_SIGNALS", ""),
        "PSYCHOLOGICAL_LEVERS": sections.get("PSYCHOLOGICAL_LEVERS", ""),
        "RISKS": sections.get("RISKS", ""),
        "TURN_MICRO_NOTE": {"SUMMARY": ""},
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

    alignment = fragments.get("ALIGNMENT_STATUS")
    if not isinstance(alignment, dict):
        raise ValueError("ALIGNMENT_STATUS must be an object")

    st_plan = str(alignment.get("SHORT_TERM_PLAN") or "").strip()
    if not st_plan:
        raise ValueError("ALIGNMENT_STATUS.SHORT_TERM_PLAN is empty")

    long_plans = alignment.get("LONG_TERM_PLANS")
    if not isinstance(long_plans, dict):
        raise ValueError("LONG_TERM_PLANS must be an object")

    lt = {}
    for k in (
        "LIFETIME_PLAN",
        "YEAR_PLAN",
        "SEASON_PLAN",
        "CHAPTER_PLAN",
        "EPISODE_PLAN",
    ):
        val = str(long_plans.get(k) or "").strip()
        lt[k] = val

    conv_crit = str(fragments.get("CONVERSATION_CRITICALITY") or "").strip()
    if not conv_crit:
        raise ValueError("CONVERSATION_CRITICALITY is empty")

    tactical = _require_text("TACTICAL_SIGNALS")
    levers = _require_text("PSYCHOLOGICAL_LEVERS")
    risks = _require_text("RISKS")

    micro = fragments.get("TURN_MICRO_NOTE")
    if not isinstance(micro, dict):
        raise ValueError("TURN_MICRO_NOTE must be an object")
    micro_summary = str(micro.get("SUMMARY") or "").strip()
    if not micro_summary:
        raise ValueError("TURN_MICRO_NOTE.SUMMARY is empty")

    return {
        "STRATEGIC_NARRATIVE": strategic_narrative,
        "ALIGNMENT_STATUS": {
            "SHORT_TERM_PLAN": st_plan,
            "LONG_TERM_PLANS": lt,
        },
        "CONVERSATION_CRITICALITY": conv_crit,
        "TACTICAL_SIGNALS": tactical,
        "PSYCHOLOGICAL_LEVERS": levers,
        "RISKS": risks,
        "TURN_MICRO_NOTE": {"SUMMARY": micro_summary},
    }


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
        }
        row.update(
            {
                "strategic_narrative": partial.get("STRATEGIC_NARRATIVE"),
                "alignment_status": partial.get("ALIGNMENT_STATUS"),
                "conversation_criticality": partial.get("CONVERSATION_CRITICALITY"),
                "tactical_signals": partial.get("TACTICAL_SIGNALS"),
                "psychological_levers": partial.get("PSYCHOLOGICAL_LEVERS"),
                "risks": partial.get("RISKS"),
                "kairos_summary": (partial.get("TURN_MICRO_NOTE") or {}).get("SUMMARY"),
                "extras": _merge_extras(existing_extras, extras_patch),
            }
        )

    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
        .execute()
    )


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
        "alignment_status": analysis["ALIGNMENT_STATUS"],
        "conversation_criticality": analysis["CONVERSATION_CRITICALITY"],
        "tactical_signals": analysis["TACTICAL_SIGNALS"],
        "psychological_levers": analysis["PSYCHOLOGICAL_LEVERS"],
        "risks": analysis["RISKS"],
        "kairos_summary": analysis["TURN_MICRO_NOTE"]["SUMMARY"],
        "turn_micro_note": analysis["TURN_MICRO_NOTE"],
        "extras": _merge_extras(existing_extras, extras_patch),
    }
    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
        .execute()
    )


def enqueue_join_job(message_id: int) -> None:
    send(JOIN_QUEUE, {"message_id": message_id})


def process_job(payload: Dict[str, Any], row_id: int) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = payload["message_id"]
    attempt = int(payload.get("kairos_retry", 0))
    is_repair = bool(payload.get("repair_mode"))
    previous_raw_text = payload.get("orig_raw_text") or ""
    root_raw_text = payload.get("root_raw_text") or previous_raw_text or ""
    previous_missing = payload.get("missing_fields") or []
    orig_system_prompt = payload.get("orig_system_prompt") or ""
    orig_user_prompt = payload.get("orig_user_prompt") or ""
    root_analysis = payload.get("root_analysis") or {}
    message_row = (
        SB.table("messages")
        .select("id,thread_id,sender,turn_index,message_text,media_analysis_text,media_payload")
        .eq("id", fan_msg_id)
        .limit(1)
        .execute()
        .data
    )
    if not message_row:
        raise ValueError(f"Message {fan_msg_id} missing")
    message_row = message_row[0]

    thread_id = message_row["thread_id"]
    turn_index = message_row.get("turn_index")
    latest_fan_text = _format_fan_turn(message_row)
    kairos_mode = (payload.get("kairos_mode") or "full").lower()
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
        )
    except FileNotFoundError:
        system_prompt, user_prompt = build_prompt_sections(
            "kairos",
            thread_id,
            raw_turns,
            latest_fan_text=latest_fan_text,
            client=SB,
        )

    try:
        if is_repair:
            raw_text, _request_payload = run_repair_call(
                root_raw_text or previous_raw_text or "",
                previous_missing,
                orig_system_prompt or system_prompt,
                orig_user_prompt or user_prompt,
            )
        else:
            raw_text, _request_payload = runpod_call(system_prompt, user_prompt)
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
        enqueue_join_job(fan_msg_id)
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
        enqueue_join_job(fan_msg_id)
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

    enqueue_join_job(fan_msg_id)
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
