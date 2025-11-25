import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone

import requests
from supabase import create_client, ClientOptions

from workers.lib.prompt_builder import build_prompt_sections, live_turn_window
from workers.lib.simple_queue import receive, ack, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

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
HORIZONS = ["EPISODE", "CHAPTER", "SEASON", "YEAR", "LIFETIME"]
HEADER_PATTERN = re.compile(
    r"^[\s>*#-]*\**\s*(?:SECTION\s*\d+\s*[:\-–—]?\s*)?(?P<header>"
    r"TACTICAL[\s_-]?PLAN[\s_-]?3[\s_-]?TURNS|"
    r"MULTI[\s_-]?HORIZON[\s_-]?PLAN|"
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
    creator_message_id: int,
) -> None:
    """
    Persist Napoleon planning fields for the fan turn.
    The creator reply message already got inserted separately.
    """
    rethink = analysis.get("RETHINK_HORIZONS") or {}

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
            "creator_reply_message_id": creator_message_id,
            "napoleon_save_note": "Merged with Kairos",
            "kairos_check": "found" if existing_row.get("strategic_narrative") else "missing",
        }
    )

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
        "napoleon_final_message": analysis["FINAL_MESSAGE"],
        "napoleon_voice_engine": analysis["VOICE_ENGINEERING_LOGIC"],
        "extras": merged_extras,
        "historian_entry": analysis.get("HISTORIAN_ENTRY", {}),
    }

    (
        SB.table("message_ai_details")
        .update(update_fields)
        .eq("message_id", fan_message_id)
        .execute()
    )

    SB.table("messages").update({"napoleon_output": analysis}).eq(
        "id", creator_message_id
    ).execute()


def insert_creator_reply(thread_id: int, final_text: str) -> int:
    # fetch next turn index
    thr = (
        SB.table("threads")
        .select("turn_count")
        .eq("id", thread_id)
        .single()
        .execute()
        .data
    )
    next_turn = (thr["turn_count"] or 0) + 1
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
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 4096,
        "temperature": 0.6,
        "top_p": 0.95,
        "repetition_penalty": 1.1,
        "presence_penalty": 0.1,
        "frequency_penalty": 0.1,
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
    plans = {
        hz: {"PLAN": "", "PROGRESS": "", "STATE": "", "NOTES": []}
        for hz in HORIZONS
    }
    if not section_text:
        raise ValueError("no_multi_horizon_section")

    found_any = False
    for line in section_text.splitlines():
        match = HORIZON_LINE_PATTERN.match(line)
        if not match:
            continue
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

    if not found_any:
        raise ValueError("no_horizon_lines")

    return plans


def _parse_rethink_section(section_text: str) -> dict:
    text = " ".join((section_text or "").split())
    if not text:
        return {"STATUS": "", "REASON": ""}

    s_match = re.search(r"STATUS\s*:\s*(yes|no)", text, re.IGNORECASE)
    r_match = re.search(r"REASON\s*:\s*(.*)", text, re.IGNORECASE)

    status = s_match.group(1).lower() if s_match else "no"
    reason = r_match.group(1).strip() if r_match else ""
    return {"STATUS": status, "REASON": reason}


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
        end = matches[i + 1][0] if i + 1 < len(matches) else len(lines)
        sections[header] = "\n".join(lines[start:end]).strip()

    required = [
        "TACTICAL_PLAN_3TURNS",
        "MULTI_HORIZON_PLAN",
        "RETHINK_HORIZONS",
        "VOICE_ENGINEERING_LOGIC",
        "FINAL_MESSAGE",
    ]
    missing = [hdr for hdr in required if hdr not in sections]
    if missing:
        return None, f"missing_sections: {', '.join(missing)}"

    try:
        tactical = _parse_tactical_section(sections["TACTICAL_PLAN_3TURNS"])
        multi_plan = _parse_multi_horizon_section(sections["MULTI_HORIZON_PLAN"])
        rethink = _parse_rethink_section(sections["RETHINK_HORIZONS"])
        voice_logic = _parse_voice_section(sections["VOICE_ENGINEERING_LOGIC"])
        final_message = sections["FINAL_MESSAGE"].strip()
        if not final_message:
            raise ValueError("empty_final_message")
    except ValueError as exc:  # noqa: BLE001
        return None, str(exc)

    analysis = {
        "TACTICAL_PLAN_3TURNS": tactical,
        "MULTI_HORIZON_PLAN": multi_plan,
        "RETHINK_HORIZONS": rethink,
        "VOICE_ENGINEERING_LOGIC": voice_logic,
        "FINAL_MESSAGE": final_message,
    }
    return analysis, None


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
        end = matches[i + 1][0] if i + 1 < len(lines) else len(lines)
        sections[header] = "\n".join(lines[start:end]).strip()

    result: dict = {}

    if "TACTICAL_PLAN_3TURNS" in sections:
        try:
            result["TACTICAL_PLAN_3TURNS"] = _parse_tactical_section(
                sections["TACTICAL_PLAN_3TURNS"]
            )
        except Exception:
            pass

    if "MULTI_HORIZON_PLAN" in sections:
        try:
            result["MULTI_HORIZON_PLAN"] = _parse_multi_horizon_section(
                sections["MULTI_HORIZON_PLAN"]
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


def _missing_required_fields(analysis: dict | None) -> list[str]:
    """
    Check for empty critical fields that would make the row or reply unusable.
    This is a lightweight guardrail to avoid saving obviously incomplete outputs.
    """
    if analysis is None:
        return ["analysis_none"]

    missing: list[str] = []

    tactical = analysis.get("TACTICAL_PLAN_3TURNS") or {}
    for key in ("TURN1_DIRECTIVE", "TURN2A_FAN_PATH", "TURN2B_FAN_PATH", "TURN3A_DIRECTIVE", "TURN3B_DIRECTIVE"):
        if not (tactical.get(key) or "").strip():
            missing.append(key)

    multi = analysis.get("MULTI_HORIZON_PLAN") or {}
    for hz in HORIZONS:
        plan_text = (multi.get(hz, {}) or {}).get("PLAN") or ""
        if not str(plan_text).strip():
            missing.append(f"{hz}_PLAN")

    final_message = (analysis.get("FINAL_MESSAGE") or "").strip()
    if not final_message:
        missing.append("FINAL_MESSAGE")

    voice_logic = analysis.get("VOICE_ENGINEERING_LOGIC") or {}
    if not (voice_logic.get("INTENT") or "").strip():
        missing.append("VOICE_INTENT")
    if not (voice_logic.get("DRAFTING") or "").strip():
        missing.append("VOICE_DRAFTING")

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
    attempt = int(payload.get("napoleon_retry", 0))
    is_repair = bool(payload.get("repair_mode"))
    previous_raw_text = payload.get("orig_raw_text") or ""
    root_raw_text = payload.get("root_raw_text") or previous_raw_text or ""
    previous_missing = payload.get("missing_fields") or []
    orig_system_prompt = payload.get("orig_system_prompt") or ""
    orig_user_prompt = payload.get("orig_user_prompt") or ""
    root_analysis = payload.get("root_analysis") or {}

    msg = (
        SB.table("messages")
        .select("thread_id,sender,message_text,turn_index")
        .eq("id", fan_message_id)
        .single()
        .execute()
        .data
    )
    if not msg:
        raise ValueError(f"Message {fan_message_id} not found")

    thread_id = msg["thread_id"]
    latest_fan_text = msg.get("message_text") or ""

    raw_turns = live_turn_window(thread_id, client=SB)
    system_prompt, user_prompt = build_prompt_sections(
        "napoleon",
        thread_id,
        raw_turns,
        latest_fan_text=latest_fan_text,
        client=SB,
    )
    prompt_log = json.dumps(
        {"system": system_prompt, "user": user_prompt},
        ensure_ascii=False,
    )
    raw_hash = hashlib.sha256(prompt_log.encode("utf-8")).hexdigest()

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
        record_napoleon_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=prompt_log,
            raw_text="",
            raw_hash=raw_hash,
            error_message=f"RunPod error: {exc}",
        )
        print(f"[Napoleon] RunPod error for fan message {fan_message_id}: {exc}")
        return True

    analysis, parse_error = parse_napoleon_headers(raw_text)

    # Basic validation for missing required fields
    missing_fields = _missing_required_fields(analysis)

    # Establish root analysis from the first attempt (best-effort partial)
    if not root_analysis:
        root_analysis = parse_napoleon_partial(raw_text)

    # If parsing failed or critical fields are missing, try repair up to 2 attempts.
    if (parse_error is not None or analysis is None or missing_fields) and attempt < 2:
        retry_payload = {
            "message_id": fan_message_id,
            "napoleon_retry": attempt + 1,
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
            f"[Napoleon] Enqueued repair attempt {attempt + 1} for fan message {fan_message_id} "
            f"(parse_error={parse_error}, missing={missing_fields})"
        )
        return True

    if is_repair:
        # Parse partial repair output and merge into root analysis
        patch = parse_napoleon_partial(raw_text)
        merged = merge_napoleon_analysis(root_analysis, patch)
        analysis = merged
        parse_error = None
        missing_fields = _missing_required_fields(analysis)

    if parse_error is not None or analysis is None or missing_fields:
        # Final failure after retries
        record_napoleon_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=prompt_log,
            raw_text=raw_text,
            raw_hash=raw_hash,
            error_message=f"Parse/validation error after retries: {parse_error or missing_fields}",
        )
        print(
            f"[Napoleon] Parse/validation error after retries for fan message {fan_message_id}: "
            f"{parse_error or missing_fields}"
        )
        return True

    rethink = analysis.get("RETHINK_HORIZONS") or {}
    rethink_status = (
        rethink.get("STATUS")
        or rethink.get("status")
        or ""
    ).lower()
    reason_text = rethink.get("REASON") or rethink.get("reason") or ""

    if rethink_status.startswith("yes"):
        multi_plan = analysis["MULTI_HORIZON_PLAN"]
        for hz in HORIZONS:
            hz_data = multi_plan.get(hz, {})
            new_plan = hz_data.get("PLAN") or ""
            status = (hz_data.get("STATE") or "ongoing").lower()
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

            changed = (new_plan != old_plan) or (status != "ongoing")
            if changed:
                send(
                    "plans.archive",
                    {
                        "fan_message_id": fan_message_id,
                        "thread_id": thread_id,
                        "horizon": hz.lower(),
                        "previous_plan": old_plan,
                        "plan_status": status,
                        "reason_for_change": reason_text,
                    },
                )

                SB.table("threads").update({col: new_plan}).eq(
                    "id", thread_id
                ).execute()

    creator_msg_id = insert_creator_reply(thread_id, analysis["FINAL_MESSAGE"])

    upsert_napoleon_details(
        fan_message_id=fan_message_id,
        thread_id=thread_id,
        prompt=prompt_log,
        raw_text=raw_text,
        raw_hash=raw_hash,
        analysis=analysis,
        creator_message_id=creator_msg_id,
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
        or {hz: {"PLAN": "", "PROGRESS": "", "STATE": "", "NOTES": []} for hz in HORIZONS},
        "RETHINK_HORIZONS": base.get("RETHINK_HORIZONS") or {"STATUS": "", "REASON": ""},
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
            target = merged["MULTI_HORIZON_PLAN"].setdefault(
                hz, {"PLAN": "", "PROGRESS": "", "STATE": "", "NOTES": []}
            )
            if not isinstance(hz_data, dict):
                continue
            for field in ("PLAN", "PROGRESS", "STATE", "NOTES"):
                val = hz_data.get(field)
                if field == "NOTES":
                    if val:
                        target["NOTES"] = val
                else:
                    if val and str(val).strip():
                        target[field] = val

    # Merge rethink
    if "RETHINK_HORIZONS" in patch and isinstance(patch["RETHINK_HORIZONS"], dict):
        for k in ("STATUS", "REASON"):
            val = patch["RETHINK_HORIZONS"].get(k)
            if val and str(val).strip():
                merged["RETHINK_HORIZONS"][k] = val

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
