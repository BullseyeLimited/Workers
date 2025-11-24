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
    r"^[\s>*#-]*\**\s*(?P<header>"
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
    r"^[\s>*-]*\s*(?P<label>"
    r"TURN\s*1(?:_CREATOR_MESSAGE)?|"
    r"TURN\s*2A(?:_FAN_PATH)?|"
    r"TURN\s*2B(?:_FAN_PATH)?|"
    r"TURN\s*3A(?:_CREATOR_REPLY)?|"
    r"TURN\s*3B(?:_CREATOR_REPLY)?"
    r")\s*:\s*(?P<value>.+)$",
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
    attached to the *fan* message.
    """
    row = {
        "message_id": fan_message_id,
        "thread_id": thread_id,
        "sender": "fan",
        "extract_status": "failed",
        "extract_error": error_message,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "raw_hash": raw_hash,
        "napoleon_prompt_raw": prompt,
        "napoleon_output_raw": raw_text,
        "extras": {
            "napoleon_raw_text_preview": (raw_text or "")[:2000],
            "napoleon_prompt_preview": (prompt or "")[:2000],
        },
    }

    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
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
    row = {
        "message_id": fan_message_id,
        "thread_id": thread_id,
        "sender": "fan",
        "extract_status": "ok",
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
        "rethink_horizons": analysis["RETHINK_HORIZONS"],
        "napoleon_final_message": analysis["FINAL_MESSAGE"],
        "napoleon_voice_engine": analysis["VOICE_ENGINEERING_LOGIC"],
        "extras": {
            "napoleon_raw_json": analysis,
            "napoleon_raw_text_preview": (raw_text or "")[:2000],
            "napoleon_prompt_preview": (prompt or "")[:2000],
            "creator_reply_message_id": creator_message_id,
        },
        "historian_entry": analysis.get("HISTORIAN_ENTRY", {}),
    }
    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
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
        "max_tokens": 6000,
        "temperature": 0.6,
        "top_p": 0.95,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=600)
    resp.raise_for_status()
    data = resp.json()

    raw_text = ""
    try:
        if "choices" in data and data["choices"]:
            choice = data["choices"][0]
            message = choice.get("message") or {}
            content = message.get("content")
            reasoning = message.get("reasoning") or message.get("reasoning_content")
            if content:
                raw_text = content
            elif reasoning:
                raw_text = reasoning
            else:
                raw_text = choice.get("text") or ""
    except Exception:
        pass

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text, payload


def _normalize_header(header: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "_", header.upper()).strip("_")
    return cleaned


def _strip_quotes(text: str) -> str:
    s = text.strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1].strip()
    return s


def _collapse_ws(text: str) -> str:
    return " ".join((text or "").split())


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
    "TURN1": "TURN1_CREATOR_MESSAGE",
    "TURN1CREATORMESSAGE": "TURN1_CREATOR_MESSAGE",
    "TURN2A": "TURN2A_FAN_PATH",
    "TURN2AFANPATH": "TURN2A_FAN_PATH",
    "TURN2B": "TURN2B_FAN_PATH",
    "TURN2BFANPATH": "TURN2B_FAN_PATH",
    "TURN3A": "TURN3A_CREATOR_REPLY",
    "TURN3ACREATORREPLY": "TURN3A_CREATOR_REPLY",
    "TURN3B": "TURN3B_CREATOR_REPLY",
    "TURN3BCREATORREPLY": "TURN3B_CREATOR_REPLY",
}


def _parse_tactical_section(section_text: str) -> dict:
    result = {
        "TURN1_CREATOR_MESSAGE": "",
        "TURN2A_FAN_PATH": "",
        "TURN2B_FAN_PATH": "",
        "TURN3A_CREATOR_REPLY": "",
        "TURN3B_CREATOR_REPLY": "",
    }
    if not section_text:
        return result

    for line in section_text.splitlines():
        match = TACTICAL_LINE_PATTERN.match(line)
        if not match:
            continue
        label = match.group("label") or ""
        normalized = re.sub(r"[^A-Z0-9]", "", label.upper())
        key = TACTICAL_KEY_MAP.get(normalized)
        if not key:
            continue
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
    text = _collapse_ws(section_text)
    if not text:
        return {"STATUS": "", "REASON": ""}

    match = re.match(r"^(yes|no)\b[:\-–—]?\s*(.*)$", text, re.IGNORECASE)
    if match:
        status = match.group(1).lower()
        reason = match.group(2).strip()
        return {"STATUS": status, "REASON": reason}

    parts = text.split(" ", 1)
    status = parts[0].lower()
    reason = parts[1].strip() if len(parts) > 1 else ""
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


def process_job(payload):
    fan_message_id = payload["message_id"]

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

    kairos_analysis = (
        SB.table("message_ai_details")
        .select(
            "strategic_narrative,"
            "alignment_status,"
            "conversation_criticality,"
            "tactical_signals,"
            "psychological_levers,"
            "risks,"
            "kairos_summary"
        )
        .eq("message_id", fan_message_id)
        .single()
        .execute()
        .data
    )
    if not kairos_analysis:
        raise ValueError(f"Kairos analysis missing for message {fan_message_id}")

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

    if parse_error is not None or analysis is None:
        record_napoleon_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=prompt_log,
            raw_text=raw_text,
            raw_hash=raw_hash,
            error_message=f"Parse error: {parse_error}",
        )
        print(
            f"[Napoleon] Parse error for fan message {fan_message_id}: {parse_error}"
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
            process_job(payload)
        except Exception as exc:  # noqa: BLE001
            print("Napoleon error:", exc)
            traceback.print_exc()
        finally:
            ack(row_id)
