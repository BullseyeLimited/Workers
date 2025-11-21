"""Kairos worker – builds prompts and records analytical outputs."""

from __future__ import annotations

import hashlib
import json
import os
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
NAPOLEON_QUEUE = "napoleon.reply"

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
    - SHORT_TERM_PLAN: one of ["aligned", "partial", "drift", "conflict"]
    - LONG_TERM_PLANS: object with keys LIFETIME_PLAN, YEAR_PLAN, SEASON_PLAN, CHAPTER_PLAN, EPISODE_PLAN. Each value must be one of ["aligned", "partial", "drift", "conflict"].
- CONVERSATION_CRITICALITY: integer 1-5 expressing urgency (5 = highest urgency). If the text is qualitative, map it to an integer 1 (low) through 5 (highest).
- TACTICAL_SIGNALS: string
- PSYCHOLOGICAL_LEVERS: string
- RISKS: string
- TURN_MICRO_NOTE: object with key SUMMARY (string).
Output ONLY valid JSON matching this shape. Do not include markdown or prose.
Do not paraphrase or inject new content; preserve the original wording in each field.
"""


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
        "model": os.getenv("RUNPOD_MODEL_NAME", "qwq-32b-ablit"),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 2048,
        "temperature": 0.0,
        "top_p": 1,
        "stop": ["### END"],
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    choice = data["choices"][0]
    raw_text = (
        choice.get("text")
        or (choice.get("message") or {}).get("content")
        or ""
    )
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
                        "DO NOT paraphrase, summarize, or omit any content. "
                        "Copy every header/value exactly as given into the JSON fields. "
                        "If a field is missing, use an empty string. Output ONLY valid JSON."
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


def _validated_analysis(fragments: dict) -> dict:
    """
    Ensure all required fields exist and have the right shapes before persisting.
    Raises ValueError on missing or invalid data.
    """
    if not isinstance(fragments, dict):
        raise ValueError("analysis is not an object")

    def _require_text(key: str) -> str:
        if key not in fragments:
            raise ValueError(f"missing field: {key}")
        return str(fragments[key] or "").strip()

    strategic_narrative = _require_text("STRATEGIC_NARRATIVE")

    alignment = fragments.get("ALIGNMENT_STATUS")
    if not isinstance(alignment, dict):
        raise ValueError("ALIGNMENT_STATUS must be an object")

    def _normalize_status(value: Any) -> str:
        v = str(value or "").strip().lower()
        allowed = {"aligned", "partial", "drift", "conflict"}
        if v not in allowed:
            raise ValueError(f"invalid alignment status: {v}")
        return v

    st_plan = _normalize_status(alignment.get("SHORT_TERM_PLAN"))
    long_plans = alignment.get("LONG_TERM_PLANS") or {}
    if not isinstance(long_plans, dict):
        raise ValueError("LONG_TERM_PLANS must be an object")
    lt = {k: _normalize_status(long_plans.get(k)) for k in (
        "LIFETIME_PLAN",
        "YEAR_PLAN",
        "SEASON_PLAN",
        "CHAPTER_PLAN",
        "EPISODE_PLAN",
    )}

    try:
        conv_crit = int(fragments.get("CONVERSATION_CRITICALITY"))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"invalid CONVERSATION_CRITICALITY: {exc}") from exc

    tactical = _require_text("TACTICAL_SIGNALS")
    levers = _require_text("PSYCHOLOGICAL_LEVERS")
    risks = _require_text("RISKS")

    micro = fragments.get("TURN_MICRO_NOTE")
    if not isinstance(micro, dict):
        raise ValueError("TURN_MICRO_NOTE must be an object")
    micro_summary = str(micro.get("SUMMARY") or "").strip()
    if not micro_summary:
        raise ValueError("TURN_MICRO_NOTE.SUMMARY missing or empty")

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
) -> None:
    """
    Upsert a message_ai_details row that marks this analysis as failed,
    and keep a snippet of the raw model output for debugging.
    """
    row = {
        "message_id": message_id,
        "thread_id": thread_id,
        "sender": "fan",
        "raw_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "extract_status": "failed",
        "extract_error": error_message,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "kairos_prompt_raw": prompt,
        "kairos_output_raw": raw_text,
        "translator_error": translator_error,
        "extras": {
            "kairos_raw_text_preview": (raw_text or "")[:2000],
        },
    }

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
    row = {
        "message_id": message_id,
        "thread_id": thread_id,
        "sender": "fan",
        "raw_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "extract_status": "ok",
        "extract_error": None,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "kairos_prompt_raw": prompt,
        "kairos_output_raw": raw_text,
        "translator_error": None,
        "strategic_narrative": analysis["STRATEGIC_NARRATIVE"],
        "alignment_status": analysis["ALIGNMENT_STATUS"],
        "conversation_criticality": int(analysis["CONVERSATION_CRITICALITY"]),
        "tactical_signals": analysis["TACTICAL_SIGNALS"],
        "psychological_levers": analysis["PSYCHOLOGICAL_LEVERS"],
        "risks": analysis["RISKS"],
        "kairos_summary": analysis["TURN_MICRO_NOTE"]["SUMMARY"],
        "extras": {
            "kairos_raw_json": analysis,
            "kairos_raw_text_preview": (raw_text or "")[:2000],
        },
    }
    (
        SB.table("message_ai_details")
        .upsert(row, on_conflict="message_id")
        .execute()
    )


def enqueue_napoleon_job(message_id: int) -> None:
    send(NAPOLEON_QUEUE, {"message_id": message_id})


def process_job(payload: Dict[str, Any], row_id: int) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = payload["message_id"]
    message_row = (
        SB.table("messages")
        .select("id,thread_id,sender,turn_index,message_text")
        .eq("id", fan_msg_id)
        .single()
        .execute()
        .data
    )
    if not message_row:
        raise ValueError(f"Message {fan_msg_id} missing")

    thread_id = message_row["thread_id"]
    turn_index = message_row.get("turn_index")
    latest_fan_text = message_row.get("message_text") or ""
    raw_turns = live_turn_window(
        thread_id,
        boundary_turn=turn_index,
        client=SB,
    )

    system_prompt, user_prompt = build_prompt_sections(
        "kairos",
        thread_id,
        raw_turns,
        latest_fan_text=latest_fan_text,
        client=SB,
    )

    try:
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
        return True

    translated, translator_error = translate_kairos_output(raw_text)
    if translator_error is not None or translated is None:
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
            error_message="Translator failure",
            translator_error=translator_error,
        )
        print(
            f"[Kairos] Translator failure for message {fan_msg_id}: {translator_error}"
        )
        return True

    try:
        analysis = _validated_analysis(translated)
    except ValueError as exc:  # noqa: BLE001
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
            error_message=f"Validation error: {exc}",
            translator_error=None,
        )
        print(f"[Kairos] Validation error for message {fan_msg_id}: {exc}")
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

    enqueue_napoleon_job(fan_msg_id)
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
