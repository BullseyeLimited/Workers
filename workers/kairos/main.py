"""Kairos worker – builds prompts and records analytical outputs."""

from __future__ import annotations

import hashlib
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from supabase import create_client, ClientOptions

from workers.lib.json_utils import safe_parse_model_json
from workers.lib.prompt_builder import build_prompt, live_turn_window
from workers.lib.simple_queue import receive, ack, send

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


def runpod_call(prompt: str) -> str:
    """
    Call the RunPod vLLM OpenAI-compatible server and return text completion.
    """
    base = os.getenv("RUNPOD_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{base}/v1/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
    }
    body = {
        "model": os.getenv("RUNPOD_MODEL_NAME", "qwq-32b-ablit"),
        "prompt": prompt,
        "max_tokens": 16384,
        "temperature": 0.3,
    }

    resp = requests.post(url, headers=headers, json=body, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    choice = data["choices"][0]
    raw_text = (
        choice.get("text")
        or (choice.get("message") or {}).get("content")
        or ""
    )
    return raw_text


def record_kairos_failure(
    message_id: int,
    thread_id: int,
    prompt: str,
    raw_text: str,
    error_message: str,
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
        .select("id,thread_id,sender")
        .eq("id", fan_msg_id)
        .single()
        .execute()
        .data
    )
    if not message_row:
        raise ValueError(f"Message {fan_msg_id} missing")

    thread_id = message_row["thread_id"]
    raw_turns = live_turn_window(thread_id, client=SB)
    prompt = build_prompt("kairos", thread_id, raw_turns, client=SB)

    try:
        raw_text = runpod_call(prompt)
    except Exception as exc:  # noqa: BLE001
        record_kairos_failure(
            message_id=fan_msg_id,
            thread_id=thread_id,
            prompt=prompt,
            raw_text="",
            error_message=f"RunPod error: {exc}",
        )
        print(f"[Kairos] RunPod error for message {fan_msg_id}: {exc}")
        return True

    analysis, parse_error = safe_parse_model_json(raw_text)

    if parse_error is not None or analysis is None:
        record_kairos_failure(
            message_id=fan_msg_id,
            thread_id=thread_id,
            prompt=prompt,
            raw_text=raw_text,
            error_message=f"JSON parse error: {parse_error}",
        )
        print(f"[Kairos] JSON parse error for message {fan_msg_id}: {parse_error}")
        return True

    upsert_kairos_details(
        message_id=fan_msg_id,
        thread_id=thread_id,
        prompt=prompt,
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
