"""Kairos worker – builds prompts and records analytical outputs."""

from __future__ import annotations

import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from supabase import create_client, ClientOptions

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
    return data["choices"][0]["text"]


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
        structured = json.loads(raw_text)
    except Exception as exc:  # noqa: BLE001
        SB.table("message_ai_details").upsert(
            {
                "message_id": fan_msg_id,
                "thread_id": thread_id,
                "sender": "fan",
                "raw_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                "extract_status": "failed",
                "extract_error": str(exc),
            },
            on_conflict="message_id",
        ).execute()
        ack(row_id)
        return False

    analysis = structured

    # ---------- DB write-back for Kairos ----------
    def upsert_kairos_details(msg_id: int, thread_id: int, analysis: dict):
        row = {
            "message_id": msg_id,
            "thread_id": thread_id,
            "sender": "fan",
            "raw_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "extract_status": "ok",
            "extract_error": None,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "strategic_narrative": analysis["STRATEGIC_NARRATIVE"],
            "alignment_status": json.dumps(analysis["ALIGNMENT_STATUS"]),
            "conversation_criticality": int(analysis["CONVERSATION_CRITICALITY"]),
            "tactical_signals": json.dumps(analysis["TACTICAL_SIGNALS"]),
            "psychological_levers": json.dumps(analysis["PSYCHOLOGICAL_LEVERS"]),
            "risks": json.dumps(analysis["RISKS"]),
            "kairos_summary": analysis["TURN_MICRO_NOTE"]["SUMMARY"],
            "extras": json.dumps(analysis.get("EXTRAS", {})),
        }
        SB.table("message_ai_details").upsert(
            row, on_conflict="message_id"
        ).execute()

    # write the row
    upsert_kairos_details(fan_msg_id, thread_id, analysis)
    SB.table("messages").update({"kairos_output": json.dumps(analysis)}).eq(
        "id", fan_msg_id
    ).execute()

    if analysis is not None:
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
