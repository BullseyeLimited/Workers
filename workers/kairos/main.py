"""Kairos worker – builds prompts and records analytical outputs."""

from __future__ import annotations

import json
import os
import time
import traceback
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
    options=ClientOptions(headers={"apikey": SUPABASE_KEY}),
)
QUEUE = "kairos.analyse"
NAPOLEON_QUEUE = "napoleon.reply"


def runpod_call(prompt: str) -> str:
    url = os.getenv("RUNPOD_URL")
    api_key = os.getenv("RUNPOD_API_KEY")
    if not url or not api_key:
        raise RuntimeError("RunPod configuration missing")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"model": "gpt-4o-mini", "prompt": prompt, "max_tokens": 1200}
    response = requests.post(url, headers=headers, json=body, timeout=120)
    response.raise_for_status()
    payload = response.json()
    try:
        return payload["choices"][0]["text"]
    except (KeyError, IndexError) as exc:
        raise ValueError(f"Unexpected RunPod response: {payload}") from exc


def process_job(payload: Dict[str, Any]) -> None:
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

    raw_text = runpod_call(prompt)
    structured = json.loads(raw_text)
    analysis = structured

    # ---------- DB write-back for Kairos ----------
    import hashlib, json, time
    from datetime import datetime, timezone

    def upsert_kairos_details(msg_id: int, thread_id: int, analysis: dict):
        row = {
            "message_id": msg_id,
            "thread_id": thread_id,
            "sender": "fan",
            "raw_hash": hashlib.sha256(
                json.dumps(analysis, sort_keys=True).encode()
            ).hexdigest(),
            "extract_status": "ok",
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "strategic_narrative": analysis["STRATEGIC_NARRATIVE"],
            "alignment_status": json.dumps(analysis["ALIGNMENT_STATUS"]),
            "conversation_criticality": int(analysis["CONVERSATION_CRITICALITY"]),
            "tactical_signals": json.dumps(analysis["TACTICAL_SIGNALS"]),
            "psychological_levers": json.dumps(analysis["PSYCHOLOGICAL_LEVERS"]),
            "risks": json.dumps(analysis["RISKS"]),
            "kairos_summary": analysis["TURN_MICRO_NOTE"]["SUMMARY"],
        }
        SB.table("message_ai_details").upsert(
            row, on_conflict="message_id"
        ).execute()

    # write the row
    upsert_kairos_details(fan_msg_id, thread_id, analysis)
    SB.table("messages").update({"kairos_output": json.dumps(analysis)}).eq(
        "id", fan_msg_id
    ).execute()

    record = _build_ai_detail_payload(
        structured,
        raw_text,
        fan_msg_id,
        thread_id,
        message_row.get("sender", "fan"),
    )
    SB.table("message_ai_details").insert(record).execute()

    send(NAPOLEON_QUEUE, {"message_id": fan_msg_id, "thread_id": thread_id})


def _build_ai_detail_payload(
    data: Dict[str, Any],
    raw_text: str,
    message_id: int,
    thread_id: int,
    sender: str,
) -> Dict[str, Any]:
    required_fields = [
        "STRATEGIC_NARRATIVE",
        "ALIGNMENT_STATUS",
        "CONVERSATION_CRITICALITY",
        "TACTICAL_SIGNALS",
        "PSYCHOLOGICAL_LEVERS",
        "RISKS",
        "TURN_MICRO_NOTE",
    ]
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise ValueError(f"Kairos output missing fields: {missing}")

    return {
        "message_id": message_id,
        "thread_id": thread_id,
        "sender": sender,
        "strategic_narrative": data["STRATEGIC_NARRATIVE"],
        "alignment_status": json.dumps(data["ALIGNMENT_STATUS"]),
        "conversation_criticality": data["CONVERSATION_CRITICALITY"],
        "tactical_signals": data["TACTICAL_SIGNALS"],
        "psychological_levers": data["PSYCHOLOGICAL_LEVERS"],
        "risks": data["RISKS"],
        "kairos_summary": json.dumps(data["TURN_MICRO_NOTE"]),
        "raw_writer_json": raw_text,
        "extract_status": "ok",
    }


if __name__ == "__main__":
    print("Kairos started - waiting for jobs")
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1); continue
        try:
            payload = job["payload"]
            process_job(payload)
            ack(job["row_id"])
        except Exception as exc:  # noqa: BLE001
            print("Kairos error:", exc)
            traceback.print_exc()
            time.sleep(2)
