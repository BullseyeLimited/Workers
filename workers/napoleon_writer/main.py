"""
Napoleon Writer worker: consumes post-planner jobs and produces the final creator DM.
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List

import requests
from supabase import create_client, ClientOptions

from workers.lib.simple_queue import receive, ack

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

QUEUE = "napoleon.compose"
PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
WRITER_PROMPT_PATH = PROMPTS_DIR / "napoleon_writer.txt"


def _load_writer_prompt() -> str:
    return WRITER_PROMPT_PATH.read_text(encoding="utf-8")


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
        "max_tokens": 1000,
        "temperature": 0.6,
        "top_p": 0.95,
        "repetition_penalty": 1.0,
        "presence_penalty": 0.0,
        "frequency_penalty": 0.1,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=300)
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


CHUNK_PATTERN = re.compile(r"^\s*(\d+)\s*\[(.*)\]\s*$")


def parse_writer_output(raw_text: str) -> List[str]:
    chunks: List[str] = []
    for line in raw_text.splitlines():
        match = CHUNK_PATTERN.match(line)
        if match:
            chunks.append(match.group(2).strip())
    if not chunks and raw_text.strip():
        chunks.append(raw_text.strip())
    return chunks


def build_writer_user_block(payload: Dict[str, Any]) -> str:
    # Keep it readable for the model; JSON with context markers.
    block = {
        "creator_identity_card": payload.get("creator_identity_card") or "",
        "fan_psychic_card": payload.get("fan_psychic_card") or {},
        "thread_history": payload.get("thread_history") or "",
        "latest_fan_message": payload.get("latest_fan_message") or "",
        "turn_directive": payload.get("turn_directive") or "",
    }
    return f"<NAPOLEON_INPUT>\n{json.dumps(block, ensure_ascii=False, indent=2)}\n</NAPOLEON_INPUT>"


def insert_creator_reply(thread_id: int, final_text: str) -> int:
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
        "ext_message_id": f"auto-writer-{int(time.time()*1000)}",
        "source_channel": "scheduler",
    }
    msg = SB.table("messages").insert(row).execute().data[0]
    return msg["id"]


def record_writer_failure(
    fan_message_id: int,
    thread_id: int,
    prompt: str,
    raw_text: str,
    error_message: str,
) -> None:
    existing_row = (
        SB.table("message_ai_details")
        .select("extras")
        .eq("message_id", fan_message_id)
        .single()
        .execute()
        .data
        or {}
    )
    merged_extras = existing_row.get("extras") or {}
    merged_extras.update(
        {
            "napoleon_writer_raw_preview": (raw_text or "")[:2000],
            "napoleon_writer_raw": raw_text,
            "napoleon_writer_prompt_raw": prompt,
        }
    )
    SB.table("message_ai_details").update(
        {
            # Do not clobber planner fields; mark status/error and stash writer data in extras.
            "napoleon_status": "failed",
            "extract_error": error_message,
            "extras": merged_extras,
        }
    ).eq("message_id", fan_message_id).execute()


def upsert_writer_details(
    fan_message_id: int,
    thread_id: int,
    creator_message_id: int,
    prompt_log: str,
    raw_text: str,
    chunks: List[str],
) -> None:
    existing_row = (
        SB.table("message_ai_details")
        .select("extras")
        .eq("message_id", fan_message_id)
        .single()
        .execute()
        .data
        or {}
    )
    merged_extras = existing_row.get("extras") or {}
    merged_extras.update(
        {
            "napoleon_writer_chunks": chunks,
            "napoleon_writer_raw_preview": (raw_text or "")[:2000],
            "napoleon_writer_raw": raw_text,
            "napoleon_writer_prompt_raw": prompt_log,
            "napoleon_prompt_preview": (prompt_log or "")[:2000],
            "creator_reply_message_id": creator_message_id,
        }
    )
    SB.table("message_ai_details").update(
        {
            # Preserve planner fields; only add writer data via extras and status flip.
            "napoleon_status": "ok",
            "extract_error": None,
            "extras": merged_extras,
        }
    ).eq("message_id", fan_message_id).execute()

    # Store writer output on the creator message for debugging.
    SB.table("messages").update(
        {"napoleon_output": {"writer_chunks": chunks, "writer_raw": raw_text}}
    ).eq("id", creator_message_id).execute()


def process_job(payload: Dict[str, Any]) -> bool:
    fan_message_id = payload["fan_message_id"]
    thread_id = payload["thread_id"]

    system_prompt = _load_writer_prompt()
    user_block = build_writer_user_block(payload)
    full_prompt_log = json.dumps({"system": system_prompt, "user": user_block}, ensure_ascii=False)

    try:
        raw_text, _ = runpod_call(system_prompt, user_block)
    except Exception as exc:  # noqa: BLE001
        record_writer_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=full_prompt_log,
            raw_text="",
            error_message=f"RunPod error: {exc}",
        )
        print(f"[Napoleon Writer] RunPod error for fan message {fan_message_id}: {exc}")
        return True

    chunks = parse_writer_output(raw_text)
    final_text = "\n".join(chunks).strip()
    if not final_text:
        record_writer_failure(
            fan_message_id=fan_message_id,
            thread_id=thread_id,
            prompt=full_prompt_log,
            raw_text=raw_text,
            error_message="empty_writer_output",
        )
        return True

    creator_message_id = insert_creator_reply(thread_id, final_text)
    upsert_writer_details(
        fan_message_id=fan_message_id,
        thread_id=thread_id,
        creator_message_id=creator_message_id,
        prompt_log=full_prompt_log,
        raw_text=raw_text,
        chunks=chunks,
    )
    return True


if __name__ == "__main__":
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        row_id = job["row_id"]
        try:
            payload = job["payload"]
            if process_job(payload):
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("Napoleon Writer error:", exc)
            traceback.print_exc()
            time.sleep(2)
