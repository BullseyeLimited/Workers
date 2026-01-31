import hashlib
import json
import os
import time
import traceback
from typing import List

import requests
from supabase import create_client, ClientOptions

from workers.lib.prompt_builder import build_prompt
from workers.lib.simple_queue import ack, receive, send
from workers.lib.time_tier import TimeTier

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
RUNPOD_URL = (os.getenv("RUNPOD_URL") or "").rstrip("/")
RUNPOD_MODEL = os.getenv("RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "chapter.abstract"
TIER = "chapter"

ABSTRACT_MAX_TOKENS = int(os.getenv("ABSTRACT_MAX_TOKENS", "30000"))
ABSTRACT_TIMEOUT_SECONDS = int(os.getenv("ABSTRACT_TIMEOUT_SECONDS", "300"))
ABSTRACT_QUEUE_VT = int(os.getenv("ABSTRACT_QUEUE_VT", "300"))
ABSTRACT_TEMPERATURE = float(os.getenv("ABSTRACT_TEMPERATURE", "0.95"))


def call_llm(prompt: str) -> str:
    if not RUNPOD_URL:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": RUNPOD_MODEL,
        "messages": [
            {"role": "system", "content": "Reasoning: high"},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": ABSTRACT_MAX_TOKENS,
        "temperature": ABSTRACT_TEMPERATURE,
    }

    resp = requests.post(
        url, headers=headers, json=payload, timeout=ABSTRACT_TIMEOUT_SECONDS
    )
    resp.raise_for_status()
    data = resp.json()
    raw_text = ""
    try:
        if data.get("choices"):
            choice = data["choices"][0]
            message = choice.get("message") or {}
            raw_text = message.get("content") or message.get("reasoning") or ""
            if not raw_text:
                raw_text = choice.get("text") or ""
    except Exception:
        pass

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text


def fetch_turns(thread_id: int, start_turn: int, end_turn: int) -> List[dict]:
    rows = (
        SB.table("messages")
        .select("id,turn_index,sender,message_text")
        .eq("thread_id", thread_id)
        .gte("turn_index", start_turn)
        .lte("turn_index", end_turn)
        .order("turn_index")
        .execute()
        .data
        or []
    )

    fan_message_ids = [
        row.get("id") for row in rows if row.get("sender") == "fan" and row.get("id")
    ]
    micro_by_message_id: dict[str, str] = {}
    if fan_message_ids:
        try:
            details_rows = (
                SB.table("message_ai_details")
                .select("message_id,kairos_summary,turn_micro_note")
                .in_("message_id", fan_message_ids)
                .execute()
                .data
                or []
            )
        except Exception:
            details_rows = []
        for detail in details_rows:
            if not isinstance(detail, dict):
                continue
            msg_id = detail.get("message_id")
            if msg_id is None:
                continue

            summary = ""
            micro = detail.get("turn_micro_note")
            if isinstance(micro, dict):
                val = micro.get("SUMMARY")
                if isinstance(val, str) and val.strip():
                    summary = val.strip()
            if not summary:
                val = detail.get("kairos_summary")
                if isinstance(val, str) and val.strip():
                    summary = val.strip()

            if summary:
                micro_by_message_id[str(msg_id)] = summary

    for row in rows:
        if row.get("sender") != "fan":
            continue
        msg_id = row.get("id")
        row["turn_micro_note"] = micro_by_message_id.get(str(msg_id)) or ""

    return rows


def render_raw_turns(rows: List[dict]) -> str:
    lines = []
    for row in rows:
        idx = row.get("turn_index")
        sender = (row.get("sender") or "?")[0].upper()
        text = row.get("message_text") or ""
        lines.append(f"[{idx}:{sender}] {text}")
        if sender == "F":
            note = (row.get("turn_micro_note") or "").strip()
            if not note:
                note = "No Kairos micro note available."
            lines.append(f"    (TURN_MICRO_NOTE): {note}")
    return "\n".join(lines)


def process_job(payload: dict) -> bool:
    thread_id = payload["thread_id"]
    end_turn = int(payload.get("end_turn") or 0)
    start_turn = int(payload.get("start_turn") or 0) or max(1, end_turn - 59)
    tier_index = payload.get("tier_index")
    attempt = int(payload.get("attempt") or 1)

    rows = fetch_turns(thread_id, start_turn, end_turn)
    if len(rows) < 1:
        print(f"[chapter_abstract_writer] no turns for thread {thread_id}")
        return True

    raw_turns = render_raw_turns(rows)
    prompt = build_prompt(
        "chapter_abstract",
        thread_id,
        raw_turns,
        client=SB,
        include_blocks=False,
        include_cards=True,
        include_plans=False,
        include_analyst=False,
        worker_tier=TimeTier.CHAPTER,
    )
    raw_text = call_llm(prompt)
    raw_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    patch_payload = {
        "thread_id": thread_id,
        "tier": "chapter",
        "start_turn": start_turn,
        "end_turn": end_turn,
        "raw_text": raw_text,
        "raw_hash": raw_hash,
        "attempt": attempt,
    }
    if tier_index is not None:
        patch_payload["tier_index"] = int(tier_index or 0) or None

    send(
        "card.patch",
        patch_payload,
    )

    return True


if __name__ == "__main__":
    while True:
        job = receive(QUEUE, ABSTRACT_QUEUE_VT)
        if not job:
            time.sleep(1)
            continue
        try:
            payload = job["payload"]
            if process_job(payload):
                ack(job["row_id"])
        except Exception as exc:  # noqa: BLE001
            print("[chapter_abstract_writer] error:", exc)
            traceback.print_exc()
            time.sleep(2)
