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

QUEUE = "episode.abstract"


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
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 800,
        "temperature": 0,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
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
        .select("turn_index,sender,message_text")
        .eq("thread_id", thread_id)
        .gte("turn_index", start_turn)
        .lte("turn_index", end_turn)
        .order("turn_index")
        .execute()
        .data
        or []
    )
    return rows


def render_raw_turns(rows: List[dict]) -> str:
    lines = []
    for row in rows:
        idx = row.get("turn_index")
        sender = (row.get("sender") or "?")[0].upper()
        text = row.get("message_text") or ""
        lines.append(f"[{idx}:{sender}] {text}")
    return "\n".join(lines)


def process_job(payload: dict) -> bool:
    thread_id = payload["thread_id"]
    end_turn = int(payload.get("end_turn") or 0)
    start_turn = max(1, end_turn - 19)

    rows = fetch_turns(thread_id, start_turn, end_turn)
    if len(rows) < 1:
        print(f"[episode_abstract_writer] no turns for thread {thread_id}")
        return True

    raw_turns = render_raw_turns(rows)
    prompt = build_prompt("episode_abstract", thread_id, raw_turns)
    raw_text = call_llm(prompt)
    raw_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    send(
        "card.patch",
        {
            "thread_id": thread_id,
            "tier": "episode",
            "start_turn": start_turn,
            "end_turn": end_turn,
            "raw_text": raw_text,
            "raw_hash": raw_hash,
        },
    )

    return True


if __name__ == "__main__":
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        try:
            payload = job["payload"]
            if process_job(payload):
                ack(job["row_id"])
        except Exception as exc:  # noqa: BLE001
            print("[episode_abstract_writer] error:", exc)
            traceback.print_exc()
            time.sleep(2)
