import hashlib
import json
import os
import time
import traceback

import requests
from supabase import create_client, ClientOptions

from workers.lib.prompt_builder import build_prompt, _recent_tier_abstracts
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

QUEUE = "year.abstract"
TIER = "year"

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


def process_job(payload: dict) -> bool:
    thread_id = payload["thread_id"]
    raw_block = payload.get("raw_block") or ""
    tier_index = int(payload.get("tier_index") or 0)
    start_turn = payload.get("start_turn")
    end_turn = payload.get("end_turn")
    attempt = int(payload.get("attempt") or 1)

    extra_ctx = _recent_tier_abstracts(thread_id, "year", client=SB)
    prompt = build_prompt(
        "year_abstract",
        thread_id,
        raw_block,
        client=SB,
        extra_context=extra_ctx,
        include_blocks=False,
        include_plans=False,
        include_analyst=False,
        worker_tier=TimeTier.YEAR,
    )
    raw_text = call_llm(prompt)
    raw_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    send(
        "card.patch",
        {
            "thread_id": thread_id,
            "tier": "year",
            "start_turn": start_turn,
            "end_turn": end_turn,
            "tier_index": tier_index,
            "raw_text": raw_text,
            "raw_hash": raw_hash,
            "attempt": attempt,
        },
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
            print("[year_abstract_writer] error:", exc)
            traceback.print_exc()
            time.sleep(2)
