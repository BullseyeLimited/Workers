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

QUEUE = "season.abstract"
TIER = "season"


def _cache_entry(thread_id: int, tier_index: int, start_turn: int | None, end_turn: int | None) -> dict | None:
    query = (
        SB.table("abstract_cache")
        .select("raw_text,raw_hash")
        .eq("thread_id", thread_id)
        .eq("tier", TIER)
        .eq("tier_index", tier_index)
    )
    if start_turn is not None:
        query = query.eq("start_turn", start_turn)
    if end_turn is not None:
        query = query.eq("end_turn", end_turn)
    rows = query.limit(1).execute().data or []
    return rows[0] if rows else None


def _upsert_cache(
    thread_id: int, tier_index: int, start_turn: int | None, end_turn: int | None, raw_text: str, raw_hash: str
):
    SB.table("abstract_cache").upsert(
        {
            "thread_id": thread_id,
            "tier": TIER,
            "tier_index": tier_index,
            "start_turn": start_turn,
            "end_turn": end_turn,
            "raw_text": raw_text,
            "raw_hash": raw_hash,
            "status": "ready",
        }
    ).execute()


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
        "max_tokens": 900,
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


def process_job(payload: dict) -> bool:
    thread_id = payload["thread_id"]
    raw_block = payload.get("raw_block") or ""
    tier_index = int(payload.get("tier_index") or 0)
    start_turn = payload.get("start_turn")
    end_turn = payload.get("end_turn")
    is_draft = bool(payload.get("draft"))

    if tier_index and is_draft and _cache_entry(thread_id, tier_index, start_turn, end_turn):
        return True

    raw_text = ""
    raw_hash = ""
    if tier_index and not is_draft:
        cached = _cache_entry(thread_id, tier_index, start_turn, end_turn)
        if cached:
            raw_text = cached.get("raw_text") or ""
            raw_hash = cached.get("raw_hash") or ""

    if not raw_text:
        extra_ctx = _recent_tier_abstracts(thread_id, "season", client=SB)
        prompt = build_prompt(
            "season_abstract",
            thread_id,
            raw_block,
            client=SB,
            extra_context=extra_ctx,
            include_blocks=False,
            include_plans=False,
            include_analyst=False,
            worker_tier=TimeTier.SEASON,
        )
        raw_text = call_llm(prompt)
        raw_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    if is_draft:
        if tier_index:
            _upsert_cache(thread_id, tier_index, start_turn, end_turn, raw_text, raw_hash)
        return True

    if not raw_hash:
        raw_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    send(
        "card.patch",
        {
            "thread_id": thread_id,
            "tier": "season",
            "start_turn": start_turn,
            "end_turn": end_turn,
            "tier_index": tier_index,
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
            print("[season_abstract_writer] error:", exc)
            traceback.print_exc()
            time.sleep(2)
