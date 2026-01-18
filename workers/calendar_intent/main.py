import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests
from supabase import create_client, ClientOptions

from workers.lib.json_utils import extract_json_object
from workers.lib.simple_queue import ack, receive

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

QUEUE = "calendar.intent"
PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "calendar_intent.txt"


def _load_prompt_template() -> str:
    if not PROMPT_PATH.exists():
        raise FileNotFoundError(f"Missing prompt template: {PROMPT_PATH}")
    return PROMPT_PATH.read_text(encoding="utf-8")


def _render_prompt(*, input_json: str) -> str:
    tpl = _load_prompt_template()
    return tpl.replace("{INPUT_JSON}", input_json)


def _format_turns_json(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    formatted = []
    for row in rows:
        turn_num = row.get("turn_index")
        sender_raw = (row.get("sender") or "").strip().lower()
        role = "user" if sender_raw.startswith("f") else "assistant"
        ts = row.get("created_at")
        if not ts:
            ts = datetime.now(timezone.utc).isoformat()
        text = (row.get("message_text") or "").strip()
        formatted.append(
            {
                "turn_number": turn_num,
                "role": role,
                "timestamp": ts,
                "text": text,
            }
        )
    return formatted


def _fetch_turns(thread_id: int, start_turn: int, end_turn: int) -> List[Dict]:
    rows = (
        SB.table("messages")
        .select("id,turn_index,sender,message_text,created_at")
        .eq("thread_id", thread_id)
        .gte("turn_index", start_turn)
        .lte("turn_index", end_turn)
        .order("turn_index")
        .execute()
        .data
        or []
    )
    return rows


def _message_id_for_turn(thread_id: int, turn_index: int) -> int | None:
    row = (
        SB.table("messages")
        .select("id")
        .eq("thread_id", thread_id)
        .eq("turn_index", turn_index)
        .single()
        .execute()
        .data
    )
    if not row:
        return None
    return row.get("id")


def _call_llm(prompt: str) -> str:
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
        "max_tokens": 600,
        "temperature": 0,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    raw_text = ""
    try:
        if data.get("choices"):
            choice = data["choices"][0]
            msg = choice.get("message") or {}
            raw_text = msg.get("content") or msg.get("reasoning") or ""
            if not raw_text:
                raw_text = choice.get("text") or ""
    except Exception:
        pass

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    return raw_text


def _parse_model_output(raw_text: str) -> Dict:
    """
    Expect a JSON object; tolerate fenced blocks.
    """
    obj = extract_json_object(raw_text)
    if not isinstance(obj, dict):
        raise ValueError("model output is not a JSON object")
    return obj


def _extract_single_intent(intent_obj: Dict) -> Optional[Dict]:
    """
    Return the single intent (if present and well-formed) from the model output.
    The prompt guarantees at most one intent.
    """
    intents = intent_obj.get("intents")
    if not intents or not isinstance(intents, list):
        return None
    first = intents[0]
    if not isinstance(first, dict):
        return None
    return first


def _lookup_creator_id(thread_id: int) -> Optional[int]:
    row = (
        SB.table("threads")
        .select("creator_id")
        .eq("id", thread_id)
        .single()
        .execute()
        .data
    )
    if not row:
        return None
    return row.get("creator_id")


def _upsert_followup_row(
    *,
    thread_id: int,
    creator_id: Optional[int],
    message_id: Optional[int],
    source_turn_index: int,
    send_at: str,
    reason: str,
    followup_message: str,
    intent_json: Dict,
    raw_hash: str,
) -> None:
    payload = {
        "message_id": message_id,
        "thread_id": thread_id,
        "creator_id": creator_id,
        "source_turn_index": source_turn_index,
        "send_at": send_at,
        "reason": reason,
        "message": followup_message,
        "intent_json": intent_json,
        "status": "pending",
        "raw_hash": raw_hash,
    }

    (
        SB.table("calendar_followups")
        .upsert(payload, on_conflict="thread_id,source_turn_index")
        .execute()
    )


def process_job(payload: dict) -> bool:
    thread_id = payload.get("thread_id")
    end_turn = payload.get("end_turn")
    now_from_payload = payload.get("now")

    if not thread_id or not end_turn:
        print("[calendar_intent] missing thread_id/end_turn in payload")
        return True

    focus_end = int(end_turn)
    focus_start = max(1, focus_end - 19)
    context_start = max(1, focus_start - 40)

    turns = _fetch_turns(thread_id, context_start, focus_end)
    if not turns:
        print(f"[calendar_intent] no turns found for thread {thread_id}")
        return True

    focus_rows = [row for row in turns if row.get("turn_index", 0) >= focus_start]
    context_rows = [row for row in turns if row.get("turn_index", 0) < focus_start]

    input_payload = {
        "now": now_from_payload
        or datetime.now(timezone.utc).isoformat(),
        "context_40_turns": _format_turns_json(context_rows),
        "last_20_turns": _format_turns_json(focus_rows),
    }

    input_json = json.dumps(input_payload, ensure_ascii=False, separators=(",", ":"))
    prompt = _render_prompt(input_json=input_json)
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()

    raw_text = _call_llm(prompt)

    try:
        intent_obj = _parse_model_output(raw_text)
    except Exception as exc:  # noqa: BLE001
        print(f"[calendar_intent] parse error: {exc}")
        return True

    focus_msg_id = _message_id_for_turn(thread_id, focus_end)

    intent = _extract_single_intent(intent_obj)
    if not intent:
        print(f"[calendar_intent] no intent detected for thread {thread_id}")
        return True

    followup = intent.get("followup") or {}
    event = intent.get("event") or {}

    send_at = followup.get("send_at")
    reason = followup.get("reason") or ""
    followup_msg = followup.get("message") or ""
    source_turn = intent.get("source_turn_number") or focus_end

    if not send_at or not followup_msg:
        print(f"[calendar_intent] missing send_at/message for thread {thread_id}")
        return True

    creator_id = _lookup_creator_id(thread_id)

    _upsert_followup_row(
        thread_id=thread_id,
        creator_id=creator_id,
        message_id=focus_msg_id,
        source_turn_index=int(source_turn),
        send_at=str(send_at),
        reason=str(reason),
        followup_message=str(followup_msg),
        intent_json=intent_obj,
        raw_hash=prompt_hash,
    )
    print(
        f"[calendar_intent] stored intent for thread {thread_id}, turn {focus_end}",
        flush=True,
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
            print("[calendar_intent] error:", exc)
            traceback.print_exc()
            time.sleep(2)
