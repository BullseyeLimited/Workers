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


_TURN1_DIRECTIVE_KEY_CANDIDATES = (
    "TURN1_DIRECTIVE",
)


def _coerce_int(value) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _extract_action_list(data: dict, keys: tuple[str, ...]) -> list:
    for key in keys:
        if key in data:
            return _as_list(data.get(key))
    return []


def _normalize_offer_items(items: list) -> list[dict]:
    normalized: list[dict] = []
    seen: set[int] = set()
    for item in items:
        if isinstance(item, dict):
            content_id = _coerce_int(item.get("content_id") or item.get("id"))
            price = (
                item.get("price")
                or item.get("offered_price")
                or item.get("amount")
            )
            purchased = item.get("purchased")
        else:
            content_id = _coerce_int(item)
            price = None
            purchased = None
        if content_id is None or content_id in seen:
            continue
        seen.add(content_id)
        row = {"content_id": content_id}
        if price not in (None, ""):
            row["offered_price"] = price
        if purchased is not None:
            row["purchased"] = bool(purchased)
        normalized.append(row)
    return normalized


def _normalize_send_items(items: list) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for item in items:
        if isinstance(item, dict):
            content_id = _coerce_int(item.get("content_id") or item.get("id"))
        else:
            content_id = _coerce_int(item)
        if content_id is None or content_id in seen:
            continue
        seen.add(content_id)
        normalized.append(content_id)
    return normalized


def _apply_content_actions(
    *,
    thread_id: int,
    creator_message_id: int,
    content_actions: dict | None,
) -> None:
    if not content_actions or not isinstance(content_actions, dict):
        return

    offer_items = _extract_action_list(
        content_actions,
        ("offers", "offer", "ppv_offers", "ppv_offer"),
    )
    send_items = _extract_action_list(
        content_actions,
        ("sends", "send", "deliveries", "delivery", "deliver", "sent"),
    )
    offers = _normalize_offer_items(offer_items)
    sends = _normalize_send_items(send_items)

    if offers:
        rows = []
        for offer in offers:
            row = {
                "thread_id": thread_id,
                "message_id": creator_message_id,
                "content_id": offer["content_id"],
            }
            if "offered_price" in offer:
                row["offered_price"] = offer["offered_price"]
            if "purchased" in offer:
                row["purchased"] = offer["purchased"]
            rows.append(row)
        try:
            SB.table("content_offers").insert(rows).execute()
        except Exception:
            pass

    if sends:
        delivered_rows = [
            {
                "thread_id": thread_id,
                "message_id": creator_message_id,
                "content_id": content_id,
            }
            for content_id in sends
        ]
        inserted = False
        try:
            SB.table("content_deliveries").insert(delivered_rows).execute()
            inserted = True
        except Exception:
            inserted = False
        if len(sends) == 1:
            try:
                SB.table("messages").update(
                    {"content_id": sends[0]}
                ).eq("id", creator_message_id).execute()
            except Exception:
                pass
        elif not inserted:
            try:
                SB.table("messages").update(
                    {"content_id": sends[0]}
                ).eq("id", creator_message_id).execute()
            except Exception:
                pass


def _format_content_label(item: dict | None, content_id: int) -> str:
    if not isinstance(item, dict):
        return f"content_id {content_id}"
    media_type = (item.get("media_type") or "").strip()
    explicitness = (item.get("explicitness") or "").strip()
    desc = (item.get("desc_short") or "").strip()
    prefix_parts = [p for p in (media_type, explicitness) if p]
    prefix = " ".join(prefix_parts) if prefix_parts else "content"
    if desc:
        return f"{prefix}: {desc}"
    return f"{prefix}: content_id {content_id}"


def _render_content_placeholders(content_actions: dict | None) -> list[str]:
    if not content_actions or not isinstance(content_actions, dict):
        return []

    offer_items = _extract_action_list(
        content_actions,
        ("offers", "offer", "ppv_offers", "ppv_offer"),
    )
    send_items = _extract_action_list(
        content_actions,
        ("sends", "send", "deliveries", "delivery", "deliver", "sent"),
    )
    offers = _normalize_offer_items(offer_items)
    sends = _normalize_send_items(send_items)

    content_ids: set[int] = set(sends)
    for offer in offers:
        content_ids.add(offer["content_id"])

    item_lookup: dict[int, dict] = {}
    if content_ids:
        try:
            rows = (
                SB.table("content_items")
                .select("id,media_type,explicitness,desc_short")
                .in_("id", list(content_ids))
                .execute()
                .data
                or []
            )
        except Exception:
            rows = []
        for row in rows:
            try:
                item_lookup[int(row.get("id"))] = row
            except Exception:
                continue

    lines: list[str] = []
    for content_id in sends:
        label = _format_content_label(item_lookup.get(content_id), content_id)
        lines.append(f"[CONTENT_SENT] {label}")

    for offer in offers:
        content_id = offer["content_id"]
        label = _format_content_label(item_lookup.get(content_id), content_id)
        status = "bought" if offer.get("purchased") is True else "pending"
        price = offer.get("offered_price")
        price_label = f" ${price}" if price is not None else ""
        lines.append(f"[CONTENT_OFFER {status}{price_label}] {label}")

    return lines


def _extract_turn1_directive(value: Any) -> str:
    """
    Napoleon Writer only needs Turn 1.

    The queue payload may contain:
    - a plain string (preferred)
    - a dict with keys for all 3 turns (legacy)
    - a stringified JSON dict (legacy)
    """

    if value is None or value == "":
        return ""

    if isinstance(value, dict):
        for key in _TURN1_DIRECTIVE_KEY_CANDIDATES:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
            if candidate is not None and candidate != "":
                return str(candidate).strip()

        nested = (
            value.get("TACTICAL_PLAN_3TURNS")
            or value.get("tactical_plan_3turns")
            or value.get("tactical_plan_3turn")
        )
        if nested is not None:
            return _extract_turn1_directive(nested)
        return ""

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""

        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                extracted = _extract_turn1_directive(parsed)
                if extracted:
                    return extracted

        # Keep only Turn 1, stopping before Turn2/Turn3 markers (legacy payloads).
        parts = re.split(
            r"\n\s*(?:TURN2A|TURN2B|TURN2|TURN3A|TURN3B|TURN3)\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        turn1_block = (parts[0] or "").strip()
        if not turn1_block:
            return ""

        header_prefix = re.compile(
            r"^\s*TURN1_DIRECTIVE\s*[:=-]\s*",
            re.IGNORECASE,
        )
        turn1_block = header_prefix.sub("", turn1_block, count=1).strip()

        lines = turn1_block.splitlines()
        if lines and re.match(
            r"^\s*TURN1_DIRECTIVE\s*$",
            lines[0],
            flags=re.IGNORECASE,
        ):
            turn1_block = "\n".join(lines[1:]).strip()

        return turn1_block

    return str(value).strip()


def build_writer_user_block(payload: Dict[str, Any]) -> str:
    # Keep it readable for the model; JSON with context markers.
    block = {
        "creator_identity_card": payload.get("creator_identity_card") or "",
        "fan_psychic_card": payload.get("fan_psychic_card") or {},
        "thread_history": payload.get("thread_history") or "",
        "latest_fan_message": payload.get("latest_fan_message") or "",
        "turn_directive": _extract_turn1_directive(payload.get("turn_directive")),
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
    content_lines = _render_content_placeholders(payload.get("content_actions"))
    if content_lines and "[CONTENT_SENT]" not in final_text and "[CONTENT_OFFER" not in final_text:
        if final_text:
            final_text = f"{final_text}\n" + "\n".join(content_lines)
        else:
            final_text = "\n".join(content_lines)
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
    _apply_content_actions(
        thread_id=thread_id,
        creator_message_id=creator_message_id,
        content_actions=payload.get("content_actions"),
    )
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
