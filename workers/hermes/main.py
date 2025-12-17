"""
Hermes router worker — decides Kairos mode, web research need, and orchestrates fork/join.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
from supabase import ClientOptions, create_client

from workers.lib.job_utils import job_exists
from workers.lib.prompt_builder import live_turn_window
from workers.lib.simple_queue import ack, receive, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for Hermes")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "hermes.route"
KAIROS_QUEUE = "kairos.analyse"
WEB_QUEUE = "web.research"
JOIN_QUEUE = "hermes.join"

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
DEFAULT_PROMPT = """
You are Hermes. Output format:
FINAL_VERDICT_SEARCH: YES|NO
FINAL_VERDICT_KAIROS: FULL|LITE|SKIP
JOIN_REQUIREMENTS: KAIROS_ONLY|WEB_ONLY|BOTH|NONE
<WEB_RESEARCH_BRIEF>...NONE or what to search...</WEB_RESEARCH_BRIEF>
"""

HEADER_PATTERNS = {
    "search": re.compile(r"FINAL_VERDICT_SEARCH\s*:\s*(YES|NO)", re.IGNORECASE),
    "kairos": re.compile(
        r"FINAL_VERDICT_KAIROS\s*:\s*(FULL|LITE|SKIP)", re.IGNORECASE
    ),
    "join": re.compile(
        r"JOIN_REQUIREMENTS\s*:\s*(KAIROS_ONLY|WEB_ONLY|BOTH|NONE)",
        re.IGNORECASE,
    ),
}
BRIEF_PATTERN = re.compile(
    r"<WEB_RESEARCH_BRIEF>(.*?)</WEB_RESEARCH_BRIEF>", re.IGNORECASE | re.DOTALL
)


def _load_prompt() -> str:
    path = PROMPTS_DIR / "hermes.txt"
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return DEFAULT_PROMPT


def _format_fan_turn(row: dict) -> str:
    """
    Render the latest fan turn as a sequence of text/media parts, so media
    descriptions produced by Argus appear inline in prompts.
    """
    parts: list[str] = []
    text = (row.get("message_text") or "").strip()
    media_analysis = (row.get("media_analysis_text") or "").strip()
    media_payload = row.get("media_payload") or {}
    items = []
    if isinstance(media_payload, dict):
        maybe_items = media_payload.get("items")
        if isinstance(maybe_items, list):
            items = maybe_items

    if text:
        parts.append(f"(text): {text}")

    if items:
        for item in items:
            if not isinstance(item, dict):
                continue
            kind = (item.get("type") or "media").lower()
            desc = (item.get("argus_text") or "").strip()
            if not desc:
                desc = (item.get("argus_preview") or "").strip()
            if not desc:
                desc = media_analysis
            if not desc:
                desc = item.get("argus_error") or "media attachment"
            parts.append(f"({kind}): {desc}")
    elif media_analysis:
        parts.append(f"(media): {media_analysis}")

    return "\n".join(parts) if parts else text


def _fail_closed_defaults() -> dict:
    return {
        "final_verdict_search": "NO",
        "final_verdict_kairos": "FULL",
        "join_requirements": "KAIROS_ONLY",
        "web_research_brief": "NONE",
    }


def parse_hermes_output(raw_text: str) -> Tuple[dict | None, str | None]:
    """
    Parse Hermes model output. Return (parsed, error_message).
    Parsed keys are uppercase values.
    """
    if not raw_text or not raw_text.strip():
        return None, "empty_output"

    parsed: dict[str, str] = {}
    missing: list[str] = []

    search_match = HEADER_PATTERNS["search"].search(raw_text)
    kairos_match = HEADER_PATTERNS["kairos"].search(raw_text)
    join_match = HEADER_PATTERNS["join"].search(raw_text)

    if search_match:
        parsed["final_verdict_search"] = search_match.group(1).upper()
    else:
        missing.append("FINAL_VERDICT_SEARCH")

    if kairos_match:
        parsed["final_verdict_kairos"] = kairos_match.group(1).upper()
    else:
        missing.append("FINAL_VERDICT_KAIROS")

    if join_match:
        parsed["join_requirements"] = join_match.group(1).upper()
    else:
        missing.append("JOIN_REQUIREMENTS")

    brief_match = BRIEF_PATTERN.search(raw_text)
    if brief_match:
        parsed["web_research_brief"] = brief_match.group(1).strip() or "NONE"
    else:
        parsed["web_research_brief"] = "NONE"

    if missing:
        return None, f"missing_sections: {', '.join(missing)}"

    return parsed, None


def _runpod_call(system_prompt: str, user_message: str) -> tuple[str, dict]:
    """
    Call the RunPod vLLM OpenAI-compatible server using chat completions.
    Returns (raw_text, request_payload) for logging.
    """
    base = os.getenv("RUNPOD_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")
    key = os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("RUNPOD_API_KEY or OPENAI_API_KEY is not set")

    url = f"{base}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
    }
    payload = {
        "model": os.getenv("HERMES_MODEL", "gpt-4o-mini"),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": float(os.getenv("HERMES_TEMPERATURE", "0.2")),
        "max_tokens": int(os.getenv("HERMES_MAX_TOKENS", "800")),
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    raw_text = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    return raw_text or "", payload


def _merge_extras(existing: dict, patch: dict) -> dict:
    merged = {}
    merged.update(existing or {})
    merged.update(patch or {})
    return merged


def _has_valid_decision(blob: dict | None) -> bool:
    if not isinstance(blob, dict):
        return False
    parsed = blob.get("parsed") if isinstance(blob.get("parsed"), dict) else None
    if not parsed:
        return False
    required = ["final_verdict_search", "final_verdict_kairos", "join_requirements"]
    return all(parsed.get(k) for k in required)


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = payload["message_id"]
    msg_row = (
        SB.table("messages")
        .select(
            "id,thread_id,turn_index,message_text,media_analysis_text,media_payload"
        )
        .eq("id", fan_msg_id)
        .single()
        .execute()
        .data
    )
    if not msg_row:
        raise ValueError(f"Message {fan_msg_id} not found")

    thread_id = msg_row["thread_id"]
    turn_index = msg_row.get("turn_index")

    details_rows = (
        SB.table("message_ai_details")
        .select("extras")
        .eq("message_id", fan_msg_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    existing_extras = {}
    if details_rows:
        existing_extras = details_rows[0].get("extras") or {}
    else:
        # Create a minimal row so we can store Hermes decisions without violating
        # NOT NULL constraints (raw_hash, kairos_status) on message_ai_details.
        try:
            SB.table("message_ai_details").insert(
                {
                    "message_id": fan_msg_id,
                    "thread_id": thread_id,
                    "sender": "fan",
                    "raw_hash": hashlib.sha256(
                        f"hermes_seed:{thread_id}:{fan_msg_id}".encode("utf-8")
                    ).hexdigest(),
                    "kairos_status": "pending",
                    "extras": existing_extras,
                }
            ).execute()
        except Exception:
            # Another worker may have inserted the row concurrently.
            pass

    # Reuse prior decision if already stored.
    hermes_blob = existing_extras.get("hermes") if isinstance(existing_extras, dict) else None
    if _has_valid_decision(hermes_blob):
        parsed = hermes_blob["parsed"]
        raw_text = hermes_blob.get("raw_output", "")
        status = hermes_blob.get("status") or "ok"
        error = hermes_blob.get("error")
    else:
        raw_turns = live_turn_window(
            thread_id,
            boundary_turn=turn_index,
            limit=20,
            exclude_message_id=fan_msg_id,
            client=SB,
        )
        latest_fan_text = _format_fan_turn(msg_row)

        # Identity cards (fan + creator)
        thread_row = (
            SB.table("threads")
            .select(
                "fan_identity_card,creator_identity_card,creator_id"
            )
            .eq("id", thread_id)
            .single()
            .execute()
            .data
            or {}
        )
        creator_card = thread_row.get("creator_identity_card")
        if not creator_card and thread_row.get("creator_id"):
            try:
                creator_row = (
                    SB.table("creators")
                    .select("creator_identity_card")
                    .eq("id", thread_row["creator_id"])
                    .single()
                    .execute()
                    .data
                    or {}
                )
                creator_card = creator_row.get("creator_identity_card")
            except Exception:
                creator_card = None

        user_payload = {
            "LATEST_FAN_MESSAGE": latest_fan_text,
            "RAW_TURNS": raw_turns,
            "FAN_IDENTITY_CARD": thread_row.get("fan_identity_card") or "Identity card: empty",
            "CREATOR_IDENTITY_CARD": creator_card or "Identity card: empty",
        }
        user_block = f"<HERMES_INPUT>\n{json.dumps(user_payload, ensure_ascii=False, indent=2)}\n</HERMES_INPUT>"

        system_prompt = _load_prompt()

        try:
            raw_text, _ = _runpod_call(system_prompt, user_block)
            parsed, error = parse_hermes_output(raw_text)
        except Exception as exc:  # noqa: BLE001
            parsed, error = None, f"runpod_error: {exc}"
            raw_text = ""
            status = "failed"
        else:
            status = "ok" if parsed else "failed"

        if not parsed:
            parsed = _fail_closed_defaults()

        hermes_blob = {
            "status": status,
            "raw_output": raw_text,
            "parsed": parsed,
            "error": error,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        merged_extras = _merge_extras(existing_extras, {"hermes": hermes_blob})
        SB.table("message_ai_details").update({"extras": merged_extras}).eq(
            "message_id", fan_msg_id
        ).execute()
    parsed = hermes_blob["parsed"]

    verdict_search = (parsed.get("final_verdict_search") or "NO").upper()
    verdict_kairos = (parsed.get("final_verdict_kairos") or "FULL").upper()
    join_requirements = (parsed.get("join_requirements") or "KAIROS_ONLY").upper()
    research_brief = parsed.get("web_research_brief") or "NONE"

    # Idempotent forks
    if verdict_kairos != "SKIP":
        kairos_mode = "lite" if verdict_kairos == "LITE" else "full"
        if not job_exists(KAIROS_QUEUE, fan_msg_id, client=SB):
            send(KAIROS_QUEUE, {"message_id": fan_msg_id, "kairos_mode": kairos_mode})

    if verdict_search == "YES":
        if not job_exists(WEB_QUEUE, fan_msg_id, client=SB):
            send(WEB_QUEUE, {"message_id": fan_msg_id, "brief": research_brief})

    if join_requirements == "NONE":
        if not job_exists(JOIN_QUEUE, fan_msg_id, client=SB):
            send(JOIN_QUEUE, {"message_id": fan_msg_id})

    return True


if __name__ == "__main__":
    print("[Hermes] started - waiting for jobs", flush=True)
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
            print("[Hermes] error:", exc)
            traceback.print_exc()
            # Let the job retry
            time.sleep(2)
