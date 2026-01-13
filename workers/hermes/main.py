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

from workers.lib.content_pack import build_content_index, format_content_pack
from workers.lib.job_utils import job_exists
from workers.lib.prompt_builder import live_turn_window
from workers.lib.json_utils import safe_parse_model_json
from workers.lib.simple_queue import ack, receive, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CONTENT_PACK_ENABLED = os.getenv("CONTENT_PACK_ENABLED", "").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

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
CONTENT_REQUEST_PATTERN = re.compile(
    r"<CONTENT_REQUEST>(.*?)</CONTENT_REQUEST>", re.IGNORECASE | re.DOTALL
)
CONTENT_HEADER_PATTERN = re.compile(r"^\s*([A-Z0-9_ -]+?)\s*:\s*(.+?)\s*$")


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


def parse_content_request(raw_text: str) -> dict | None:
    if not raw_text:
        return None
    match = CONTENT_REQUEST_PATTERN.search(raw_text)
    if not match:
        return None
    payload = match.group(1).strip()
    if not payload:
        return None
    data, error = safe_parse_model_json(payload)
    if not error and isinstance(data, dict):
        return data

    request: dict = {}
    lines = [line for line in payload.splitlines() if line.strip()]
    for line in lines:
        header_match = CONTENT_HEADER_PATTERN.match(line)
        if not header_match:
            continue
        raw_key = header_match.group(1).strip().lower().replace(" ", "_")
        raw_value = header_match.group(2).strip()
        if not raw_value:
            continue

        if raw_key in {"zoom"}:
            try:
                request["zoom"] = int(raw_value)
            except Exception:
                continue
        elif raw_key in {"script_id", "script"}:
            request["script_id"] = raw_value
        elif raw_key in {"include_shoot_extras", "shoot_extras"}:
            val = raw_value.strip().lower()
            request["include_shoot_extras"] = val in {"1", "true", "yes", "on"}
        elif raw_key in {"media_expand", "media"}:
            if "," in raw_value:
                values = [v.strip() for v in raw_value.split(",") if v.strip()]
            else:
                values = [v.strip() for v in raw_value.split() if v.strip()]
            if values:
                request["media_expand"] = values
        elif raw_key in {"content_ids", "item_ids", "content_id", "item_id"}:
            raw_items = [v.strip() for v in raw_value.replace(" ", ",").split(",") if v.strip()]
            ids: list[int] = []
            for raw_item in raw_items:
                try:
                    ids.append(int(raw_item))
                except Exception:
                    continue
            if ids:
                request["content_ids"] = ids
        elif raw_key in {
            "global_focus_ids",
            "global_focus",
            "global_ids",
            "bubble2_ids",
            "scriptless_ids",
        }:
            raw_items = [v.strip() for v in raw_value.replace(" ", ",").split(",") if v.strip()]
            ids: list[int] = []
            for raw_item in raw_items:
                try:
                    ids.append(int(raw_item))
                except Exception:
                    continue
            if ids:
                request["global_focus_ids"] = ids
        elif raw_key in {"creator_id"}:
            try:
                request["creator_id"] = int(raw_value)
            except Exception:
                continue

    return request or None


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
    attempt = int(payload.get("hermes_retry", 0))
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

    new_decision = False
    # Reuse prior decision if already stored.
    hermes_blob = existing_extras.get("hermes") if isinstance(existing_extras, dict) else None
    if _has_valid_decision(hermes_blob):
        parsed = hermes_blob["parsed"]
        raw_text = hermes_blob.get("raw_output", "")
        status = hermes_blob.get("status") or "ok"
        error = hermes_blob.get("error")
    else:
        new_decision = True
        raw_turns = live_turn_window(
            thread_id,
            boundary_turn=turn_index,
            limit=20,
            exclude_message_id=fan_msg_id,
            client=SB,
        )
        raw_turns = raw_turns or "[No raw turns provided]"
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

        fan_card = thread_row.get("fan_identity_card") or "Identity card: empty"
        creator_card = creator_card or "Identity card: empty"
        content_index_block = ""
        creator_id = thread_row.get("creator_id")
        if CONTENT_PACK_ENABLED and creator_id:
            try:
                content_index = build_content_index(
                    SB,
                    creator_id=creator_id,
                    thread_id=thread_id,
                )
                packed_index = format_content_pack(content_index)
                if packed_index:
                    content_index_block = (
                        "\n\n<CONTENT_INDEX>\n"
                        f"{packed_index}\n"
                        "</CONTENT_INDEX>"
                    )
            except Exception:
                content_index_block = ""
        user_block = (
            "<HERMES_INPUT>\n"
            f"{raw_turns}\n\n"
            "LATEST_FAN_MESSAGE:\n"
            f"{latest_fan_text}\n\n"
            "FAN_IDENTITY_CARD:\n"
            f"{fan_card}\n\n"
            "CREATOR_IDENTITY_CARD:\n"
            f"{creator_card}"
            f"{content_index_block}\n"
            "</HERMES_INPUT>"
        )

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

        if not parsed and attempt < 1:
            send(QUEUE, {"message_id": fan_msg_id, "hermes_retry": attempt + 1})
            return True

        if not parsed:
            parsed = _fail_closed_defaults()

        content_request = parse_content_request(raw_text) if CONTENT_PACK_ENABLED else None
        hermes_blob = {
            "status": status,
            "raw_output": raw_text,
            "parsed": parsed,
            "error": error,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        merged_extras = _merge_extras(existing_extras, {"hermes": hermes_blob})
        update_payload = {
            "extras": merged_extras,
            "hermes_status": status,
            "hermes_output_raw": raw_text,
        }
        if content_request and CONTENT_PACK_ENABLED:
            update_payload.update(
                {
                    "content_request": content_request,
                    "content_pack_status": "pending",
                    "content_pack_error": None,
                    "content_pack": None,
                    "content_pack_created_at": None,
                }
            )
        SB.table("message_ai_details").update(update_payload).eq(
            "message_id", fan_msg_id
        ).execute()
    if (not new_decision) and _has_valid_decision(hermes_blob):
        SB.table("message_ai_details").update(
            {
                "hermes_status": hermes_blob.get("status") or "ok",
                "hermes_output_raw": hermes_blob.get("raw_output", ""),
            }
        ).eq("message_id", fan_msg_id).execute()
    parsed = hermes_blob["parsed"]

    verdict_search = (parsed.get("final_verdict_search") or "NO").upper()
    verdict_kairos = (parsed.get("final_verdict_kairos") or "FULL").upper()
    research_brief = parsed.get("web_research_brief") or "NONE"
    need_kairos = verdict_kairos != "SKIP"
    need_web = verdict_search == "YES"

    # Idempotent forks
    if need_kairos:
        kairos_mode = "lite" if verdict_kairos == "LITE" else "full"
        if not job_exists(KAIROS_QUEUE, fan_msg_id, client=SB):
            send(KAIROS_QUEUE, {"message_id": fan_msg_id, "kairos_mode": kairos_mode})

    if need_web:
        if not job_exists(WEB_QUEUE, fan_msg_id, client=SB):
            send(WEB_QUEUE, {"message_id": fan_msg_id, "brief": research_brief})

    if not need_kairos and not need_web:
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
