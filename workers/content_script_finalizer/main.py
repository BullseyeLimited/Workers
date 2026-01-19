"""
Script finalizer worker — assigns stage/sequence and updates content_scripts metadata.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from supabase import ClientOptions, create_client

from workers.lib.json_utils import safe_parse_model_json
from workers.lib.simple_queue import ack, receive, send


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for script finalizer")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "content.script_finalize"

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"

RUNPOD_URL = os.getenv("RUNPOD_URL", "").rstrip("/")
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
RUNPOD_MODEL_NAME = os.getenv("RUNPOD_MODEL_NAME")

SCRIPT_MODEL = os.getenv("CONTENT_SCRIPT_MODEL") or RUNPOD_MODEL_NAME

MAX_RETRIES = int(os.getenv("CONTENT_SCRIPT_MAX_RETRIES", "2"))

CONTROLLED_EXPLICITNESS = {"sfw", "tease", "nsfw"}
CONTROLLED_TIME_OF_DAY = {"day", "night", "anytime"}
CONTROLLED_OUTFIT_CATEGORY = {
    "casual",
    "sleepwear",
    "athleisure",
    "gymwear",
    "swimwear",
    "dress",
    "lingerie",
    "robe_or_towel",
    "cosplay",
    "nude_or_topless",
    "unknown",
}
CONTROLLED_CAMERA_ANGLE = {
    "front_selfie",
    "mirror_selfie",
    "over_shoulder",
    "high_angle",
    "low_angle",
    "eye_level",
    "top_down",
    "side_profile",
    "back_view",
    "tripod_static",
    "handheld",
}
CONTROLLED_STAGE = {"setup", "tease", "build", "climax", "after"}


def _normalize_header(value: str) -> str:
    cleaned = str(value).strip().lower()
    cleaned = cleaned.strip(" \t:;-")
    cleaned = cleaned.replace("_", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned


SCRIPT_HEADER_ALIASES = {
    "title": ("title",),
    "summary": ("summary",),
    "time_of_day": ("time of day", "time_of_day"),
    "location_primary": ("location primary", "location_primary"),
    "outfit_category": ("outfit category", "outfit_category"),
    "focus_tags": ("focus tags", "focus_tags"),
}

ITEM_HEADER_ALIASES = {
    "id": ("id", "item id", "item_id"),
    "stage": ("stage",),
    "sequence_position": ("sequence position", "sequence_position", "sequence"),
}

SCRIPT_FIELD_MAP = {
    _normalize_header(alias): field
    for field, aliases in SCRIPT_HEADER_ALIASES.items()
    for alias in aliases
}
ITEM_FIELD_MAP = {
    _normalize_header(alias): field
    for field, aliases in ITEM_HEADER_ALIASES.items()
    for alias in aliases
}


def _load_prompt(filename: str) -> str:
    path = PROMPTS_DIR / filename
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _normalize_tag(value: str) -> str:
    cleaned = str(value).strip().lower().replace("_", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        values = value
    elif isinstance(value, str):
        if "," in value:
            values = [v.strip() for v in value.split(",")]
        else:
            values = [v.strip() for v in value.split()]
    else:
        values = [value]
    cleaned: List[str] = []
    for item in values:
        if item is None:
            continue
        tag = _normalize_tag(item)
        if not tag or tag in cleaned:
            continue
        cleaned.append(tag)
    return cleaned


def _normalize_controlled(value: Any, allowed: set[str]) -> Optional[str]:
    if value is None:
        return None
    text = _normalize_tag(value)
    if not text:
        return None
    return text if text in allowed else None


def _normalize_time_of_day(value: Any) -> Optional[str]:
    """
    Normalize time_of_day to our 3-way vocabulary: day | night | anytime.
    Accept legacy values (morning/afternoon/evening) and common synonyms.
    """

    token = _normalize_tag(value)
    if not token:
        return None

    if token in {"day", "daytime", "day time", "daylight"}:
        return "day"
    if token in {"night", "nighttime", "night time"}:
        return "night"
    if token in {"any", "anytime", "unspecified", "unknown"}:
        return "anytime"

    if token in {"morning", "afternoon", "noon"}:
        return "day"
    if token in {"evening", "sunset", "twilight", "dusk"}:
        return "night"

    return token if token in CONTROLLED_TIME_OF_DAY else None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _extract_payload(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    return data


def _parse_header_output(text: str) -> Optional[Dict[str, Any]]:
    lines = text.splitlines()
    section: Optional[str] = None
    current_field: Optional[str] = None
    buffer: List[str] = []

    script_update: Dict[str, Any] = {}
    items: List[Dict[str, Any]] = []
    current_item: Optional[Dict[str, Any]] = None

    def flush_field() -> None:
        nonlocal buffer, current_field
        if not current_field:
            buffer = []
            return
        value = "\n".join(buffer).strip()
        if section == "script":
            script_update[current_field] = value
        elif section == "item" and current_item is not None:
            current_item[current_field] = value
        buffer = []

    def flush_item() -> None:
        nonlocal current_item
        if current_item:
            items.append(current_item)
        current_item = None

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if current_field:
                buffer.append("")
            continue

        normalized = _normalize_header(stripped)

        if normalized in {"script update", "script_update"}:
            flush_field()
            flush_item()
            section = "script"
            current_field = None
            continue
        if normalized in {"item updates", "item_updates", "items"}:
            flush_field()
            flush_item()
            section = "item"
            current_field = None
            continue
        if normalized in {"item", "item update", "item_update"}:
            flush_field()
            flush_item()
            section = "item"
            current_item = {}
            current_field = None
            continue
        if normalized in {"end", "### end"}:
            flush_field()
            flush_item()
            break

        if ":" in stripped:
            left, right = stripped.split(":", 1)
            normalized_left = _normalize_header(left)
            if section == "script" and normalized_left in SCRIPT_FIELD_MAP:
                flush_field()
                current_field = SCRIPT_FIELD_MAP[normalized_left]
                buffer.append(right.strip())
                continue
            if section == "item" and normalized_left in ITEM_FIELD_MAP:
                if current_item is None:
                    current_item = {}
                flush_field()
                current_field = ITEM_FIELD_MAP[normalized_left]
                buffer.append(right.strip())
                continue

        if section == "script" and normalized in SCRIPT_FIELD_MAP:
            flush_field()
            current_field = SCRIPT_FIELD_MAP[normalized]
            continue
        if section == "item" and normalized in ITEM_FIELD_MAP:
            if current_item is None:
                current_item = {}
            flush_field()
            current_field = ITEM_FIELD_MAP[normalized]
            continue

        if current_field:
            buffer.append(stripped)

    flush_field()
    flush_item()

    if not script_update and not items:
        return None
    return {
        "content_scripts_update": script_update,
        "content_items_updates": items,
    }


def _sanitize_script_update(update: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "title",
        "summary",
        "time_of_day",
        "location_primary",
        "outfit_category",
        "focus_tags",
    }
    cleaned: Dict[str, Any] = {}
    for field in allowed:
        if field not in update:
            continue
        value = update[field]
        if field in {"title", "summary", "location_primary"}:
            cleaned[field] = value.strip() if isinstance(value, str) else value
            continue
        if field == "time_of_day":
            normalized = _normalize_time_of_day(value)
            if normalized is not None:
                cleaned[field] = normalized
            continue
        if field == "outfit_category":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_OUTFIT_CATEGORY)
            continue
        if field == "focus_tags":
            cleaned[field] = _normalize_list(value)
            continue
    return cleaned


def _sanitize_item_update(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(update, dict):
        return None
    content_id = _coerce_int(update.get("id"))
    if content_id is None:
        return None

    allowed = {
        "stage",
        "sequence_position",
        "explicitness",
        "time_of_day",
        "location_primary",
        "location_tags",
        "outfit_category",
        "outfit_layers",
        "mood_tags",
        "action_tags",
        "body_focus",
        "camera_angle",
    }
    cleaned: Dict[str, Any] = {"id": content_id}
    for field in allowed:
        if field not in update:
            continue
        value = update[field]
        if field == "stage":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_STAGE)
            continue
        if field == "sequence_position":
            cleaned[field] = _coerce_int(value)
            continue
        if field == "explicitness":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_EXPLICITNESS)
            continue
        if field == "time_of_day":
            normalized = _normalize_time_of_day(value)
            if normalized is not None:
                cleaned[field] = normalized
            continue
        if field == "outfit_category":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_OUTFIT_CATEGORY)
            continue
        if field == "camera_angle":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_CAMERA_ANGLE)
            continue
        if field in {"location_primary"}:
            cleaned[field] = value.strip() if isinstance(value, str) else value
            continue
        if field in {"location_tags", "outfit_layers", "mood_tags", "action_tags", "body_focus"}:
            cleaned[field] = _normalize_list(value)
            continue
    return cleaned


def _run_model(prompt: str, script_row: Dict[str, Any], items: List[Dict[str, Any]]) -> Tuple[str, Optional[str]]:
    if not RUNPOD_URL:
        return "", "runpod_url_missing"
    if not RUNPOD_API_KEY:
        return "", "runpod_api_key_missing"
    if not SCRIPT_MODEL:
        return "", "model_not_configured"

    user_content = (
        "<SCRIPT>\n"
        f"{json.dumps(script_row, ensure_ascii=False)}\n"
        "</SCRIPT>\n\n"
        "<SCRIPT_ITEMS>\n"
        f"{json.dumps(items, ensure_ascii=False)}\n"
        "</SCRIPT_ITEMS>"
    )

    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
    }
    payload = {
        "model": SCRIPT_MODEL,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": float(os.getenv("CONTENT_TEMPERATURE", "0.2")),
        "max_tokens": int(os.getenv("CONTENT_MAX_TOKENS", "2000")),
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return "", f"runpod_error: {exc}"

    raw_text = ""
    try:
        choice0 = (data.get("choices") or [{}])[0] or {}
        msg = choice0.get("message") or {}
        raw_text = (
            msg.get("content")
            or msg.get("reasoning")
            or msg.get("reasoning_content")
            or choice0.get("text")
            or ""
        )
    except Exception:
        raw_text = ""
    raw_text = raw_text.strip()
    if not raw_text:
        return "", "model_empty_output"
    return raw_text, None


def _update_finalize_status(
    script_id: str,
    *,
    status: str,
    error: Optional[str],
    attempts: int,
) -> None:
    SB.table("content_scripts").update(
        {
            "finalize_status": status,
            "finalize_error": error,
            "finalize_attempts": attempts,
            "finalize_updated_at": datetime.now(timezone.utc).isoformat(),
        }
    ).eq("id", script_id).execute()


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload or "script_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")
    script_id = str(payload["script_id"])

    script_rows = (
        SB.table("content_scripts")
        .select("*")
        .eq("id", script_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not script_rows:
        return True
    script_row = script_rows[0]

    status = (script_row.get("finalize_status") or "").lower().strip()
    attempts = _coerce_int(script_row.get("finalize_attempts")) or 0
    if status == "ok":
        return True

    items = (
        SB.table("content_items")
        .select("*")
        .eq("script_id", script_id)
        .order("id", desc=False)
        .execute()
        .data
        or []
    )
    if not items:
        _update_finalize_status(
            script_id,
            status="failed",
            error="no_items_for_script",
            attempts=attempts + 1,
        )
        return True

    prompt = _load_prompt("content_script_finalizer.txt")
    if not prompt:
        _update_finalize_status(
            script_id,
            status="failed",
            error="prompt_missing:content_script_finalizer.txt",
            attempts=attempts + 1,
        )
        return True

    raw_text, error = _run_model(prompt, script_row, items)
    if error:
        attempts += 1
        status = "pending" if attempts <= MAX_RETRIES else "failed"
        _update_finalize_status(script_id, status=status, error=error, attempts=attempts)
        if status == "pending":
            send(QUEUE, {"script_id": script_id})
        return True

    payload_data = _parse_header_output(raw_text)
    parse_error = None
    if not payload_data:
        data, parse_error = safe_parse_model_json(raw_text)
        if not parse_error and data:
            payload_data = _extract_payload(data)

    if not payload_data:
        attempts += 1
        status = "pending" if attempts <= MAX_RETRIES else "failed"
        error_text = f"parse_error:{parse_error}" if parse_error else "parse_error:header_missing"
        _update_finalize_status(script_id, status=status, error=error_text, attempts=attempts)
        if status == "pending":
            send(QUEUE, {"script_id": script_id})
        return True

    script_update_raw = payload_data.get("content_scripts_update") or {}
    items_update_raw = payload_data.get("content_items_updates") or []

    script_update = (
        _sanitize_script_update(script_update_raw)
        if isinstance(script_update_raw, dict)
        else {}
    )
    if script_update:
        SB.table("content_scripts").update(script_update).eq("id", script_id).execute()

    if isinstance(items_update_raw, list):
        for item_update in items_update_raw:
            clean_update = _sanitize_item_update(item_update)
            if not clean_update or "id" not in clean_update:
                continue
            content_id = clean_update.pop("id")
            if not clean_update:
                continue
            SB.table("content_items").update(clean_update).eq("id", content_id).execute()

    _update_finalize_status(script_id, status="ok", error=None, attempts=attempts)
    return True


if __name__ == "__main__":
    print("[content_script_finalizer] started - waiting for jobs", flush=True)
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        row_id = job["row_id"]
        payload = job["payload"]
        try:
            ok = process_job(payload)
            if ok:
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("[content_script_finalizer] error:", exc, flush=True)
            traceback.print_exc()
            time.sleep(2)
