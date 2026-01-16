"""
Content ingestor worker — enriches content_items rows using media-aware prompts.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from supabase import ClientOptions, create_client

from workers.lib.job_utils import job_exists
from workers.lib.json_utils import safe_parse_model_json
from workers.lib.simple_queue import ack, receive, send


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for content ingestor")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "content.ingest"
SCRIPT_QUEUE = "content.script_finalize"

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"

RUNPOD_URL = os.getenv("RUNPOD_URL", "").rstrip("/")
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
RUNPOD_MODEL_NAME = os.getenv("RUNPOD_MODEL_NAME")

PHOTO_MODEL = os.getenv("CONTENT_PHOTO_MODEL") or RUNPOD_MODEL_NAME
VIDEO_MODEL = os.getenv("CONTENT_VIDEO_MODEL") or RUNPOD_MODEL_NAME
VOICE_MODEL = os.getenv("CONTENT_VOICE_MODEL") or RUNPOD_MODEL_NAME

MAX_RETRIES = int(os.getenv("CONTENT_INGEST_MAX_RETRIES", "2"))

MEDIA_TYPE_ALIASES = {
    "image": "photo",
    "img": "photo",
    "photo": "photo",
    "picture": "photo",
    "pic": "photo",
    "video": "video",
    "audio": "voice",
    "voice": "voice",
    "voice_note": "voice",
    "voicenote": "voice",
    "sound": "voice",
}

CONTROLLED_EXPLICITNESS = {"sfw", "tease", "nsfw"}
CONTROLLED_TIME_OF_DAY = {"morning", "afternoon", "evening", "night", "anytime"}
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
CONTROLLED_SHOT_TYPE = {
    "establishing_wide",
    "wide",
    "full_body",
    "three_quarter",
    "medium",
    "medium_close",
    "close_up",
    "detail_close",
    "face_close",
}
CONTROLLED_LIGHTING = {
    "natural_daylight",
    "golden_hour",
    "warm_indoor",
    "cool_indoor",
    "mixed_light",
    "low_light",
    "flash",
    "ring_light",
    "bathroom_vanity",
    "neon_or_colored",
}


def _load_prompt(filename: str) -> str:
    path = PROMPTS_DIR / filename
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _normalize_media_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = str(value).strip().lower()
    return MEDIA_TYPE_ALIASES.get(key, key)


def _infer_media_type(row: Dict[str, Any]) -> Optional[str]:
    media_type = _normalize_media_type(row.get("media_type"))
    if media_type:
        return media_type

    mime = (row.get("mimetype") or "").lower().strip()
    if mime.startswith("image/"):
        return "photo"
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("audio/"):
        return "voice"

    url = (row.get("url_main") or row.get("url_thumb") or "").lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if url.endswith(ext):
            return "photo"
    for ext in (".mp4", ".mov", ".m4v", ".webm"):
        if url.endswith(ext):
            return "video"
    for ext in (".mp3", ".m4a", ".wav", ".aac", ".ogg"):
        if url.endswith(ext):
            return "voice"
    return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None
    return str(value).strip() or None


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


def _extract_update(data: Dict[str, Any], content_id: int) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    if "CONTENT_ITEM_UPDATE" in data and isinstance(data["CONTENT_ITEM_UPDATE"], dict):
        payload = data["CONTENT_ITEM_UPDATE"]
    elif "content_item_update" in data and isinstance(data["content_item_update"], dict):
        payload = data["content_item_update"]
    else:
        payload = data
    if not isinstance(payload, dict):
        return None
    payload.setdefault("id", content_id)
    return payload


def _sanitize_update(update: Dict[str, Any]) -> Dict[str, Any]:
    allowed_fields = {
        "title",
        "desc_short",
        "desc_long",
        "duration_seconds",
        "voice_transcript",
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
        "shot_type",
        "lighting",
    }
    cleaned: Dict[str, Any] = {}
    for field in allowed_fields:
        if field not in update:
            continue
        value = update[field]
        if field in {"title", "desc_short", "desc_long", "voice_transcript", "location_primary"}:
            cleaned[field] = _clean_text(value)
            continue
        if field in {"location_tags", "outfit_layers", "mood_tags", "action_tags", "body_focus"}:
            cleaned[field] = _normalize_list(value)
            continue
        if field == "duration_seconds":
            cleaned[field] = _coerce_int(value)
            continue
        if field == "explicitness":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_EXPLICITNESS)
            continue
        if field == "time_of_day":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_TIME_OF_DAY)
            continue
        if field == "outfit_category":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_OUTFIT_CATEGORY)
            continue
        if field == "camera_angle":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_CAMERA_ANGLE)
            continue
        if field == "shot_type":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_SHOT_TYPE)
            continue
        if field == "lighting":
            cleaned[field] = _normalize_controlled(value, CONTROLLED_LIGHTING)
            continue
    # Drop empty lists to avoid noisy updates.
    for field in list(cleaned.keys()):
        if isinstance(cleaned[field], list) and not cleaned[field]:
            cleaned[field] = []
    return cleaned


def _build_user_content(media_type: str, row: Dict[str, Any], url: str):
    payload = json.dumps(row, ensure_ascii=False)
    if media_type == "photo":
        return [
            {
                "type": "text",
                "text": f"<CONTENT_ITEM>\n{payload}\n</CONTENT_ITEM>\n\nIMAGE_URL:\n{url}",
            },
            {"type": "image_url", "image_url": {"url": url}},
        ]
    if media_type == "video":
        return f"<CONTENT_ITEM>\n{payload}\n</CONTENT_ITEM>\n\nVIDEO_URL:\n{url}"
    if media_type == "voice":
        return f"<CONTENT_ITEM>\n{payload}\n</CONTENT_ITEM>\n\nVOICE_URL:\n{url}"
    return f"<CONTENT_ITEM>\n{payload}\n</CONTENT_ITEM>"


def _select_model(media_type: str) -> Optional[str]:
    if media_type == "photo":
        return PHOTO_MODEL
    if media_type == "video":
        return VIDEO_MODEL
    if media_type == "voice":
        return VOICE_MODEL
    return None


def _runpod_call(
    *,
    model: str,
    system_prompt: str,
    user_content: Any,
    temperature: float = 0.2,
    max_tokens: int = 2000,
) -> Tuple[str, Optional[str]]:
    """
    Call the RunPod vLLM OpenAI-compatible server using chat completions.
    Returns (raw_text, error_message).
    """
    if not RUNPOD_URL:
        return "", "runpod_url_missing"
    if not RUNPOD_API_KEY:
        return "", "runpod_api_key_missing"
    if not model:
        return "", "model_not_configured"

    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": float(os.getenv("CONTENT_TEMPERATURE", str(temperature))),
        "max_tokens": int(os.getenv("CONTENT_MAX_TOKENS", str(max_tokens))),
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
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
    return raw_text.strip(), None


def _run_model(media_type: str, prompt: str, row: Dict[str, Any], url: str) -> Tuple[str, Optional[str]]:
    model = _select_model(media_type)
    if not model:
        return "", "model_not_configured"

    user_content = _build_user_content(media_type, row, url)
    raw_text, error = _runpod_call(
        model=model,
        system_prompt=prompt,
        user_content=user_content,
        temperature=0.2,
        max_tokens=2000,
    )
    if error:
        return "", error
    if not raw_text:
        return "", "model_empty_output"
    return raw_text, None


def _update_ingest_status(
    content_id: int,
    *,
    status: str,
    error: Optional[str],
    attempts: int,
) -> None:
    SB.table("content_items").update(
        {
            "ingest_status": status,
            "ingest_error": error,
            "ingest_attempts": attempts,
            "ingest_updated_at": datetime.now(timezone.utc).isoformat(),
        }
    ).eq("id", content_id).execute()


def _maybe_enqueue_script_finalize(script_id: str) -> None:
    if not script_id:
        return
    # Skip if already enqueued or finalized
    if job_exists(SCRIPT_QUEUE, script_id, client=SB, field="script_id"):
        return
    script_rows = (
        SB.table("content_scripts")
        .select("id,finalize_status")
        .eq("id", script_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not script_rows:
        # Only finalize scripts that exist (scriptless/bubble-2 items should never enqueue this).
        return
    if script_rows and script_rows[0].get("finalize_status") == "ok":
        return
    pending = (
        SB.table("content_items")
        .select("id")
        .eq("script_id", script_id)
        .neq("ingest_status", "ok")
        .limit(1)
        .execute()
        .data
        or []
    )
    if pending:
        return
    send(SCRIPT_QUEUE, {"script_id": script_id})


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload or "content_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")
    content_id = payload["content_id"]

    rows = (
        SB.table("content_items")
        .select("*")
        .eq("id", content_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return True
    row = rows[0]

    status = (row.get("ingest_status") or "").lower().strip()
    attempts = _coerce_int(row.get("ingest_attempts")) or 0
    if status == "ok":
        return True

    media_type = _infer_media_type(row)
    if not media_type:
        _update_ingest_status(
            content_id,
            status="failed",
            error="unknown_media_type",
            attempts=attempts,
        )
        return True

    url = row.get("url_main") or row.get("url_thumb")
    if not url:
        _update_ingest_status(
            content_id,
            status="failed",
            error="missing_media_url",
            attempts=attempts,
        )
        return True

    prompt_name = f"content_{media_type}.txt"
    prompt = _load_prompt(prompt_name)
    if not prompt:
        _update_ingest_status(
            content_id,
            status="failed",
            error=f"prompt_missing:{prompt_name}",
            attempts=attempts,
        )
        return True

    raw_text, error = _run_model(media_type, prompt, row, url)
    if error:
        attempts += 1
        status = "pending" if attempts <= MAX_RETRIES else "failed"
        _update_ingest_status(content_id, status=status, error=error, attempts=attempts)
        if status == "pending":
            send(QUEUE, {"content_id": content_id})
        return True

    data, parse_error = safe_parse_model_json(raw_text)
    if parse_error or not data:
        attempts += 1
        status = "pending" if attempts <= MAX_RETRIES else "failed"
        _update_ingest_status(
            content_id,
            status=status,
            error=f"parse_error:{parse_error}",
            attempts=attempts,
        )
        if status == "pending":
            send(QUEUE, {"content_id": content_id})
        return True

    update = _extract_update(data, content_id)
    if not update:
        _update_ingest_status(
            content_id,
            status="failed",
            error="invalid_update_payload",
            attempts=attempts + 1,
        )
        return True

    update_payload = _sanitize_update(update)
    if update_payload:
        SB.table("content_items").update(update_payload).eq("id", content_id).execute()

    _update_ingest_status(content_id, status="ok", error=None, attempts=attempts)

    script_id = row.get("script_id")
    if script_id:
        _maybe_enqueue_script_finalize(str(script_id))

    return True


if __name__ == "__main__":
    print("[content_ingestor] started - waiting for jobs", flush=True)
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
            print("[content_ingestor] error:", exc, flush=True)
            traceback.print_exc()
            time.sleep(2)
