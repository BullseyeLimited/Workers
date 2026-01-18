"""
Content ingestor worker — enriches content_items rows using media-aware prompts.
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from supabase import ClientOptions, create_client

from workers.lib.job_utils import job_exists
from workers.lib.json_utils import safe_parse_model_json
from workers.lib.simple_queue import ack, receive, send
from workers.content_script_finalizer.main import process_job as process_finalize_job


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
HANDLE_SCRIPT_FINALIZE = os.getenv("CONTENT_HANDLE_SCRIPT_FINALIZE", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"

RUNPOD_URL = os.getenv("RUNPOD_URL", "").rstrip("/")
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY") or os.getenv("OPENAI_API_KEY")
RUNPOD_MODEL_NAME = os.getenv("RUNPOD_MODEL_NAME")

PHOTO_MODEL = os.getenv("CONTENT_PHOTO_MODEL") or RUNPOD_MODEL_NAME
VIDEO_MODEL = os.getenv("CONTENT_VIDEO_MODEL") or RUNPOD_MODEL_NAME
VOICE_MODEL = os.getenv("CONTENT_VOICE_MODEL") or RUNPOD_MODEL_NAME

MAX_RETRIES = int(os.getenv("CONTENT_INGEST_MAX_RETRIES", "2"))

RUNPOD_GATE_ENABLED = os.getenv("CONTENT_RUNPOD_GATE_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
RUNPOD_HEALTHCHECK_PATH = os.getenv("CONTENT_RUNPOD_HEALTHCHECK_PATH", "/v1/models")
RUNPOD_HEALTHCHECK_TIMEOUT_SECONDS = float(
    os.getenv("CONTENT_RUNPOD_HEALTHCHECK_TIMEOUT_SECONDS", "2")
)
RUNPOD_GATE_SLEEP_SECONDS = float(os.getenv("CONTENT_RUNPOD_GATE_SLEEP_SECONDS", "5"))
RUNPOD_HEALTHCHECK_CACHE_SECONDS = float(
    os.getenv("CONTENT_RUNPOD_HEALTHCHECK_CACHE_SECONDS", "5")
)

INGEST_SWEEPER_ENABLED = os.getenv("CONTENT_INGEST_SWEEPER_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
INGEST_SWEEPER_INTERVAL_SECONDS = float(os.getenv("CONTENT_INGEST_SWEEPER_INTERVAL_SECONDS", "60"))
INGEST_SWEEPER_BATCH_SIZE = int(os.getenv("CONTENT_INGEST_SWEEPER_BATCH_SIZE", "25"))
INGEST_STALE_PROCESSING_SECONDS = int(os.getenv("CONTENT_INGEST_STALE_PROCESSING_SECONDS", "900"))

WORKER_ID = (
    os.getenv("WORKER_ID")
    or os.getenv("FLY_ALLOC_ID")
    or os.getenv("FLY_MACHINE_ID")
    or os.getenv("HOSTNAME")
    or "unknown"
)

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
    if media_type in {"photo", "video", "voice"}:
        return media_type

    mime = (row.get("mimetype") or "").lower().strip()
    if mime.startswith("image/"):
        return "photo"
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("audio/"):
        return "voice"

    url_raw = row.get("url_main") or row.get("url_thumb") or ""
    url = str(url_raw).lower()
    url_path = urlparse(url).path.lower() if url else ""
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if url_path.endswith(ext):
            return "photo"
    for ext in (".mp4", ".mov", ".m4v", ".webm"):
        if url_path.endswith(ext):
            return "video"
    for ext in (".mp3", ".m4a", ".wav", ".aac", ".ogg", ".opus"):
        if url_path.endswith(ext):
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


_LIST_SPLIT_RE = re.compile(r"[,\n;|]+")

_last_runpod_check_at = 0.0
_last_runpod_check_ok = False
_last_runpod_check_error: Optional[str] = None
_last_runpod_gate_notice: Optional[str] = None
_last_sweep_at = 0.0

_content_items_columns: Optional[set[str]] = None

_UNSET = object()


def _load_content_items_columns() -> set[str]:
    global _content_items_columns
    if _content_items_columns is not None:
        return _content_items_columns
    try:
        rows = SB.table("content_items").select("*").limit(1).execute().data or []
    except Exception:
        rows = []
    if rows and isinstance(rows[0], dict):
        _content_items_columns = set(rows[0].keys())
    else:
        # Fall back to the core fields this worker relies on.
        _content_items_columns = {
            "id",
            "ingest_status",
            "ingest_error",
            "ingest_attempts",
            "ingest_updated_at",
        }
    return _content_items_columns


def _filter_content_items_update(update: Dict[str, Any]) -> Dict[str, Any]:
    allowed = _load_content_items_columns()
    return {k: v for k, v in update.items() if k in allowed}


def _runpod_healthcheck_url() -> str:
    path = RUNPOD_HEALTHCHECK_PATH or ""
    if not path:
        return RUNPOD_URL
    if path.startswith("/"):
        return f"{RUNPOD_URL}{path}"
    return f"{RUNPOD_URL}/{path}"


def _set_runpod_unavailable_cache(error: Optional[str]) -> None:
    global _last_runpod_check_at, _last_runpod_check_ok, _last_runpod_check_error
    _last_runpod_check_at = time.time()
    _last_runpod_check_ok = False
    _last_runpod_check_error = error


def _runpod_is_reachable() -> Tuple[bool, Optional[str]]:
    """
    Cheap reachability probe so we don't consume/mark jobs while the pod is off.
    Returns (reachable, error_code_or_message).
    """
    global _last_runpod_check_at, _last_runpod_check_ok, _last_runpod_check_error

    now = time.time()
    if now - _last_runpod_check_at < RUNPOD_HEALTHCHECK_CACHE_SECONDS:
        return _last_runpod_check_ok, _last_runpod_check_error

    _last_runpod_check_at = now

    if not RUNPOD_URL:
        _last_runpod_check_ok = False
        _last_runpod_check_error = "runpod_url_missing"
        return _last_runpod_check_ok, _last_runpod_check_error

    # If auth/model config is missing, don't claim jobs. Keep everything pending until fixed.
    if not RUNPOD_API_KEY:
        _last_runpod_check_ok = False
        _last_runpod_check_error = "runpod_api_key_missing"
        return _last_runpod_check_ok, _last_runpod_check_error

    if not (PHOTO_MODEL or VIDEO_MODEL or VOICE_MODEL):
        _last_runpod_check_ok = False
        _last_runpod_check_error = "model_not_configured"
        return _last_runpod_check_ok, _last_runpod_check_error

    health_url = _runpod_healthcheck_url()
    headers = {"Authorization": f"Bearer {RUNPOD_API_KEY}"} if RUNPOD_API_KEY else {}
    try:
        # Any HTTP response means the server is up; we only care about network reachability here.
        requests.get(health_url, headers=headers, timeout=RUNPOD_HEALTHCHECK_TIMEOUT_SECONDS)
    except requests.exceptions.RequestException as exc:  # noqa: BLE001
        _last_runpod_check_ok = False
        _last_runpod_check_error = f"runpod_unreachable: {exc}"
        return _last_runpod_check_ok, _last_runpod_check_error

    _last_runpod_check_ok = True
    _last_runpod_check_error = None
    return _last_runpod_check_ok, _last_runpod_check_error


def _normalize_tag(value: str) -> str:
    """
    Normalize a tag-like string to a stable snake_case token.

    Notes:
    - Keep `_` as part of the token (underscore joins words).
    - Treat commas/newlines/semicolons as separators (handled in _normalize_list).
    """
    text = str(value).strip().lower()
    if not text:
        return ""
    # Trim common wrappers/bullets the model might output.
    text = text.strip(" \t-–—•*[](){}<>\"'")
    # Normalize separators to underscores.
    text = re.sub(r"[\s\-\/]+", "_", text)
    # Remove anything that's not alphanumeric or underscore.
    text = re.sub(r"[^a-z0-9_]+", "", text)
    # Collapse repeats.
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _normalize_header(value: str) -> str:
    cleaned = str(value).strip().lower()
    cleaned = cleaned.strip(" \t:;-—–")
    cleaned = cleaned.replace("_", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned


HEADER_ALIASES = {
    "title": ("title",),
    "desc_short": ("short description", "short_desc", "desc_short"),
    "desc_long": ("long description", "long_desc", "desc_long"),
    "explicitness": ("explicitness", "explicitness rating"),
    "time_of_day": ("time of day", "time_of_day", "timeofday"),
    "location_primary": ("location primary", "primary location", "location_primary"),
    "location_tags": ("location tags", "location_tags"),
    "outfit_category": ("outfit category", "outfit_category"),
    "outfit_layers": ("outfit layers", "outfit_layers"),
    "mood_tags": ("mood tags", "mood_tags"),
    "action_tags": ("action tags", "action_tags"),
    "body_focus": ("body focus", "body_focus"),
    "camera_angle": ("camera angle", "camera_angle"),
    "shot_type": ("shot type", "shot_type"),
    "lighting": ("lighting",),
    "duration_seconds": ("duration seconds", "duration_seconds", "duration"),
    "voice_transcript": ("voice transcript", "voice_transcript", "transcript"),
}
HEADER_FIELD_MAP = {
    _normalize_header(alias): field
    for field, aliases in HEADER_ALIASES.items()
    for alias in aliases
}


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        values = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        values = [v.strip() for v in _LIST_SPLIT_RE.split(raw) if v.strip()]
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


def _normalize_token(value: Any) -> Optional[str]:
    """
    Normalize a single token-like scalar (e.g., explicitness, time_of_day) without
    enforcing a controlled vocabulary.
    """
    if value is None:
        return None
    text = _normalize_tag(value)
    return text or None


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


def _parse_header_output(text: str) -> Dict[str, Any]:
    lines = text.splitlines()
    update: Dict[str, Any] = {}
    current_field: Optional[str] = None
    buffer: List[str] = []

    def flush() -> None:
        nonlocal buffer, current_field
        if not current_field:
            buffer = []
            return
        value = "\n".join(buffer).strip()
        update[current_field] = value
        buffer = []

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if current_field:
                buffer.append("")
            continue

        normalized = _normalize_header(stripped)
        if normalized in HEADER_FIELD_MAP:
            flush()
            current_field = HEADER_FIELD_MAP[normalized]
            continue

        if ":" in stripped:
            left, right = stripped.split(":", 1)
            normalized_left = _normalize_header(left)
            if normalized_left in HEADER_FIELD_MAP:
                flush()
                current_field = HEADER_FIELD_MAP[normalized_left]
                buffer.append(right.strip())
                continue

        for sep in (" - ", " — ", " – "):
            if sep in stripped:
                left, right = stripped.split(sep, 1)
                normalized_left = _normalize_header(left)
                if normalized_left in HEADER_FIELD_MAP:
                    flush()
                    current_field = HEADER_FIELD_MAP[normalized_left]
                    buffer.append(right.strip())
                    break
        else:
            if current_field:
                buffer.append(stripped)

    flush()
    return {k: v for k, v in update.items() if v != ""}


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
        if field in {"explicitness", "time_of_day", "outfit_category", "camera_angle", "shot_type", "lighting"}:
            cleaned[field] = _normalize_token(value)
            continue
    # Drop empty lists to avoid noisy updates.
    for field in list(cleaned.keys()):
        if isinstance(cleaned[field], list) and not cleaned[field]:
            cleaned[field] = []
    return cleaned


def _build_user_content(media_type: str, row: Dict[str, Any], url: str):
    raise NotImplementedError


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
    messages: List[Dict[str, Any]],
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
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout as exc:
        return "", f"runpod_timeout: {exc}"
    except requests.exceptions.ConnectionError as exc:
        return "", f"runpod_unreachable: {exc}"
    except requests.exceptions.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        try:
            code_int = int(status_code) if status_code is not None else None
        except Exception:
            code_int = None
        if code_int is not None and code_int >= 500:
            return "", f"runpod_unavailable_http: {code_int}"
        return "", f"runpod_error: {exc}"
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


def _build_messages(media_type: str, prompt: str, row: Dict[str, Any], url: str) -> List[Dict[str, Any]]:
    if media_type == "video":
        # Video models often require the media to be attached via `video_url` content.
        # We also include VIDEO_URL text to stay compatible with the prompt format.
        user_text = f"{prompt}\n\nVIDEO_URL:\n{url}"
        return [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {"type": "video_url", "video_url": {"url": url}},
                ],
            }
        ]

    if media_type == "photo":
        return [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"IMAGE_URL:\n{url}"},
                    {"type": "image_url", "image_url": {"url": url}},
                ],
            },
        ]

    if media_type == "voice":
        return [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"VOICE_URL:\n{url}"},
        ]

    return [{"role": "system", "content": prompt}, {"role": "user", "content": str(url)}]


def _run_model(
    media_type: str, prompt: str, row: Dict[str, Any], url: str, *, model: str
) -> Tuple[str, Optional[str]]:
    messages = _build_messages(media_type, prompt, row, url)

    temperature = float(os.getenv("CONTENT_TEMPERATURE", "0.2"))
    if media_type == "video":
        max_tokens = int(os.getenv("CONTENT_VIDEO_MAX_TOKENS", "30000"))
    else:
        max_tokens = int(os.getenv("CONTENT_MAX_TOKENS", "2000"))

    raw_text, error = _runpod_call(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
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
    started_at: Any = _UNSET,
    completed_at: Any = _UNSET,
    duration_ms: Any = _UNSET,
    worker_id: Any = _UNSET,
    model: Any = _UNSET,
    prompt: Any = _UNSET,
    error_details: Any = _UNSET,
) -> None:
    update: Dict[str, Any] = {
        "ingest_status": status,
        "ingest_error": error,
        "ingest_attempts": attempts,
        "ingest_updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if started_at is not _UNSET:
        update["ingest_started_at"] = started_at
    if completed_at is not _UNSET:
        update["ingest_completed_at"] = completed_at
    if duration_ms is not _UNSET:
        update["ingest_duration_ms"] = duration_ms
    if worker_id is not _UNSET:
        update["ingest_worker_id"] = worker_id
    if model is not _UNSET:
        update["ingest_model"] = model
    if prompt is not _UNSET:
        update["ingest_prompt"] = prompt
    if error_details is not _UNSET:
        update["ingest_error_details"] = error_details

    SB.table("content_items").update(_filter_content_items_update(update)).eq("id", content_id).execute()


def _reset_stale_processing_items(*, limit: int) -> int:
    if limit <= 0:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=INGEST_STALE_PROCESSING_SECONDS)).isoformat()
    try:
        rows = (
            SB.table("content_items")
            .select("id,ingest_attempts,ingest_updated_at")
            .eq("ingest_status", "processing")
            .lt("ingest_updated_at", cutoff)
            .order("id", desc=False)
            .limit(limit)
            .execute()
            .data
            or []
        )
    except Exception:
        return 0

    reset_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        content_id = _coerce_int(row.get("id"))
        if content_id is None:
            continue
        attempts = _coerce_int(row.get("ingest_attempts")) or 0
        _update_ingest_status(
            content_id,
            status="pending",
            error="stale_processing_reset",
            attempts=attempts,
            completed_at=datetime.now(timezone.utc).isoformat(),
            worker_id=WORKER_ID,
        )
        send(QUEUE, {"content_id": content_id})
        reset_count += 1
    return reset_count


def _enqueue_missing_pending_ingest_jobs(*, limit: int) -> int:
    if limit <= 0:
        return 0
    try:
        rows = (
            SB.table("content_items")
            .select("id,url_main,url_thumb,ingest_status")
            .eq("ingest_status", "pending")
            .order("id", desc=False)
            .limit(limit)
            .execute()
            .data
            or []
        )
    except Exception:
        return 0

    enqueued = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        content_id = _coerce_int(row.get("id"))
        if content_id is None:
            continue
        if not (row.get("url_main") or row.get("url_thumb")):
            continue
        if job_exists(QUEUE, content_id, client=SB, field="content_id"):
            continue
        send(QUEUE, {"content_id": content_id})
        enqueued += 1
    return enqueued


def _maybe_run_sweeper() -> None:
    global _last_sweep_at
    if not INGEST_SWEEPER_ENABLED:
        return
    now = time.time()
    if now - _last_sweep_at < INGEST_SWEEPER_INTERVAL_SECONDS:
        return
    _last_sweep_at = now

    reset_count = _reset_stale_processing_items(limit=INGEST_SWEEPER_BATCH_SIZE)
    enqueued = _enqueue_missing_pending_ingest_jobs(limit=INGEST_SWEEPER_BATCH_SIZE)
    if reset_count or enqueued:
        print(
            f"[content_ingestor] sweeper: reset={reset_count} enqueued={enqueued}",
            flush=True,
        )


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

    model = _select_model(media_type)
    if not model:
        _update_ingest_status(
            content_id,
            status="failed",
            error="model_not_configured",
            attempts=attempts,
        )
        return True

    # Mark row as in-flight only after we have everything we need.
    attempt_started_at = datetime.now(timezone.utc)
    attempt_started_monotonic = time.monotonic()
    _update_ingest_status(
        content_id,
        status="processing",
        error=None,
        attempts=attempts,
        started_at=attempt_started_at.isoformat(),
        completed_at=None,
        duration_ms=None,
        worker_id=WORKER_ID,
        model=model,
        prompt=prompt_name,
        error_details=None,
    )

    raw_text, error = _run_model(media_type, prompt, row, url, model=model)
    duration_ms = int((time.monotonic() - attempt_started_monotonic) * 1000)
    attempt_completed_at = datetime.now(timezone.utc).isoformat()
    if error:
        err_lower = str(error).lower()
        is_unavailable = err_lower.startswith("runpod_unreachable:") or err_lower.startswith(
            "runpod_timeout:"
        )
        is_unavailable = is_unavailable or err_lower.startswith("runpod_unavailable_http:")
        if is_unavailable:
            # Pod is off/unreachable. Do not count attempts or mark failed.
            _set_runpod_unavailable_cache(error)
            _update_ingest_status(
                content_id,
                status="pending",
                error="runpod_unavailable",
                attempts=attempts,
                completed_at=attempt_completed_at,
                duration_ms=duration_ms,
                worker_id=WORKER_ID,
                error_details=str(error),
            )
            # Returning False prevents ack; the same queue item will reappear after VT.
            return False

        attempts += 1
        status = "pending" if attempts <= MAX_RETRIES else "failed"
        _update_ingest_status(
            content_id,
            status=status,
            error=error,
            attempts=attempts,
            completed_at=attempt_completed_at,
            duration_ms=duration_ms,
            worker_id=WORKER_ID,
            error_details=None,
        )
        if status == "pending":
            send(QUEUE, {"content_id": content_id})
        return True

    data, parse_error = safe_parse_model_json(raw_text)
    update = None
    if data and not parse_error:
        update = _extract_update(data, content_id)
    if not update:
        header_update = _parse_header_output(raw_text)
        if header_update:
            header_update.setdefault("id", content_id)
            update = header_update
        else:
            attempts += 1
            status = "pending" if attempts <= MAX_RETRIES else "failed"
            error = parse_error or "parse_error:no_header_fields"
            _update_ingest_status(
                content_id,
                status=status,
                error=str(error),
                attempts=attempts,
                completed_at=attempt_completed_at,
                duration_ms=duration_ms,
                worker_id=WORKER_ID,
                error_details=None,
            )
            if status == "pending":
                send(QUEUE, {"content_id": content_id})
            return True

    if "id" not in update:
        update["id"] = content_id
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

    _update_ingest_status(
        content_id,
        status="ok",
        error=None,
        attempts=attempts,
        completed_at=attempt_completed_at,
        duration_ms=duration_ms,
        worker_id=WORKER_ID,
        error_details=None,
    )

    script_id = row.get("script_id")
    if script_id:
        _maybe_enqueue_script_finalize(str(script_id))

    return True


if __name__ == "__main__":
    print("[content_ingestor] started - waiting for jobs", flush=True)
    prefer_finalize = True
    while True:
        _maybe_run_sweeper()

        if HANDLE_SCRIPT_FINALIZE:
            queue_order = (SCRIPT_QUEUE, QUEUE) if prefer_finalize else (QUEUE, SCRIPT_QUEUE)
            job = receive(queue_order[0], 30) or receive(queue_order[1], 30)
            prefer_finalize = not prefer_finalize
        else:
            job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        row_id = job["row_id"]
        queue_name = job.get("queue") or QUEUE
        payload = job["payload"]

        if RUNPOD_GATE_ENABLED and queue_name == QUEUE:
            reachable, gate_error = _runpod_is_reachable()
            if not reachable:
                notice = gate_error or "runpod_unreachable"
                if notice != _last_runpod_gate_notice:
                    print(
                        f"[content_ingestor] runpod not ready ({notice}); waiting…",
                        flush=True,
                    )
                    _last_runpod_gate_notice = notice
                try:
                    # Release the job immediately so we don't hold it invisible while the pod is off.
                    SB.table("job_queue").update(
                        {"available_at": datetime.now(timezone.utc).isoformat()}
                    ).eq("id", row_id).execute()
                except Exception:
                    pass
                time.sleep(RUNPOD_GATE_SLEEP_SECONDS)
                continue

        try:
            if queue_name == QUEUE:
                ok = process_job(payload)
            else:
                ok = process_finalize_job(payload)
            if ok:
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("[content_ingestor] error:", exc, flush=True)
            traceback.print_exc()
            time.sleep(2)
