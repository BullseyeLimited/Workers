"""
Content ingestor worker — enriches content_items rows using media-aware prompts.
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
import uuid
import subprocess
import tempfile
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

VIDEO_DURATION_ENABLED = os.getenv("CONTENT_VIDEO_DURATION_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
VIDEO_DURATION_PROBE_BYTES = int(os.getenv("CONTENT_VIDEO_DURATION_PROBE_BYTES", "2097152"))
VIDEO_DURATION_TIMEOUT_SECONDS = float(os.getenv("CONTENT_VIDEO_DURATION_TIMEOUT_SECONDS", "10"))
VIDEO_DURATION_FULL_DOWNLOAD_MAX_BYTES = int(
    os.getenv("CONTENT_VIDEO_DURATION_FULL_DOWNLOAD_MAX_BYTES", "100000000")
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

CONTENT_WAV_SWEEPER_ENABLED = os.getenv("CONTENT_WAV_SWEEPER_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
CONTENT_WAV_SWEEPER_INTERVAL_SECONDS = float(
    os.getenv("CONTENT_WAV_SWEEPER_INTERVAL_SECONDS", str(INGEST_SWEEPER_INTERVAL_SECONDS))
)
CONTENT_WAV_SWEEPER_BATCH_SIZE = int(os.getenv("CONTENT_WAV_SWEEPER_BATCH_SIZE", "10"))
CONTENT_WAV_MAX_ATTEMPTS = int(os.getenv("CONTENT_WAV_MAX_ATTEMPTS", "3"))
CONTENT_WAV_SIGNED_URL_EXPIRES_SECONDS = int(
    os.getenv("CONTENT_WAV_SIGNED_URL_EXPIRES_SECONDS", "3600")
)
CONTENT_WAV_SAMPLE_RATE = int(os.getenv("CONTENT_WAV_SAMPLE_RATE", "16000"))
CONTENT_WAV_CHANNELS = int(os.getenv("CONTENT_WAV_CHANNELS", "1"))
CONTENT_WAV_AUTOGEN_ON_INGEST = os.getenv("CONTENT_WAV_AUTOGEN_ON_INGEST", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

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


def _csv_set(env: str, default: str) -> set[str]:
    raw = os.getenv(env, default)
    parts = [p.strip().lower() for p in str(raw).split(",")]
    return {p for p in parts if p}


# Script folder conventions (Bubble 1)
#   @handle/scripts/<script_slug>/core/<file>
#   @handle/scripts/<script_slug>/ammo/<file>
# Global ammo (Bubble 2)
#   @handle/ammo/<file>
SCRIPT_ROOT_FOLDERS = _csv_set("CONTENT_SCRIPT_ROOT_FOLDERS", "scripts")
SCRIPT_CORE_FOLDERS = _csv_set("CONTENT_SCRIPT_CORE_FOLDERS", "core")
SCRIPT_AMMO_FOLDERS = _csv_set("CONTENT_SCRIPT_AMMO_FOLDERS", "ammo")
GLOBAL_AMMO_FOLDERS = _csv_set("CONTENT_GLOBAL_AMMO_FOLDERS", "ammo")

SCRIPT_SHOOT_ID_NAMESPACE = uuid.NAMESPACE_URL


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


def _extract_mp4_duration_seconds_from_bytes(data: bytes) -> Optional[int]:
    """
    Extract duration from an MP4/MOV by parsing the `mvhd` atom.
    Returns integer seconds or None if not found.
    """
    if not data:
        return None

    start = 0
    needle = b"mvhd"
    while True:
        idx = data.find(needle, start)
        if idx == -1:
            return None

        box_start = idx - 4
        if box_start < 0:
            start = idx + 4
            continue

        if box_start + 8 > len(data):
            start = idx + 4
            continue

        size = int.from_bytes(data[box_start : box_start + 4], "big", signed=False)
        header_size = 8
        if size == 1:
            # 64-bit extended size
            if box_start + 16 > len(data):
                start = idx + 4
                continue
            size = int.from_bytes(data[box_start + 8 : box_start + 16], "big", signed=False)
            header_size = 16

        fields_start = box_start + header_size
        if fields_start + 4 > len(data):
            start = idx + 4
            continue

        version = data[fields_start]
        if version == 0:
            # version/flags (4) + creation (4) + modification (4) + timescale (4) + duration (4)
            needed = 4 + 4 + 4 + 4 + 4
            if fields_start + needed > len(data):
                start = idx + 4
                continue
            timescale = int.from_bytes(data[fields_start + 12 : fields_start + 16], "big", signed=False)
            duration = int.from_bytes(data[fields_start + 16 : fields_start + 20], "big", signed=False)
        elif version == 1:
            # version/flags (4) + creation (8) + modification (8) + timescale (4) + duration (8)
            needed = 4 + 8 + 8 + 4 + 8
            if fields_start + needed > len(data):
                start = idx + 4
                continue
            timescale = int.from_bytes(data[fields_start + 20 : fields_start + 24], "big", signed=False)
            duration = int.from_bytes(data[fields_start + 24 : fields_start + 32], "big", signed=False)
        else:
            start = idx + 4
            continue

        if timescale <= 0 or duration <= 0:
            start = idx + 4
            continue

        seconds = duration / timescale
        if seconds <= 0:
            start = idx + 4
            continue

        return max(1, int(round(seconds)))


def _fetch_url_bytes(url: str, *, range_header: str, max_bytes: int) -> Optional[bytes]:
    headers = {"Range": range_header}
    try:
        with requests.get(url, headers=headers, stream=True, timeout=VIDEO_DURATION_TIMEOUT_SECONDS) as resp:
            if resp.status_code not in {200, 206}:
                return None
            chunks: List[bytes] = []
            received = 0
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    break
                if received + len(chunk) > max_bytes:
                    chunk = chunk[: max_bytes - received]
                chunks.append(chunk)
                received += len(chunk)
                if received >= max_bytes:
                    break
            return b"".join(chunks) if chunks else b""
    except Exception:
        return None


def _get_video_duration_seconds(url: str) -> Optional[int]:
    """
    Best-effort duration extraction for MP4/MOV URLs using ranged reads.
    """
    if not url:
        return None
    path = urlparse(str(url)).path.lower()
    if not any(path.endswith(ext) for ext in (".mp4", ".mov", ".m4v")):
        return None

    probe = max(64 * 1024, int(VIDEO_DURATION_PROBE_BYTES))

    head = _fetch_url_bytes(url, range_header=f"bytes=0-{probe - 1}", max_bytes=probe)
    if head is not None:
        duration = _extract_mp4_duration_seconds_from_bytes(head)
        if duration is not None:
            return duration

    tail = _fetch_url_bytes(url, range_header=f"bytes=-{probe}", max_bytes=probe)
    if tail is not None:
        duration = _extract_mp4_duration_seconds_from_bytes(tail)
        if duration is not None:
            return duration

    # Fallback: download the full file only if reasonably small.
    try:
        with requests.head(url, allow_redirects=True, timeout=VIDEO_DURATION_TIMEOUT_SECONDS) as resp:
            length_raw = resp.headers.get("Content-Length") or resp.headers.get("content-length")
    except Exception:
        length_raw = None

    try:
        length = int(length_raw) if length_raw else None
    except Exception:
        length = None

    if length is None or length > VIDEO_DURATION_FULL_DOWNLOAD_MAX_BYTES:
        return None

    blob = _fetch_url_bytes(url, range_header=f"bytes=0-{length - 1}", max_bytes=length)
    if blob is None:
        return None
    return _extract_mp4_duration_seconds_from_bytes(blob)


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
_last_wav_sweep_at = 0.0

_content_items_columns: Optional[set[str]] = None
_content_scripts_columns: Optional[set[str]] = None

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


def _load_content_scripts_columns() -> set[str]:
    global _content_scripts_columns
    if _content_scripts_columns is not None:
        return _content_scripts_columns
    try:
        rows = SB.table("content_scripts").select("*").limit(1).execute().data or []
    except Exception:
        rows = []
    if rows and isinstance(rows[0], dict):
        _content_scripts_columns = set(rows[0].keys())
    else:
        _content_scripts_columns = {
            "id",
            "creator_id",
            "shoot_id",
            "finalize_status",
            "finalize_error",
            "finalize_attempts",
            "finalize_updated_at",
        }
    return _content_scripts_columns


def _filter_content_scripts_update(update: Dict[str, Any]) -> Dict[str, Any]:
    allowed = _load_content_scripts_columns()
    return {k: v for k, v in update.items() if k in allowed}


def _extract_storage_path(row: Dict[str, Any]) -> Optional[str]:
    """
    Prefer storage_path (if present). Fall back to parsing url_main/url_thumb.
    Returns a bucket-relative path like: "@alice/scripts/script_1/core/video.mp4".
    """

    raw = row.get("storage_path")
    if raw is not None and str(raw).strip():
        return str(raw).strip()

    url = row.get("url_main") or row.get("url_thumb")
    if not url or not str(url).strip():
        return None

    try:
        parsed = urlparse(str(url))
    except Exception:
        return None

    path = parsed.path or ""
    for marker in ("/object/public/", "/object/sign/"):
        if marker not in path:
            continue
        rest = path.split(marker, 1)[1]
        if not rest or "/" not in rest:
            return None
        # rest is "<bucket>/<path>"
        return rest.split("/", 1)[1] or None
    return None


def _parse_storage_url(url: str) -> Optional[Tuple[str, str, Optional[bool]]]:
    if not url:
        return None
    try:
        parsed = urlparse(str(url))
    except Exception:
        return None
    path = parsed.path or ""
    for marker, is_public in (
        ("/storage/v1/object/public/", True),
        ("/storage/v1/object/sign/", False),
        ("/storage/v1/object/", None),
    ):
        if marker not in path:
            continue
        rest = path.split(marker, 1)[1]
        if not rest or "/" not in rest:
            return None
        bucket, obj_path = rest.split("/", 1)
        return bucket, obj_path, is_public
    return None


def _extract_storage_bucket_and_path(row: Dict[str, Any]) -> Optional[Tuple[str, str, bool]]:
    url = row.get("url_main") or row.get("url_thumb")
    parsed = _parse_storage_url(str(url)) if url else None
    if not parsed:
        return None
    bucket, obj_path, is_public = parsed
    storage_path = row.get("storage_path")
    if storage_path is not None and str(storage_path).strip():
        obj_path = str(storage_path).strip()
    return bucket, obj_path, bool(is_public)


def _build_wav_storage_path(source_path: str) -> str:
    if not source_path:
        return ""
    source_path = str(source_path).strip().lstrip("/")
    if not source_path:
        return ""
    folder = ""
    name = source_path
    if "/" in source_path:
        folder, name = source_path.rsplit("/", 1)
    root, ext = os.path.splitext(name)
    if ext.lower() == ".wav":
        return source_path
    root = root or name
    wav_name = f"{root}.wav"
    return f"{folder}/{wav_name}" if folder else wav_name


def _public_storage_url(bucket: str, obj_path: str) -> str:
    base = SUPABASE_URL.rstrip("/")
    return f"{base}/storage/v1/object/public/{bucket}/{obj_path}"


def _signed_storage_url(bucket: str, obj_path: str) -> Optional[str]:
    base = SUPABASE_URL.rstrip("/")
    sign_url = f"{base}/storage/v1/object/sign/{bucket}/{obj_path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            sign_url,
            headers=headers,
            json={"expiresIn": CONTENT_WAV_SIGNED_URL_EXPIRES_SECONDS},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
    except Exception:
        return None
    signed = (
        data.get("signedURL")
        or data.get("signedUrl")
        or data.get("signed_url")
        or ""
    )
    if not signed:
        return None
    if signed.startswith("http"):
        return signed
    if signed.startswith("/"):
        return f"{base}{signed}"
    return f"{base}/{signed.lstrip('/')}"


def _download_url_to_file(url: str, dest_path: str, *, headers: Optional[Dict[str, str]] = None) -> None:
    with requests.get(url, headers=headers, stream=True, timeout=30) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def _download_storage_object(bucket: str, obj_path: str, dest_path: str) -> None:
    base = SUPABASE_URL.rstrip("/")
    url = f"{base}/storage/v1/object/{bucket}/{obj_path}"
    headers = {"Authorization": f"Bearer {SUPABASE_KEY}", "apikey": SUPABASE_KEY}
    _download_url_to_file(url, dest_path, headers=headers)


def _convert_to_wav(input_path: str) -> str:
    output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
    output_file.close()
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-vn",
        "-acodec",
        "pcm_s16le",
        "-ar",
        str(CONTENT_WAV_SAMPLE_RATE),
        "-ac",
        str(CONTENT_WAV_CHANNELS),
        output_file.name,
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception:
        try:
            os.unlink(output_file.name)
        except Exception:
            pass
        raise
    return output_file.name


def _upload_wav_to_storage(bucket: str, obj_path: str, wav_path: str) -> None:
    base = SUPABASE_URL.rstrip("/")
    url = f"{base}/storage/v1/object/{bucket}/{obj_path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "audio/wav",
        "x-upsert": "true",
    }
    with open(wav_path, "rb") as handle:
        resp = requests.post(url, headers=headers, data=handle, timeout=120)
    resp.raise_for_status()


def _audio_wav_meta(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("meta")
    if not isinstance(meta, dict):
        return {}
    wav_meta = meta.get("audio_wav")
    return wav_meta if isinstance(wav_meta, dict) else {}


def _write_audio_wav_meta(content_id: int, row: Dict[str, Any], wav_meta: Dict[str, Any]) -> None:
    meta = row.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    else:
        meta = dict(meta)
    meta["audio_wav"] = wav_meta
    SB.table("content_items").update({"meta": meta}).eq("id", content_id).execute()


def _build_wav_payload_url(bucket: str, obj_path: str, *, is_public: bool) -> Optional[str]:
    if not bucket or not obj_path:
        return None
    if is_public:
        return _public_storage_url(bucket, obj_path)
    return _signed_storage_url(bucket, obj_path)


def _ensure_audio_wav(
    row: Dict[str, Any], media_type: str, source_url: str
) -> Optional[str]:
    if media_type not in {"video", "voice"}:
        return None
    if not source_url:
        return None
    content_id = _coerce_int(row.get("id"))
    if content_id is None:
        return None

    wav_meta = _audio_wav_meta(row)
    status = str(wav_meta.get("status") or "").lower()
    attempts = _coerce_int(wav_meta.get("attempts")) or 0
    if status == "ok":
        bucket = wav_meta.get("bucket")
        obj_path = wav_meta.get("path")
        is_public = bool(wav_meta.get("is_public"))
        if bucket and obj_path:
            return _build_wav_payload_url(bucket, obj_path, is_public=is_public)
        return None
    if status == "processing":
        return None
    if attempts >= CONTENT_WAV_MAX_ATTEMPTS:
        return None

    parsed = _extract_storage_bucket_and_path(row)
    if not parsed:
        return None
    bucket, source_path, is_public = parsed
    if not bucket or not source_path:
        return None

    wav_path = _build_wav_storage_path(source_path)
    if not wav_path:
        return None

    now_iso = datetime.now(timezone.utc).isoformat()
    wav_meta = {
        "status": "processing",
        "attempts": attempts + 1,
        "bucket": bucket,
        "path": wav_path,
        "is_public": bool(is_public),
        "updated_at": now_iso,
    }
    _write_audio_wav_meta(content_id, row, wav_meta)

    if wav_path == source_path:
        wav_meta["status"] = "ok"
        wav_meta["generated_at"] = now_iso
        wav_meta["updated_at"] = now_iso
        _write_audio_wav_meta(content_id, row, wav_meta)
        return _build_wav_payload_url(bucket, wav_path, is_public=bool(is_public))

    source_tmp = None
    wav_tmp = None
    try:
        source_tmp = tempfile.NamedTemporaryFile(delete=False)
        source_tmp.close()
        _download_url_to_file(source_url, source_tmp.name)
    except Exception:
        if source_tmp:
            try:
                os.unlink(source_tmp.name)
            except Exception:
                pass
        try:
            source_tmp = tempfile.NamedTemporaryFile(delete=False)
            source_tmp.close()
            _download_storage_object(bucket, source_path, source_tmp.name)
        except Exception as exc:
            if source_tmp:
                try:
                    os.unlink(source_tmp.name)
                except Exception:
                    pass
            wav_meta["status"] = "failed"
            wav_meta["error"] = f"download_failed: {exc}"
            wav_meta["updated_at"] = datetime.now(timezone.utc).isoformat()
            _write_audio_wav_meta(content_id, row, wav_meta)
            return None

    try:
        wav_tmp = _convert_to_wav(source_tmp.name)
        _upload_wav_to_storage(bucket, wav_path, wav_tmp)
        wav_meta["status"] = "ok"
        wav_meta["generated_at"] = datetime.now(timezone.utc).isoformat()
        wav_meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        _write_audio_wav_meta(content_id, row, wav_meta)
        return _build_wav_payload_url(bucket, wav_path, is_public=bool(is_public))
    except Exception as exc:
        wav_meta["status"] = "failed"
        wav_meta["error"] = f"wav_failed: {exc}"
        wav_meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        _write_audio_wav_meta(content_id, row, wav_meta)
        return None
    finally:
        if source_tmp:
            try:
                os.unlink(source_tmp.name)
            except Exception:
                pass
        if wav_tmp:
            try:
                os.unlink(wav_tmp)
            except Exception:
                pass


def _is_placeholder_storage_object(storage_path: Optional[str]) -> bool:
    if not storage_path:
        return False
    name = str(storage_path).split("/")[-1]
    return name == ".emptyFolderPlaceholder"


def _parse_script_assignment(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Derive script assignment from Storage path.
    Returns:
      {
        "shoot_id": <uuid-str>,
        "is_ammo": bool,
        "handle": str,
        "root": str,
        "shoot_slug": str,
      }
    """

    storage_path = _extract_storage_path(row)
    if not storage_path or _is_placeholder_storage_object(storage_path):
        return None

    parts = [p for p in str(storage_path).split("/") if p]
    if len(parts) < 3:
        return None

    handle = parts[0].strip()
    if not handle:
        return None

    root = parts[1].strip().lower()
    if root in GLOBAL_AMMO_FOLDERS:
        return None
    if root not in SCRIPT_ROOT_FOLDERS:
        return None

    shoot_slug = parts[2].strip()
    if not shoot_slug:
        return None

    role = parts[3].strip().lower() if len(parts) >= 4 else ""
    if role in SCRIPT_AMMO_FOLDERS:
        is_ammo = True
    elif role in SCRIPT_CORE_FOLDERS:
        is_ammo = False
    elif not role and len(parts) == 3:
        # If no explicit role folder, default to core.
        is_ammo = False
    else:
        return None

    creator_id = row.get("creator_id")
    if creator_id is None:
        return None

    seed = f"{creator_id}:{root}:{shoot_slug}"
    shoot_id = str(uuid.uuid5(SCRIPT_SHOOT_ID_NAMESPACE, seed))
    return {
        "shoot_id": shoot_id,
        "is_ammo": is_ammo,
        "handle": handle,
        "root": root,
        "shoot_slug": shoot_slug,
    }


def _get_or_create_script(
    *, creator_id: Any, shoot_id: str, meta: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    if creator_id is None or not shoot_id:
        return None

    try:
        rows = (
            SB.table("content_scripts")
            .select("id,finalize_status,meta")
            .eq("creator_id", creator_id)
            .eq("shoot_id", shoot_id)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        rows = []
    script_row = rows[0] if rows and isinstance(rows[0], dict) else None

    if not script_row:
        payload = _filter_content_scripts_update(
            {
                "creator_id": creator_id,
                "shoot_id": shoot_id,
                "meta": meta,
                "finalize_status": "pending",
                "finalize_error": None,
                "finalize_attempts": 0,
                "finalize_updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        try:
            inserted = SB.table("content_scripts").insert(payload).execute().data or []
        except Exception:
            inserted = []
        script_row = inserted[0] if inserted and isinstance(inserted[0], dict) else None

        if not script_row:
            try:
                rows = (
                    SB.table("content_scripts")
                    .select("id,finalize_status,meta")
                    .eq("creator_id", creator_id)
                    .eq("shoot_id", shoot_id)
                    .limit(1)
                    .execute()
                    .data
                    or []
                )
            except Exception:
                rows = []
            script_row = rows[0] if rows and isinstance(rows[0], dict) else None

    return script_row


def _maybe_attach_script_to_content_item(content_id: int, row: Dict[str, Any]) -> Dict[str, Any]:
    assignment = _parse_script_assignment(row)
    if not assignment:
        return row

    shoot_id = assignment["shoot_id"]
    is_ammo = bool(assignment["is_ammo"])
    meta = {
        "handle": assignment.get("handle"),
        "script_root": assignment.get("root"),
        "script_slug": assignment.get("shoot_slug"),
        "storage_prefix": "/".join(
            p
            for p in (
                assignment.get("handle"),
                assignment.get("root"),
                assignment.get("shoot_slug"),
            )
            if p
        ),
    }

    script_row = _get_or_create_script(
        creator_id=row.get("creator_id"),
        shoot_id=shoot_id,
        meta=meta,
    )
    if not script_row:
        return row

    desired_script_id = None
    if not is_ammo and script_row.get("id"):
        desired_script_id = script_row.get("id")

    updates: Dict[str, Any] = {}
    if row.get("shoot_id") != shoot_id:
        updates["shoot_id"] = shoot_id
    if desired_script_id is not None and row.get("script_id") != desired_script_id:
        updates["script_id"] = desired_script_id
    if is_ammo and row.get("script_id") is not None:
        updates["script_id"] = None

    if updates:
        filtered = _filter_content_items_update(updates)
        if filtered:
            try:
                SB.table("content_items").update(filtered).eq("id", content_id).execute()
            except Exception:
                pass
            row.update(filtered)

    return row


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


def _normalize_time_of_day(value: Any) -> Optional[str]:
    """
    Normalize time_of_day to our current 3-way vocabulary: day | night | anytime.
    Accept legacy values (morning/afternoon/evening) and common synonyms.
    """

    token = _normalize_token(value)
    if not token:
        return None

    if token in {"day", "daytime", "day_time", "daylight"}:
        return "day"
    if token in {"night", "nighttime", "night_time"}:
        return "night"
    if token in {"any", "anytime", "unspecified", "unknown"}:
        return "anytime"

    # Legacy values from older prompts.
    if token in {"morning", "afternoon", "noon"}:
        return "day"
    if token in {"evening", "sunset", "twilight", "dusk"}:
        return "night"

    return token if token in {"day", "night", "anytime"} else None


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
        if field == "time_of_day":
            normalized = _normalize_time_of_day(value)
            if normalized is not None:
                cleaned[field] = normalized
            continue
        if field in {"explicitness", "outfit_category", "camera_angle"}:
            normalized = _normalize_token(value)
            if normalized is not None:
                cleaned[field] = normalized
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


def _build_messages(
    media_type: str,
    prompt: str,
    row: Dict[str, Any],
    url: str,
    *,
    duration_seconds: Optional[int] = None,
    wav_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if media_type == "video":
        # Video models often require the media to be attached via `video_url` content.
        # We also include VIDEO_URL text to stay compatible with the prompt format.
        duration_text = (
            f"\n\nVIDEO_DURATION_SECONDS:\n{int(duration_seconds)}" if duration_seconds is not None else ""
        )
        user_text = f"{prompt}\n\nVIDEO_URL:\n{url}{duration_text}"
        content = [
            {"type": "text", "text": user_text},
            {"type": "video_url", "video_url": {"url": url}},
        ]
        if wav_url:
            content.append({"type": "audio_url", "audio_url": {"url": wav_url}})
        return [
            {
                "role": "user",
                "content": content,
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
        user_content: List[Dict[str, Any]] | str
        if wav_url:
            user_content = [
                {"type": "text", "text": f"VOICE_URL:\n{url}"},
                {"type": "audio_url", "audio_url": {"url": wav_url}},
            ]
        else:
            user_content = f"VOICE_URL:\n{url}"
        return [{"role": "system", "content": prompt}, {"role": "user", "content": user_content}]

    return [{"role": "system", "content": prompt}, {"role": "user", "content": str(url)}]


def _run_model(
    media_type: str,
    prompt: str,
    row: Dict[str, Any],
    url: str,
    *,
    model: str,
    wav_url: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    duration_seconds = None
    if media_type == "video":
        duration_seconds = _coerce_int(row.get("duration_seconds"))
        if duration_seconds is not None and duration_seconds <= 0:
            duration_seconds = None

    messages = _build_messages(
        media_type,
        prompt,
        row,
        url,
        duration_seconds=duration_seconds,
        wav_url=wav_url,
    )

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


def _sweep_missing_wav_assets(*, limit: int) -> int:
    if limit <= 0:
        return 0
    try:
        rows = (
            SB.table("content_items")
            .select("id,media_type,url_main,url_thumb,meta,storage_path")
            .in_("media_type", ["video", "voice"])
            .order("id", desc=False)
            .limit(limit)
            .execute()
            .data
            or []
        )
    except Exception:
        return 0

    generated = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        media_type = _infer_media_type(row)
        if media_type not in {"video", "voice"}:
            continue
        wav_meta = _audio_wav_meta(row)
        if str(wav_meta.get("status") or "").lower() == "ok":
            continue
        url = row.get("url_main") or row.get("url_thumb")
        if not url:
            continue
        wav_url = _ensure_audio_wav(row, media_type, str(url))
        if wav_url:
            generated += 1
    return generated


def _maybe_run_wav_sweeper() -> None:
    global _last_wav_sweep_at
    if not CONTENT_WAV_SWEEPER_ENABLED:
        return
    now = time.time()
    if now - _last_wav_sweep_at < CONTENT_WAV_SWEEPER_INTERVAL_SECONDS:
        return
    _last_wav_sweep_at = now
    generated = _sweep_missing_wav_assets(limit=CONTENT_WAV_SWEEPER_BATCH_SIZE)
    if generated:
        print(f"[content_ingestor] wav sweeper: generated={generated}", flush=True)


def _maybe_enqueue_script_finalize(script_id: str) -> None:
    if not script_id:
        return
    # Skip if already enqueued or finalized
    if job_exists(SCRIPT_QUEUE, script_id, client=SB, field="script_id"):
        return
    script_rows = (
        SB.table("content_scripts")
        .select("id,finalize_status,meta")
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

    # Ensure core items are fully ingested before finalizing.
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

    # If we have script meta, also check for any core items that haven't been attached yet.
    script_meta = script_rows[0].get("meta") or {}
    storage_prefix = None
    if isinstance(script_meta, dict):
        storage_prefix = script_meta.get("storage_prefix")
    if storage_prefix:
        core_marker = f"/{storage_prefix}/core/"
        try:
            rows = (
                SB.table("content_items")
                .select("id,url_main,url_thumb,ingest_status")
                .neq("ingest_status", "ok")
                .or_(f"url_main.like.%{core_marker}%,url_thumb.like.%{core_marker}%")
                .limit(5)
                .execute()
                .data
                or []
            )
        except Exception:
            rows = []
        for row in rows:
            url_main = str(row.get("url_main") or "")
            url_thumb = str(row.get("url_thumb") or "")
            if ".emptyFolderPlaceholder" in url_main or ".emptyFolderPlaceholder" in url_thumb:
                continue
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

    # Attach script metadata (core vs ammo) based on Storage path conventions.
    row = _maybe_attach_script_to_content_item(content_id, row)

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

    if media_type == "video" and VIDEO_DURATION_ENABLED:
        current_duration = _coerce_int(row.get("duration_seconds"))
        duration = _get_video_duration_seconds(str(url))
        if duration is not None:
            if current_duration != duration:
                SB.table("content_items").update({"duration_seconds": duration}).eq("id", content_id).execute()
            row["duration_seconds"] = duration

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

    wav_url = None
    if CONTENT_WAV_AUTOGEN_ON_INGEST:
        try:
            wav_url = _ensure_audio_wav(row, media_type, str(url))
        except Exception as exc:  # noqa: BLE001
            print(f"[content_ingestor] wav generation failed: {exc}", flush=True)
            wav_url = None

    raw_text, error = _run_model(media_type, prompt, row, url, model=model, wav_url=wav_url)
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
    # Duration is set from the media file (more reliable than model guesses).
    if media_type == "video":
        update_payload.pop("duration_seconds", None)
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
        _maybe_run_wav_sweeper()

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
