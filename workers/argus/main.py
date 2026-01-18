"""
Argus worker — processes media attachments before handing off to Kairos.

Design goals:
- Keep fan text intact; add media analysis alongside it.
- If processing fails, mark status and continue pipeline so Kairos still runs.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from supabase import ClientOptions, create_client

from workers.lib.prompt_builder import live_turn_window
from workers.lib.job_utils import job_exists
from workers.lib.reply_run_tracking import (
    is_run_active,
    set_run_current_step,
    step_started_at,
    upsert_step,
)
from workers.lib.simple_queue import ack, receive, send

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

QUEUE = "argus.analyse"
JOIN_QUEUE = "hermes.join"

OPENAI_KEY = os.getenv("ARGUS_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("ARGUS_OPENAI_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"
VISION_MODEL = os.getenv("ARGUS_VISION_MODEL", "gpt-4o-mini")

OPENAI_CLIENT = OpenAI(api_key=OPENAI_KEY, base_url=OPENAI_BASE_URL) if OPENAI_KEY else None

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
ARGUS_VIDEO_MODEL = os.getenv("ARGUS_VIDEO_MODEL")
ARGUS_PHOTO_MODEL = os.getenv("ARGUS_PHOTO_MODEL")
ARGUS_VOICE_MODEL = os.getenv("ARGUS_VOICE_MODEL")
ARGUS_LINK_MODEL = (
    os.getenv("ARGUS_LINK_MODEL")
    or os.getenv("ARGUS_TEXT_MODEL")
    or VISION_MODEL
)
ARGUS_LINK_TIMEOUT = float(os.getenv("ARGUS_LINK_TIMEOUT", "10"))
ARGUS_LINK_MAX_BYTES = int(os.getenv("ARGUS_LINK_MAX_BYTES", str(1_500_000)))
ARGUS_LINK_MAX_TEXT_CHARS = int(os.getenv("ARGUS_LINK_MAX_TEXT", str(12000)))
ARGUS_LINK_MAX_IMAGES = int(os.getenv("ARGUS_LINK_MAX_IMAGES", "3"))
ARGUS_CONTEXT_TURNS = int(os.getenv("ARGUS_CONTEXT_TURNS", "6"))

_ARGUS_VIDEO_PROMPT = None
_ARGUS_PHOTO_PROMPT = None
_ARGUS_VOICE_PROMPT = None
_ARGUS_LINK_PROMPT = None


def _load_prompt(name: str) -> str | None:
    path = PROMPTS_DIR / name
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _video_prompt() -> str:
    global _ARGUS_VIDEO_PROMPT
    if _ARGUS_VIDEO_PROMPT is None:
        _ARGUS_VIDEO_PROMPT = _load_prompt("argus_video.txt")
    return _ARGUS_VIDEO_PROMPT or ""


def _photo_prompt() -> str:
    global _ARGUS_PHOTO_PROMPT
    if _ARGUS_PHOTO_PROMPT is None:
        _ARGUS_PHOTO_PROMPT = _load_prompt("argus_photo.txt")
    return _ARGUS_PHOTO_PROMPT or ""


def _voice_prompt() -> str:
    global _ARGUS_VOICE_PROMPT
    if _ARGUS_VOICE_PROMPT is None:
        _ARGUS_VOICE_PROMPT = _load_prompt("argus_voice.txt")
    return _ARGUS_VOICE_PROMPT or ""


def _link_prompt() -> str:
    global _ARGUS_LINK_PROMPT
    if _ARGUS_LINK_PROMPT is None:
        _ARGUS_LINK_PROMPT = _load_prompt("argus_link.txt")
    return _ARGUS_LINK_PROMPT or ""


def _clean(items) -> List[dict]:
    if isinstance(items, list):
        return [i for i in items if isinstance(i, dict)]
    return []


def _normalize_media_kind(item: dict) -> str:
    """
    Normalize assorted media type labels (gif/sticker/screenshot/etc.) into
    image/video/voice buckets so they route to the right describer.
    """
    type_val = (item.get("type") or "").strip().lower()
    mime = (item.get("mime_type") or "").strip().lower()
    url = (item.get("url") or item.get("signed_url") or item.get("href") or "").lower()

    image_types = {
        "image",
        "photo",
        "picture",
        "pic",
        "screenshot",
        "screen",
        "screen_capture",
        "screen-capture",
        "screen grab",
        "screen-grab",
    }
    gif_types = {"gif", "animated_gif", "gifv"}
    sticker_types = {"sticker", "sticker_pack", "sticker-pack"}
    video_types = {"video", "mp4", "mov", "video/mp4", "video/quicktime"}
    voice_types = {"audio", "voice", "voice_note", "voice-note", "voice message"}
    link_types = {"link", "url", "website", "web", "page"}

    def is_animated() -> bool:
        if item.get("animated") or item.get("is_animated"):
            return True
        try:
            if float(item.get("duration") or 0) > 0:
                return True
        except Exception:
            pass
        try:
            if int(item.get("frames") or 0) > 1:
                return True
        except Exception:
            pass
        if "gif" in mime:
            return True
        if url.endswith(".gif"):
            return True
        return False

    if type_val in image_types:
        base = "image"
    elif type_val in sticker_types:
        base = "sticker"
    elif type_val in gif_types:
        base = "gif"
    elif type_val in video_types:
        base = "video"
    elif type_val in voice_types:
        base = "voice"
    elif type_val in link_types:
        base = "link"
    else:
        base = type_val or "unknown"

    animated = is_animated()

    # Route stickers/gifs into image or video depending on animation.
    if base in {"gif", "sticker"}:
        return "video" if animated else "image"

    # If mime/url indicates gif but type was unknown, treat accordingly.
    if base == "unknown" and animated:
        return "video"

    if base == "unknown":
        if url.startswith("http"):
            return "link"
        if "html" in mime:
            return "link"

    return base


def _describe_images_multi(items: List[dict], context: str) -> Tuple[List[str] | None, str | None]:
    """
    Batch describe one or more images in a single vision call.
    Returns (list_of_descriptions_matching_items, error|None).
    """
    if not items:
        return [], None

    prompt = _photo_prompt()
    if not prompt:
        return None, "photo_prompt_missing"
    if not OPENAI_CLIENT:
        return None, "vision_client_unavailable"

    model = ARGUS_PHOTO_MODEL or VISION_MODEL

    urls: list[str] = []
    for itm in items:
        url = itm.get("url") or itm.get("signed_url") or itm.get("href")
        if url:
            urls.append(url)
        else:
            urls.append("")

    # Build user content: include context + enumerated URLs + actual image_url blocks (order matters).
    text_block = (
        "MESSAGE_CONTEXT:\n"
        f"{context[:4000] if context else 'None provided'}\n\n"
        "PHOTOS:\n" + "\n".join([f"Photo {idx+1} URL: {u or '[missing url]'}" for idx, u in enumerate(urls)])
    )
    user_content: list[dict] = [{"type": "text", "text": text_block}]
    for url in urls:
        # Even if URL is missing, keep order; model may handle blanks poorly but keeps alignment.
        if url:
            user_content.append({"type": "image_url", "image_url": {"url": url}})
        else:
            user_content.append({"type": "text", "text": "[missing image url]"})

    try:
        resp = OPENAI_CLIENT.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0.2,
            max_tokens=1600,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"vision_error: {exc}"

    content = (resp.choices[0].message.content or "").strip()
    if not content:
        return None, "vision_empty_response"

    try:
        data = json.loads(content)
    except Exception as exc:  # noqa: BLE001
        return None, f"photo_json_error: {exc}"

    # Accept legacy single-image format or new multi-image format.
    descriptions: list[str] = []
    if isinstance(data, dict) and "images" in data and isinstance(data["images"], list):
        for entry in data["images"]:
            if isinstance(entry, dict):
                paragraphs = entry.get("paragraphs")
                if isinstance(paragraphs, list) and paragraphs:
                    desc = "\n\n".join(str(p) for p in paragraphs if p)
                    descriptions.append(desc.strip())
                else:
                    descriptions.append("")
            else:
                descriptions.append("")
    elif isinstance(data, dict) and "paragraphs" in data:
        paragraphs = data.get("paragraphs")
        if isinstance(paragraphs, list) and paragraphs:
            desc = "\n\n".join(str(p) for p in paragraphs if p)
            descriptions.append(desc.strip())

    # Ensure length matches inputs; pad/truncate as needed.
    if len(descriptions) < len(items):
        descriptions.extend([""] * (len(items) - len(descriptions)))
    if len(descriptions) > len(items):
        descriptions = descriptions[: len(items)]

    return descriptions, None


def _describe_image(url: str, context: str) -> Tuple[str | None, str | None]:
    """
    Photo description using the Argus Photo prompt with vision support.
    """
    if not url:
        return None, "missing_image_url"
    if not OPENAI_CLIENT:
        return None, "vision_client_unavailable"

    prompt = _photo_prompt()
    if not prompt:
        return None, "photo_prompt_missing"

    model = ARGUS_PHOTO_MODEL or VISION_MODEL
    try:
        resp = OPENAI_CLIENT.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "MESSAGE_CONTEXT:\n"
                                f"{context[:4000] if context else 'None provided'}\n\n"
                                "PHOTO:\n"
                                f"Image URL: {url}"
                            ),
                        },
                        {"type": "image_url", "image_url": {"url": url}},
                    ],
                },
            ],
            temperature=0.2,
            max_tokens=1200,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"vision_error: {exc}"

    message = resp.choices[0].message
    content = (message.content or "").strip()
    if not content:
        return None, "vision_empty_response"

    try:
        data = json.loads(content)
        paragraphs = data.get("paragraphs") if isinstance(data, dict) else None
    except Exception as exc:  # noqa: BLE001
        return content, f"photo_json_error: {exc}"

    if isinstance(paragraphs, list) and paragraphs:
        narration = "\n\n".join(str(p) for p in paragraphs if p)
    else:
        narration = content

    return narration.strip(), None


def _describe_video(url: str, context: str) -> Tuple[str | None, str | None]:
    """
    Best-effort video description using a configured OpenAI-compatible endpoint.
    Expects the backend to be able to fetch/process the video URL.
    """
    if not url:
        return None, "missing_video_url"
    if not OPENAI_CLIENT:
        return None, "video_client_unavailable"
    if not ARGUS_VIDEO_MODEL:
        return None, "video_model_not_configured"

    prompt = _video_prompt()
    if not prompt:
        return None, "video_prompt_missing"

    try:
        resp = OPENAI_CLIENT.chat.completions.create(
            model=ARGUS_VIDEO_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": (
                        f"Video URL: {url}\n\n"
                        f"Optional chat context (may be empty):\n{context[:4000] if context else 'None'}"
                    ),
                },
            ],
            temperature=0.2,
            max_tokens=1400,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"video_error: {exc}"

    content = (resp.choices[0].message.content or "").strip()
    if not content:
        return None, "video_empty_response"

    try:
        data = json.loads(content)
        beats = data.get("beats") if isinstance(data, dict) else None
    except Exception as exc:  # noqa: BLE001
        return content, f"video_json_error: {exc}"

    narration = ""
    if isinstance(beats, list) and beats:
        narration = "\n\n".join(str(b) for b in beats if b)
    else:
        narration = content

    return narration.strip(), None


def _describe_voice(url: str, context: str) -> Tuple[str | None, str | None]:
    """
    Voice note transcription/description using a configured OpenAI-compatible endpoint.
    Expects the backend to be able to fetch/process the audio URL.
    """
    if not url:
        return None, "missing_audio_url"
    if not OPENAI_CLIENT:
        return None, "voice_client_unavailable"
    if not ARGUS_VOICE_MODEL:
        return None, "voice_model_not_configured"

    prompt = _voice_prompt()
    if not prompt:
        return None, "voice_prompt_missing"

    try:
        resp = OPENAI_CLIENT.chat.completions.create(
            model=ARGUS_VOICE_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": (
                        "VOICE_NOTE:\n"
                        f"{url}\n\n"
                        "MESSAGE_CONTEXT:\n"
                        f"{context[:4000] if context else 'None provided'}"
                    ),
                },
            ],
            temperature=0.2,
            max_tokens=1400,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"voice_error: {exc}"

    content = (resp.choices[0].message.content or "").strip()
    if not content:
        return None, "voice_empty_response"

    try:
        data = json.loads(content)
        beats = data.get("beats") if isinstance(data, dict) else None
    except Exception as exc:  # noqa: BLE001
        return content, f"voice_json_error: {exc}"

    if isinstance(beats, list) and beats:
        narration = "\n\n".join(str(b) for b in beats if b)
    else:
        narration = content

    return narration.strip(), None


def _extract_html_text(html: str) -> Tuple[str, Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    meta: Dict[str, str] = {}
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        meta["title"] = title_tag.string.strip()
    for m in soup.find_all("meta"):
        name = (m.get("name") or m.get("property") or "").strip().lower()
        content = (m.get("content") or "").strip()
        if not name or not content:
            continue
        meta[name] = content

    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    collapsed = "\n".join(lines)
    if len(collapsed) > ARGUS_LINK_MAX_TEXT_CHARS:
        collapsed = collapsed[:ARGUS_LINK_MAX_TEXT_CHARS]
    return collapsed, meta


def _extract_image_urls(html: str, meta: Dict[str, str], base_url: str) -> List[str]:
    """
    Collect a few representative image URLs from the page:
    - OpenGraph/Twitter cards
    - First <img> tags (src/srcset/data-src)
    """
    urls: list[str] = []
    seen = set()

    def _add(raw: str | None):
        if not raw:
            return
        candidate = urljoin(base_url, raw.strip())
        parsed = urlparse(candidate)
        if parsed.scheme not in {"http", "https"}:
            return
        if candidate in seen:
            return
        seen.add(candidate)
        urls.append(candidate)

    # Meta previews first
    for key in ("og:image", "og:image:secure_url", "twitter:image"):
        _add(meta.get(key))

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("img"):
        src = tag.get("src") or tag.get("data-src") or ""
        if not src and tag.get("srcset"):
            srcset = tag.get("srcset") or ""
            src = srcset.split(",")[0].split()[0] if srcset else ""
        _add(src)
        if len(urls) >= ARGUS_LINK_MAX_IMAGES:
            break

    return urls[:ARGUS_LINK_MAX_IMAGES]


def _fetch_link_content(url: str) -> Tuple[Dict[str, Any] | None, str | None]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None, "unsupported_scheme"
    headers = {
        "User-Agent": os.getenv("ARGUS_LINK_USER_AGENT", "ArgusLinkFetcher/1.0"),
        "Accept": "*/*",
    }
    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=ARGUS_LINK_TIMEOUT,
            allow_redirects=True,
            stream=True,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"link_fetch_error: {exc}"

    content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    status = resp.status_code
    final_url = resp.url

    buf = bytearray()
    total = 0
    try:
        for chunk in resp.iter_content(chunk_size=8192):
            if not chunk:
                continue
            if total + len(chunk) > ARGUS_LINK_MAX_BYTES:
                buf.extend(chunk[: ARGUS_LINK_MAX_BYTES - total])
                total = ARGUS_LINK_MAX_BYTES
                break
            buf.extend(chunk)
            total += len(chunk)
    except Exception as exc:  # noqa: BLE001
        resp.close()
        return None, f"link_stream_error: {exc}"

    encoding = resp.encoding or resp.apparent_encoding or "utf-8"
    try:
        text = buf.decode(encoding, errors="ignore")
    except LookupError:
        text = buf.decode("utf-8", errors="ignore")

    resp.close()

    return (
        {
            "status": status,
            "content_type": content_type,
            "final_url": final_url,
            "text": text,
        },
        None,
    )


def _describe_link(url: str, context: str) -> Tuple[str | None, str | None, List[dict]]:
    """
    Fetch a URL, extract readable text/metadata, and summarize for Kairos/Napoleon.
    Falls back to media handlers if the URL is actually an image/video/audio.
    """
    if not url:
        return None, "missing_link_url", []
    if not OPENAI_CLIENT:
        return None, "link_client_unavailable", []

    fetch_result, fetch_error = _fetch_link_content(url)
    if fetch_error:
        return None, fetch_error, []
    assert fetch_result is not None
    content_type = fetch_result.get("content_type") or ""
    final_url = fetch_result.get("final_url") or url

    # Route true media links into existing describers.
    lowered_url = final_url.lower()
    if content_type.startswith("image/") or lowered_url.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")):
        desc, err = _describe_image(final_url, context)
        return desc, err, []
    if content_type.startswith("video/") or lowered_url.endswith((".mp4", ".mov", ".webm")):
        desc, err = _describe_video(final_url, context)
        return desc, err, []
    if content_type.startswith("audio/") or lowered_url.endswith((".mp3", ".wav", ".m4a", ".ogg", ".opus")):
        desc, err = _describe_voice(final_url, context)
        return desc, err, []

    prompt = _link_prompt()
    if not prompt:
        return None, "link_prompt_missing", []

    raw_text = fetch_result.get("text") or ""
    meta: Dict[str, str] = {}
    body_text = raw_text
    if "html" in content_type or "<html" in raw_text[:200].lower():
        body_text, meta = _extract_html_text(raw_text)
    else:
        body_text = raw_text[:ARGUS_LINK_MAX_TEXT_CHARS]

    meta_title = meta.get("og:title") or meta.get("twitter:title") or meta.get("title") or ""
    meta_desc = meta.get("og:description") or meta.get("twitter:description") or meta.get("description") or ""
    site_name = meta.get("og:site_name") or meta.get("twitter:site") or ""
    image_urls = _extract_image_urls(raw_text, meta, final_url)

    user_block = (
        f"ORIGINAL_URL: {url}\n"
        f"FINAL_URL: {final_url}\n"
        f"HTTP_STATUS: {fetch_result.get('status')}\n"
        f"CONTENT_TYPE: {content_type or 'unknown'}\n"
        f"SITE_NAME: {site_name}\n"
        f"TITLE: {meta_title}\n"
        f"DESCRIPTION: {meta_desc}\n"
        "MESSAGE_CONTEXT:\n"
        f"{context[:4000] if context else 'None provided'}\n\n"
        "PAGE_TEXT:\n"
        f"{body_text or '[empty]'}"
    )

    try:
        resp = OPENAI_CLIENT.chat.completions.create(
            model=ARGUS_LINK_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_block},
            ],
            temperature=0.2,
            max_tokens=1400,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"link_error: {exc}", []

    content = (resp.choices[0].message.content or "").strip()
    if not content:
        return None, "link_empty_response", []

    try:
        data = json.loads(content)
        paragraphs = data.get("paragraphs") if isinstance(data, dict) else None
    except Exception as exc:  # noqa: BLE001
        return content, f"link_json_error: {exc}", []

    narration = ""
    if isinstance(paragraphs, list) and paragraphs:
        narration = "\n\n".join(str(p) for p in paragraphs if p)
    else:
        narration = content

    derived_images = [
        {
            "type": "image",
            "url": img,
            "parent_url": final_url,
        }
        for img in image_urls
    ]

    return narration.strip(), None, derived_images


def _fallback_analysis(item: dict) -> str:
    kind = (item.get("type") or "unknown").lower()
    url = item.get("url") or item.get("signed_url") or item.get("href") or "[no url]"
    return f"{kind.title()} attachment at {url}. Automated analysis unavailable."


def _merge_context(thread_id: int, turn_index: int | None, message_id: int | None) -> str:
    turns = ARGUS_CONTEXT_TURNS if ARGUS_CONTEXT_TURNS > 0 else 6
    return live_turn_window(
        thread_id,
        boundary_turn=turn_index,
        limit=turns,
        client=SB,
        exclude_message_id=message_id,
    )


def _process_items(items: List[dict], context: str) -> Tuple[List[str], List[str], List[dict]]:
    analyses: List[str] = []
    errors: List[str] = []

    # Stage 1: normalize and expand link items into (link + derived images).
    expanded: List[dict] = []
    for item in _clean(items):
        kind = _normalize_media_kind(item)
        if kind == "link":
            url = item.get("url") or item.get("signed_url") or item.get("href")
            desc, err, derived_images = _describe_link(url, context)
            enriched = dict(item)
            if desc:
                enriched["argus_text"] = desc
                enriched["argus_preview"] = desc[:500]
            if err:
                enriched["argus_error"] = err
                errors.append(err)
            expanded.append(enriched)
            if derived_images:
                expanded.extend(derived_images)
        else:
            expanded.append(item)

    # Stage 2: batch describe any images that still need text.
    image_indices = [
        idx
        for idx, itm in enumerate(expanded)
        if _normalize_media_kind(itm) == "image" and not itm.get("argus_text")
    ]
    if image_indices:
        images_subset = [expanded[idx] for idx in image_indices]
        batch_descs, batch_err = _describe_images_multi(images_subset, context)
        if batch_err:
            errors.append(batch_err)
        if batch_descs:
            for pos, idx in enumerate(image_indices):
                if pos < len(batch_descs):
                    desc_val = batch_descs[pos] or ""
                    expanded[idx]["argus_text"] = desc_val
                    expanded[idx]["argus_preview"] = desc_val[:500]

    # Stage 3: process all items (including derived images).
    processed: List[dict] = []
    for item in expanded:
        kind = _normalize_media_kind(item)
        url = item.get("url") or item.get("signed_url") or item.get("href")
        desc: str | None = item.get("argus_text")
        err: str | None = item.get("argus_error")

        if not desc:
            if kind in {"audio", "voice"}:
                desc, err2 = _describe_voice(url, context)
            elif kind == "video":
                desc, err2 = _describe_video(url, context)
            elif kind == "image":
                err2 = "image_missing_description"
            else:
                err2 = "unknown_media_type"
            if err2:
                errors.append(err2)
                if not err:
                    err = err2

        if not desc:
            # Provide a fallback description so downstream still gets a usable note.
            desc = _fallback_analysis(item)
        else:
            desc = desc.strip()

        analyses.append(desc)

        enriched = dict(item)
        if desc:
            enriched["argus_text"] = desc
            enriched["argus_preview"] = desc[:500]
        if err:
            enriched["argus_error"] = err
        processed.append(enriched)

    return analyses, errors, processed


def _update_message(
    message_id: int,
    *,
    status: str,
    analysis_text: str | None,
    media_payload: dict | None,
    media_error: str | None,
) -> None:
    SB.table("messages").update(
        {
            "media_status": status,
            "media_analysis_text": analysis_text,
            "media_payload": media_payload,
            "media_error": media_error,
        }
    ).eq("id", message_id).execute()


def process_job(payload: Dict[str, Any]) -> bool:
    msg_id = payload.get("message_id")
    if not msg_id:
        return True
    run_id = payload.get("run_id")
    if run_id:
        set_run_current_step(str(run_id), "argus", client=SB)
        started_at = step_started_at(
            run_id=str(run_id), step="argus", attempt=0, client=SB
        ) or datetime.now(timezone.utc).isoformat()
        upsert_step(
            run_id=str(run_id),
            step="argus",
            attempt=0,
            status="running",
            client=SB,
            message_id=int(msg_id),
            started_at=started_at,
            meta={"queue": QUEUE},
        )
        if not is_run_active(str(run_id), client=SB):
            upsert_step(
                run_id=str(run_id),
                step="argus",
                attempt=0,
                status="canceled",
                client=SB,
                message_id=int(msg_id),
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="run_canceled",
            )
            return True

    row = (
        SB.table("messages")
        .select("id,thread_id,turn_index,message_text,media_payload,media_status")
        .eq("id", msg_id)
        .single()
        .execute()
        .data
    )
    if not row:
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="argus",
                attempt=0,
                status="failed",
                client=SB,
                message_id=int(msg_id),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="message_not_found",
            )
        return True

    thread_id = row.get("thread_id")
    turn_index = row.get("turn_index")
    payload_items = (row.get("media_payload") or {}).get("items") or []
    items = _clean(payload_items)

    if not items:
        _update_message(
            msg_id,
            status="error",
            analysis_text=None,
            media_payload={"items": items},
            media_error="no_media_items",
        )
        if not job_exists(JOIN_QUEUE, msg_id, client=SB):
            join_payload = {"message_id": msg_id}
            if run_id:
                join_payload["run_id"] = str(run_id)
            send(JOIN_QUEUE, join_payload)
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="argus",
                attempt=0,
                status="failed",
                client=SB,
                message_id=int(msg_id),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="no_media_items",
            )
        return True

    context = _merge_context(thread_id, turn_index, msg_id)
    analyses, errors, processed_items = _process_items(items, context)

    analysis_text = "\n\n".join(a for a in analyses if a).strip()
    status = "ok" if analysis_text else "error"
    media_error = "; ".join(errors) if errors else None

    _update_message(
        msg_id,
        status=status,
        analysis_text=analysis_text,
        media_payload={"items": processed_items},
        media_error=media_error,
    )

    # Wake Hermes Join so Napoleon can proceed once Hermes/Kairos/Web are ready.
    if not job_exists(JOIN_QUEUE, msg_id, client=SB):
        join_payload = {"message_id": msg_id}
        if run_id:
            join_payload["run_id"] = str(run_id)
        send(JOIN_QUEUE, join_payload)
    if run_id:
        upsert_step(
            run_id=str(run_id),
            step="argus",
            attempt=0,
            status="ok" if status == "ok" else "failed",
            client=SB,
            message_id=int(msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error=None if status == "ok" else (media_error or "argus_error"),
        )
    return True


if __name__ == "__main__":
    print("[Argus] started - waiting for jobs", flush=True)
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
            print("[Argus] error:", exc)
            traceback.print_exc()
            time.sleep(2)
