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
from pathlib import Path
from typing import Any, Dict, List, Tuple

from openai import OpenAI
from supabase import ClientOptions, create_client

from workers.lib.prompt_builder import live_turn_window
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

OPENAI_KEY = os.getenv("ARGUS_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("ARGUS_OPENAI_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"
VISION_MODEL = os.getenv("ARGUS_VISION_MODEL", "gpt-4o-mini")

OPENAI_CLIENT = OpenAI(api_key=OPENAI_KEY, base_url=OPENAI_BASE_URL) if OPENAI_KEY else None

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
ARGUS_VIDEO_MODEL = os.getenv("ARGUS_VIDEO_MODEL")
ARGUS_PHOTO_MODEL = os.getenv("ARGUS_PHOTO_MODEL")
ARGUS_CONTEXT_TURNS = int(os.getenv("ARGUS_CONTEXT_TURNS", "6"))

_ARGUS_VIDEO_PROMPT = None
_ARGUS_PHOTO_PROMPT = None


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


def _clean(items) -> List[dict]:
    if isinstance(items, list):
        return [i for i in items if isinstance(i, dict)]
    return []


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


def _fallback_analysis(item: dict) -> str:
    kind = (item.get("type") or "unknown").lower()
    url = item.get("url") or item.get("signed_url") or item.get("href") or "[no url]"
    return f"{kind.title()} attachment at {url}. Automated analysis unavailable."


def _merge_context(thread_id: int, turn_index: int | None) -> str:
    turns = ARGUS_CONTEXT_TURNS if ARGUS_CONTEXT_TURNS > 0 else 6
    return live_turn_window(
        thread_id,
        boundary_turn=turn_index,
        limit=turns,
        client=SB,
    )


def _process_items(items: List[dict], context: str) -> Tuple[List[str], List[str], List[dict]]:
    analyses: List[str] = []
    errors: List[str] = []
    processed: List[dict] = []

    for item in items:
        kind = (item.get("type") or "").lower()
        url = item.get("url") or item.get("signed_url") or item.get("href")
        desc: str | None = None
        err: str | None = None

        if kind == "image":
            desc, err = _describe_image(url, context)
        elif kind in {"audio", "voice"}:
            err = "audio_processing_not_implemented"
        elif kind == "video":
            desc, err = _describe_video(url, context)
        else:
            err = "unknown_media_type"

        if desc:
            analyses.append(desc.strip())
        else:
            # Provide a fallback description so downstream still gets a usable note.
            analyses.append(_fallback_analysis(item))

        if err:
            errors.append(err)

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

    row = (
        SB.table("messages")
        .select("id,thread_id,turn_index,message_text,media_payload,media_status")
        .eq("id", msg_id)
        .single()
        .execute()
        .data
    )
    if not row:
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
        send("kairos.analyse", {"message_id": msg_id})
        return True

    context = _merge_context(thread_id, turn_index)
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

    # Proceed to Kairos regardless of Argus status to avoid blocking the pipeline.
    send("kairos.analyse", {"message_id": msg_id})
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
