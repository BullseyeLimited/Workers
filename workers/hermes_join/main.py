"""
Hermes join gate — enqueues Napoleon once required upstream results are present (or failed).
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from supabase import ClientOptions, create_client

from workers.lib.content_pack import build_content_pack
from workers.lib.job_utils import job_exists
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
    raise RuntimeError("Missing Supabase configuration for Hermes Join")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "hermes.join"
NAPOLEON_QUEUE = "napoleon.reply"

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


def _fail_closed_defaults() -> dict:
    return {
        "final_verdict_search": "NO",
        "final_verdict_kairos": "FULL",
        "join_requirements": "KAIROS_ONLY",
        "web_research_brief": "NONE",
    }


def _parse_hermes_output(raw_text: str) -> dict | None:
    if not raw_text or not raw_text.strip():
        return None

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
        return None

    return parsed


def _merge_extras(existing: dict, patch: dict) -> dict:
    merged = {}
    merged.update(existing or {})
    merged.update(patch or {})
    return merged


def _extract_content_request(value) -> dict | None:
    if not value:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _build_content_pack_from_request(
    request: dict, thread_id: int, *, client
) -> dict:
    allowed_keys = {
        "zoom",
        "script_id",
        "include_shoot_extras",
        "media_expand",
        "content_ids",
        "global_focus_ids",
        "creator_id",
    }
    params = {k: request.get(k) for k in allowed_keys if k in request}
    if not params.get("creator_id"):
        thread_rows = (
            client.table("threads")
            .select("creator_id")
            .eq("id", thread_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        thread_row = thread_rows[0] if thread_rows else {}
        params["creator_id"] = thread_row.get("creator_id")
    return build_content_pack(client, thread_id=thread_id, **params)


def compute_requirements(hermes_parsed: dict) -> tuple[bool, bool]:
    verdict_search = (hermes_parsed.get("final_verdict_search") or "NO").upper()
    verdict_kairos = (hermes_parsed.get("final_verdict_kairos") or "FULL").upper()
    need_kairos = verdict_kairos != "SKIP"
    need_web = verdict_search == "YES"
    return need_kairos, need_web


def completion_state(
    hermes_parsed: dict, kairos_status: str | None, web_status: str | None
) -> tuple[bool, bool, bool]:
    need_kairos, need_web = compute_requirements(hermes_parsed)
    kairos_done = (not need_kairos) or (kairos_status in {"ok", "failed"})
    web_done = (not need_web) or (web_status in {"ok", "failed"})
    return need_kairos, need_web, (kairos_done and web_done)


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = payload["message_id"]
    select_fields = (
        "thread_id,kairos_status,web_research_status,extras,hermes_status,hermes_output_raw"
    )
    if CONTENT_PACK_ENABLED:
        select_fields += (
            ",content_request,content_pack,content_pack_status,content_pack_error,content_pack_created_at"
        )
    details_rows = (
        SB.table("message_ai_details")
        .select(select_fields)
        .eq("message_id", fan_msg_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not details_rows:
        # Hermes hasn't persisted a routing decision row yet (or wrong DB/key).
        return True
    details = details_rows[0]

    # If this turn has media, wait for Argus to finish enriching the message row
    # before letting Napoleon proceed.
    try:
        msg_rows = (
            SB.table("messages")
            .select("media_status,media_payload")
            .eq("id", fan_msg_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        msg_row = msg_rows[0] if msg_rows else {}
    except Exception:
        msg_row = {}

    media_payload = msg_row.get("media_payload") or {}
    has_media = False
    if isinstance(media_payload, dict):
        items = media_payload.get("items")
        has_media = isinstance(items, list) and len(items) > 0
    media_status = (msg_row.get("media_status") or "").strip().lower()
    if has_media and media_status == "pending":
        return True

    extras = details.get("extras") or {}
    hermes_status = details.get("hermes_status")
    hermes_output_raw = details.get("hermes_output_raw") or ""
    hermes_parsed = None
    hermes_blob = extras.get("hermes")
    if isinstance(hermes_blob, dict):
        parsed = hermes_blob.get("parsed")
        if isinstance(parsed, dict):
            hermes_parsed = parsed

    if not hermes_parsed:
        if hermes_output_raw.strip():
            hermes_parsed = _parse_hermes_output(hermes_output_raw)
            if not hermes_parsed and hermes_status:
                hermes_parsed = _fail_closed_defaults()
        elif hermes_status:
            hermes_parsed = _fail_closed_defaults()

    if not hermes_parsed:
        # Hermes decision missing; nothing to do yet.
        return True

    content_request = _extract_content_request(details.get("content_request"))
    if CONTENT_PACK_ENABLED and not details.get("content_pack"):
        # Always provide Napoleon with a content pack when enabled.
        # If Hermes did not specify a camera request, default to Zoom 0 (overview).
        request = content_request or {"zoom": 0}
        try:
            content_pack = _build_content_pack_from_request(
                request, details["thread_id"], client=SB
            )
            SB.table("message_ai_details").update(
                {
                    "content_pack": content_pack,
                    "content_pack_status": "ok",
                    "content_pack_error": None,
                    "content_pack_created_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("message_id", fan_msg_id).execute()
        except Exception as exc:  # noqa: BLE001
            SB.table("message_ai_details").update(
                {
                    "content_pack_status": "failed",
                    "content_pack_error": str(exc),
                }
            ).eq("message_id", fan_msg_id).execute()

    web_blob = extras.get("web_research") or {}
    web_status = web_blob.get("status") or details.get("web_research_status")

    need_kairos, need_web, done = completion_state(
        hermes_parsed,
        details.get("kairos_status"),
        web_status,
    )

    if not done:
        return True

    join_blob = extras.get("join") or {}
    if join_blob.get("napoleon_enqueued_at"):
        return True

    if job_exists(NAPOLEON_QUEUE, fan_msg_id, client=SB):
        return True

    send(NAPOLEON_QUEUE, {"message_id": fan_msg_id})
    join_blob["napoleon_enqueued_at"] = datetime.now(timezone.utc).isoformat()
    merged_extras = _merge_extras(extras, {"join": join_blob})

    SB.table("message_ai_details").update({"extras": merged_extras}).eq(
        "message_id", fan_msg_id
    ).execute()

    return True


if __name__ == "__main__":
    print("[HermesJoin] started - waiting for jobs", flush=True)
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
            print("[HermesJoin] error:", exc)
            traceback.print_exc()
            time.sleep(2)
