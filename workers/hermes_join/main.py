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

from workers.lib.cards import compact_psychic_card
from workers.lib.content_pack import build_content_pack
from workers.lib.job_utils import job_exists
from workers.lib.prompt_builder import live_turn_window
from workers.lib.reply_run_tracking import (
    is_run_active,
    set_run_current_step,
    step_started_at,
    upsert_step,
)
from workers.lib.simple_queue import ack, receive, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CONTENT_PACK_ENABLED = os.getenv("CONTENT_PACK_ENABLED", "").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
IRIS_CONTROL_ENABLED = os.getenv("IRIS_CONTROL_ENABLED", "").lower() in {
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
WRITER_QUEUE = "napoleon.compose"

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


def _normalize_mode(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"skip", "skipped", "none", "no", "off", "0"}:
        return "skip"
    if text in {"lite", "light", "fast", "quick", "low"}:
        return "lite"
    if text in {"full", "deep", "high"}:
        return "full"
    return None


def _extract_iris_modes(extras: dict) -> tuple[str, str | None, str, bool]:
    """
    Best-effort extract of Iris routing decisions from message_ai_details.extras.
    Returns (hermes_mode, kairos_mode, napoleon_mode, has_iris).

    When Iris control is disabled (default), this always returns legacy defaults
    so Iris output cannot affect routing.
    """

    defaults = ("full", None, "full", False)
    if not IRIS_CONTROL_ENABLED:
        return defaults
    if not isinstance(extras, dict):
        return defaults
    blob = extras.get("iris")
    if not isinstance(blob, dict):
        return defaults
    parsed = blob.get("parsed")
    if not isinstance(parsed, dict):
        return defaults
    hermes = _normalize_mode(parsed.get("hermes") or parsed.get("hermes_mode")) or "lite"
    kairos = _normalize_mode(parsed.get("kairos") or parsed.get("kairos_mode"))
    napoleon = _normalize_mode(parsed.get("napoleon") or parsed.get("napoleon_mode")) or "lite"
    return hermes, kairos, napoleon, True


def _format_fan_turn(row: dict) -> str:
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


def _build_writer_payload(
    *, fan_msg_id: int, thread_id: int, turn_index: int | None, details: dict, msg_row: dict, run_id: str | None
) -> dict:
    raw_turns = live_turn_window(
        thread_id,
        boundary_turn=turn_index,
        limit=20,
        client=SB,
        exclude_message_id=fan_msg_id,
    )

    thread_rows = (
        SB.table("threads")
        .select("creator_psychic_card,fan_psychic_card")
        .eq("id", thread_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    thread_row = thread_rows[0] if thread_rows and isinstance(thread_rows[0], dict) else {}

    fan_psychic_card = compact_psychic_card(
        thread_row.get("fan_psychic_card"),
        max_entries_per_segment=2,
        drop_superseded=True,
        entry_fields=("id", "text", "confidence", "origin_tier"),
    )

    directive = (
        "Reply FAST and naturally.\n"
        "Keep it short.\n"
        "Acknowledge what he said.\n"
        "Answer any direct question.\n"
        "Ask 1 simple follow-up question.\n"
        "Match the vibe (flirty/teasing/soft) without overdoing it.\n"
        "Do not over-explain."
    )

    writer_payload = {
        "fan_message_id": int(fan_msg_id),
        "thread_id": int(thread_id),
        "creator_psychic_card": thread_row.get("creator_psychic_card") or {},
        "thread_history": raw_turns or "",
        "latest_fan_message": _format_fan_turn(msg_row) or (msg_row.get("message_text") or ""),
        "turn_directive": directive,
    }
    moment_compass = (details.get("moment_compass") or "").strip()
    if moment_compass:
        writer_payload["moment_compass"] = moment_compass
    if fan_psychic_card:
        writer_payload["fan_psychic_card"] = fan_psychic_card
    if run_id:
        writer_payload["run_id"] = str(run_id)
    return writer_payload


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
    run_id = payload.get("run_id")
    if run_id:
        set_run_current_step(str(run_id), "join", client=SB)
        started_at = step_started_at(
            run_id=str(run_id), step="join", attempt=0, client=SB
        ) or datetime.now(timezone.utc).isoformat()
        upsert_step(
            run_id=str(run_id),
            step="join",
            attempt=0,
            status="running",
            client=SB,
            message_id=int(fan_msg_id),
            started_at=started_at,
            meta={"queue": QUEUE},
        )
        if not is_run_active(str(run_id), client=SB):
            upsert_step(
                run_id=str(run_id),
                step="join",
                attempt=0,
                status="canceled",
                client=SB,
                message_id=int(fan_msg_id),
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="run_canceled",
            )
            return True
    select_fields = (
        "thread_id,kairos_status,web_research_status,extras,hermes_status,hermes_output_raw,moment_compass"
    )
    if CONTENT_PACK_ENABLED:
        select_fields += (
            ",content_request,content_pack,content_pack_status,content_pack_error,content_pack_created_at"
        )
    try:
        details_rows = (
            SB.table("message_ai_details")
            .select(select_fields)
            .eq("message_id", fan_msg_id)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception:
        # Back-compat: older schemas may not have moment_compass.
        fallback_fields = select_fields.replace(",moment_compass", "")
        details_rows = (
            SB.table("message_ai_details")
            .select(fallback_fields)
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
    # before letting downstream steps proceed.
    try:
        msg_rows = (
            SB.table("messages")
            .select("media_status,media_payload,message_text,media_analysis_text,turn_index")
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
    iris_hermes_mode, iris_kairos_mode, iris_napoleon_mode, has_iris = _extract_iris_modes(extras)
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

    # If Iris control is enabled and Iris decided to SKIP Hermes, Hermes output may
    # never exist. In that case, build a minimal "routing decision" from Iris so
    # we can still gate correctly.
    if not hermes_parsed and has_iris and iris_hermes_mode == "skip":
        hermes_parsed = {
            "final_verdict_search": "NO",
            "final_verdict_kairos": (iris_kairos_mode or "lite").upper(),
            "join_requirements": "NONE",
            "web_research_brief": "NONE",
        }

    # If Hermes is expected to run (Iris did not skip it), but we don't have a
    # Hermes decision yet, wait.
    if not hermes_parsed and (not has_iris or iris_hermes_mode != "skip"):
        return True

    # Iris can optionally override Kairos mode, but only when explicitly enabled.
    if hermes_parsed and has_iris and iris_kairos_mode:
        hermes_parsed["final_verdict_kairos"] = (iris_kairos_mode or "lite").upper()

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

    if not need_kairos:
        current_kairos = (details.get("kairos_status") or "").strip().lower()
        if not current_kairos or current_kairos == "pending":
            try:
                SB.table("message_ai_details").update({"kairos_status": "ok"}).eq(
                    "message_id", fan_msg_id
                ).execute()
            except Exception:
                pass

    if not done:
        return True

    join_blob = extras.get("join") or {}
    if join_blob.get("napoleon_enqueued_at"):
        return True

    if join_blob.get("writer_enqueued_at"):
        return True

    if iris_napoleon_mode != "skip":
        if job_exists(NAPOLEON_QUEUE, fan_msg_id, client=SB):
            return True
    else:
        if job_exists(WRITER_QUEUE, fan_msg_id, client=SB, field="fan_message_id"):
            return True

    if run_id and not is_run_active(str(run_id), client=SB):
        upsert_step(
            run_id=str(run_id),
            step="join",
            attempt=0,
            status="canceled",
            client=SB,
            message_id=int(fan_msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error="run_canceled_pre_enqueue_napoleon",
        )
        return True

    if iris_napoleon_mode == "skip":
        writer_payload = _build_writer_payload(
            fan_msg_id=int(fan_msg_id),
            thread_id=int(details["thread_id"]),
            turn_index=msg_row.get("turn_index"),
            details=details,
            msg_row=msg_row,
            run_id=str(run_id) if run_id else None,
        )
        send(WRITER_QUEUE, writer_payload)
        join_blob["writer_enqueued_at"] = datetime.now(timezone.utc).isoformat()
    else:
        napoleon_payload = {"message_id": fan_msg_id}
        if has_iris:
            napoleon_payload["napoleon_mode"] = iris_napoleon_mode
        if run_id:
            napoleon_payload["run_id"] = str(run_id)
        send(NAPOLEON_QUEUE, napoleon_payload)
        join_blob["napoleon_enqueued_at"] = datetime.now(timezone.utc).isoformat()
    merged_extras = _merge_extras(extras, {"join": join_blob})

    SB.table("message_ai_details").update({"extras": merged_extras}).eq(
        "message_id", fan_msg_id
    ).execute()

    if run_id:
        upsert_step(
            run_id=str(run_id),
            step="join",
            attempt=0,
            status="ok",
            client=SB,
            message_id=int(fan_msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            meta={
                "need_kairos": need_kairos,
                "need_web": need_web,
            },
        )
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
