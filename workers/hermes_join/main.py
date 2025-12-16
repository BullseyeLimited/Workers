"""
Hermes join gate — enqueues Napoleon once required upstream results are present (or failed).
"""

from __future__ import annotations

import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from supabase import ClientOptions, create_client

from workers.lib.job_utils import job_exists
from workers.lib.simple_queue import ack, receive, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

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


def _merge_extras(existing: dict, patch: dict) -> dict:
    merged = {}
    merged.update(existing or {})
    merged.update(patch or {})
    return merged


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
    details = (
        SB.table("message_ai_details")
        .select("thread_id,kairos_status,extras")
        .eq("message_id", fan_msg_id)
        .single()
        .execute()
        .data
    )
    if not details:
        return True

    extras = details.get("extras") or {}
    hermes_parsed = None
    hermes_blob = extras.get("hermes")
    if isinstance(hermes_blob, dict):
        parsed = hermes_blob.get("parsed")
        if isinstance(parsed, dict):
            hermes_parsed = parsed

    if not hermes_parsed:
        # Hermes decision missing; nothing to do yet.
        return True

    web_blob = extras.get("web_research") or {}
    web_status = web_blob.get("status")

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
