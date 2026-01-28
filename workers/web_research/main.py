"""
Web research worker — calls a web-capable model and stores findings on message_ai_details.extras.web_research.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from openai import OpenAI
from supabase import ClientOptions, create_client

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

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for WebResearch")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

QUEUE = "web.research"
JOIN_QUEUE = "iris.join"
PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"

OPENAI_API_KEY = os.getenv("WEB_RESEARCH_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL")
WEB_RESEARCH_MODEL = os.getenv("WEB_RESEARCH_MODEL", "gpt-4o-mini")

OPENAI_CLIENT = (
    OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL) if OPENAI_API_KEY else None
)


def _load_prompt() -> str:
    path = PROMPTS_DIR / "web_research.txt"
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return "You are a factual web research assistant. Return concise findings and sources."


def _merge_extras(existing: dict, patch: dict) -> dict:
    merged = {}
    merged.update(existing or {})
    merged.update(patch or {})
    return merged


def _extract_response_text(resp) -> str:
    if hasattr(resp, "output_text") and resp.output_text:
        return resp.output_text
    output = getattr(resp, "output", None) or []
    for item in output:
        content = getattr(item, "content", None) or []
        for part in content:
            if getattr(part, "type", None) == "output_text":
                text = getattr(part, "text", None)
                if text:
                    return text
    return ""


def _call_model(system_prompt: str, user_payload: dict) -> str:
    if not OPENAI_CLIENT:
        raise RuntimeError("WEB_RESEARCH_OPENAI_API_KEY or OPENAI_API_KEY is not set")

    prompt_block = (
        f"<WEB_RESEARCH_INPUT>\n"
        f"{json.dumps(user_payload, ensure_ascii=False, indent=2)}\n"
        f"</WEB_RESEARCH_INPUT>"
    )
    use_web_search = os.getenv("WEB_RESEARCH_USE_WEB_SEARCH", "true").lower() in {
        "1",
        "true",
        "yes",
    }
    if use_web_search:
        try:
            resp = OPENAI_CLIENT.responses.create(
                model=WEB_RESEARCH_MODEL,
                tools=[{"type": "web_search"}],
                temperature=float(os.getenv("WEB_RESEARCH_TEMPERATURE", "0.2")),
                max_output_tokens=int(os.getenv("WEB_RESEARCH_MAX_TOKENS", "1200")),
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt_block},
                ],
            )
            raw_text = _extract_response_text(resp)
            if raw_text:
                return raw_text
        except Exception:
            pass

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt_block},
    ]
    resp = OPENAI_CLIENT.chat.completions.create(
        model=WEB_RESEARCH_MODEL,
        messages=messages,
        temperature=float(os.getenv("WEB_RESEARCH_TEMPERATURE", "0.2")),
        max_tokens=int(os.getenv("WEB_RESEARCH_MAX_TOKENS", "1200")),
    )
    raw_text = (
        resp.choices[0].message.content
        if resp and resp.choices and resp.choices[0].message
        else ""
    )
    return raw_text or ""


def process_job(payload: Dict[str, Any]) -> bool:
    if not payload or "message_id" not in payload:
        raise ValueError(f"Malformed job payload: {payload}")

    fan_msg_id = payload["message_id"]
    brief = payload.get("brief") or "NONE"
    attempt = int(payload.get("web_retry", 0))
    run_id = payload.get("run_id")

    if run_id:
        set_run_current_step(str(run_id), "web", client=SB)
        started_at = step_started_at(
            run_id=str(run_id), step="web", attempt=attempt, client=SB
        ) or datetime.now(timezone.utc).isoformat()
        upsert_step(
            run_id=str(run_id),
            step="web",
            attempt=attempt,
            status="running",
            client=SB,
            message_id=int(fan_msg_id),
            started_at=started_at,
            meta={"queue": QUEUE, "brief": brief},
        )
        if not is_run_active(str(run_id), client=SB):
            upsert_step(
                run_id=str(run_id),
                step="web",
                attempt=attempt,
                status="canceled",
                client=SB,
                message_id=int(fan_msg_id),
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="run_canceled",
            )
            return True

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
        # Create a minimal row so we can store web research output without
        # violating NOT NULL constraints (raw_hash, kairos_status).
        try:
            SB.table("message_ai_details").insert(
                {
                    "message_id": fan_msg_id,
                    "thread_id": thread_id,
                    "sender": "fan",
                    "raw_hash": hashlib.sha256(
                        f"web_research_seed:{thread_id}:{fan_msg_id}".encode("utf-8")
                    ).hexdigest(),
                    "kairos_status": "pending",
                    "extras": existing_extras,
                }
            ).execute()
        except Exception:
            pass

    system_prompt = _load_prompt()
    user_payload = {
        "brief": brief,
        "message_text": msg_row.get("message_text") or "",
        "media_analysis_text": msg_row.get("media_analysis_text") or "",
        "media_payload": msg_row.get("media_payload") or {},
    }

    status = "failed"
    raw_text = ""
    error = None
    facts_pack: Any = None

    try:
        raw_text = _call_model(system_prompt, user_payload)
        if not raw_text.strip():
            raise RuntimeError("empty_output")
        status = "ok"
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = str(exc)

    if status == "failed" and attempt < 1:
        retry_payload = {
            "message_id": fan_msg_id,
            "brief": brief,
            "web_retry": attempt + 1,
            "run_id": str(run_id) if run_id else None,
        }
        send(QUEUE, retry_payload)
        web_blob = {
            "status": "retrying",
            "brief": brief,
            "raw_output": raw_text,
            "facts_pack": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "error": error,
        }
        merged_extras = _merge_extras(existing_extras, {"web_research": web_blob})
        SB.table("message_ai_details").update(
            {
                "extras": merged_extras,
                "web_research_status": "retrying",
            }
        ).eq("message_id", fan_msg_id).execute()
        if run_id:
            upsert_step(
                run_id=str(run_id),
                step="web",
                attempt=attempt,
                status="failed",
                client=SB,
                message_id=int(fan_msg_id),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error=error or "retrying",
            )
        return True

    if raw_text:
        try:
            facts_pack = json.loads(raw_text)
        except Exception:
            facts_pack = raw_text

    web_blob = {
        "status": status,
        "brief": brief,
        "raw_output": raw_text,
        "facts_pack": facts_pack,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        web_blob["error"] = error

    merged_extras = _merge_extras(existing_extras, {"web_research": web_blob})
    SB.table("message_ai_details").update(
        {
            "extras": merged_extras,
            "web_research_status": status,
            "web_research_output_raw": raw_text,
            "web_research_facts_pack": facts_pack,
        }
    ).eq("message_id", fan_msg_id).execute()

    if run_id and not is_run_active(str(run_id), client=SB):
        upsert_step(
            run_id=str(run_id),
            step="web",
            attempt=attempt,
            status="canceled",
            client=SB,
            message_id=int(fan_msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error="run_canceled_post_compute",
        )
        return True

    if not job_exists(JOIN_QUEUE, fan_msg_id, client=SB):
        join_payload = {"message_id": fan_msg_id}
        if run_id:
            join_payload["run_id"] = str(run_id)
        send(JOIN_QUEUE, join_payload)

    if run_id:
        upsert_step(
            run_id=str(run_id),
            step="web",
            attempt=attempt,
            status="ok" if status == "ok" else "failed",
            client=SB,
            message_id=int(fan_msg_id),
            ended_at=datetime.now(timezone.utc).isoformat(),
            error=error,
        )
    return True


if __name__ == "__main__":
    print("[WebResearch] started - waiting for jobs", flush=True)
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
            print("[WebResearch] error:", exc)
            traceback.print_exc()
            # Let the job retry
            time.sleep(2)
