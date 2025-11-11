"""Prompt construction helpers shared across workers.

This module centralizes all prompt-building logic so that each worker can stay
focused on orchestration instead of rebuilding context plumbing every time.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable

from supabase import create_client

_SB = None  # Lazily created Supabase client

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
SUMMARY_BLOCK_SPEC = {
    "LIFETIME_BLOCK": ("lifetime", 2),
    "YEAR_BLOCK": ("year", 3),
    "SEASON_BLOCK": ("season", 4),
    "CHAPTER_BLOCK": ("chapter", 6),
    "EPISODE_BLOCK": ("episode", 6),
}
CARD_TAGS = {
    "fan_psychic": "FAN_PSYCHIC_CARD",
    "fan_identity": "FAN_IDENTITY_CARD",
    "creator_psychic": "CREATOR_PSYCHIC_CARD",
    "creator_identity": "CREATOR_IDENTITY_CARD",
}
NARRATIVE_KEYS = (
    "narrative",
    "narrative_text",
    "narrative_summary",
    "narrative_card",
    "narrative_output",
    "narrative_body",
)
ABSTRACT_KEYS = (
    "abstract",
    "abstract_text",
    "abstract_summary",
    "abstract_card",
    "abstract_output",
    "abstract_body",
)
MAX_RECENT_TURNS = 40
FRESH_FAN_SUMMARY_LIMIT = 10


def _resolve_client(client=None):
    """Return whichever Supabase client should be used for this call."""

    global _SB
    if client is not None:
        return client
    if _SB is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise RuntimeError("Supabase credentials missing in environment")
        _SB = create_client(url, key)
    return _SB


@lru_cache(maxsize=16)
def _load_template(template_name: str) -> str:
    path = PROMPTS_DIR / f"{template_name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Prompt template not found: {path}")
    return path.read_text(encoding="utf-8")


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("text", "body", "summary", "content"):
            if key in value and value[key]:
                inner = value[key]
                if isinstance(inner, str):
                    return inner
                return json.dumps(inner, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _extract_first(row: Dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        if key in row and row[key]:
            return _stringify(row[key])
    return ""


def make_block(thread_id: int, tier: str, limit: int = 4, *, client=None) -> str:
    """Return a zebra block of summaries for a given tier."""

    sb = _resolve_client(client)
    response = (
        sb.table("summaries")
        .select(
            "id,tier,label,start_turn,end_turn,"
            "narrative,narrative_text,narrative_summary,narrative_card," \
            "abstract,abstract_text,abstract_summary,abstract_card"
        )
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("end_turn", desc=True)
        .limit(limit)
        .execute()
    )
    rows = response.data or []
    if not rows:
        return f"[No {tier.title()} summaries yet]"

    blocks = []
    for idx, row in enumerate(reversed(rows), 1):
        label = row.get("label") or row.get("title")
        if not label:
            start, end = row.get("start_turn"), row.get("end_turn")
            if start is not None and end is not None:
                label = f"{tier.title()} {start}-{end}"
            else:
                label = f"{tier.title()} #{row.get('id') or idx}"
        narrative = _extract_first(row, NARRATIVE_KEYS)
        abstract = _extract_first(row, ABSTRACT_KEYS)
        if narrative:
            blocks.append(f"{label} - Narrative:\n{narrative.strip()}")
        if abstract:
            blocks.append(f"{label} - Abstract:\n{abstract.strip()}")
    return "\n\n".join(blocks)


def _load_cards(thread_id: int, *, client=None) -> Dict[str, str]:
    sb = _resolve_client(client)
    placeholders = {
        macro: f"[{macro.replace('_', ' ').title()} unavailable]"
        for macro in CARD_TAGS.values()
    }
    response = (
        sb.table("cards")
        .select("card_type,card_text,text,body,content")
        .eq("thread_id", thread_id)
        .in_("card_type", list(CARD_TAGS.keys()))
        .execute()
    )
    rows = response.data or []
    for row in rows:
        macro = CARD_TAGS.get(row.get("card_type"))
        if not macro:
            continue
        text = _stringify(
            row.get("card_text")
            or row.get("text")
            or row.get("body")
            or row.get("content")
        ).strip()
        placeholders[macro] = text or placeholders[macro]
    return placeholders


def live_turn_window(
    thread_id: int,
    boundary_turn: int | None = None,
    *,
    client=None,
) -> str:
    """Return a zebra block of the latest turns around the Episode boundary."""

    sb = _resolve_client(client)
    if boundary_turn is None:
        boundary_resp = (
            sb.table("summaries")
            .select("end_turn")
            .eq("thread_id", thread_id)
            .eq("tier", "episode")
            .order("end_turn", desc=True)
            .limit(1)
            .execute()
        )
        boundary_rows = boundary_resp.data or []
        boundary_turn = (
            boundary_rows[0].get("end_turn")
            if boundary_rows and boundary_rows[0].get("end_turn") is not None
            else 0
        )

    rows = (
        sb.table("messages")
        .select("id,turn_index,sender,message_text")
        .eq("thread_id", thread_id)
        .gt("turn_index", boundary_turn)
        .order("turn_index", desc=True)
        .limit(MAX_RECENT_TURNS)
        .execute()
        .data
        or []
    )
    if not rows:
        return "[No recent turns]"

    fan_ids = [
        row["id"]
        for idx, row in enumerate(rows)
        if idx < FRESH_FAN_SUMMARY_LIMIT and row.get("sender") == "fan"
    ]

    summaries: Dict[int, str] = {}
    if fan_ids:
        detail_rows = (
            sb.table("message_ai_details")
            .select("message_id,kairos_summary")
            .in_("message_id", fan_ids)
            .execute()
            .data
            or []
        )
        for detail in detail_rows:
            summary = _extract_summary_text(detail.get("kairos_summary"))
            if summary:
                summaries[detail["message_id"]] = summary

    lines = []
    for idx, row in enumerate(rows):
        turn = row["turn_index"]
        sender_key = row.get("sender") or ""
        sender = sender_key.title() or "?"
        text = _clean_turn_text(row.get("message_text"))
        prefix = f"{turn:04d} {sender}: {text}"
        if idx < FRESH_FAN_SUMMARY_LIMIT and sender_key == "fan":
            summary = summaries.get(row["id"], "N/A")
            lines.append(f"{prefix} / Summary: {summary}")
        else:
            lines.append(prefix)

    return "\n".join(lines)


def _extract_summary_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
    elif isinstance(value, dict):
        parsed = value
    else:
        return ""
    if isinstance(parsed, dict):
        summary = parsed.get("summary")
        if summary:
            return str(summary)
    return json.dumps(parsed, ensure_ascii=False)


def _clean_turn_text(text: Any) -> str:
    if not text:
        return "(empty)"
    single_line = " ".join(str(text).split())
    return single_line[:2000]


def latest_kairos_json(thread_id: int, *, client=None) -> str:
    sb = _resolve_client(client)
    row = (
        sb.table("messages")
        .select("message_ai_details")
        .eq("thread_id", thread_id)
        .neq("message_ai_details", None)
        .order("turn_index", desc=True)
        .limit(1)
        .execute()
        .data
    )
    payload = row[0]["message_ai_details"] if row else {}
    return json.dumps(payload or {}, ensure_ascii=False)


def latest_plan_fields(thread_id: int, *, client=None) -> dict:
    sb = _resolve_client(client)
    row = (
        sb.table("message_ai_details")
        .select(
            "tactical_plan_3turn,"
            "plan_episode,plan_chapter,plan_season,plan_year,plan_lifetime"
        )
        .eq("thread_id", thread_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    row = row[0] if row else {}
    return {
        "CREATOR_TACTICAL_PLAN_3TURN": row.get("tactical_plan_3turn") or "",
        "CREATOR_EPISODE_PLAN": row.get("plan_episode") or "",
        "CREATOR_CHAPTER_PLAN": row.get("plan_chapter") or "",
        "CREATOR_SEASON_PLAN": row.get("plan_season") or "",
        "CREATOR_YEAR_PLAN": row.get("plan_year") or "",
        "CREATOR_LIFETIME_PLAN": row.get("plan_lifetime") or "",
    }


def build_prompt(
    template_name: str,
    thread_id: int,
    raw_turns: str,
    *,
    client=None,
) -> str:
    """Load a template and replace all macro tags in one pass."""

    sb = _resolve_client(client)
    template = _load_template(template_name)

    context: Dict[str, str] = {
        "RAW_TURNS": raw_turns.strip() if raw_turns else "[No raw turns provided]",
    }

    for macro, (tier, limit) in SUMMARY_BLOCK_SPEC.items():
        context[macro] = make_block(thread_id, tier, limit, client=sb)

    context.update(_load_cards(thread_id, client=sb))

    context.update(latest_plan_fields(thread_id, client=sb))

    first_line = raw_turns.splitlines()[0] if raw_turns else ""
    context["FAN_LATEST_VERBATIM"] = first_line

    context["ANALYST_ANALYSIS_JSON"] = latest_kairos_json(thread_id, client=sb)

    for key, value in context.items():
        template = template.replace(f"{{{key}}}", value)

    return template


__all__ = ["build_prompt", "live_turn_window", "make_block"]
