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
            "id,thread_id,tier,tier_index,"
            "start_turn,end_turn,"
            "narrative_summary,abstract_summary"
        )
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index", desc=True)
        .limit(limit)
        .execute()
    )
    rows = response.data or []
    if not rows:
        return f"[No {tier.title()} summaries yet]"

    blocks = []
    for idx, row in enumerate(reversed(rows), 1):
        start, end = row.get("start_turn"), row.get("end_turn")
        if start is not None and end is not None:
            label = f"{tier.title()} {start}-{end}"
        else:
            label = f"{tier.title()} #{row.get('id') or idx}"
        narrative = row.get("narrative_summary")
        abstract = row.get("abstract_summary")
        if narrative:
            blocks.append(f"{label} – Narrative:\n{narrative.strip()}")
        if abstract:
            blocks.append(f"{label} – Abstract:\n{abstract.strip()}")
    return "\n\n".join(blocks)


def _load_cards(thread_id: int, *, client=None) -> Dict[str, str]:
    sb = _resolve_client(client)
    row = (
        sb.table("threads")
        .select(
            "fan_identity_card,fan_psychic_card,"
            "creator_identity_card,creator_psychic_card"
        )
        .eq("id", thread_id)
        .single()
        .execute()
        .data
        or {}
    )

    return {
        "FAN_IDENTITY_CARD": row.get("fan_identity_card") or "[Fan Identity Card unavailable]",
        "FAN_PSYCHIC_CARD": row.get("fan_psychic_card") or "[Fan Psychic Card unavailable]",
        "CREATOR_IDENTITY_CARD": row.get("creator_identity_card") or "[Creator Identity Card unavailable]",
        "CREATOR_PSYCHIC_CARD": row.get("creator_psychic_card") or "[Creator Psychic Card unavailable]",
    }


def live_turn_window(
    thread_id: int,
    boundary_turn: int | None = None,
    limit: int = MAX_RECENT_TURNS,
    *,
    client=None,
) -> str:
    sb = _resolve_client(client)
    if boundary_turn is None:
        latest = (
            sb.table("messages")
            .select("turn_index")
            .eq("thread_id", thread_id)
            .order("turn_index", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        boundary_turn = latest[0]["turn_index"] if latest else 0

    rows = (
        sb.table("messages")
        .select(
            "id,turn_index,sender,message_text,"
            "message_ai_details:message_ai_details!"
            "message_ai_details_message_id_fkey(kairos_summary)"
        )
        .eq("thread_id", thread_id)
        .gt("turn_index", boundary_turn)
        .order("turn_index", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )

    if not rows:
        return ""

    half = len(rows) // 2 or 1
    lines: list[str] = []
    for idx, row in enumerate(rows):
        sender_key = (row.get("sender") or "").strip()[:1].upper() or "?"
        text = row.get("message_text") or ""
        details = row.get("message_ai_details") or {}
        if isinstance(details, list):
            details = details[0] if details else {}
        summary = details.get("kairos_summary")

        if idx < half:
            lines.append(f"[{sender_key}] {text}")
        else:
            if sender_key == "F" and summary:
                lines.append(f"[F_SUM] {summary}")
            else:
                lines.append(f"[{sender_key}] {text}")

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
    """Return the most recent Kairos analysis row for this thread."""

    sb = _resolve_client(client)
    rows = (
        sb.table("message_ai_details")
        .select("*")
        .eq("thread_id", thread_id)
        .eq("sender", "fan")
        .eq("extract_status", "ok")
        .order("message_id", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )

    if not rows:
        return ""

    return json.dumps(rows[0], ensure_ascii=False)


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


def recent_plan_summaries(
    thread_id: int, horizon: str, limit: int = 5, *, client=None
) -> str:
    sb = _resolve_client(client)
    rows = (
        sb.table("plan_history")
        .select("summary_json")
        .eq("thread_id", thread_id)
        .eq("horizon", horizon)
        .order("id", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    return "\n".join(
        json.dumps(row["summary_json"], ensure_ascii=False) for row in rows
    )


def _render_template(
    template_name: str,
    thread_id: int,
    raw_turns: str,
    *,
    client=None,
) -> str:
    """Shared renderer that replaces macros in a prompt template."""

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

    context.update(
        {
            "EPISODE_PLAN_HISTORY": recent_plan_summaries(
                thread_id, "episode", 5, client=sb
            ),
            "CHAPTER_PLAN_HISTORY": recent_plan_summaries(
                thread_id, "chapter", 5, client=sb
            ),
            "SEASON_PLAN_HISTORY": recent_plan_summaries(
                thread_id, "season", 3, client=sb
            ),
            "YEAR_PLAN_HISTORY": recent_plan_summaries(
                thread_id, "year", 3, client=sb
            ),
            "LIFETIME_PLAN_HISTORY": recent_plan_summaries(
                thread_id, "lifetime", 2, client=sb
            ),
        }
    )

    for key, value in context.items():
        template = template.replace(f"{{{key}}}", value)

    return template


def build_prompt(
    template_name: str,
    thread_id: int,
    raw_turns: str,
    *,
    client=None,
) -> str:
    """Load a template and replace all macro tags in one pass."""
    return _render_template(template_name, thread_id, raw_turns, client=client)


def build_prompt_sections(
    template_name: str,
    thread_id: int,
    raw_turns: str,
    *,
    client=None,
) -> tuple[str, str]:
    """
    Render a template and split into (system_prompt, user_message) for chat models.
    Expects the template to contain <ANALYST_INPUT> ... </ANALYST_INPUT> markers.
    """
    rendered = _render_template(template_name, thread_id, raw_turns, client=client)
    start = rendered.find("<ANALYST_INPUT>")
    end = rendered.find("</ANALYST_INPUT>")

    if start == -1 or end == -1 or end <= start:
        # Fallback: treat entire prompt as user content
        return rendered.strip(), ""

    system_prompt = rendered[: start].strip()
    user_body = rendered[start + len("<ANALYST_INPUT>") : end].strip()
    user_message = f"<ANALYST_INPUT>\n{user_body}\n</ANALYST_INPUT>"
    return system_prompt, user_message


__all__ = ["build_prompt", "build_prompt_sections", "live_turn_window", "make_block"]
