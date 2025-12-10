"""Prompt construction helpers shared across workers.

This module centralizes all prompt-building logic so that each worker can stay
focused on orchestration instead of rebuilding context plumbing every time.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable

from supabase import create_client
from workers.lib.cards import filter_card_by_tier
from workers.lib.time_tier import parse_tier

_SB = None  # Lazily created Supabase client

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
SUMMARY_BLOCK_SPEC = {
    "LIFETIME_BLOCK": ("lifetime", 6),
    "YEAR_BLOCK": ("year", 6),
    "SEASON_BLOCK": ("season", 6),
    "CHAPTER_BLOCK": ("chapter", 6),
    "EPISODE_BLOCK": ("episode", 6),
}
ROLLUP_SPEC = {
    # lower tier -> (upper tier, chunk size)
    "episode": ("chapter", 3),
    "chapter": ("season", 3),
    "season": ("year", 3),
    "year": ("lifetime", 3),
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


def _format_psychic_card(card: Any) -> str:
    """
    Return a condensed psychic card. If all segments are empty (or card missing),
    render a clear placeholder so the prompt doesn't show an empty schema.
    """

    if card is None:
        return "Psychic card: empty"
    if isinstance(card, str):
        stripped = card.strip()
        return stripped if stripped else "Psychic card: empty"
    if isinstance(card, dict):
        segments = card.get("segments") or {}
        names = card.get("segment_names") or {}
        lines: list[str] = []
        for key, value in segments.items():
            if not value:
                continue
            label = names.get(str(key)) or str(key)
            lines.append(f"{label}: {_stringify(value)}")
        if not lines:
            return "Psychic card: empty"
        return "Psychic card:\n" + "\n".join(lines)
    return _stringify(card)


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


def _latest_summary_end(thread_id: int, tier: str, *, client=None) -> int:
    """Return the end_turn of the most recent summary for a tier."""

    sb = _resolve_client(client)
    row = (
        sb.table("summaries")
        .select("end_turn")
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not row:
        return 0
    return int(row[0].get("end_turn") or 0)


def _consumed_threshold(thread_id: int, tier: str, *, client=None) -> int:
    """
    Return the tier_index threshold of lower-tier summaries that have been rolled up.

    If 2 Chapter summaries exist and each consumes 3 Episodes, the Episode tier_index
    threshold would be 6 (skip tiers <= 6 when building blocks).
    """

    spec = ROLLUP_SPEC.get(tier)
    if not spec:
        return 0

    upper_tier, chunk_size = spec
    sb = _resolve_client(client)
    upper_row = (
        sb.table("summaries")
        .select("tier_index")
        .eq("thread_id", thread_id)
        .eq("tier", upper_tier)
        .order("tier_index", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not upper_row:
        return 0

    upper_count = int(upper_row[0].get("tier_index") or 0)
    return upper_count * chunk_size


def make_block(thread_id: int, tier: str, limit: int = 4, *, client=None) -> str:
    """Return a zebra block of summaries for a given tier."""

    sb = _resolve_client(client)
    threshold = _consumed_threshold(thread_id, tier, client=sb)
    query = (
        sb.table("summaries")
        .select(
            "id,thread_id,tier,tier_index,"
            "start_turn,end_turn,"
            "narrative_summary,abstract_summary"
        )
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index", desc=True)
    )
    if threshold:
        query = query.gt("tier_index", threshold)

    rows = (query.limit(limit).execute().data) or []
    if not rows:
        return f"[No {tier.title()} summaries yet]"

    blocks = []
    total = len(rows)
    for idx, row in enumerate(reversed(rows), 1):
        # Newest summary should be #1 (like turns), oldest gets the highest number.
        recency_number = total - idx + 1
        start, end = row.get("start_turn"), row.get("end_turn")
        if start is not None and end is not None:
            label = f"{tier.title()} {recency_number} – Turns {start}-{end}"
        else:
            label = f"{tier.title()} {recency_number} – #{row.get('id') or idx}"
        narrative = row.get("narrative_summary")
        abstract = row.get("abstract_summary")
        if narrative:
            blocks.append(f"{label} – Narrative:\n{narrative.strip()}")
        if abstract:
            blocks.append(f"{label} – Abstract:\n{abstract.strip()}")
    return "\n\n".join(blocks)


def _load_cards(thread_id: int, *, client=None, worker_tier=None) -> Dict[str, str]:
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

    fan_psychic = row.get("fan_psychic_card") or "[Fan Psychic Card unavailable]"
    if worker_tier is not None and isinstance(fan_psychic, dict):
        try:
            tier_enum = parse_tier(worker_tier)
            fan_psychic = filter_card_by_tier(fan_psychic, tier_enum)
        except ValueError:
            # Fall back to the unfiltered card if tier parsing fails
            pass

    return {
        "FAN_IDENTITY_CARD": row.get("fan_identity_card") or "Identity card: empty",
        "FAN_PSYCHIC_CARD": _format_psychic_card(fan_psychic),
        "CREATOR_IDENTITY_CARD": row.get("creator_identity_card") or "Identity card: empty",
        "CREATOR_PSYCHIC_CARD": _format_psychic_card(row.get("creator_psychic_card")),
    }


def _recent_episode_abstracts(thread_id: int, *, client=None, count: int = 2) -> Dict[str, str]:
    """Fetch the two freshest episode abstracts for rolling history slots."""
    sb = _resolve_client(client)
    rows = (
        sb.table("summaries")
        .select("tier,tier_index,abstract_summary")
        .eq("thread_id", thread_id)
        .eq("tier", "episode")
        .order("tier_index", desc=True)
        .limit(count)
        .execute()
        .data
        or []
    )
    values = {
        "Abstract_N-2": "[No Episode N-2]",
        "Abstract_N-1": "[No Episode N-1]",
    }
    if rows:
        values["Abstract_N-1"] = rows[0].get("abstract_summary") or values["Abstract_N-1"]
    if len(rows) > 1:
        values["Abstract_N-2"] = rows[1].get("abstract_summary") or values["Abstract_N-2"]
    return values


def _recent_tier_abstracts(thread_id: int, tier: str, *, client=None, count: int = 2) -> Dict[str, str]:
    """Fetch freshest abstracts for an arbitrary tier."""
    sb = _resolve_client(client)
    rows = (
        sb.table("summaries")
        .select("tier,tier_index,abstract_summary")
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index", desc=True)
        .limit(count)
        .execute()
        .data
        or []
    )
    values = {
        "Abstract_N-2": f"[No {tier.title()} N-2]",
        "Abstract_N-1": f"[No {tier.title()} N-1]",
    }
    if rows:
        values["Abstract_N-1"] = rows[0].get("abstract_summary") or values["Abstract_N-1"]
    if len(rows) > 1:
        values["Abstract_N-2"] = rows[1].get("abstract_summary") or values["Abstract_N-2"]
    return values


def live_turn_window(
    thread_id: int,
    boundary_turn: int | None = None,
    limit: int = MAX_RECENT_TURNS,
    *,
    client=None,
) -> str:
    sb = _resolve_client(client)
    cutoff_turn = _latest_summary_end(thread_id, "episode", client=sb)

    query = (
        sb.table("messages")
        .select("id,turn_index,sender,message_text,created_at")
        .eq("thread_id", thread_id)
        .gt("turn_index", cutoff_turn)
        .order("turn_index", desc=True)
        .limit(limit)
    )
    if boundary_turn is not None:
        # Exclude the current turn; only include earlier turns in the context window.
        query = query.lt("turn_index", boundary_turn)

    rows = query.execute().data or []

    if not rows:
        return ""

    # Present oldest -> newest (top to bottom), numbering by recency (newest = 1).
    lines: list[str] = []
    total = len(rows)
    for idx, row in enumerate(reversed(rows), 1):
        turn_number = total - idx + 1  # newest message becomes Turn 1
        sender_raw = (row.get("sender") or "").strip().lower()
        if sender_raw.startswith("f"):
            sender_label = "Fan"
        elif sender_raw.startswith("c"):
            sender_label = "Creator"
        else:
            sender_label = sender_raw.title() or "Unknown"
        turn_label = f"Turn {turn_number}"
        text = _clean_turn_text(row.get("message_text"))
        ts = _format_turn_timestamp(row.get("created_at"))
        lines.append(f"{turn_label} @ {ts} ({sender_label}): {text}")

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
    # Strip common storage prefixes like "Message 1:" to make turns feel like chat.
    if single_line.lower().startswith("message 1:"):
        single_line = single_line[len("message 1:") :].lstrip()
    return single_line[:2000]


def _format_turn_timestamp(value: Any) -> str:
    """Render a compact UTC timestamp for a turn."""
    if not value:
        return "unknown time"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        try:
            cleaned = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        except ValueError:
            return value
    else:
        return str(value)

    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def latest_kairos_json(thread_id: int, *, client=None) -> str:
    """Return the most recent Kairos analysis row for this thread."""

    sb = _resolve_client(client)
    rows = (
        sb.table("message_ai_details")
        .select(
            "message_id,strategic_narrative,alignment_status,"
            "conversation_criticality,tactical_signals,psychological_levers,"
            "risks,turn_micro_note,kairos_summary"
        )
        .eq("thread_id", thread_id)
        .eq("sender", "fan")
        .eq("kairos_status", "ok")
        .order("message_id", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )

    if not rows:
        return ""

    row = rows[0]
    safe_keys = [
        "message_id",
        "strategic_narrative",
        "alignment_status",
        "conversation_criticality",
        "tactical_signals",
        "psychological_levers",
        "risks",
        "turn_micro_note",
        "kairos_summary",
    ]
    clean = {k: row.get(k) for k in safe_keys if row.get(k) is not None}
    return json.dumps(clean, ensure_ascii=False)


def latest_plan_fields(thread_id: int, *, client=None) -> dict:
    sb = _resolve_client(client)
    # Prefer canonical plans stored on the thread; fall back to last Napoleon snapshot.
    thread_row = (
        sb.table("threads")
        .select(
            "episode_plan,chapter_plan,season_plan,year_plan,lifetime_plan"
        )
        .eq("id", thread_id)
        .single()
        .execute()
        .data
        or {}
    )

    if any(
        thread_row.get(col) for col in
        ("episode_plan", "chapter_plan", "season_plan", "year_plan", "lifetime_plan")
    ):
        row = thread_row
    else:
        snapshot = (
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
        row = snapshot[0] if snapshot else {}

    return {
        "CREATOR_TACTICAL_PLAN_3TURN": row.get("tactical_plan_3turn") or "",
        "CREATOR_EPISODE_PLAN": row.get("plan_episode") or row.get("episode_plan") or "",
        "CREATOR_CHAPTER_PLAN": row.get("plan_chapter") or row.get("chapter_plan") or "",
        "CREATOR_SEASON_PLAN": row.get("plan_season") or row.get("season_plan") or "",
        "CREATOR_YEAR_PLAN": row.get("plan_year") or row.get("year_plan") or "",
        "CREATOR_LIFETIME_PLAN": row.get("plan_lifetime") or row.get("lifetime_plan") or "",
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
    latest_fan_text: str | None = None,
    *,
    client=None,
    extra_context: Dict[str, str] | None = None,
    include_blocks: bool = True,
    include_plans: bool = True,
    include_analyst: bool = True,
    include_episode_rolling: bool = False,
    worker_tier=None,
) -> str:
    """Shared renderer that replaces macros in a prompt template."""

    sb = _resolve_client(client)
    template = _load_template(template_name)

    context: Dict[str, str] = {
        "RAW_TURNS": raw_turns.strip() if raw_turns else "[No raw turns provided]",
    }

    if include_blocks:
        for macro, (tier, limit) in SUMMARY_BLOCK_SPEC.items():
            context[macro] = make_block(thread_id, tier, limit, client=sb)

    context.update(_load_cards(thread_id, client=sb, worker_tier=worker_tier))

    if include_episode_rolling:
        context.update(_recent_episode_abstracts(thread_id, client=sb))

    if extra_context:
        context.update(extra_context)

    if latest_fan_text is not None:
        context["FAN_LATEST_VERBATIM"] = str(latest_fan_text)
    else:
        first_line = raw_turns.splitlines()[0] if raw_turns else ""
        context["FAN_LATEST_VERBATIM"] = first_line

    if include_analyst:
        context["ANALYST_ANALYSIS_JSON"] = latest_kairos_json(thread_id, client=sb)

    if include_plans:
        context.update(latest_plan_fields(thread_id, client=sb))
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
        template = template.replace(f"{{{key}}}", str(value))

    return template


def build_prompt(
    template_name: str,
    thread_id: int,
    raw_turns: str,
    latest_fan_text: str | None = None,
    *,
    client=None,
    extra_context: Dict[str, str] | None = None,
    include_blocks: bool = True,
    include_plans: bool = True,
    include_analyst: bool = True,
    include_episode_rolling: bool = False,
    worker_tier=None,
) -> str:
    """Load a template and replace all macro tags in one pass."""
    return _render_template(
        template_name,
        thread_id,
        raw_turns,
        latest_fan_text=latest_fan_text,
        client=client,
        extra_context=extra_context,
        include_blocks=include_blocks,
        include_plans=include_plans,
        include_analyst=include_analyst,
        include_episode_rolling=include_episode_rolling,
        worker_tier=worker_tier,
    )


def build_prompt_sections(
    template_name: str,
    thread_id: int,
    raw_turns: str,
    latest_fan_text: str | None = None,
    *,
    client=None,
    extra_context: Dict[str, str] | None = None,
    include_blocks: bool = True,
    include_plans: bool = True,
    include_analyst: bool = True,
    include_episode_rolling: bool = False,
    worker_tier=None,
) -> tuple[str, str]:
    """
    Render a template and split into (system_prompt, user_message) for chat models.
    Expects the template to contain a marked user block (e.g., <ANALYST_INPUT> or <NAPOLEON_INPUT>).
    """
    rendered = _render_template(
        template_name,
        thread_id,
        raw_turns,
        latest_fan_text=latest_fan_text,
        client=client,
        extra_context=extra_context,
        include_blocks=include_blocks,
        include_plans=include_plans,
        include_analyst=include_analyst,
        include_episode_rolling=include_episode_rolling,
        worker_tier=worker_tier,
    )

    # Prefer <NAPOLEON_INPUT> when present; otherwise fall back to analyst markers.
    marker = "NAPOLEON_INPUT" if "<NAPOLEON_INPUT>" in rendered else "ANALYST_INPUT"
    start_tag = f"<{marker}>"
    end_tag = f"</{marker}>"

    # Use the last block to avoid matching instructional mentions near the top.
    start = rendered.rfind(start_tag)
    end = rendered.rfind(end_tag)

    if start == -1 or end == -1 or end <= start:
        # Fallback: treat entire prompt as user content
        return rendered.strip(), ""

    system_prompt = rendered[: start].strip()
    user_body = rendered[start + len(start_tag) : end].strip()
    user_message = f"{start_tag}\n{user_body}\n{end_tag}"
    return system_prompt, user_message


__all__ = [
    "build_prompt",
    "build_prompt_sections",
    "live_turn_window",
    "make_block",
    "_recent_tier_abstracts",
]
