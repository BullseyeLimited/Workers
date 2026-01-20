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
MIN_RECENT_TURNS = 20
FRESH_FAN_SUMMARY_LIMIT = 10


_IDENTITY_PLACEHOLDERS = {
    "n/a",
    "na",
    "none",
    "[none]",
    "null",
    "nil",
    "tbd",
    "todo",
    "unknown",
}


def _turn_rollover_cutoff(boundary_turn: int | None) -> int:
    """
    Return the turn_index cutoff for the raw-turn rolling window.

    Intended behavior: keep raw turns in a 20–40 window.
    - <20 prior turns: show all.
    - 20–39 prior turns: show all.
    - At 40 prior turns: consume the oldest 20 (cutoff=20), leaving 20.
    - Then grow again until the next 20-turn boundary, etc.
    """
    if boundary_turn is None:
        return 0
    try:
        prior_turns = max(0, int(boundary_turn) - 1)
    except Exception:
        return 0
    if MIN_RECENT_TURNS <= 0:
        return 0
    completed_blocks = prior_turns // MIN_RECENT_TURNS
    consumed_blocks = max(0, completed_blocks - 1)
    return consumed_blocks * MIN_RECENT_TURNS


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


def _format_identity_card_text(card_text: str) -> str:
    """
    Condense the fan identity card plain-text format by removing empty categories.

    Expected input format (from fan_card_narrative_writer):
      LONG_TERM
      category:
      fact line
      ...

      SHORT_TERM
      ...
    """
    if not isinstance(card_text, str) or not card_text.strip():
        return ""

    section = None
    category = None
    order: dict[str, list[str]] = {"LONG_TERM": [], "SHORT_TERM": []}
    entries: dict[str, dict[str, list[str]]] = {"LONG_TERM": {}, "SHORT_TERM": {}}

    for raw in card_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        upper = line.upper()
        if upper in {"LONG_TERM", "SHORT_TERM"}:
            section = upper
            category = None
            continue
        if line.endswith(":"):
            category = line[:-1].strip()
            if section and category and category not in order[section]:
                order[section].append(category)
            continue
        if section and category:
            entries[section].setdefault(category, []).append(line)

    output: list[str] = []
    for sec in ("LONG_TERM", "SHORT_TERM"):
        cats = [cat for cat in order[sec] if entries.get(sec, {}).get(cat)]
        if not cats:
            continue
        output.append(sec)
        for cat in cats:
            output.append(f"{cat}:")
            output.extend(entries[sec][cat])
        output.append("")

    return "\n".join(output).strip()


def _format_identity_card_json(card: Any) -> str:
    """
    Best-effort condensation of an identity card JSON snapshot.

    The stored identity card shape can be very large (mostly `null` / empty notes).
    This formatter emits only non-empty leaf values so prompts don't get spammed
    with `None`/`null` placeholders.
    """

    def _is_meaningful_string(value: str) -> bool:
        stripped = (value or "").strip()
        if not stripped:
            return False
        if stripped.lower() in _IDENTITY_PLACEHOLDERS:
            return False
        return True

    facts: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def walk(node: Any, path: list[str]) -> None:
        if node is None:
            return
        if isinstance(node, str):
            if not _is_meaningful_string(node):
                return
            key = "/".join(path)
            pair = (key, node.strip())
            if pair not in seen:
                seen.add(pair)
                facts.append(pair)
            return
        if isinstance(node, (int, float, bool)):
            key = "/".join(path)
            pair = (key, str(node))
            if pair not in seen:
                seen.add(pair)
                facts.append(pair)
            return
        if isinstance(node, list):
            for idx, item in enumerate(node):
                walk(item, path + [str(idx)])
            return
        if isinstance(node, dict):
            notes = node.get("notes")
            if isinstance(notes, str) and _is_meaningful_string(notes):
                key = "/".join(path)
                pair = (key, notes.strip())
                if pair not in seen:
                    seen.add(pair)
                    facts.append(pair)
            for k, v in node.items():
                if k == "notes":
                    continue
                walk(v, path + [str(k)])
            return

    walk(card, [])

    if not facts:
        return ""

    lines: list[str] = []
    for key, value in facts:
        label = key.replace("/", ".") if key else "fact"
        lines.append(f"{label}: {value}")
    return "\n".join(lines).strip()


def _format_identity_card(card: Any, *, card_text: str | None = None) -> str:
    if isinstance(card_text, str) and card_text.strip():
        formatted = _format_identity_card_text(card_text)
        if formatted:
            return formatted

    if card is None:
        return "Identity card: empty"

    if isinstance(card, str):
        stripped = card.strip()
        if not stripped:
            return "Identity card: empty"
        try:
            parsed = json.loads(stripped)
        except Exception:
            parsed = None
        if isinstance(parsed, (dict, list)):
            formatted = _format_identity_card_json(parsed)
            return formatted if formatted else "Identity card: empty"
        return stripped

    if isinstance(card, (dict, list)):
        formatted = _format_identity_card_json(card)
        return formatted if formatted else "Identity card: empty"

    return _stringify(card) or "Identity card: empty"


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


def make_block(
    thread_id: int,
    tier: str,
    limit: int = 4,
    *,
    client=None,
    max_end_turn: int | None = None,
) -> str:
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
    if max_end_turn is not None:
        query = query.lte("end_turn", max_end_turn)

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
            label = f"{tier.title()} {recency_number}"
            start_ts, end_ts = _summary_time_bounds(thread_id, start, end, client=sb)
            if start_ts or end_ts:
                label = (
                    f"{label} ("
                    f"{_format_turn_timestamp(start_ts) if start_ts else '?'}"
                    f" \u2192 "
                    f"{_format_turn_timestamp(end_ts) if end_ts else '?'}"
                    f")"
                )
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
    try:
        row = (
            sb.table("threads")
            .select(
                "fan_identity_card,fan_identity_card_raw,fan_psychic_card,"
                "creator_identity_card,creator_psychic_card,"
                "creator_id"
            )
            .eq("id", thread_id)
            .single()
            .execute()
            .data
            or {}
        )
    except Exception:
        # Backwards-compatible fallback for schemas without fan_identity_card_raw.
        row = (
            sb.table("threads")
            .select(
                "fan_identity_card,fan_psychic_card,"
                "creator_identity_card,creator_psychic_card,"
                "creator_id"
            )
            .eq("id", thread_id)
            .single()
            .execute()
            .data
            or {}
        )

    creator_row = {}
    creator_id = row.get("creator_id")
    if creator_id:
        try:
            creator_row = (
                sb.table("creators")
                .select("creator_identity_card,creator_psychic_card")
                .eq("id", creator_id)
                .single()
                .execute()
                .data
                or {}
            )
        except Exception:
            creator_row = {}

    fan_psychic = row.get("fan_psychic_card") or "[Fan Psychic Card unavailable]"
    if worker_tier is not None and isinstance(fan_psychic, dict):
        try:
            tier_enum = parse_tier(worker_tier)
            fan_psychic = filter_card_by_tier(fan_psychic, tier_enum)
        except ValueError:
            # Fall back to the unfiltered card if tier parsing fails
            pass

    creator_identity_card_value = (
        row.get("creator_identity_card")
        or creator_row.get("creator_identity_card")
    )
    creator_psychic_card = (
        row.get("creator_psychic_card")
        or creator_row.get("creator_psychic_card")
    )

    return {
        "FAN_IDENTITY_CARD": _format_identity_card(
            row.get("fan_identity_card"),
            card_text=row.get("fan_identity_card_raw"),
        ),
        "FAN_PSYCHIC_CARD": _format_psychic_card(fan_psychic),
        "CREATOR_IDENTITY_CARD": _format_identity_card(creator_identity_card_value),
        "CREATOR_PSYCHIC_CARD": _format_psychic_card(creator_psychic_card),
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
    exclude_message_id: int | None = None,
) -> str:
    sb = _resolve_client(client)

    # Rolling window (20–40): always keep at least 20 raw turns once available,
    # allow growth up to 40, then "consume" the oldest 20 into an Episode summary.
    #
    # IMPORTANT: Episode summaries may be created as soon as a 20-turn block completes,
    # but they must NOT truncate the raw turn window until the 40-turn ceiling is hit.
    # If the caller didn't provide a boundary, treat the latest stored message as
    # the boundary so cutoff math still works.
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
        if latest:
            try:
                boundary_turn = int(latest[0].get("turn_index") or 0) + 1
            except Exception:
                boundary_turn = None

    cutoff_turn = _turn_rollover_cutoff(boundary_turn)

    # Safety: only apply a non-zero cutoff when the corresponding Episode summary exists.
    if cutoff_turn:
        try:
            has_episode = (
                sb.table("summaries")
                .select("id")
                .eq("thread_id", thread_id)
                .eq("tier", "episode")
                .eq("end_turn", cutoff_turn)
                .limit(1)
                .execute()
                .data
            )
        except Exception:
            has_episode = []
        if not has_episode:
            cutoff_turn = 0

    query = (
        sb.table("messages")
        .select("id,turn_index,sender,message_text,media_analysis_text,created_at,content_id")
        .eq("thread_id", thread_id)
        .gt("turn_index", cutoff_turn)
        .order("turn_index", desc=True)
        .limit(limit)
    )
    if boundary_turn is not None:
        # Exclude the current turn; only include earlier turns in the context window.
        query = query.lt("turn_index", boundary_turn)
    if exclude_message_id is not None:
        query = query.neq("id", exclude_message_id)

    rows = query.execute().data or []

    if not rows:
        return ""

    message_ids = [
        row.get("id") for row in rows if isinstance(row, dict) and row.get("id")
    ]
    message_id_set = {mid for mid in message_ids if mid is not None}
    offers_by_message: dict[int, list[dict]] = {}
    deliveries_by_message: dict[int, list[int]] = {}
    if message_id_set:
        try:
            offer_rows = (
                sb.table("content_offers")
                .select("message_id,content_id,offered_price,purchased")
                .in_("message_id", list(message_id_set))
                .execute()
                .data
                or []
            )
        except Exception:
            offer_rows = []
        for row in offer_rows:
            msg_id = row.get("message_id")
            if msg_id is None:
                continue
            offers_by_message.setdefault(msg_id, []).append(row)

        try:
            delivery_rows = (
                sb.table("content_deliveries")
                .select("message_id,content_id")
                .in_("message_id", list(message_id_set))
                .execute()
                .data
                or []
            )
        except Exception:
            delivery_rows = []
        for row in delivery_rows:
            msg_id = row.get("message_id")
            if msg_id is None:
                continue
            content_id = row.get("content_id")
            if content_id is None:
                continue
            deliveries_by_message.setdefault(msg_id, []).append(content_id)

    for row in rows:
        msg_id = row.get("id")
        if msg_id is None:
            continue
        content_id = row.get("content_id")
        if content_id is None:
            continue
        deliveries_by_message.setdefault(msg_id, []).append(content_id)

    content_ids: set[int] = set()
    for content_list in deliveries_by_message.values():
        for content_id in content_list:
            try:
                content_ids.add(int(content_id))
            except Exception:
                continue
    for offer_list in offers_by_message.values():
        for offer in offer_list:
            try:
                content_ids.add(int(offer.get("content_id")))
            except Exception:
                continue

    item_lookup: dict[int, dict] = {}
    if content_ids:
        try:
            content_rows = (
                sb.table("content_items")
                .select("id,media_type,explicitness,desc_short")
                .in_("id", list(content_ids))
                .execute()
                .data
                or []
            )
        except Exception:
            content_rows = []
        for row in content_rows:
            try:
                item_lookup[int(row.get("id"))] = row
            except Exception:
                continue

    def _format_content_label(item: dict | None, content_id: int) -> str:
        if not isinstance(item, dict):
            return f"content_id {content_id}"
        media_type = (item.get("media_type") or "").strip()
        explicitness = (item.get("explicitness") or "").strip()
        desc = (item.get("desc_short") or "").strip()
        prefix_parts = [p for p in (media_type, explicitness) if p]
        prefix = " ".join(prefix_parts) if prefix_parts else "content"
        if desc:
            return f"{prefix}: {desc}"
        return f"{prefix}: content_id {content_id}"

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
        media_analysis = row.get("media_analysis_text")
        if media_analysis:
            cleaned_media = _clean_turn_text(media_analysis)
            text = f"{text}\n[MEDIA ANALYSIS]\n{cleaned_media}".strip()
        content_lines: list[str] = []
        if "[CONTENT_SENT]" not in text and "[CONTENT_OFFER" not in text:
            msg_id = row.get("id")
            if msg_id in deliveries_by_message:
                seen: set[int] = set()
                for content_id in deliveries_by_message.get(msg_id, []):
                    try:
                        cid = int(content_id)
                    except Exception:
                        continue
                    if cid in seen:
                        continue
                    seen.add(cid)
                    label = _format_content_label(item_lookup.get(cid), cid)
                    content_lines.append(f"[CONTENT_SENT] {label}")
            for offer in offers_by_message.get(msg_id, []):
                try:
                    cid = int(offer.get("content_id"))
                except Exception:
                    continue
                status = "bought" if offer.get("purchased") is True else "pending"
                price = offer.get("offered_price")
                price_label = f" ${price}" if price is not None else ""
                label = _format_content_label(item_lookup.get(cid), cid)
                content_lines.append(f"[CONTENT_OFFER {status}{price_label}] {label}")
        if content_lines:
            text = f"{text}\n" + "\n".join(content_lines)
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


def _summary_time_bounds(
    thread_id: int, start_turn: int | None, end_turn: int | None, *, client=None
) -> tuple[Any | None, Any | None]:
    """Return (earliest_created_at, latest_created_at) for a turn span."""

    sb = _resolve_client(client)

    def _query(desc: bool):
        q = (
            sb.table("messages")
            .select("created_at,turn_index")
            .eq("thread_id", thread_id)
        )
        if start_turn is not None:
            q = q.gte("turn_index", start_turn)
        if end_turn is not None:
            q = q.lte("turn_index", end_turn)
        q = q.order("turn_index", desc=desc).limit(1)
        rows = q.execute().data or []
        return rows[0].get("created_at") if rows else None

    earliest = _query(desc=False)
    latest = _query(desc=True)
    return earliest, latest


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
    # Prefer canonical plans stored on the thread; use the latest Napoleon snapshot
    # for tactical continuity and as a fallback for missing horizon plans.
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

    # Tactical plan should come from the latest successful Napoleon output so the
    # model can "continue where it left off" (TURN3 -> next TURN1). Using the
    # newest message_ai_details row is unreliable because Hermes seeds a placeholder
    # row for the current fan message before Napoleon runs.
    snapshot_row: dict = {}
    try:
        snapshot_rows = (
            sb.table("message_ai_details")
            .select(
                "message_id,extracted_at,tactical_plan_3turn,"
                "plan_episode,plan_chapter,plan_season,plan_year,plan_lifetime"
            )
            .eq("thread_id", thread_id)
            .eq("sender", "fan")
            .eq("napoleon_status", "ok")
            .order("extracted_at", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        if snapshot_rows and isinstance(snapshot_rows[0], dict):
            snapshot_row = snapshot_rows[0]
    except Exception:
        # Fail open: if the schema differs (older DB) or the query fails, try a more
        # forgiving fallback; otherwise keep the snapshot empty.
        try:
            snapshot_rows = (
                sb.table("message_ai_details")
                .select(
                    "message_id,tactical_plan_3turn,"
                    "plan_episode,plan_chapter,plan_season,plan_year,plan_lifetime"
                )
                .eq("thread_id", thread_id)
                .eq("sender", "fan")
                .eq("napoleon_status", "ok")
                .order("message_id", desc=True)
                .limit(1)
                .execute()
                .data
                or []
            )
            if snapshot_rows and isinstance(snapshot_rows[0], dict):
                snapshot_row = snapshot_rows[0]
        except Exception:
            # Last-chance fallback: do not rely on napoleon_status existing; scan a
            # small recent window for any non-empty tactical plan.
            try:
                snapshot_rows = (
                    sb.table("message_ai_details")
                    .select(
                        "message_id,tactical_plan_3turn,"
                        "plan_episode,plan_chapter,plan_season,plan_year,plan_lifetime"
                    )
                    .eq("thread_id", thread_id)
                    .eq("sender", "fan")
                    .order("message_id", desc=True)
                    .limit(25)
                    .execute()
                    .data
                    or []
                )
                for row in snapshot_rows:
                    if isinstance(row, dict) and row.get("tactical_plan_3turn"):
                        snapshot_row = row
                        break
            except Exception:
                snapshot_row = {}

    def _plan_value(thread_key: str, snapshot_key: str):
        thread_value = thread_row.get(thread_key)
        if thread_value not in (None, ""):
            return thread_value
        return snapshot_row.get(snapshot_key) or ""

    return {
        "CREATOR_TACTICAL_PLAN_3TURN": snapshot_row.get("tactical_plan_3turn") or "",
        "CREATOR_EPISODE_PLAN": _plan_value("episode_plan", "plan_episode"),
        "CREATOR_CHAPTER_PLAN": _plan_value("chapter_plan", "plan_chapter"),
        "CREATOR_SEASON_PLAN": _plan_value("season_plan", "plan_season"),
        "CREATOR_YEAR_PLAN": _plan_value("year_plan", "plan_year"),
        "CREATOR_LIFETIME_PLAN": _plan_value("lifetime_plan", "plan_lifetime"),
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
    boundary_turn: int | None = None,
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
        # For Episode summaries, mirror the 20–40 raw-turn rollover:
        # the Episode summary can exist early (at 20 turns) but should only be
        # shown once those 20 turns have been "consumed" (at 40 turns).
        episode_visible_end_turn = _turn_rollover_cutoff(boundary_turn)
        if episode_visible_end_turn:
            try:
                has_episode = (
                    sb.table("summaries")
                    .select("id")
                    .eq("thread_id", thread_id)
                    .eq("tier", "episode")
                    .eq("end_turn", episode_visible_end_turn)
                    .limit(1)
                    .execute()
                    .data
                )
            except Exception:
                has_episode = []
            if not has_episode:
                episode_visible_end_turn = 0

        for macro, (tier, limit) in SUMMARY_BLOCK_SPEC.items():
            if tier == "episode":
                # If cutoff is 0, this intentionally yields "[No Episode summaries yet]".
                context[macro] = make_block(
                    thread_id,
                    tier,
                    limit,
                    client=sb,
                    max_end_turn=episode_visible_end_turn or 0,
                )
            else:
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
    boundary_turn: int | None = None,
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
        boundary_turn=boundary_turn,
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
    boundary_turn: int | None = None,
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
        boundary_turn=boundary_turn,
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
