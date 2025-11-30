import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from supabase import create_client, ClientOptions

from workers.lib.cards import CONFIDENCE_ORDER, append_origin_tag, ensure_card_shape, make_entry
from workers.lib.simple_queue import ack, receive
from workers.lib.time_tier import parse_tier

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

QUEUE = "card.patch"
TIER_PIPELINE = {
    "episode": {
        "next": "chapter",
        "lower": "episode",
        "chunk_size": 3,
        "trigger_backlog": 6,
        "queue": "chapter.abstract",
    },
    "chapter": {
        "next": "season",
        "lower": "chapter",
        "chunk_size": 3,
        "trigger_backlog": 6,
        "queue": "season.abstract",
    },
    "season": {
        "next": "year",
        "lower": "season",
        "chunk_size": 3,
        "trigger_backlog": 6,
        "queue": "year.abstract",
    },
    "year": {
        "next": "lifetime",
        "lower": "year",
        "chunk_size": 3,
        "trigger_backlog": 6,
        "queue": "lifetime.abstract",
    },
}

HEADER_RE = re.compile(
    r"^(ADD|REINFORCE|REVISE)\s+SEGMENT_(\d+_[A-Z0-9_]+)\s*$",
    re.IGNORECASE,
)
TEXT_RE = re.compile(r"^TEXT:\s*(.+)\[([a-z]+)]\s*$", re.IGNORECASE)
CONF_RE = re.compile(
    r"^\s*CONFIDENCE:\s*(tentative|possible|likely|confident|canonical)\s*$",
    re.IGNORECASE,
)
REASON_RE = re.compile(r"^\s*REASON:\s*(.+)\s*$", re.IGNORECASE)


def _parse_confidence(value: str) -> str:
    value = (value or "").strip().lower()
    if value not in CONFIDENCE_ORDER:
        raise ValueError(f"invalid confidence '{value}'")
    return value


def _normalize_text(text: str, confidence: str) -> str:
    """Ensure the text ends with the bracketed confidence."""
    text = (text or "").strip()
    suffix = f"[{confidence}]"
    if not text.lower().endswith(f"[{confidence}]"):
        if text.endswith("]"):
            # Strip trailing bracket chunk and append normalized suffix
            left = text[: text.rfind("[")].rstrip()
            return f"{left} {suffix}".strip()
        return f"{text} {suffix}".strip()
    return text


def parse_patch_output(raw_text: str) -> Tuple[List[dict], str]:
    """
    Parse the Episode/Chapter abstract output into patch blocks + summary.
    Returns (patches, summary_text).
    """
    lines = (raw_text or "").replace("\r", "").splitlines()
    idx = 0
    patches: List[dict] = []

    while idx < len(lines):
        # Skip any blank separators
        while idx < len(lines) and not lines[idx].strip():
            idx += 1

        if idx >= len(lines):
            break

        line = lines[idx].strip()
        header = HEADER_RE.match(line)
        if not header:
            break  # Summary starts here

        action = header.group(1).lower()
        segment_full = header.group(2)
        if "_" in segment_full:
            segment_id, segment_label = segment_full.split("_", 1)
        else:
            segment_id, segment_label = segment_full, ""
        idx += 1

        if idx >= len(lines):
            raise ValueError(f"missing TEXT for segment {segment_full}")

        text_line = lines[idx].strip()
        text_match = TEXT_RE.match(text_line)
        if not text_match:
            raise ValueError(f"malformed TEXT for segment {segment_full}")
        text_body = text_match.group(1).strip()
        text_conf = _parse_confidence(text_match.group(2))
        idx += 1

        if idx >= len(lines):
            raise ValueError(f"missing CONFIDENCE for segment {segment_full}")

        conf_line = lines[idx].strip()
        conf_match = CONF_RE.match(conf_line)
        if not conf_match:
            raise ValueError(f"malformed CONFIDENCE for segment {segment_full}")
        confidence = _parse_confidence(conf_match.group(1))
        if confidence != text_conf:
            raise ValueError(
                f"confidence mismatch for segment {segment_full} "
                f"(text={text_conf}, confidence_line={confidence})"
            )
        idx += 1

        if idx >= len(lines):
            raise ValueError(f"missing REASON for segment {segment_full}")

        reason_line = lines[idx].strip()
        reason_match = REASON_RE.match(reason_line)
        if not reason_match:
            raise ValueError(f"missing REASON for segment {segment_full}")
        reason = reason_match.group(1).strip()
        idx += 1

        while idx < len(lines) and not lines[idx].strip():
            idx += 1

        text = _normalize_text(text_body, confidence)

        patches.append(
            {
                "action": action,
                "segment_id": segment_id,
                "segment_label": segment_label,
                "segment_full_label": segment_full,
                "text": text,
                "confidence": confidence,
                "reason": reason,
            }
        )

        if idx < len(lines) and not HEADER_RE.match(lines[idx].strip()):
            break

    summary = "\n".join(lines[idx:]).strip()
    if not summary:
        raise ValueError("summary section missing")
    return patches, summary


def _one_step_confidence(current: str, incoming: str) -> str:
    """Raise confidence by at most one step, honoring the declared incoming level."""

    cur = _parse_confidence(current)
    inc = _parse_confidence(incoming)
    cur_idx = CONFIDENCE_ORDER.index(cur)
    inc_idx = CONFIDENCE_ORDER.index(inc)
    if inc_idx <= cur_idx:
        return cur
    next_idx = min(cur_idx + 1, inc_idx, len(CONFIDENCE_ORDER) - 1)
    return CONFIDENCE_ORDER[next_idx]


def _active_entry(entries: List[dict]) -> dict | None:
    """Return the most recent non-superseded entry in a segment bucket."""

    if not entries:
        return None

    superseded_ids = {
        entry.get("supersedes")
        for entry in entries
        if entry.get("supersedes")
    }
    live_entries = [
        entry
        for entry in entries
        if not entry.get("superseded_by") and entry.get("id") not in superseded_ids
    ]
    if live_entries:
        return live_entries[-1]
    return entries[-1]


def _entry_origin(entry: dict, fallback) -> str:
    """Determine the origin tier string for an entry, defaulting safely."""

    try:
        return parse_tier(entry.get("origin_tier") or entry.get("tier") or fallback).name
    except Exception:  # noqa: BLE001
        return parse_tier(fallback).name


def apply_patches_to_card(
    card: dict, summary_id: int, tier: str, patches: List[dict]
) -> dict:
    """Pure helper to apply patches into an in-memory card representation."""

    card = ensure_card_shape(card)
    segments = card.get("segments") or {}
    try:
        worker_tier = parse_tier(tier)
    except Exception:  # noqa: BLE001
        worker_tier = parse_tier("episode")

    for patch in patches:
        seg_id = str(patch["segment_id"])
        if seg_id not in segments:
            # Unknown segment; skip quietly but keep history
            continue

        bucket = segments.get(seg_id) or []
        segments[seg_id] = bucket
        current = _active_entry(bucket)
        patch_conf = _parse_confidence(patch["confidence"])
        patch_text = _normalize_text(patch["text"], patch_conf)
        reason = patch.get("reason") or ""

        if patch["action"] == "add":
            if current:
                print(
                    f"[card_patch_applier] duplicate ADD for segment {seg_id}; skipping"
                )
                continue

            entry_text = append_origin_tag(patch_text, worker_tier)
            now_entry = make_entry(
                text=entry_text,
                confidence=patch_conf,
                action="add",
                summary_id=summary_id,
                tier=tier,
                reason=reason,
                origin_tier=worker_tier.name,
            )
            bucket.append(now_entry)
            continue

        if patch["action"] == "reinforce":
            if not current:
                print(
                    f"[card_patch_applier] REINFORCE but no row for segment {seg_id}"
                )
                continue

            origin_name = _entry_origin(current, worker_tier)
            current_conf = current.get("confidence") or patch_conf
            try:
                bumped_conf = _one_step_confidence(current_conf, patch_conf)
            except Exception:
                bumped_conf = patch_conf

            current["confidence"] = bumped_conf
            current["text"] = append_origin_tag(
                _normalize_text(patch_text, bumped_conf),
                parse_tier(origin_name),
            )
            current["reason"] = reason or current.get("reason")
            evidence = current.setdefault("evidence", [])
            evidence.append(
                {
                    "summary_id": summary_id,
                    "reason": reason,
                    "confidence": patch_conf,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "action": "reinforce",
                }
            )
            continue

        if patch["action"] == "revise":
            if not current:
                print(f"[card_patch_applier] REVISE but no row for segment {seg_id}")
                continue

            origin_name = _entry_origin(current, worker_tier)
            current["confidence"] = patch_conf
            current["text"] = append_origin_tag(
                _normalize_text(patch_text, patch_conf),
                parse_tier(origin_name),
            )
            current["reason"] = reason or current.get("reason")
            evidence = current.setdefault("evidence", [])
            evidence.append(
                {
                    "summary_id": summary_id,
                    "reason": reason,
                    "confidence": patch_conf,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "action": "revise",
                }
            )

    card["segments"] = segments
    card["cards_updated_at"] = datetime.now(timezone.utc).isoformat()
    return card


def _next_tier_index(thread_id: int, tier: str) -> int:
    rows = (
        SB.table("summaries")
        .select("tier_index")
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return 1
    return (rows[0].get("tier_index") or 0) + 1


def _load_card(thread_id: int) -> dict:
    row = (
        SB.table("threads")
        .select("fan_psychic_card")
        .eq("id", thread_id)
        .single()
        .execute()
        .data
        or {}
    )
    return ensure_card_shape(row.get("fan_psychic_card"))


def _save_card(thread_id: int, card: dict):
    SB.table("threads").update(
        {
            "fan_psychic_card": card,
            "cards_updated_at": datetime.now(timezone.utc).isoformat(),
        }
    ).eq("id", thread_id).execute()


def _apply_patches(thread_id: int, summary_id: int, tier: str, patches: List[dict]):
    card = _load_card(thread_id)
    updated = apply_patches_to_card(card, summary_id, tier, patches)
    _save_card(thread_id, updated)


def _insert_summary_row(
    *,
    thread_id: int,
    tier: str,
    start_turn: int | None,
    end_turn: int | None,
    tier_index: int | None,
    abstract_summary: str,
    patches: List[dict],
    raw_text: str,
    raw_hash: str,
    extract_status: str,
) -> int:
    summary_idx = tier_index or _next_tier_index(thread_id, tier)
    payload = {
        "thread_id": thread_id,
        "tier": tier,
        "tier_index": summary_idx,
        "start_turn": start_turn,
        "end_turn": end_turn,
        "abstract_summary": abstract_summary,
        "fan_psychic_card_action": patches,
        "raw_writer_json": {"raw_text": raw_text},
        "raw_hash": raw_hash,
        "extract_status": extract_status,
    }
    res = SB.table("summaries").insert(payload).execute().data
    if not res:
        raise RuntimeError("failed to insert summary row")
    return res[0]["id"]


def _fetch_summaries(thread_id: int, tier: str) -> List[dict]:
    return (
        SB.table("summaries")
        .select("id,tier_index,start_turn,end_turn,abstract_summary")
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index")
        .execute()
        .data
        or []
    )


def _maybe_enqueue_next(thread_id: int, tier: str):
    cfg = TIER_PIPELINE.get(tier)
    if not cfg:
        return

    lower_tier = cfg["lower"]
    upper_tier = cfg["next"]
    chunk_size = cfg["chunk_size"]
    trigger = cfg["trigger_backlog"]
    queue_name = cfg["queue"]

    lower_rows = _fetch_summaries(thread_id, lower_tier)
    upper_rows = _fetch_summaries(thread_id, upper_tier)

    lower_count = len(lower_rows)
    upper_count = len(upper_rows)
    consumed = upper_count * chunk_size
    backlog = lower_count - consumed

    if backlog < trigger:
        return

    chunk = lower_rows[consumed : consumed + chunk_size]
    if len(chunk) < chunk_size:
        return

    lines = []
    for row in chunk:
        idx = row.get("tier_index")
        abstract = row.get("abstract_summary") or ""
        lines.append(f"{lower_tier.title()} {idx}: {abstract}")
    raw_block = "\n\n".join(lines)

    start_turn = min(row.get("start_turn") or 0 for row in chunk) or None
    end_turn = max(row.get("end_turn") or 0 for row in chunk) or None
    next_tier_index = upper_count + 1

    payload = {
        "thread_id": thread_id,
        "tier": upper_tier,
        "start_turn": start_turn,
        "end_turn": end_turn,
        "tier_index": next_tier_index,
        "raw_block": raw_block,
    }

    SB.table("job_queue").insert(
        {"queue": queue_name, "payload": json.dumps(payload, ensure_ascii=False)}
    ).execute()


def process_job(payload: Dict) -> bool:
    thread_id = payload["thread_id"]
    tier = payload.get("tier") or "episode"
    start_turn = payload.get("start_turn")
    end_turn = payload.get("end_turn")
    tier_index = payload.get("tier_index")
    raw_text = payload.get("raw_text") or ""
    raw_hash = payload.get("raw_hash") or hashlib.sha256(
        raw_text.encode("utf-8")
    ).hexdigest()

    try:
        patches, summary = parse_patch_output(raw_text)
        extract_status = "ok"
    except Exception as exc:  # noqa: BLE001
        patches, summary = [], ""
        extract_status = "failed"
        print(f"[card_patch_applier] parse failed: {exc}")

    summary_id = _insert_summary_row(
        thread_id=thread_id,
        tier=tier,
        start_turn=start_turn,
        end_turn=end_turn,
        tier_index=tier_index,
        abstract_summary=summary,
        patches=patches,
        raw_text=raw_text,
        raw_hash=raw_hash,
        extract_status=extract_status,
    )

    if extract_status == "ok" and patches:
        _apply_patches(thread_id, summary_id, tier, patches)

    if extract_status == "ok":
        _maybe_enqueue_next(thread_id, tier)

    return True


if __name__ == "__main__":
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1)
            continue
        try:
            payload = job["payload"]
            if process_job(payload):
                ack(job["row_id"])
        except Exception as exc:  # noqa: BLE001
            print("[card_patch_applier] error:", exc)
            traceback.print_exc()
            # Let the job retry
            time.sleep(2)
