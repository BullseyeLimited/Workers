import difflib
import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from supabase import create_client, ClientOptions

from workers.lib.cards import (
    CONFIDENCE_ORDER,
    SEGMENT_NAMES,
    append_origin_tag,
    ensure_card_shape,
    make_entry,
)
from workers.lib.simple_queue import ack, receive, send
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
ABSTRACT_QUEUE_MAP = {
    "episode": "episode.abstract",
    "chapter": "chapter.abstract",
    "season": "season.abstract",
    "year": "year.abstract",
    "lifetime": "lifetime.abstract",
}

MAX_RETRIES = int(os.getenv("ABSTRACT_MAX_RETRIES", "5"))
TARGET_MATCH_MIN_RATIO = float(os.getenv("ABSTRACT_TARGET_MATCH_MIN", "0.6"))

HEADER_RE = re.compile(
    r"^(ADD|REINFORCE|REVISE)\s+SEGMENT[\s_\-:]*([A-Z0-9_\-\. ]+)\s*$",
    re.IGNORECASE,
)
TEXT_KEY_RE = re.compile(r"^\s*TEXT\b", re.IGNORECASE)
CONF_KEY_RE = re.compile(r"^\s*CONFIDENCE\b", re.IGNORECASE)
REASON_KEY_RE = re.compile(r"^\s*REASON\b", re.IGNORECASE)
TARGET_ID_KEY_RE = re.compile(r"^\s*TARGET_ID\b", re.IGNORECASE)
TARGET_KEY_RE = re.compile(r"^\s*TARGET\b", re.IGNORECASE)
CONF_BRACKET_RE = re.compile(
    r"\[(tentative|possible|likely|confident|canonical)\]\s*$",
    re.IGNORECASE,
)

SEGMENT_LABEL_MAP = {}
for seg_id, label in SEGMENT_NAMES.items():
    normalized = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    SEGMENT_LABEL_MAP[normalized] = seg_id
    stripped = re.sub(r"^\d+\.?", "", label).strip()
    normalized_stripped = re.sub(r"[^a-z0-9]+", "_", stripped.lower()).strip("_")
    if normalized_stripped:
        SEGMENT_LABEL_MAP[normalized_stripped] = seg_id


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


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")


def _parse_header(line: str):
    match = HEADER_RE.match(line.strip())
    if not match:
        return None
    action = match.group(1).lower()
    tail = (match.group(2) or "").strip()
    if not tail:
        return None

    seg_id = None
    label = tail
    id_match = re.search(r"\d+", tail)
    if id_match:
        seg_id = str(int(id_match.group(0)))
        label = tail[id_match.end() :].strip(" _.-")
    normalized_label = _normalize_label(label)
    if not seg_id and normalized_label in SEGMENT_LABEL_MAP:
        seg_id = SEGMENT_LABEL_MAP[normalized_label]

    if not seg_id:
        return None

    segment_label = label or SEGMENT_NAMES.get(seg_id) or ""
    return action, seg_id, segment_label, tail


def _split_value(line: str) -> str:
    if ":" in line:
        return line.split(":", 1)[1].strip()
    if "-" in line:
        return line.split("-", 1)[1].strip()
    return ""


def _parse_block(lines: List[str]) -> Tuple[str | None, str, str, str, str]:
    text = None
    confidence = None
    reason = ""
    target_id = ""
    target_text = ""

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if TARGET_ID_KEY_RE.match(line):
            value = _split_value(line)
            target_id = value.strip() or target_id
            continue

        if TARGET_KEY_RE.match(line):
            value = _split_value(line)
            target_text = value.strip() or target_text
            continue

        if TEXT_KEY_RE.match(line):
            value = _split_value(line)
            if not value:
                value = line.split(None, 1)[1] if len(line.split(None, 1)) > 1 else ""
            match = CONF_BRACKET_RE.search(value)
            if match:
                confidence = match.group(1).lower()
                value = CONF_BRACKET_RE.sub("", value).strip()
            text = value.strip() or text
            continue

        if CONF_KEY_RE.match(line):
            value = _split_value(line).lower()
            if value in CONFIDENCE_ORDER:
                confidence = value
            continue

        if REASON_KEY_RE.match(line):
            value = _split_value(line)
            reason = value.strip() or reason
            continue

    if text is None:
        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            if (
                TEXT_KEY_RE.match(line)
                or CONF_KEY_RE.match(line)
                or REASON_KEY_RE.match(line)
                or TARGET_ID_KEY_RE.match(line)
                or TARGET_KEY_RE.match(line)
            ):
                continue
            match = CONF_BRACKET_RE.search(line)
            if match:
                confidence = match.group(1).lower()
                line = CONF_BRACKET_RE.sub("", line).strip()
            text = line.strip()
            break

    if not confidence or confidence not in CONFIDENCE_ORDER:
        confidence = "possible"

    return text, confidence, reason, target_id, target_text


def parse_patch_output(raw_text: str) -> Tuple[List[dict], str]:
    """
    Parse the Episode/Chapter abstract output into patch blocks + summary.
    Returns (patches, summary_text).
    """
    lines = (raw_text or "").replace("\r", "").splitlines()
    idx = 0
    patches: List[dict] = []
    last_block_end = 0

    while idx < len(lines):
        header = _parse_header(lines[idx])
        if not header:
            idx += 1
            continue
        action, segment_id, segment_label, segment_full = header
        idx += 1

        block_lines: List[str] = []
        while idx < len(lines) and not _parse_header(lines[idx]):
            block_lines.append(lines[idx])
            idx += 1

        last_block_end = idx
        text_body, confidence, reason, target_id, target_text = _parse_block(block_lines)
        if not text_body:
            continue

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
                "target_id": target_id or None,
                "target_text": target_text or None,
            }
        )

    summary = "\n".join(lines[last_block_end:]).strip()
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


def _strip_confidence(text: str) -> str:
    return re.sub(r"\[[a-z]+\]\s*$", "", (text or "").strip(), flags=re.IGNORECASE).strip()


def _find_best_match(entries: List[dict], text: str, *, min_ratio: float = 0.72) -> dict | None:
    if not entries:
        return None
    needle = _strip_confidence(text).lower()
    if not needle:
        return None
    best = None
    best_score = 0.0
    for entry in entries:
        cand = _strip_confidence(entry.get("text") or "").lower()
        if not cand:
            continue
        score = difflib.SequenceMatcher(None, needle, cand).ratio()
        if score > best_score:
            best_score = score
            best = entry
    if best_score >= min_ratio:
        return best
    return None


def _find_entry_by_id(entries: List[dict], target_id: str | None) -> dict | None:
    if not target_id:
        return None
    for entry in entries:
        if str(entry.get("id")) == str(target_id):
            return entry
    return None


def _match_ratio(left: str, right: str) -> float:
    left_clean = _strip_confidence(left).lower()
    right_clean = _strip_confidence(right).lower()
    if not left_clean or not right_clean:
        return 0.0
    return difflib.SequenceMatcher(None, left_clean, right_clean).ratio()


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
        patch_conf = _parse_confidence(patch["confidence"])
        patch_text = _normalize_text(patch["text"], patch_conf)
        reason = patch.get("reason") or ""
        target_id = patch.get("target_id")
        target_text = patch.get("target_text") or ""
        target_entry = None
        if target_id:
            target_entry = _find_entry_by_id(bucket, target_id)
            if target_entry and target_text:
                if _match_ratio(target_text, target_entry.get("text") or "") < TARGET_MATCH_MIN_RATIO:
                    target_entry = None
        if not target_entry and target_text:
            target_entry = _find_best_match(bucket, target_text, min_ratio=TARGET_MATCH_MIN_RATIO)

        if patch["action"] == "add":
            match = _find_best_match(bucket, patch_text)
            if match:
                # Treat ADD as reinforce if it matches existing text.
                origin_name = _entry_origin(match, worker_tier)
                current_conf = match.get("confidence") or patch_conf
                try:
                    bumped_conf = _one_step_confidence(current_conf, patch_conf)
                except Exception:
                    bumped_conf = patch_conf
                match["confidence"] = bumped_conf
                match["text"] = append_origin_tag(
                    _normalize_text(patch_text, bumped_conf),
                    parse_tier(origin_name),
                )
                match["reason"] = reason or match.get("reason")
                evidence = match.setdefault("evidence", [])
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
            target = target_entry or _find_best_match(bucket, patch_text)
            if not target:
                # No existing entry to reinforce; fall back to ADD behavior.
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

            origin_name = _entry_origin(target, worker_tier)
            current_conf = target.get("confidence") or patch_conf
            try:
                bumped_conf = _one_step_confidence(current_conf, patch_conf)
            except Exception:
                bumped_conf = patch_conf

            target["confidence"] = bumped_conf
            target["text"] = append_origin_tag(
                _normalize_text(patch_text, bumped_conf),
                parse_tier(origin_name),
            )
            target["reason"] = reason or target.get("reason")
            evidence = target.setdefault("evidence", [])
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
            target = target_entry or _find_best_match(bucket, patch_text)
            if not target:
                # No existing entry to revise; fall back to ADD behavior.
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

            origin_name = _entry_origin(target, worker_tier)
            target["confidence"] = patch_conf
            target["text"] = append_origin_tag(
                _normalize_text(patch_text, patch_conf),
                parse_tier(origin_name),
            )
            target["reason"] = reason or target.get("reason")
            evidence = target.setdefault("evidence", [])
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
    # Try update first (if row exists), then insert if missing.
    res = (
        SB.table("summaries")
        .update(
            {
                "abstract_summary": abstract_summary,
                "fan_psychic_card_action": patches,
                "raw_writer_json": {"raw_text": raw_text},
                "raw_hash": raw_hash,
                "extract_status": extract_status,
            }
        )
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .eq("start_turn", start_turn)
        .eq("end_turn", end_turn)
        .execute()
        .data
    )
    if not res:
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
        .eq("extract_status", "ok")
        .order("tier_index")
        .execute()
        .data
        or []
    )


def _pending_job(
    queue_name: str,
    thread_id: int,
    start_turn: int | None,
    end_turn: int | None,
    tier_index: int | None,
) -> bool:
    query = (
        SB.table("job_queue")
        .select("id")
        .eq("queue", queue_name)
        .filter("payload->>thread_id", "eq", str(thread_id))
    )
    if start_turn is not None:
        query = query.filter("payload->>start_turn", "eq", str(start_turn))
    if end_turn is not None:
        query = query.filter("payload->>end_turn", "eq", str(end_turn))
    if tier_index is not None:
        query = query.filter("payload->>tier_index", "eq", str(tier_index))
    rows = query.limit(1).execute().data
    return bool(rows)


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

    chunk = lower_rows[consumed : consumed + chunk_size]
    if len(chunk) < chunk_size:
        return

    start_turn = min(row.get("start_turn") or 0 for row in chunk) or None
    end_turn = max(row.get("end_turn") or 0 for row in chunk) or None
    next_tier_index = upper_count + 1

    lines = []
    for row in chunk:
        idx = row.get("tier_index")
        abstract = row.get("abstract_summary") or ""
        lines.append(f"{lower_tier.title()} {idx}: {abstract}")
    raw_block = "\n\n".join(lines)

    if backlog < trigger:
        return

    if _pending_job(queue_name, thread_id, start_turn, end_turn, next_tier_index):
        return

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
    attempt = int(payload.get("attempt") or 1)

    try:
        patches, summary = parse_patch_output(raw_text)
        extract_status = "ok"
    except Exception as exc:  # noqa: BLE001
        patches, summary = [], ""
        extract_status = "failed"
        print(f"[card_patch_applier] parse failed: {exc}")

    if extract_status != "ok":
        if attempt < MAX_RETRIES:
            queue = ABSTRACT_QUEUE_MAP.get(tier)
            if queue:
                retry_payload = {
                    "thread_id": thread_id,
                    "start_turn": start_turn,
                    "end_turn": end_turn,
                    "tier_index": tier_index,
                    "attempt": attempt + 1,
                }
                raw_block = payload.get("raw_block")
                if raw_block:
                    retry_payload["raw_block"] = raw_block
                send(queue, retry_payload)
            return True

        _insert_summary_row(
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
        return True

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

    if patches:
        _apply_patches(thread_id, summary_id, tier, patches)

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
