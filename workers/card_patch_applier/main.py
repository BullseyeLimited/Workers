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
    ensure_card_shape,
    make_entry,
    upgrade_confidence,
)
from workers.lib.simple_queue import ack, receive

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

HEADER_RE = re.compile(r"^(ADD|REINFORCE|REVISE)\s+SEGMENT_(\d+)\s*$", re.IGNORECASE)
CONF_RE = re.compile(r"^\s*CONFIDENCE:\s*(\w+)\s*$", re.IGNORECASE)
TEXT_PREFIX = "TEXT:"
REASON_PREFIX = "REASON:"


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
    lines = (raw_text or "").splitlines()
    idx = 0
    patches: List[dict] = []

    def _collect_until(stop_pred):
        nonlocal idx
        collected: List[str] = []
        while idx < len(lines) and not stop_pred(lines[idx]):
            collected.append(lines[idx])
            idx += 1
        return collected

    while idx < len(lines):
        line = lines[idx].strip()
        header = HEADER_RE.match(line)
        if not header:
            break  # Summary starts here

        action = header.group(1).lower()
        segment_id = header.group(2)
        idx += 1

        if idx >= len(lines) or not lines[idx].strip().lower().startswith(
            TEXT_PREFIX.lower()
        ):
            raise ValueError(f"missing TEXT for segment {segment_id}")

        text_lines = []
        # First TEXT line
        first_text = lines[idx].split(":", 1)[1] if ":" in lines[idx] else ""
        text_lines.append(first_text.strip())
        idx += 1
        # Additional TEXT continuation lines until CONFIDENCE
        text_lines.extend(
            [ln.strip() for ln in _collect_until(lambda l: CONF_RE.match(l))]
        )

        if idx >= len(lines):
            raise ValueError(f"missing CONFIDENCE for segment {segment_id}")

        conf_match = CONF_RE.match(lines[idx])
        if not conf_match:
            raise ValueError(f"malformed CONFIDENCE for segment {segment_id}")
        confidence = _parse_confidence(conf_match.group(1))
        idx += 1

        reason_parts: List[str] = []
        if idx < len(lines) and lines[idx].strip().lower().startswith(
            REASON_PREFIX.lower()
        ):
            first_reason = lines[idx].split(":", 1)[1] if ":" in lines[idx] else ""
            reason_parts.append(first_reason.strip())
            idx += 1
            reason_parts.extend(
                [
                    ln.strip()
                    for ln in _collect_until(
                        lambda l: HEADER_RE.match(l) or not l.strip()
                    )
                ]
            )

        text = " ".join([t for t in text_lines if t]).strip()
        reason = " ".join([r for r in reason_parts if r]).strip()
        text = _normalize_text(text, confidence)

        patches.append(
            {
                "action": action,
                "segment_id": segment_id,
                "text": text,
                "confidence": confidence,
                "reason": reason,
            }
        )

        # If the next non-empty line is not another header, treat remaining as summary.
        while idx < len(lines) and not lines[idx].strip():
            idx += 1
        if idx < len(lines) and not HEADER_RE.match(lines[idx].strip()):
            break

    summary = "\n".join(lines[idx:]).strip()
    return patches, summary


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
    segments = card.get("segments") or {}

    for patch in patches:
        seg_id = str(patch["segment_id"])
        if seg_id not in segments:
            # Unknown segment; skip quietly but keep history
            continue

        action = patch["action"]
        text = patch["text"]
        confidence = patch["confidence"]
        reason = patch["reason"]
        now_entry = make_entry(
            text=text,
            confidence=confidence,
            action=action,
            summary_id=summary_id,
            tier=tier,
            reason=reason,
        )

        bucket = segments[seg_id]
        if action == "add":
            bucket.append(now_entry)
        elif action == "reinforce":
            if bucket:
                target = bucket[-1]
                target_conf = target.get("confidence") or confidence
                target["confidence"] = upgrade_confidence(
                    target_conf, confidence
                )
                # Optionally refresh text if a new nuance was supplied
                target["text"] = text or target.get("text")
                evidence = target.setdefault("evidence", [])
                evidence.append(
                    {
                        "summary_id": summary_id,
                        "reason": reason,
                        "confidence": confidence,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
            else:
                bucket.append(now_entry)
        elif action == "revise":
            supersedes = bucket[-1]["id"] if bucket else None
            now_entry["supersedes"] = supersedes
            bucket.append(now_entry)
        else:
            # Unknown action; ignore
            continue

    card["segments"] = segments
    _save_card(thread_id, card)


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
