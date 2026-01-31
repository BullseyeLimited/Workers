import difflib
import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests
from postgrest.exceptions import APIError
from supabase import create_client, ClientOptions

from workers.lib.cards import (
    CONFIDENCE_ORDER,
    SEGMENT_NAMES,
    append_origin_tag,
    ensure_card_shape,
    make_entry,
    prune_fan_psychic_card,
)
from workers.lib.simple_queue import ack, receive, send
from workers.lib.time_tier import TimeTier, parse_tier

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
RUNPOD_URL = (os.getenv("RUNPOD_URL") or "").rstrip("/")
RUNPOD_MODEL = os.getenv("RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored")
RUNPOD_REWRITE_MODEL = os.getenv("RUNPOD_REWRITE_MODEL_NAME") or RUNPOD_MODEL

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
REWRITE_ENABLED = (os.getenv("ABSTRACT_OUTPUT_REWRITE") or "1").strip().lower() not in {
    "0",
    "false",
    "no",
}
REWRITE_MAX_TOKENS = int(os.getenv("ABSTRACT_OUTPUT_REWRITE_MAX_TOKENS", "20000"))
REWRITE_TIMEOUT_SECONDS = int(os.getenv("ABSTRACT_OUTPUT_REWRITE_TIMEOUT", "120"))

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

REWRITE_HINT_RE = re.compile(
    r"(?im)^\s*(add|revise|reinforce)\b.*\bsegment\b"
    r"|^\s*segment[\s_\-:]*\d+"
    r"|^\s*text\b"
    r"|^\s*confidence\b"
    r"|^\s*reason\b"
    r"|^\s*target_id\b"
    r"|^\s*target\b"
)
_WS_RE = re.compile(r"\s+")

_META_LEAK_RE = re.compile(
    r"(?is)"
    r"\bthe user wants me to act\b"
    r"|ready to engage with high-level reasoning"
    r"|<\s*rolling_history\s*>"
    r"|<\s*reference_profiles\s*>"
    r"|<\s*raw_turns\s*>"
    r"|begin episode_input"
    r"|important boundary"
    r"|role\s*&\s*context"
    r"|constraint checklist"
    r"|mental sandbox"
    r"|output contract"
)

SEGMENT_LABEL_MAP = {}
for seg_id, label in SEGMENT_NAMES.items():
    normalized = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    SEGMENT_LABEL_MAP[normalized] = seg_id
    stripped = re.sub(r"^\d+\.?", "", label).strip()
    normalized_stripped = re.sub(r"[^a-z0-9]+", "_", stripped.lower()).strip("_")
    if normalized_stripped:
        SEGMENT_LABEL_MAP[normalized_stripped] = seg_id


def _api_error_code(exc: Exception) -> str | None:
    if not isinstance(exc, APIError):
        return None
    code = getattr(exc, "code", None)
    if code is not None:
        return str(code)
    try:
        raw = exc.json()
        if isinstance(raw, dict) and raw.get("code") is not None:
            return str(raw.get("code"))
    except Exception:
        pass
    return None


def _sanitize_patches(patches: List[dict]) -> List[dict]:
    """
    Keep only the fields we need for applying patches and persisting to DB.

    This avoids schema/constraint issues if the DB expects a specific JSON shape.
    """
    cleaned: List[dict] = []
    for patch in patches or []:
        if not isinstance(patch, dict):
            continue

        action = (patch.get("action") or "").strip().lower()
        if action not in {"add", "reinforce", "revise"}:
            continue

        seg_raw = patch.get("segment_id")
        try:
            seg_id = int(seg_raw) if seg_raw is not None else None
        except (TypeError, ValueError):
            seg_id = None
        if seg_id is None:
            continue

        confidence = (patch.get("confidence") or "possible").strip().lower()
        if confidence not in CONFIDENCE_ORDER:
            confidence = "possible"

        text = (patch.get("text") or "").strip()
        if not text:
            continue
        text = _normalize_text(text, confidence)

        row = {
            "action": action,
            "segment_id": seg_id,
            "text": text,
            "confidence": confidence,
            "reason": (patch.get("reason") or "").strip(),
        }

        raw_target_id = patch.get("target_id")
        target_id = str(raw_target_id).strip() if raw_target_id is not None else ""
        if target_id:
            row["target_id"] = target_id

        raw_target_text = patch.get("target_text")
        target_text = str(raw_target_text).strip() if raw_target_text is not None else ""
        if target_text:
            row["target_text"] = target_text

        cleaned.append(row)
    return cleaned


def _looks_like_prompt_leak(text: str) -> bool:
    """
    Heuristic: detect when the model echoed system/prompt scaffolding instead of outputting a real abstract.

    This is intentionally conservative: if we detect a leak, we retry the abstract writer rather than
    persisting junk and cascading rollups from empty placeholders.
    """
    if not (text or "").strip():
        return False
    return bool(_META_LEAK_RE.search(text))


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
    Parse an abstract-writer output into patch blocks.

    Output contract:
    - Zero or more patch blocks.
    - If there are no patch blocks, the output must be either empty/whitespace
      or exactly "NO_UPDATES".
    - No trailing prose is allowed after patch blocks.

    Returns (patches, trailing_text). trailing_text is always "" for valid outputs.
    """
    lines = (raw_text or "").replace("\r", "").splitlines()
    idx = 0
    patches: List[dict] = []
    last_consumed = 0

    while idx < len(lines):
        header = _parse_header(lines[idx])
        if not header:
            idx += 1
            continue

        action, segment_id, segment_label, segment_full = header
        idx += 1

        block_lines: List[str] = []
        while idx < len(lines):
            if _parse_header(lines[idx]):
                break

            block_lines.append(lines[idx])
            idx += 1

            # Patch blocks are expected to end at the REASON line; anything after is summary.
            if REASON_KEY_RE.match(block_lines[-1]):
                break

        last_consumed = idx
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

    if patches:
        trailing = "\n".join(lines[last_consumed:]).strip()
        if trailing:
            raise ValueError("unexpected trailing text after patch blocks")
        return patches, ""

    stripped = "\n".join(lines).strip()
    if not stripped:
        return [], ""
    if stripped.strip().upper() == "NO_UPDATES":
        return [], ""
    raise ValueError("no patch blocks found")


def _should_attempt_rewrite(
    raw_text: str, *, patches: List[dict], extract_status: str
) -> bool:
    if not REWRITE_ENABLED:
        return False
    if not RUNPOD_URL:
        return False
    if not (raw_text or "").strip():
        return False
    if extract_status != "ok":
        return True
    if patches:
        return False
    # Only try rewrite when the output *looks* like it contains card-update
    # instructions that we failed to parse into patch blocks.
    return bool(REWRITE_HINT_RE.search(raw_text or ""))


def _rewrite_to_patch_contract(raw_text: str, tier: str) -> str | None:
    """
    Ask a second model pass to reformat an abstract writer output into strict patch blocks.

    Critical safety rule: the rewriter must not add/omit facts; it should only reformat
    whatever is already explicitly present in the original output.
    """
    if not RUNPOD_URL:
        return None

    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
        "Content-Type": "application/json",
    }

    user_prompt = f"""You are a STRICT OUTPUT FORMATTER.

Your job: take the ORIGINAL_OUTPUT (from a different model) and rewrite it into the exact patch format expected by a regex parser.

CRITICAL RULES (DO NOT BREAK THESE):
- Do NOT add new facts, new updates, or new interpretations.
- Do NOT paraphrase. Copy the original sentences/phrases as-is whenever possible.
- If the ORIGINAL_OUTPUT does not explicitly contain a card update instruction, output NO patch block for it.
- If you cannot confidently reformat without changing meaning, output the ORIGINAL_OUTPUT unchanged.

    OUTPUT CONTRACT (MUST MATCH EXACTLY):
    - Output ONLY patch blocks (0+), OR output exactly NO_UPDATES.
    - Do NOT output any summary/prose after patch blocks.

PATCH BLOCK FORMAT:
ADD SEGMENT_22
TEXT: <copy-pasted text> [possible]
CONFIDENCE: possible
REASON: <copy-pasted evidence line(s)>

REINFORCE SEGMENT_22
TARGET_ID: <copy-pasted id if present>
TARGET: <copy-pasted old text if present>
TEXT: <copy-pasted text> [likely]
CONFIDENCE: likely
REASON: <copy-pasted evidence line(s)>

REVISE SEGMENT_22
TARGET_ID: <copy-pasted id if present>
TARGET: <copy-pasted old text if present>
TEXT: <copy-pasted text> [likely]
CONFIDENCE: likely
REASON: <copy-pasted evidence line(s)>

Allowed confidence values (must match): tentative / possible / likely / confident / canonical
TEXT must end with a bracket tag matching CONFIDENCE (e.g. [likely]).

    If the ORIGINAL_OUTPUT contains no explicit card updates, output exactly:
    NO_UPDATES

    Tier for context: {tier}

ORIGINAL_OUTPUT:
<ORIGINAL_OUTPUT>
{raw_text}
</ORIGINAL_OUTPUT>
"""

    payload = {
        "model": RUNPOD_REWRITE_MODEL,
        "messages": [
            {"role": "system", "content": "You are a careful formatter. No creativity."},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": REWRITE_MAX_TOKENS,
        "temperature": 0,
    }

    resp = requests.post(
        url, headers=headers, json=payload, timeout=REWRITE_TIMEOUT_SECONDS
    )
    resp.raise_for_status()
    data = resp.json()
    try:
        if data.get("choices"):
            choice = data["choices"][0]
            message = choice.get("message") or {}
            raw = message.get("content") or message.get("reasoning") or ""
            if not raw:
                raw = choice.get("text") or ""
            raw = (raw or "").strip()
            return raw or None
    except Exception:
        return None
    return None


def _normalize_ws(value: str) -> str:
    return _WS_RE.sub(" ", (value or "").strip())


def _contains_verbatim(haystack: str, needle: str) -> bool:
    """
    Case-insensitive containment check after whitespace normalization.

    This is used as a safety gate so the rewrite pass can't introduce new facts:
    every emitted patch text/reason/summary must already exist in the original output.
    """
    needle_norm = _normalize_ws(needle)
    if not needle_norm:
        return True
    return needle_norm.lower() in _normalize_ws(haystack).lower()


def _rewrite_is_verbatim(original: str, *, patches: List[dict], summary: str) -> bool:
    if not _contains_verbatim(original, summary):
        return False
    for patch in patches or []:
        text_body = _strip_confidence(patch.get("text") or "")
        if not _contains_verbatim(original, text_body):
            return False
        reason = patch.get("reason") or ""
        if reason and not _contains_verbatim(original, reason):
            return False
        target_text = patch.get("target_text") or ""
        if target_text and not _contains_verbatim(original, target_text):
            return False
        target_id = str(patch.get("target_id") or "").strip()
        if target_id and target_id not in (original or ""):
            return False
    return True


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
    # Stored card entries end with multiple bracket tags, e.g. "[tentative] [EPISODE]".
    # Strip all trailing bracket tags so fuzzy matching compares the underlying text.
    clean = (text or "").strip()
    clean = re.sub(r"(?:\[[^\]]+\]\s*)+$", "", clean).strip()
    return clean


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


def _entry_origin_tier(entry: dict, fallback: TimeTier = TimeTier.TURN) -> TimeTier:
    """Determine the origin tier enum for an entry, defaulting safely."""

    try:
        return parse_tier(entry.get("origin_tier") or entry.get("tier") or fallback)
    except Exception:  # noqa: BLE001
        return fallback


def _live_entries(entries: List[dict]) -> List[dict]:
    """Return entries excluding superseded ones (best-effort)."""

    if not entries:
        return []
    superseded_ids = {
        entry.get("supersedes")
        for entry in entries
        if entry.get("supersedes")
    }
    live = [
        entry
        for entry in entries
        if not entry.get("superseded_by") and entry.get("id") not in superseded_ids
    ]
    return live or list(entries)


def _is_add_only_tier(worker_tier: TimeTier) -> bool:
    """Lower tiers can only append; only SEASON+ may modify existing entries."""

    return worker_tier < TimeTier.SEASON


def _dispute_tag(tier: TimeTier) -> str:
    return f"[DISPUTES:{tier.name}]"


def _has_dispute_tag(text: str) -> bool:
    return "[disputes:" in (text or "").lower()


def _append_dispute_tag(text: str, disputed_tier: TimeTier) -> str:
    """Append a dispute tag (before the origin tag that gets appended later)."""

    clean = (text or "").strip()
    tag = _dispute_tag(disputed_tier)
    if clean.lower().endswith(tag.lower()):
        return clean[:-len(tag)] + tag
    if tag.lower() in clean.lower():
        return clean
    return f"{clean} {tag}".strip()


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
    add_only = _is_add_only_tier(worker_tier)

    NOTE_MAX_CONFIDENCE: str = os.getenv("PSYCHIC_CARD_NOTE_MAX_CONFIDENCE", "likely")

    def _clamp_confidence(value: str, *, max_value: str) -> str:
        value = (value or "").strip().lower()
        max_value = (max_value or "").strip().lower()
        if value not in CONFIDENCE_ORDER:
            return "possible"
        if max_value not in CONFIDENCE_ORDER:
            return value
        return (
            value
            if CONFIDENCE_ORDER.index(value) <= CONFIDENCE_ORDER.index(max_value)
            else max_value
        )

    def _touch_meta(target: dict, *, seen_action: str, reason: str, confidence: str) -> None:
        # Lightweight "note log" so Season+ can decide what to promote without bloating
        # the card with near-duplicate entries.
        try:
            target["seen_count"] = int(target.get("seen_count") or 0) + 1
        except Exception:
            target["seen_count"] = 1

        seen_by_tier = target.setdefault("seen_by_tier", {})
        if isinstance(seen_by_tier, dict):
            key = worker_tier.name
            try:
                seen_by_tier[key] = int(seen_by_tier.get(key) or 0) + 1
            except Exception:
                seen_by_tier[key] = 1

        target["last_seen_at"] = datetime.now(timezone.utc).isoformat()
        target["last_seen_summary_id"] = summary_id
        target["last_seen_tier"] = worker_tier.name
        target["last_seen_action"] = seen_action
        if reason:
            target["last_seen_reason"] = reason
        if confidence:
            target["last_seen_confidence"] = confidence

    def _extract_dispute_tier(text: str) -> TimeTier | None:
        match = re.search(r"\[DISPUTES:([A-Z]+)\]", text or "")
        if not match:
            return None
        try:
            return parse_tier(match.group(1))
        except Exception:  # noqa: BLE001
            return None

    def _add_entry(
        *,
        text: str,
        confidence: str,
        reason: str,
        action: str,
        disputed_tier: TimeTier | None = None,
        supersedes: str | None = None,
        origin: TimeTier | None = None,
    ) -> dict:
        origin = origin or worker_tier
        entry_text = _normalize_text(text, confidence)
        if disputed_tier is not None:
            entry_text = _append_dispute_tag(entry_text, disputed_tier)
        entry_text = append_origin_tag(entry_text, origin)
        now_entry = make_entry(
            text=entry_text,
            confidence=confidence,
            action=action,  # type: ignore[arg-type]
            summary_id=summary_id,
            tier=tier,
            reason=reason,
            origin_tier=origin.name,
            supersedes=supersedes,
        )
        now_entry["seen_count"] = 1
        now_entry["seen_by_tier"] = {worker_tier.name: 1}
        now_entry["last_seen_at"] = now_entry.get("created_at")
        now_entry["last_seen_summary_id"] = summary_id
        now_entry["last_seen_tier"] = worker_tier.name
        now_entry["last_seen_action"] = action
        return now_entry

    def _supersede(
        *,
        bucket: list[dict],
        target: dict,
        text: str,
        confidence: str,
        reason: str,
        action: str,
    ) -> None:
        supersedes_id = str(target.get("id")) if target.get("id") else None
        now_entry = _add_entry(
            text=text,
            confidence=confidence,
            reason=reason,
            action=action,
            supersedes=supersedes_id,
            origin=worker_tier,
        )
        bucket.append(now_entry)
        # Best-effort: mark the old entry as superseded.
        if target.get("id"):
            target["superseded_by"] = now_entry.get("id")

    def _mutate_in_place(
        *,
        target: dict,
        text: str,
        confidence: str,
        reason: str,
        evidence_action: str,
    ) -> None:
        origin_tier = _entry_origin_tier(target, fallback=worker_tier)
        existing_dispute = _extract_dispute_tier(target.get("text") or "")
        next_text = _normalize_text(text, confidence)
        if existing_dispute is not None:
            next_text = _append_dispute_tag(next_text, existing_dispute)

        target["confidence"] = confidence
        target["text"] = append_origin_tag(next_text, origin_tier)
        target["reason"] = reason or target.get("reason")
        _touch_meta(
            target,
            seen_action=evidence_action,
            reason=reason,
            confidence=confidence,
        )
        evidence = target.setdefault("evidence", [])
        if isinstance(evidence, list):
            evidence.append(
                {
                    "summary_id": summary_id,
                    "reason": reason,
                    "confidence": confidence,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "action": evidence_action,
                }
            )
            # Prevent unbounded growth in the stored card JSON.
            if len(evidence) > 5:
                target["evidence"] = evidence[-5:]

    for patch in patches:
        seg_id = str(patch["segment_id"])
        if seg_id not in segments:
            # Unknown segment; skip quietly but keep history
            continue

        bucket = segments.get(seg_id) or []
        segments[seg_id] = bucket
        live_bucket = _live_entries([e for e in bucket if isinstance(e, dict)])
        patch_conf = _parse_confidence(patch["confidence"])
        if add_only:
            patch_conf = _clamp_confidence(patch_conf, max_value=NOTE_MAX_CONFIDENCE)
        patch_text = _normalize_text(patch["text"], patch_conf)
        reason = patch.get("reason") or ""
        target_id = patch.get("target_id")
        target_text = patch.get("target_text") or ""
        patch_action = (patch.get("action") or "").lower()

        # Resolve an explicit target (if present) against live entries first.
        target_any = None
        if target_id:
            target_any = _find_entry_by_id(live_bucket, target_id)
            if target_any and target_text:
                if (
                    _match_ratio(target_text, target_any.get("text") or "")
                    < TARGET_MATCH_MIN_RATIO
                ):
                    target_any = None
        if not target_any and target_text:
            target_any = _find_best_match(
                live_bucket, target_text, min_ratio=TARGET_MATCH_MIN_RATIO
            )

        if add_only:
            # Episode/Chapter behavior:
            # - Operate in the NOTES layer (origin < SEASON) to avoid bloating the card.
            # - Never edit stable Season+ entries; if targeted, append a DISPUTES note.
            stable_entries = [
                entry
                for entry in live_bucket
                if _entry_origin_tier(entry, fallback=TimeTier.TURN) >= TimeTier.SEASON
            ]
            note_entries = [
                entry
                for entry in live_bucket
                if _entry_origin_tier(entry, fallback=TimeTier.TURN) < TimeTier.SEASON
            ]

            # If model tries to edit a stable entry, record a dispute note instead.
            if patch_action in {"reinforce", "revise"} and target_any:
                target_tier = _entry_origin_tier(target_any, fallback=worker_tier)
                if target_tier >= TimeTier.SEASON:
                    now_entry = _add_entry(
                        text=patch_text,
                        confidence=patch_conf,
                        reason=reason,
                        action="add",
                        disputed_tier=target_tier,
                        origin=worker_tier,
                    )
                    bucket.append(now_entry)
                    continue

            target_note = None
            if target_any and _entry_origin_tier(target_any, fallback=worker_tier) < TimeTier.SEASON:
                target_note = target_any
            if not target_note and target_id:
                target_note = _find_entry_by_id(note_entries, target_id)
            if not target_note and target_text:
                target_note = _find_best_match(
                    note_entries,
                    target_text,
                    min_ratio=TARGET_MATCH_MIN_RATIO,
                )
            if not target_note:
                target_note = _find_best_match(
                    note_entries,
                    patch_text,
                    min_ratio=TARGET_MATCH_MIN_RATIO,
                )

            # If this is just repeating a stable fact, don't create a redundant note.
            if patch_action == "add" and not target_note:
                if _find_best_match(
                    stable_entries, patch_text, min_ratio=TARGET_MATCH_MIN_RATIO
                ):
                    continue

            if target_note:
                current_conf = target_note.get("confidence") or patch_conf
                try:
                    bumped = _one_step_confidence(current_conf, patch_conf)
                except Exception:
                    bumped = patch_conf
                bumped = _clamp_confidence(bumped, max_value=NOTE_MAX_CONFIDENCE)

                if patch_action in {"add", "reinforce"}:
                    base = _strip_confidence(target_note.get("text") or patch_text)
                    _mutate_in_place(
                        target=target_note,
                        text=base,
                        confidence=bumped,
                        reason=reason,
                        evidence_action="reinforce",
                    )
                else:
                    base = _strip_confidence(patch_text)
                    _mutate_in_place(
                        target=target_note,
                        text=base,
                        confidence=bumped,
                        reason=reason,
                        evidence_action="revise",
                    )
                continue

            now_entry = _add_entry(
                text=patch_text,
                confidence=patch_conf,
                reason=reason,
                action="add",
                origin=worker_tier,
            )
            bucket.append(now_entry)
            continue

        # SEASON+ behavior: can modify/promote lower tiers, but never overwrite higher tiers.
        modifiable = [
            entry
            for entry in live_bucket
            if _entry_origin_tier(entry, fallback=TimeTier.TURN) <= worker_tier
        ]
        higher = [
            entry
            for entry in live_bucket
            if _entry_origin_tier(entry, fallback=TimeTier.TURN) > worker_tier
        ]

        if patch_action == "add":
            match = _find_best_match(modifiable, patch_text)
            if match:
                match_tier = _entry_origin_tier(match, fallback=worker_tier)
                current_conf = match.get("confidence") or patch_conf
                try:
                    bumped = _one_step_confidence(current_conf, patch_conf)
                except Exception:
                    bumped = patch_conf
                if match_tier < worker_tier:
                    _supersede(
                        bucket=bucket,
                        target=match,
                        text=patch_text,
                        confidence=bumped,
                        reason=reason,
                        action="reinforce",
                    )
                else:
                    _mutate_in_place(
                        target=match,
                        text=patch_text,
                        confidence=bumped,
                        reason=reason,
                        evidence_action="reinforce",
                    )
                continue

            now_entry = _add_entry(
                text=patch_text,
                confidence=patch_conf,
                reason=reason,
                action="add",
                origin=worker_tier,
            )
            bucket.append(now_entry)
            continue

        if patch_action == "reinforce":
            if target_any and _entry_origin_tier(target_any, fallback=worker_tier) > worker_tier:
                now_entry = _add_entry(
                    text=patch_text,
                    confidence=patch_conf,
                    reason=reason,
                    action="add",
                    disputed_tier=_entry_origin_tier(target_any, fallback=worker_tier),
                    origin=worker_tier,
                )
                bucket.append(now_entry)
                continue

            target = None
            if target_any and _entry_origin_tier(target_any, fallback=worker_tier) <= worker_tier:
                target = target_any
            if not target:
                target = _find_best_match(modifiable, patch_text)

            if not target:
                higher_match = _find_best_match(
                    higher,
                    target_text or patch_text,
                    min_ratio=TARGET_MATCH_MIN_RATIO,
                )
                now_entry = _add_entry(
                    text=patch_text,
                    confidence=patch_conf,
                    reason=reason,
                    action="add",
                    disputed_tier=_entry_origin_tier(higher_match, fallback=worker_tier)
                    if higher_match
                    else None,
                    origin=worker_tier,
                )
                bucket.append(now_entry)
                continue

            target_tier = _entry_origin_tier(target, fallback=worker_tier)
            current_conf = target.get("confidence") or patch_conf
            try:
                bumped = _one_step_confidence(current_conf, patch_conf)
            except Exception:
                bumped = patch_conf

            if target_tier < worker_tier:
                _supersede(
                    bucket=bucket,
                    target=target,
                    text=patch_text,
                    confidence=bumped,
                    reason=reason,
                    action="reinforce",
                )
            else:
                _mutate_in_place(
                    target=target,
                    text=patch_text,
                    confidence=bumped,
                    reason=reason,
                    evidence_action="reinforce",
                )
            continue

        if patch_action == "revise":
            if target_any and _entry_origin_tier(target_any, fallback=worker_tier) > worker_tier:
                now_entry = _add_entry(
                    text=patch_text,
                    confidence=patch_conf,
                    reason=reason,
                    action="add",
                    disputed_tier=_entry_origin_tier(target_any, fallback=worker_tier),
                    origin=worker_tier,
                )
                bucket.append(now_entry)
                continue

            target = None
            if target_any and _entry_origin_tier(target_any, fallback=worker_tier) <= worker_tier:
                target = target_any
            if not target:
                target = _find_best_match(modifiable, patch_text) or _active_entry(modifiable)

            if not target:
                higher_match = _find_best_match(
                    higher,
                    target_text or patch_text,
                    min_ratio=TARGET_MATCH_MIN_RATIO,
                )
                now_entry = _add_entry(
                    text=patch_text,
                    confidence=patch_conf,
                    reason=reason,
                    action="add",
                    disputed_tier=_entry_origin_tier(higher_match, fallback=worker_tier)
                    if higher_match
                    else None,
                    origin=worker_tier,
                )
                bucket.append(now_entry)
                continue

            target_tier = _entry_origin_tier(target, fallback=worker_tier)
            if target_tier < worker_tier:
                _supersede(
                    bucket=bucket,
                    target=target,
                    text=patch_text,
                    confidence=patch_conf,
                    reason=reason,
                    action="revise",
                )
            else:
                _mutate_in_place(
                    target=target,
                    text=patch_text,
                    confidence=patch_conf,
                    reason=reason,
                    evidence_action="revise",
                )
            continue

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
    stable_max = int(os.getenv("PSYCHIC_CARD_STABLE_MAX_PER_SEGMENT", "3"))
    notes_max = int(os.getenv("PSYCHIC_CARD_NOTES_MAX_PER_SEGMENT", "5"))
    try:
        card = prune_fan_psychic_card(
            card,
            stable_max_per_segment=stable_max,
            notes_max_per_segment=notes_max,
            drop_superseded=True,
        )
    except Exception:
        # Never fail the pipeline due to compaction; fallback to raw card.
        card = ensure_card_shape(card)
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
    writer_json: dict | None = None,
) -> int:
    summary_idx = tier_index or _next_tier_index(thread_id, tier)
    writer_json = writer_json or {"raw_text": raw_text}
    # Abstract summaries are deprecated; keep the column empty for backwards compatibility.
    abstract_summary = ""

    def _writer_json_with_patches() -> dict:
        merged = dict(writer_json or {})
        merged.setdefault("raw_text", raw_text)
        merged.setdefault("patches", patches)
        return merged

    # Candidate encodings for fan_psychic_card_action; DB schemas vary.
    # - Some expect a JSON array of patch objects.
    # - Some expect a JSON object (e.g., wrapper) or disallow NULL keys/values.
    action_candidates: List[object | None] = []
    if patches:
        action_candidates.append({"patches": patches})
        action_candidates.append(patches)
    action_candidates.append(None)

    base_update = {
        "abstract_summary": abstract_summary,
        "raw_writer_json": writer_json,
        "raw_hash": raw_hash,
        "extract_status": extract_status,
    }

    # Try update first (if row exists), then insert if missing.
    for fan_action in action_candidates:
        update_payload = dict(base_update)
        update_payload["fan_psychic_card_action"] = fan_action
        if fan_action is None and patches:
            update_payload["raw_writer_json"] = _writer_json_with_patches()

        try:
            res = (
                SB.table("summaries")
                .update(update_payload)
                .eq("thread_id", thread_id)
                .eq("tier", tier)
                .eq("start_turn", start_turn)
                .eq("end_turn", end_turn)
                .execute()
                .data
                or []
            )
        except APIError as exc:
            if _api_error_code(exc) == "23514":
                continue
            raise

        if res:
            return res[0]["id"]
        # If no row matched, stop trying alternate encodings; move to insert.
        break

    for fan_action in action_candidates:
        payload = {
            "thread_id": thread_id,
            "tier": tier,
            "tier_index": summary_idx,
            "start_turn": start_turn,
            "end_turn": end_turn,
            "abstract_summary": abstract_summary,
            "fan_psychic_card_action": fan_action,
            "raw_writer_json": writer_json,
            "raw_hash": raw_hash,
            "extract_status": extract_status,
        }
        if fan_action is None and patches:
            payload["raw_writer_json"] = _writer_json_with_patches()

        try:
            res = SB.table("summaries").insert(payload).execute().data or []
        except APIError as exc:
            if _api_error_code(exc) == "23514":
                continue
            raise

        if res:
            return res[0]["id"]

    raise RuntimeError("failed to insert summary row")


def _fetch_summaries(thread_id: int, tier: str) -> List[dict]:
    rows = (
        SB.table("summaries")
        .select(
            "id,tier_index,start_turn,end_turn,raw_writer_json,fan_psychic_card_action"
        )
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .eq("extract_status", "ok")
        .order("tier_index")
        .execute()
        .data
        or []
    )
    # Only treat a contiguous prefix as usable.
    # This prevents higher-tier rollups from consuming past a missing/invalid tier_index
    # and allows the pipeline to self-heal.
    usable: List[dict] = []
    expected = 1
    for row in rows:
        try:
            idx = int(row.get("tier_index") or 0)
        except Exception:
            break
        if idx != expected:
            break

        raw_writer_json = row.get("raw_writer_json") or {}
        raw_text = ""
        if isinstance(raw_writer_json, dict):
            raw_text = raw_writer_json.get("raw_text") or ""
        if raw_text and _looks_like_prompt_leak(raw_text):
            break

        usable.append(row)
        expected += 1
    return usable


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

    def _extract_patches(row: dict) -> list[dict]:
        raw_writer_json = row.get("raw_writer_json") or {}
        if isinstance(raw_writer_json, dict):
            patches = raw_writer_json.get("patches")
            if isinstance(patches, list):
                return [p for p in patches if isinstance(p, dict)]

        action_blob = row.get("fan_psychic_card_action")
        if isinstance(action_blob, dict):
            patches = action_blob.get("patches")
            if isinstance(patches, list):
                return [p for p in patches if isinstance(p, dict)]
        if isinstance(action_blob, list):
            return [p for p in action_blob if isinstance(p, dict)]

        return []

    def _format_patch_block(patch: dict) -> str:
        header = patch.get("segment_full_label") or patch.get("segment_label") or patch.get(
            "segment_id"
        )
        header = str(header or "").strip()
        if header and not header.lower().startswith("segment"):
            header = f"SEGMENT_{header}"
        action = str(patch.get("action") or "").strip().upper() or "ADD"
        lines = [f"{action} {header}".strip()]
        target_id = patch.get("target_id")
        if target_id:
            lines.append(f"TARGET_ID: {target_id}")
        target_text = patch.get("target_text")
        if target_text:
            lines.append(f"TARGET: {target_text}")
        text = patch.get("text") or ""
        conf = patch.get("confidence") or ""
        reason = patch.get("reason") or ""
        lines.append(f"TEXT: {text}".rstrip())
        lines.append(f"CONFIDENCE: {conf}".rstrip())
        lines.append(f"REASON: {reason}".rstrip())
        return "\n".join(lines).strip()

    lines = []
    for row in chunk:
        idx = row.get("tier_index")
        patches = _extract_patches(row)
        if patches:
            rendered = "\n\n".join(_format_patch_block(p) for p in patches).strip()
        else:
            rendered = "NO_UPDATES"
        lines.append(f"{lower_tier.title()} {idx}:\n{rendered}")
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
    send(queue_name, payload)


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

    writer_json: dict = {"raw_text": raw_text}

    try:
        patches, _ = parse_patch_output(raw_text)
        patches = _sanitize_patches(patches)
        extract_status = "ok"
    except Exception as exc:  # noqa: BLE001
        patches = []
        extract_status = "failed"
        print(f"[card_patch_applier] parse failed: {exc}")

    # If the model output is clearly prompt/meta leakage, treat as a failure so we retry.
    if extract_status == "ok" and _looks_like_prompt_leak(raw_text):
        patches = []
        extract_status = "failed"
        writer_json["validation_error"] = "prompt_leak"

    if _should_attempt_rewrite(raw_text, patches=patches, extract_status=extract_status):
        try:
            rewritten = _rewrite_to_patch_contract(raw_text, tier) or ""
        except Exception as exc:  # noqa: BLE001
            rewritten = ""
            writer_json["rewrite_error"] = str(exc)

        if rewritten and rewritten.strip() and rewritten.strip() != (raw_text or "").strip():
            writer_json["rewritten_text"] = rewritten
            writer_json["rewritten_model"] = RUNPOD_REWRITE_MODEL
            writer_json["rewritten_hash"] = hashlib.sha256(
                rewritten.encode("utf-8")
            ).hexdigest()
            try:
                rewritten_patches, rewritten_summary = parse_patch_output(rewritten)
                rewritten_patches = _sanitize_patches(rewritten_patches)
                if _rewrite_is_verbatim(
                    raw_text, patches=rewritten_patches, summary=rewritten_summary
                ):
                    patches = rewritten_patches
                    extract_status = "ok"
                else:
                    writer_json["rewrite_rejected"] = "not_verbatim"
            except Exception as exc:  # noqa: BLE001
                writer_json["rewrite_parse_error"] = str(exc)

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
            abstract_summary="",
            patches=patches,
            raw_text=raw_text,
            raw_hash=raw_hash,
            extract_status=extract_status,
            writer_json=writer_json,
        )
        return True

    summary_id = _insert_summary_row(
        thread_id=thread_id,
        tier=tier,
        start_turn=start_turn,
        end_turn=end_turn,
        tier_index=tier_index,
        abstract_summary="",
        patches=patches,
        raw_text=raw_text,
        raw_hash=raw_hash,
        extract_status=extract_status,
        writer_json=writer_json,
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
