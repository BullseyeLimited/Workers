"""Fan Psychic Card helpers: template, normalization, and confidence utils."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from workers.lib.time_tier import TimeTier, parse_tier, tier_tag

Confidence = Literal["tentative", "possible", "likely", "confident", "canonical"]

# Canonical map of segment id -> name for the fan psychic card.
SEGMENT_NAMES: Dict[str, str] = {
    "1": "01.CORE_MOTIVATIONS_VALUES",
    "2": "02.SELF_NARRATIVE_IDENTITY_THEMES",
    "3": "03.WORLDVIEW_INTERPRETATION_FRAMES",
    "4": "04.AESTHETIC_TASTE_CONSTELLATION_GENERAL",
    "5": "05.RELATIONSHIP_TEMPLATE_SOUGHT",
    "6": "06.COGNITIVE_IMAGINATION_STYLE",
    "7": "07.ATTACHMENT_INTIMACY_STYLE",
    "8": "08.TRUST_SAFETY_SCHEMA",
    "9": "09.CONSENT_BOUNDARY_POSTURE_GENERAL",
    "10": "10.VULNERABILITY_DISCLOSURE_SCHEMA",
    "11": "11.RECIPROCITY_FAIRNESS_EXPECTATIONS",
    "12": "12.PARASOCIAL_FRAME_PERSONA_REALISM",
    "13": "13.EMOTIONAL_LABOR_EXPECTATIONS",
    "14": "14.RELATIONAL_CURRENCIES_MAP",
    "15": "15.EMOTIONAL_LANDSCAPE_TRIGGER_MAP",
    "16": "16.REGULATION_COPING_DEFENSE_REPERTOIRE",
    "17": "17.STRESSORS_SOOTHERS_GENERAL_CLASSES",
    "18": "18.SHAME_GUILT_SENSITIVITY_ZONES",
    "19": "19.SELF_ESTEEM_VALIDATION_NEEDS",
    "20": "20.JEALOUSY_POSSESSIVENESS_PROFILE",
    "21": "21.AFTERCARE_COMEDOWN_PROTOCOL",
    "22": "22.TONE_REGISTER_LEXICON_MAP",
    "23": "23.HUMOR_PLAY_BOUNDARIES",
    "24": "24.CADENCE_LENGTH_DENSITY_PREFERENCES",
    "25": "25.TEMPORAL_PATTERNS_TIMING_RHYTHM",
    "26": "26.GUIDANCE_AGENCY_PREFERENCES",
    "27": "27.AMBIGUITY_META_COMM_TOLERANCE",
    "28": "28.CONFLICT_REPAIR_STYLE",
    "29": "29.COGNITIVE_LOAD_COMPLEXITY_TOLERANCE",
    "30": "30.SUGGESTIBILITY_PRIMING_SENSITIVITY",
    "31": "31.AROUSAL_INHIBITION_PROFILE",
    "32": "32.POLARITY_POWER_DYNAMICS",
    "33": "33.FANTASY_ROLEPLAY_FETISH_CANON",
    "34": "34.SEXUAL_ARC_LEDGER",
    "35": "35.PACE_ESCALATION_STYLE",
    "36": "36.DIRTY_TALK_SENSORY_PREFERENCES",
    "37": "37.AVERSION_SQUICK_MAP",
    "38": "38.THIRD_PARTY_VOYEUR_DYNAMICS",
    "39": "39.CONSENT_FRAMING_IN_FANTASY",
    "40": "40.MEDIA_MODALITY_PROFILE",
    "41": "41.AESTHETIC_ATMOSPHERE_PREFS_EROTIC",
    "42": "42.NARRATIVE_IMMERSION_MODE",
    "43": "43.NARRATIVE_TROPES_STORY_MECHANICS",
    "44": "44.HABITUATION_VARIETY_HEURISTICS",
    "45": "45.RITUALS_SYMBOLS_SHARED_TOKENS",
    "46": "46.CONTINUITY_CALLBACK_APPETITE",
    "47": "47.CHANNEL_CONTEXT_BOUNDARIES",
    "48": "48.MONETIZATION_PSYCHOLOGY_OFFER_FIT",
    "49": "49.PRICE_SENSITIVITY_FRAMING_PREFERENCES",
    "50": "50.UPSELL_TIMING_RECEPTIVITY",
    "51": "51.TRIBUTE_GIFTING_SEMANTICS",
    "52": "52.CULTURAL_ETHICAL_SENSIBILITY_MAP",
    "53": "53.STATUS_PRESTIGE_EXCLUSIVITY_ORIENTATION",
    "54": "54.TABOO_BOUNDARIES_RED_LINES",
    "55": "55.CREATOR_FAN_INTERPLAY_MAP",
    "56": "56.PERSONA_GUARDRAILS_CANON_GOVERNANCE",
    "57": "57.CHANGE_LOG_TRENDLINES",
    "58": "58.HYPOTHESIS_REGISTER",
    "59": "59.EVIDENCE_CONFIDENCE_LEDGER",
    "60": "60.UNKNOWNS_OPEN_QUESTIONS",
    "61": "61.SHADOW_SELF_PROJECTION_TARGETS",
    "62": "62.COGNITIVE_DISTORTIONS_LOGIC_BUGS",
    "63": "63.SECURITY_TESTING_PUSH_PULL_PATTERNS",
    "64": "64.EMOTIONAL_CO_REGULATION_DYNAMICS",
    "65": "65.EPISTEMIC_TRUST_PERSUASION_KEYS",
    "66": "66.BIOGRAPHICAL_EMOTIONAL_WEIGHTING",
}

CONFIDENCE_ORDER: List[Confidence] = [
    "tentative",
    "possible",
    "likely",
    "confident",
    "canonical",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_base_card() -> dict:
    """Return a pristine fan psychic card with all segments present and empty."""
    return {
        "version": 1,
        "segment_names": SEGMENT_NAMES,
        "segments": {seg_id: [] for seg_id in SEGMENT_NAMES},
        "cards_updated_at": _now_iso(),
    }


def ensure_card_shape(card: Optional[dict]) -> dict:
    """Guarantee the expected shape on a loaded card (non-destructive)."""
    if not isinstance(card, dict):
        return new_base_card()

    # Shallow copy to avoid mutating caller's object
    normalized = deepcopy(card)
    normalized["version"] = normalized.get("version", 1)
    normalized["segment_names"] = SEGMENT_NAMES
    segments = normalized.setdefault("segments", {})

    for seg_id in SEGMENT_NAMES:
        segments.setdefault(seg_id, [])

    normalized["cards_updated_at"] = _now_iso()
    return normalized


def upgrade_confidence(current: Confidence, incoming: Confidence) -> Confidence:
    """Pick the stronger of two confidence levels."""
    current_idx = CONFIDENCE_ORDER.index(current)
    incoming_idx = CONFIDENCE_ORDER.index(incoming)
    return CONFIDENCE_ORDER[max(current_idx, incoming_idx)]


def append_origin_tag(text: str, origin_tier: TimeTier) -> str:
    """Append a provenance tag for the originating tier if missing."""

    tag = tier_tag(origin_tier)
    clean = (text or "").strip()
    if clean.lower().endswith(tag.lower()):
        # Preserve canonical casing of the tag even if the stored text differs
        if clean.endswith(tag):
            return clean
        return f"{clean[: -len(tag)]}{tag}"
    return f"{clean} {tag}".strip()


def _coerce_origin_tier(value, default: TimeTier = TimeTier.TURN) -> TimeTier:
    try:
        return parse_tier(value)
    except Exception:  # noqa: BLE001
        return default


def filter_card_by_tier(card: dict, max_origin_tier: TimeTier) -> dict:
    """
    Return a copy of the card limited to entries visible to the given tier.

    Visibility rules:
    - Only include entries whose origin tier is <= max_origin_tier.
    - Exclude entries that have been superseded.
    """

    if not isinstance(card, dict):
        return card

    filtered = deepcopy(card)
    segments = filtered.get("segments")
    if not isinstance(segments, dict):
        return filtered

    for seg_id, entries in segments.items():
        if not isinstance(entries, list):
            segments[seg_id] = []
            continue

        superseded_ids = {
            entry.get("supersedes")
            for entry in entries
            if entry.get("supersedes")
        }

        visible_entries = []
        for entry in entries:
            entry_id = entry.get("id")
            origin = _coerce_origin_tier(
                entry.get("origin_tier") or entry.get("tier"),
                default=TimeTier.TURN,
            )
            if origin > max_origin_tier:
                continue
            if entry.get("superseded_by") or (entry_id in superseded_ids):
                continue
            visible_entries.append(entry)

        segments[seg_id] = visible_entries

    return filtered


def prune_fan_psychic_card(
    card: Optional[dict],
    *,
    stable_max_per_segment: int = 3,
    notes_max_per_segment: int = 5,
    drop_superseded: bool = True,
) -> dict:
    """
    Return a smaller, storage-friendly card by trimming each segment.

    Policy:
    - Treat Season+ entries as STABLE facts (more trusted) and keep up to `stable_max_per_segment`.
    - Treat Turn/Episode/Chapter entries as NOTES (lower trust) and keep up to `notes_max_per_segment`.
    - Optionally drop superseded entries to avoid unbounded growth in threads.fan_psychic_card.

    Rationale:
    - Full history already exists in summaries; the threads card should stay compact for prompt use.
    """

    pruned = ensure_card_shape(card)
    segments = pruned.get("segments")
    if not isinstance(segments, dict):
        return pruned

    def _entry_origin(entry: dict) -> TimeTier:
        try:
            return parse_tier(entry.get("origin_tier") or entry.get("tier") or TimeTier.TURN)
        except Exception:  # noqa: BLE001
            return TimeTier.TURN

    def _confidence_index(entry: dict) -> int:
        conf = entry.get("confidence")
        if isinstance(conf, str) and conf in CONFIDENCE_ORDER:
            return CONFIDENCE_ORDER.index(conf)
        return 0

    def _stamp(entry: dict) -> str:
        for key in ("last_seen_at", "created_at"):
            value = entry.get(key)
            if isinstance(value, str) and value:
                return value
        return ""

    def _seen_count(entry: dict) -> int:
        try:
            return int(entry.get("seen_count") or 0)
        except Exception:
            return 0

    for seg_id, entries in list(segments.items()):
        if not isinstance(entries, list):
            segments[seg_id] = []
            continue

        dict_entries = [e for e in entries if isinstance(e, dict)]
        superseded_ids = {
            e.get("supersedes") for e in dict_entries if e.get("supersedes")
        }

        live: list[dict] = []
        for entry in dict_entries:
            if drop_superseded:
                if entry.get("superseded_by"):
                    continue
                if entry.get("id") in superseded_ids:
                    continue
            live.append(entry)

        stable = [e for e in live if _entry_origin(e) >= TimeTier.SEASON]
        notes = [e for e in live if _entry_origin(e) < TimeTier.SEASON]

        stable.sort(
            key=lambda e: (
                _entry_origin(e),
                _confidence_index(e),
                _seen_count(e),
                _stamp(e),
            ),
            reverse=True,
        )
        notes.sort(
            key=lambda e: (
                _seen_count(e),
                _confidence_index(e),
                _stamp(e),
            ),
            reverse=True,
        )

        keep_stable = stable[: max(stable_max_per_segment, 0)]
        keep_notes = notes[: max(notes_max_per_segment, 0)]
        segments[seg_id] = keep_stable + keep_notes

    pruned["cards_updated_at"] = _now_iso()
    return pruned


def compact_psychic_card(
    card: Any,
    *,
    key_by: Literal["id", "name"] = "name",
    max_entries_per_segment: int | None = None,
    drop_superseded: bool = False,
    entry_fields: tuple[str, ...] | None = None,
) -> dict | str | None:
    """
    Return a compact psychic card suitable for logs/prompts.

    - Drops empty segments.
    - Drops entries with blank `text` fields.
    - If no segments remain, returns None.
    - By default, keys segments by their human segment name (no numeric ids).
    - Optionally limits each segment to the most relevant entries for prompt usage.
    """

    if card is None:
        return None

    if isinstance(card, str):
        stripped = card.strip()
        return stripped or None

    if not isinstance(card, dict):
        return card

    segments = card.get("segments")
    if not isinstance(segments, dict):
        # Unknown shape; only keep it if it has non-empty data.
        return card if any(v for v in card.values()) else None

    segment_names = card.get("segment_names")
    names: dict[str, str] = segment_names if isinstance(segment_names, dict) else {}

    def _is_dispute_entry(entry: Any) -> bool:
        if isinstance(entry, dict):
            text = entry.get("text")
            return isinstance(text, str) and "[disputes:" in text.lower()
        if isinstance(entry, str):
            return "[disputes:" in entry.lower()
        return False

    def _entry_origin(entry: Any) -> TimeTier:
        if isinstance(entry, dict):
            try:
                return parse_tier(entry.get("origin_tier") or entry.get("tier"))
            except Exception:  # noqa: BLE001
                return TimeTier.TURN
        # Best-effort: detect trailing provenance tags like "[EPISODE]".
        if isinstance(entry, str):
            for tier_name in ("lifetime", "year", "season", "chapter", "episode", "turn"):
                if f"[{tier_name}]".lower() in entry.lower():
                    try:
                        return parse_tier(tier_name)
                    except Exception:  # noqa: BLE001
                        break
        return TimeTier.TURN

    def _confidence_index(entry: Any) -> int:
        if isinstance(entry, dict):
            conf = entry.get("confidence")
            if isinstance(conf, str) and conf in CONFIDENCE_ORDER:
                return CONFIDENCE_ORDER.index(conf)
        return 0

    def _created_at(entry: Any) -> str:
        if isinstance(entry, dict):
            value = entry.get("created_at")
            return value if isinstance(value, str) else ""
        return ""

    def _drop_superseded_entries(entries: list[Any]) -> list[Any]:
        dict_entries = [e for e in entries if isinstance(e, dict)]
        superseded_ids = {
            e.get("supersedes") for e in dict_entries if e.get("supersedes")
        }
        cleaned: list[Any] = []
        for entry in entries:
            if isinstance(entry, dict):
                if entry.get("superseded_by"):
                    continue
                if entry.get("id") in superseded_ids:
                    continue
            cleaned.append(entry)
        return cleaned

    def _select_entries(entries: list[Any]) -> list[Any]:
        if max_entries_per_segment is None or max_entries_per_segment <= 0:
            return entries

        # Group by origin tier (highest tier wins). Within a tier, prefer:
        # non-dispute > higher confidence > newer.
        buckets: dict[TimeTier, list[Any]] = {}
        for entry in entries:
            buckets.setdefault(_entry_origin(entry), []).append(entry)

        tiers = sorted(buckets.keys(), reverse=True)
        if not tiers:
            return []

        def _rank(e: Any):
            return (
                not _is_dispute_entry(e),
                _confidence_index(e),
                _created_at(e),
            )

        selected: list[Any] = []
        # Always take 1 from the highest tier.
        top_tier = tiers[0]
        top_sorted = sorted(buckets[top_tier], key=_rank, reverse=True)
        selected.append(top_sorted[0])
        if len(selected) >= max_entries_per_segment:
            return selected

        # Prefer 1 from the next-highest tier; if none exists, take more from top tier.
        if len(tiers) > 1:
            second_tier = tiers[1]
            second_sorted = sorted(buckets[second_tier], key=_rank, reverse=True)
            selected.append(second_sorted[0])
        else:
            selected.extend(top_sorted[1:max_entries_per_segment])

        return selected[:max_entries_per_segment]

    def _project_fields(entry: Any) -> Any:
        if entry_fields is None or not isinstance(entry, dict):
            return entry
        return {k: entry.get(k) for k in entry_fields if k in entry}

    compact_segments: dict[str, list[Any]] = {}
    for seg_id, entries in segments.items():
        if not entries:
            continue

        cleaned_entries: list[Any] = []
        if isinstance(entries, list):
            for entry in entries:
                if entry is None:
                    continue
                if isinstance(entry, str):
                    text = entry.strip()
                    if text:
                        cleaned_entries.append(text)
                    continue
                if isinstance(entry, dict):
                    maybe_text = entry.get("text")
                    if isinstance(maybe_text, str) and not maybe_text.strip():
                        continue
                    cleaned_entries.append(entry)
                    continue
                if entry:
                    cleaned_entries.append(entry)
        else:
            cleaned_entries = [entries]

        if not cleaned_entries:
            continue

        if drop_superseded:
            cleaned_entries = _drop_superseded_entries(cleaned_entries)
            if not cleaned_entries:
                continue

        cleaned_entries = _select_entries(cleaned_entries)
        if not cleaned_entries:
            continue

        if entry_fields is not None:
            cleaned_entries = [_project_fields(e) for e in cleaned_entries]

        seg_id_str = str(seg_id)
        if key_by == "name":
            seg_key = names.get(seg_id_str) or seg_id_str
        else:
            seg_key = seg_id_str
        compact_segments[seg_key] = cleaned_entries

    if not compact_segments:
        return None

    compact: dict[str, Any] = {"segments": compact_segments}
    if card.get("version") is not None:
        compact["version"] = card.get("version")
    if card.get("cards_updated_at"):
        compact["cards_updated_at"] = card.get("cards_updated_at")

    if key_by == "id" and names:
        compact["segment_names"] = {
            seg_id: names.get(seg_id, seg_id) for seg_id in compact_segments
        }

    return compact


def make_entry(
    *,
    text: str,
    confidence: Confidence,
    action: Literal["add", "reinforce", "revise"],
    summary_id: int,
    tier: str,
    reason: str,
    origin_tier: str | None = None,
    supersedes: str | None = None,
) -> dict:
    """Construct a standardized entry for a segment."""
    return {
        "id": str(uuid4()),
        "text": text,
        "confidence": confidence,
        "action": action,
        "tier": tier,
        "summary_id": summary_id,
        "reason": reason,
        "created_at": _now_iso(),
        "origin_tier": origin_tier,
        "supersedes": supersedes,
    }


__all__ = [
    "SEGMENT_NAMES",
    "CONFIDENCE_ORDER",
    "ensure_card_shape",
    "new_base_card",
    "upgrade_confidence",
    "make_entry",
    "append_origin_tag",
    "filter_card_by_tier",
    "prune_fan_psychic_card",
]
