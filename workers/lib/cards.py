"""Fan Psychic Card helpers: template, normalization, and confidence utils."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional
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
]
