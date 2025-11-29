"""Fan Psychic Card helpers: template, normalization, and confidence utils."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional
from uuid import uuid4

Confidence = Literal["tentative", "possible", "likely", "confident", "canonical"]

# Canonical map of segment id -> name for the fan psychic card.
SEGMENT_NAMES: Dict[str, str] = {
    "1": "CORE_MOTIVATIONS_AND_VALUES",
    "2": "SELF_NARRATIVE_AND_IDENTITY_THEMES",
    "3": "WORLDVIEW_AND_INTERPRETATION_FRAMES",
    "4": "AESTHETIC_TASTE_CONSTELLATION_GENERAL",
    "5": "RELATIONSHIP_TEMPLATE_SOUGHT",
    "6": "COGNITIVE_AND_IMAGINATION_STYLE",
    "7": "ATTACHMENT_AND_INTIMACY_STYLE",
    "8": "TRUST_AND_SAFETY_SCHEMA",
    "9": "CONSENT_AND_BOUNDARY_POSTURE_GENERAL",
    "10": "VULNERABILITY_AND_DISCLOSURE_SCHEMA",
    "11": "RECIPROCITY_AND_FAIRNESS_EXPECTATIONS",
    "12": "PARASOCIAL_FRAME_AND_PERSONA_REALISM",
    "13": "EMOTIONAL_LABOR_EXPECTATIONS",
    "14": "RELATIONAL_CURRENCIES_MAP",
    "15": "EMOTIONAL_LANDSCAPE_AND_TRIGGER_MAP",
    "16": "REGULATION_COPING_AND_DEFENSE_REPERTOIRE",
    "17": "STRESSORS_AND_SOOTHERS_GENERAL_CLASSES",
    "18": "SHAME_GUILT_AND_SENSITIVITY_ZONES",
    "19": "SELF_ESTEEM_AND_VALIDATION_NEEDS",
    "20": "JEALOUSY_AND_POSSESSIVENESS_PROFILE",
    "21": "AFTERCARE_AND_COMEDOWN_PROTOCOL",
    "22": "TONE_REGISTER_AND_LEXICON_MAP",
    "23": "HUMOR_AND_PLAY_BOUNDARIES",
    "24": "CADENCE_LENGTH_AND_DENSITY_PREFERENCES",
    "25": "TEMPORAL_PATTERNS_TIMING_AND_RHYTHM",
    "26": "GUIDANCE_AND_AGENCY_PREFERENCES",
    "27": "AMBIGUITY_AND_META_COMMUNICATION_TOLERANCE",
    "28": "CONFLICT_AND_REPAIR_STYLE",
    "29": "COGNITIVE_LOAD_AND_COMPLEXITY_TOLERANCE",
    "30": "SUGGESTIBILITY_AND_PRIMING_SENSITIVITY",
    "31": "AROUSAL_AND_INHIBITION_PROFILE",
    "32": "POLARITY_AND_POWER_DYNAMICS",
    "33": "FANTASY_ROLEPLAY_AND_FETISH_CANON",
    "34": "SEXUAL_ARC_LEDGER",
    "35": "PACE_AND_ESCALATION_STYLE",
    "36": "DIRTY_TALK_AND_SENSORY_PREFERENCES",
    "37": "AVERSION_SQUICK_MAP",
    "38": "THIRD_PARTY_AND_VOYEUR_DYNAMICS",
    "39": "CONSENT_FRAMING_IN_FANTASY",
    "40": "MEDIA_AND_MODALITY_PROFILE",
    "41": "AESTHETIC_AND_ATMOSPHERE_PREFERENCES_EROTIC_CONTEXT",
    "42": "NARRATIVE_IMMERSION_MODE",
    "43": "NARRATIVE_TROPES_AND_STORY_MECHANICS",
    "44": "HABITUATION_AND_VARIETY_HEURISTICS",
    "45": "RITUALS_SYMBOLS_AND_SHARED_TOKENS",
    "46": "CONTINUITY_AND_CALLBACK_APPETITE",
    "47": "CHANNEL_AND_CONTEXT_BOUNDARIES",
    "48": "MONETIZATION_PSYCHOLOGY_AND_OFFER_FIT",
    "49": "PRICE_SENSITIVITY_AND_FRAMING_PREFERENCES",
    "50": "UPSELL_AND_TIMING_RECEPTIVITY",
    "51": "TRIBUTE_AND_GIFTING_SEMANTICS",
    "52": "CULTURAL_AND_ETHICAL_SENSIBILITY_MAP",
    "53": "STATUS_PRESTIGE_AND_EXCLUSIVITY_ORIENTATION",
    "54": "TABOO_BOUNDARIES_AND_RED_LINES",
    "55": "CREATOR_FAN_INTERPLAY_MAP",
    "56": "PERSONA_GUARDRAILS_AND_CANON_GOVERNANCE",
    "57": "CHANGE_LOG_AND_TRENDLINES",
    "58": "HYPOTHESIS_REGISTER",
    "59": "EVIDENCE_AND_CONFIDENCE_LEDGER",
    "60": "UNKNOWNS_AND_OPEN_QUESTIONS",
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
    normalized.setdefault("version", 1)
    normalized.setdefault("segment_names", SEGMENT_NAMES)
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


def make_entry(
    *,
    text: str,
    confidence: Confidence,
    action: Literal["add", "reinforce", "revise"],
    summary_id: int,
    tier: str,
    reason: str,
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
        "supersedes": supersedes,
    }


__all__ = [
    "SEGMENT_NAMES",
    "CONFIDENCE_ORDER",
    "ensure_card_shape",
    "new_base_card",
    "upgrade_confidence",
    "make_entry",
]
