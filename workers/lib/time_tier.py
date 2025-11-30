"""Time hierarchy utilities shared by abstract workers."""

from __future__ import annotations

from enum import IntEnum
from typing import Any


class TimeTier(IntEnum):
    TURN = 0
    EPISODE = 1
    CHAPTER = 2
    SEASON = 3
    YEAR = 4
    LIFETIME = 5


_NAME_LOOKUP = {
    "turn": TimeTier.TURN,
    "episode": TimeTier.EPISODE,
    "chapter": TimeTier.CHAPTER,
    "season": TimeTier.SEASON,
    "year": TimeTier.YEAR,
    "lifetime": TimeTier.LIFETIME,
}


def parse_tier(value: Any) -> TimeTier:
    """Normalize a tier-like input into a TimeTier enum."""

    if isinstance(value, TimeTier):
        return value
    if isinstance(value, int):
        try:
            return TimeTier(value)
        except ValueError as exc:  # noqa: B904
            raise ValueError(f"invalid tier value {value}") from exc

    key = (str(value or "")).strip().lower()
    if key in _NAME_LOOKUP:
        return _NAME_LOOKUP[key]

    raise ValueError(f"unknown tier '{value}'")


def tier_tag(tier: TimeTier) -> str:
    """Return the bracketed provenance tag for a tier."""

    return f"[{tier.name}]"


__all__ = ["TimeTier", "parse_tier", "tier_tag"]
