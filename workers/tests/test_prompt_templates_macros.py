from __future__ import annotations

import re
from pathlib import Path


_MACRO_RE = re.compile(r"\{([^{}\n]+)\}")


def _extract_macros(text: str) -> set[str]:
    return {match.strip() for match in _MACRO_RE.findall(text or "") if match.strip()}


def test_abstract_prompt_templates_reference_expected_macros() -> None:
    prompts_dir = Path(__file__).resolve().parents[2] / "prompts"
    expectations: dict[str, set[str]] = {
        "chapter_abstract.txt": {
            "RAW_TURNS",
            "FAN_IDENTITY_CARD",
            "FAN_PSYCHIC_CARD",
        },
        "season_abstract.txt": {"RAW_TURNS", "FAN_IDENTITY_CARD", "FAN_PSYCHIC_CARD"},
        "year_abstract.txt": {"RAW_TURNS", "FAN_IDENTITY_CARD", "FAN_PSYCHIC_CARD"},
        "lifetime_abstract.txt": {"RAW_TURNS", "FAN_IDENTITY_CARD", "FAN_PSYCHIC_CARD"},
    }

    for filename, required in expectations.items():
        text = (prompts_dir / filename).read_text(encoding="utf-8")
        macros = _extract_macros(text)
        missing = required - macros
        assert not missing, f"{filename} missing macros: {sorted(missing)}"
