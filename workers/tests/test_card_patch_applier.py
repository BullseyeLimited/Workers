import sys
import types
import unittest

# Provide a lightweight Supabase stub so pure functions can be imported without the real client.
if "supabase" not in sys.modules:
    class _ClientOptions:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class _StubClient:
        def table(self, *args, **kwargs):
            return self

        def select(self, *args, **kwargs):
            return self

        def eq(self, *args, **kwargs):
            return self

        def order(self, *args, **kwargs):
            return self

        def limit(self, *args, **kwargs):
            return self

        def single(self):
            return self

        def execute(self):
            return types.SimpleNamespace(data=[])

    sys.modules["supabase"] = types.SimpleNamespace(
        create_client=lambda *args, **kwargs: _StubClient(),
        ClientOptions=_ClientOptions,
    )

from postgrest.exceptions import APIError

from workers.card_patch_applier.main import _api_error_code, apply_patches_to_card, parse_patch_output
from workers.lib.cards import ensure_card_shape, filter_card_by_tier
from workers.lib.time_tier import TimeTier


class ParsePatchOutputTests(unittest.TestCase):
    def test_parses_blocks_and_summary(self):
        raw = """ADD SEGMENT_1_CORE_MOTIVATIONS
TEXT: First insight [tentative]
CONFIDENCE: tentative
REASON: evidence A

REINFORCE SEGMENT_2_TRUST_AND_SAFETY
TEXT: Continues behavior [possible]
CONFIDENCE: possible
REASON: evidence B

Summary starts here.
More summary lines."""

        patches, summary = parse_patch_output(raw)

        self.assertEqual(2, len(patches))
        self.assertEqual("1", patches[0]["segment_id"])
        self.assertEqual("CORE_MOTIVATIONS", patches[0]["segment_label"])
        self.assertEqual("reinforce", patches[1]["action"])
        self.assertTrue(summary.startswith("Summary starts here."))

    def test_parser_handles_crlf_and_missing_blank_line(self):
        raw = "ADD SEGMENT_3_MONETIZATION\r\nTEXT: Willing to tip [likely]\r\nCONFIDENCE: likely\r\nREASON: mentioned tipping\r\nSummary with no blank line."
        patches, summary = parse_patch_output(raw)

        self.assertEqual(1, len(patches))
        self.assertEqual("3", patches[0]["segment_id"])
        self.assertEqual("Summary with no blank line.", summary)

    def test_parser_requires_summary(self):
        raw = """ADD SEGMENT_4_BOUNDARIES
TEXT: Cautious about oversharing [possible]
CONFIDENCE: possible
REASON: pulled back when asked"""

        with self.assertRaises(ValueError):
            parse_patch_output(raw)


class ApiErrorCodeTests(unittest.TestCase):
    def test_extracts_code_from_postgrest_apierror(self):
        exc = APIError(
            {
                "message": "new row for relation violates check constraint",
                "code": "23514",
                "hint": None,
                "details": None,
            }
        )
        self.assertEqual("23514", _api_error_code(exc))


class CardVisibilityTests(unittest.TestCase):
    def test_filters_out_higher_tier_entries(self):
        card = ensure_card_shape(
            {
                "segments": {
                    "1": [
                        {
                            "id": "a1",
                            "text": "Episode fact [tentative] [EPISODE]",
                            "origin_tier": "EPISODE",
                        },
                        {
                            "id": "b1",
                            "text": "Season fact [possible] [SEASON]",
                            "origin_tier": "SEASON",
                        },
                    ]
                }
            }
        )

        filtered = filter_card_by_tier(card, TimeTier.EPISODE)
        self.assertEqual(1, len(filtered["segments"]["1"]))
        self.assertEqual("a1", filtered["segments"]["1"][0]["id"])


class MutationLogicTests(unittest.TestCase):
    def test_provenance_and_confidence_bump(self):
        card = ensure_card_shape({})
        segment_id = "1"

        add_patch = [
            {
                "action": "add",
                "segment_id": segment_id,
                "segment_label": "CORE",
                "segment_full_label": "1_CORE",
                "text": "Enjoys praise [tentative]",
                "confidence": "tentative",
                "reason": "turn 1",
            }
        ]
        card = apply_patches_to_card(card, summary_id=1, tier="episode", patches=add_patch)
        entry = card["segments"][segment_id][0]

        self.assertEqual("tentative", entry["confidence"])
        self.assertIn("[EPISODE]", entry["text"])
        self.assertEqual(entry.get("origin_tier"), "EPISODE")

        reinforce_patch = [
            {
                "action": "reinforce",
                "segment_id": segment_id,
                "segment_label": "CORE",
                "segment_full_label": "1_CORE",
                "text": "Enjoys praise [confident]",
                "confidence": "confident",
                "reason": "turn 5",
            }
        ]
        card = apply_patches_to_card(
            card, summary_id=2, tier="episode", patches=reinforce_patch
        )
        self.assertEqual(2, len(card["segments"][segment_id]))
        # Episode-tier workers are append-only; REINFORCE becomes a competing entry.
        entry = card["segments"][segment_id][1]

        self.assertEqual("confident", entry["confidence"])
        self.assertEqual(1, entry["text"].count("[EPISODE]"))
        self.assertIn("[confident]", entry["text"])

        revise_patch = [
            {
                "action": "revise",
                "segment_id": segment_id,
                "segment_label": "CORE",
                "segment_full_label": "1_CORE",
                "text": "Responds very well to warm praise [likely]",
                "confidence": "likely",
                "reason": "turn 9",
            }
        ]
        card = apply_patches_to_card(card, summary_id=3, tier="episode", patches=revise_patch)
        self.assertEqual(3, len(card["segments"][segment_id]))
        entry = card["segments"][segment_id][2]

        self.assertEqual("likely", entry["confidence"])
        self.assertEqual(1, entry["text"].count("[EPISODE]"))
        self.assertIn("[likely]", entry["text"])


if __name__ == "__main__":
    unittest.main()
