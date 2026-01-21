import unittest


from workers.lib.cards import compact_psychic_card, make_entry, new_base_card
from workers.lib.prompt_builder import _format_psychic_card


class PsychicCardCompactionTests(unittest.TestCase):
    def test_compact_psychic_card_returns_none_for_all_empty_segments(self):
        card = new_base_card()
        self.assertIsNone(compact_psychic_card(card))

    def test_compact_psychic_card_keeps_only_nonempty_segments_by_name(self):
        card = new_base_card()
        card["segments"]["7"] = [
            make_entry(
                text="Wants slow-burn intimacy",
                confidence="likely",
                action="add",
                summary_id=1,
                tier="turn",
                reason="test",
            )
        ]
        compact = compact_psychic_card(card)
        self.assertIsInstance(compact, dict)
        segments = compact.get("segments")
        self.assertIsInstance(segments, dict)
        self.assertEqual(set(segments.keys()), {"07.ATTACHMENT_INTIMACY_STYLE"})
        self.assertEqual(
            segments["07.ATTACHMENT_INTIMACY_STYLE"][0].get("text"),
            "Wants slow-burn intimacy",
        )

    def test_compact_psychic_card_drops_blank_text_entries(self):
        card = new_base_card()
        card["segments"]["7"] = [{"text": "   "}, {"text": "Keeps it flirty"}]
        compact = compact_psychic_card(card)
        self.assertIsInstance(compact, dict)
        segments = compact.get("segments") or {}
        self.assertEqual(
            segments["07.ATTACHMENT_INTIMACY_STYLE"][0].get("text"),
            "Keeps it flirty",
        )

    def test_format_psychic_card_returns_blank_when_empty(self):
        card = new_base_card()
        self.assertEqual("", _format_psychic_card(card))


if __name__ == "__main__":
    unittest.main()

