import unittest


from workers.lib.prompt_builder import (
    _format_identity_card,
    _format_identity_card_text,
)


class PromptBuilderIdentityCardFormatTests(unittest.TestCase):
    def test_format_identity_card_text_strips_empty_categories(self):
        card_text = "\n".join(
            [
                "LONG_TERM",
                "family:",
                "profile:",
                "Name: Dusan.",
                "work:",
                "",
                "SHORT_TERM",
                "location:",
                "In the US.",
                "activity:",
            ]
        )
        formatted = _format_identity_card_text(card_text)
        self.assertIn("LONG_TERM", formatted)
        self.assertIn("profile:", formatted)
        self.assertIn("Name: Dusan.", formatted)
        self.assertIn("SHORT_TERM", formatted)
        self.assertIn("location:", formatted)
        self.assertIn("In the US.", formatted)
        self.assertNotIn("family:", formatted)
        self.assertNotIn("work:", formatted)
        self.assertNotIn("activity:", formatted)

    def test_format_identity_card_returns_empty_placeholder_for_blank_plain_text(self):
        empty_template = "\n".join(
            [
                "LONG_TERM",
                "family:",
                "profile:",
                "",
                "SHORT_TERM",
                "location:",
            ]
        )
        formatted = _format_identity_card({}, card_text=empty_template)
        self.assertEqual("Identity card: empty", formatted)

    def test_format_identity_card_json_omits_null_spam(self):
        json_card = {
            "long_term": {
                "family": {"parents": {"notes": None}},
                "work": {"titles": {"notes": None}},
            },
            "short_term": {"location": {"current": {"notes": None}}},
        }
        formatted = _format_identity_card(json_card)
        self.assertEqual("Identity card: empty", formatted)
        self.assertNotIn("None", formatted)

    def test_format_identity_card_json_includes_notes(self):
        json_card = {
            "long_term": {"family": {"parents": {"notes": "Parents live in Serbia."}}}
        }
        formatted = _format_identity_card(json_card)
        self.assertIn("long_term.family.parents: Parents live in Serbia.", formatted)


if __name__ == "__main__":
    unittest.main()

