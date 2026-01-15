import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.napoleon.main import parse_content_actions


class NapoleonContentActionsTests(unittest.TestCase):
    def test_returns_none_when_missing_block(self):
        parsed, err = parse_content_actions("no content actions here")
        self.assertIsNone(parsed)
        self.assertIsNone(err)

    def test_parses_empty_block(self):
        parsed, err = parse_content_actions("<CONTENT_ACTIONS>\n\n</CONTENT_ACTIONS>")
        self.assertEqual({}, parsed)
        self.assertIsNone(err)

    def test_parses_header_lines_sends_and_offers(self):
        raw = """SECTION 1: TACTICAL_PLAN_3TURNS
TURN1_DIRECTIVE: hi
SECTION 2: RETHINK_HORIZONS
STATUS: no
<CONTENT_ACTIONS>
SENDS: 255, 256
OFFERS: 300 $9.99, 301 12
</CONTENT_ACTIONS>"""
        parsed, err = parse_content_actions(raw)
        self.assertIsNone(err)
        self.assertEqual([255, 256], parsed.get("sends"))
        self.assertEqual(
            [
                {"content_id": 300, "offered_price": "9.99"},
                {"content_id": 301, "offered_price": "12"},
            ],
            parsed.get("offers"),
        )

    def test_header_lines_none_is_empty(self):
        raw = """<CONTENT_ACTIONS>
SENDS: NONE
OFFERS: NONE
</CONTENT_ACTIONS>"""
        parsed, err = parse_content_actions(raw)
        self.assertIsNone(err)
        self.assertEqual({}, parsed)

    def test_parses_json_object_compat(self):
        raw = """<CONTENT_ACTIONS>
{"sends":[255],"offers":[{"content_id":300,"offered_price":9.99}]}
</CONTENT_ACTIONS>"""
        parsed, err = parse_content_actions(raw)
        self.assertIsNone(err)
        self.assertEqual([255], parsed.get("sends"))
        self.assertEqual([{"content_id": 300, "offered_price": 9.99}], parsed.get("offers"))

    def test_parses_json_list_compat(self):
        raw = """<CONTENT_ACTIONS>
[255, 256]
</CONTENT_ACTIONS>"""
        parsed, err = parse_content_actions(raw)
        self.assertIsNone(err)
        self.assertEqual({"sends": [255, 256]}, parsed)


if __name__ == "__main__":
    unittest.main()

