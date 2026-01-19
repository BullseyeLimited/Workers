import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
os.environ.setdefault("RUNPOD_MODEL_NAME", "test-model")

from workers.content_script_finalizer.main import _parse_header_output  # noqa: E402


class ScriptFinalizerParserTests(unittest.TestCase):
    def test_parse_header_output(self):
        raw = """SCRIPT_UPDATE
TITLE
Cozy check-in -> bedroom tease
SUMMARY
She starts with a playful greeting. She moves closer and smiles. She teases the camera.
She slowly builds tension. She leans in and gives a flirty look. She peaks with a reveal.
She ends with a soft goodbye.
TIME_OF_DAY
night
LOCATION_PRIMARY
bedroom
OUTFIT_CATEGORY
lingerie
FOCUS_TAGS
bedroom, flirty, tease
ITEM_UPDATES
ITEM
ID
101
STAGE
setup
SEQUENCE_POSITION
1
ITEM
ID
102
STAGE
tease
SEQUENCE_POSITION
2
END
"""
        payload = _parse_header_output(raw)
        self.assertIsNotNone(payload)
        self.assertEqual("Cozy check-in -> bedroom tease", payload["content_scripts_update"]["title"])
        self.assertEqual("night", payload["content_scripts_update"]["time_of_day"])
        items = payload["content_items_updates"]
        self.assertEqual(2, len(items))
        self.assertEqual("101", items[0]["id"])
        self.assertEqual("setup", items[0]["stage"])


if __name__ == "__main__":
    unittest.main()
