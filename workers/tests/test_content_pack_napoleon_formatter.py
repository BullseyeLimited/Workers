import unittest

from workers.lib.content_pack import format_content_pack_for_napoleon


class TestContentPackNapoleonFormatter(unittest.TestCase):
    def test_zoom1_has_headers_and_no_created_at(self):
        pack = {
            "zoom": 1,
            "script": {
                "title": "Gym Tease",
                "summary": "A playful gym arc",
                "created_at": "2026-01-01T00:00:00Z",
            },
            "script_items": [
                "id 10, photo, tease, setup pic, stage: setup, sequence: 1",
                "id 11, photo, tease, aftercare pic, stage: after, sequence: 2",
                "id 12, photo, tease, build pic, stage: build, sequence: 1",
            ],
            "shoot_extras": [
                "id 20, photo, tease, extra 1",
            ],
            "global_focus": {
                "items": [
                    "id 30, photo, sfw, scriptless 1",
                    "id 31, voice note, tease, scriptless 2",
                ]
            },
        }

        text = format_content_pack_for_napoleon(pack)
        self.assertIn("CONTENT PACK (ZOOM 1)", text)
        self.assertIn("SCRIPT OVERVIEW", text)
        self.assertIn("SCRIPT CONTENT", text)
        self.assertIn("SCRIPTLESS CONTENT", text)
        self.assertIn("Content 1:", text)
        self.assertNotIn("created_at", text)
        self.assertNotIn("2026-01-01", text)

        # Stage order is oldest-first: SETUP before BUILD-UP before AFTERCARE.
        lines = text.splitlines()
        setup_idx = lines.index("SETUP")
        build_idx = lines.index("BUILD-UP")
        after_idx = lines.index("AFTERCARE")
        self.assertTrue(setup_idx < build_idx < after_idx)
