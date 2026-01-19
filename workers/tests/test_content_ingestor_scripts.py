import os
import unittest
import uuid

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
os.environ.setdefault("RUNPOD_MODEL_NAME", "test-model")

from workers.content_ingestor.main import (  # noqa: E402
    SCRIPT_SHOOT_ID_NAMESPACE,
    _extract_storage_path,
    _parse_script_assignment,
)


class ContentIngestorScriptsTests(unittest.TestCase):
    def test_extract_storage_path_prefers_storage_path(self):
        row = {
            "storage_path": " @alice/scripts/script_1/core/video.mp4 ",
            "url_main": "https://example.supabase.co/storage/v1/object/public/Content/@alice/scripts/script_1/core/video.mp4",
        }
        self.assertEqual("@alice/scripts/script_1/core/video.mp4", _extract_storage_path(row))

    def test_extract_storage_path_parses_public_url(self):
        row = {
            "url_main": "https://takibtvdaivmaavikqpi.supabase.co/storage/v1/object/public/Content/@alice/scripts/script_1/core/video.mp4"
        }
        self.assertEqual("@alice/scripts/script_1/core/video.mp4", _extract_storage_path(row))

    def test_parse_script_assignment_core(self):
        row = {"creator_id": 4, "storage_path": "@alice/scripts/script_1/core/video.mp4"}
        assignment = _parse_script_assignment(row)
        self.assertIsNotNone(assignment)
        self.assertFalse(assignment["is_ammo"])
        expected = str(uuid.uuid5(SCRIPT_SHOOT_ID_NAMESPACE, "4:scripts:script_1"))
        self.assertEqual(expected, assignment["shoot_id"])

    def test_parse_script_assignment_ammo(self):
        row = {"creator_id": 4, "storage_path": "@alice/scripts/script_1/ammo/video.mp4"}
        assignment = _parse_script_assignment(row)
        self.assertIsNotNone(assignment)
        self.assertTrue(assignment["is_ammo"])

    def test_parse_script_assignment_global_ammo_returns_none(self):
        row = {"creator_id": 4, "storage_path": "@alice/ammo/video.mp4"}
        self.assertIsNone(_parse_script_assignment(row))


if __name__ == "__main__":
    unittest.main()

