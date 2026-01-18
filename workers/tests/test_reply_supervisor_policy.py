import os
import unittest
from datetime import datetime, timezone

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.reply_supervisor.main import _extract_json_object, _parse_ts


class ReplySupervisorPolicyTests(unittest.TestCase):
    def test_parse_ts_accepts_datetime(self):
        now = datetime.now(timezone.utc)
        self.assertEqual(now, _parse_ts(now))

    def test_parse_ts_accepts_iso_z(self):
        parsed = _parse_ts("2025-01-01T00:00:00Z")
        self.assertIsNotNone(parsed)
        self.assertEqual(timezone.utc, parsed.tzinfo)

    def test_parse_ts_rejects_empty(self):
        self.assertIsNone(_parse_ts(None))
        self.assertIsNone(_parse_ts(""))
        self.assertIsNone(_parse_ts("   "))

    def test_extract_json_object_strict(self):
        self.assertEqual({"a": 1}, _extract_json_object('{"a": 1}'))

    def test_extract_json_object_embedded(self):
        blob = "sure here you go\n{\"decision\":\"DEFER\",\"reason\":\"ok\"}\nthanks"
        self.assertEqual(
            {"decision": "DEFER", "reason": "ok"},
            _extract_json_object(blob),
        )


if __name__ == "__main__":
    unittest.main()
