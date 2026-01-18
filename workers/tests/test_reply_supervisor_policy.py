import os
import unittest
from datetime import datetime, timezone

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.reply_supervisor.main import _middle_window_decision, _parse_ts


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

    def test_middle_window_flags_emergency(self):
        self.assertEqual("ABORT_REDO", _middle_window_decision("my mother died"))
        self.assertEqual("ABORT_REDO", _middle_window_decision("I am in the hospital"))

    def test_middle_window_defaults_defer(self):
        self.assertEqual("DEFER", _middle_window_decision("lol ok"))


if __name__ == "__main__":
    unittest.main()

