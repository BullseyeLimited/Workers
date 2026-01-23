import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.napoleon.main import parse_napoleon_headers


class NapoleonMomentCompassGateTests(unittest.TestCase):
    def test_parses_moment_compass_gate_yes(self):
        raw = """SECTION 1: TACTICAL_PLAN_3TURNS
TURN1_DIRECTIVE: do x
SECTION 2: RETHINK_HORIZONS
STATUS: no
SECTION 3: MOMENT_COMPASS_TO_COMPOSER
YES
END
"""
        parsed, err = parse_napoleon_headers(raw)
        self.assertIsNone(err)
        self.assertEqual("yes", parsed.get("MOMENT_COMPASS_TO_COMPOSER"))

    def test_defaults_moment_compass_gate_no_when_missing(self):
        raw = """SECTION 1: TACTICAL_PLAN_3TURNS
TURN1_DIRECTIVE: do x
SECTION 2: RETHINK_HORIZONS
STATUS: no
END
"""
        parsed, err = parse_napoleon_headers(raw)
        self.assertIsNone(err)
        self.assertEqual("no", parsed.get("MOMENT_COMPASS_TO_COMPOSER"))


if __name__ == "__main__":
    unittest.main()

