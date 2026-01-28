import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.iris.main import parse_iris_output


class IrisParserTests(unittest.TestCase):
    def test_parses_header_modes_and_reasons(self):
        raw = """HERMES_MODE: LITE
HERMES_REASON: normal convo
KAIROS_MODE: FULL
KAIROS_REASON: emotionally loaded
NAPOLEON_MODE: SKIP
NAPOLEON_REASON: trivial reply"""
        parsed, err = parse_iris_output(raw)
        self.assertIsNone(err)
        self.assertEqual("lite", parsed["hermes"])
        self.assertEqual("normal convo", parsed["hermes_reason"])
        self.assertEqual("full", parsed["kairos"])
        self.assertEqual("emotionally loaded", parsed["kairos_reason"])
        self.assertEqual("skip", parsed["napoleon"])
        self.assertEqual("trivial reply", parsed["napoleon_reason"])

    def test_rejects_json(self):
        raw = '{"hermes":"lite","kairos":"skip","napoleon":"full"}'
        parsed, err = parse_iris_output(raw)
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)

    def test_parses_regex_fallback(self):
        raw = """
        some text
        HERMES_MODE: FULL
        HERMES_REASON: complex context
        KAIROS: LITE
        KAIROS_REASON: normal vibe
        NAPOLEON_MODE: SKIP
        NAPOLEON_REASON: simple reply
        """
        parsed, err = parse_iris_output(raw)
        self.assertIsNone(err)
        self.assertEqual("full", parsed["hermes"])
        self.assertEqual("lite", parsed["kairos"])
        self.assertEqual("skip", parsed["napoleon"])

    def test_tolerates_decoration_and_separators(self):
        raw = """HERMES_MODE = **LITE**
HERMES_REASON - fast + low stakes
KAIROS_MODE: SKIP.
KAIROS_REASON: empty turn
NAPOLEON_MODE: FULL (needs planning)
NAPOLEON_REASON: multi-part request"""
        parsed, err = parse_iris_output(raw)
        self.assertIsNone(err)
        self.assertEqual("lite", parsed["hermes"])
        self.assertEqual("skip", parsed["kairos"])
        self.assertEqual("full", parsed["napoleon"])

    def test_missing_fields(self):
        raw = '{"hermes":"lite"}'
        parsed, err = parse_iris_output(raw)
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)


if __name__ == "__main__":
    unittest.main()
