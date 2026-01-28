import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.hermes.main import parse_hermes_output


class HermesParserTests(unittest.TestCase):
    def test_parses_standard_output(self):
        raw = """FINAL_VERDICT_SEARCH: YES
MOMENT_LOCATION: unknown
IDENTITY_KEYS: creator_profile
<WEB_RESEARCH_BRIEF>Check company news</WEB_RESEARCH_BRIEF>"""
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(err)
        self.assertEqual("YES", parsed["final_verdict_search"])
        self.assertEqual("Check company news", parsed["web_research_brief"])
        self.assertEqual("unknown", parsed["moment_location"])

    def test_parses_with_noise_and_spacing(self):
        raw = """
        some intro text
        final_verdict_search   :   no
        notes...
        moment_location : Bedroom
        identity_keys: none
        <WEB_RESEARCH_BRIEF>
        NONE
        </WEB_RESEARCH_BRIEF>
        """
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(err)
        self.assertEqual("NO", parsed["final_verdict_search"])
        self.assertEqual("bedroom", parsed["moment_location"])
        self.assertEqual("NONE", parsed["web_research_brief"])

    def test_missing_headers_fail(self):
        raw = "<WEB_RESEARCH_BRIEF>something</WEB_RESEARCH_BRIEF>"
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)

    def test_parses_moment_location(self):
        raw = """FINAL_VERDICT_SEARCH: NO
MOMENT_LOCATION: Bedroom
<WEB_RESEARCH_BRIEF>NONE</WEB_RESEARCH_BRIEF>"""
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(err)
        self.assertEqual("bedroom", parsed["moment_location"])


if __name__ == "__main__":
    unittest.main()
