import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.hermes.main import parse_hermes_output


class HermesParserTests(unittest.TestCase):
    def test_parses_standard_output(self):
        raw = """FINAL_VERDICT_SEARCH: YES
FINAL_VERDICT_KAIROS: lite
JOIN_REQUIREMENTS: both
<WEB_RESEARCH_BRIEF>Check company news</WEB_RESEARCH_BRIEF>"""
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(err)
        self.assertEqual("YES", parsed["final_verdict_search"])
        self.assertEqual("LITE", parsed["final_verdict_kairos"])
        self.assertEqual("BOTH", parsed["join_requirements"])
        self.assertEqual("Check company news", parsed["web_research_brief"])

    def test_parses_with_noise_and_spacing(self):
        raw = """
        some intro text
        final_verdict_search   :   no
        notes...
        FINAL_VERDICT_KAIROS:SKIP

        join_requirements : web_only
        <WEB_RESEARCH_BRIEF>
        NONE
        </WEB_RESEARCH_BRIEF>
        """
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(err)
        self.assertEqual("NO", parsed["final_verdict_search"])
        self.assertEqual("SKIP", parsed["final_verdict_kairos"])
        self.assertEqual("WEB_ONLY", parsed["join_requirements"])
        self.assertEqual("NONE", parsed["web_research_brief"])

    def test_missing_headers_fail(self):
        raw = "<WEB_RESEARCH_BRIEF>something</WEB_RESEARCH_BRIEF>"
        parsed, err = parse_hermes_output(raw)
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)


if __name__ == "__main__":
    unittest.main()
