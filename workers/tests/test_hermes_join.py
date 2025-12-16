import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.hermes_join.main import completion_state, compute_requirements


class HermesJoinTests(unittest.TestCase):
    def test_requirements_parsing(self):
        hermes = {"final_verdict_search": "YES", "final_verdict_kairos": "FULL"}
        need_kairos, need_web = compute_requirements(hermes)
        self.assertTrue(need_kairos)
        self.assertTrue(need_web)

    def test_completion_waits_for_needed_steps(self):
        hermes = {"final_verdict_search": "YES", "final_verdict_kairos": "FULL"}
        _, _, done = completion_state(hermes, "pending", None)
        self.assertFalse(done)

    def test_completion_when_skipped(self):
        hermes = {"final_verdict_search": "NO", "final_verdict_kairos": "SKIP"}
        need_kairos, need_web, done = completion_state(hermes, None, None)
        self.assertFalse(need_kairos)
        self.assertFalse(need_web)
        self.assertTrue(done)

    def test_completion_with_failures(self):
        hermes = {"final_verdict_search": "YES", "final_verdict_kairos": "FULL"}
        _, _, done = completion_state(hermes, "failed", "failed")
        self.assertTrue(done)


if __name__ == "__main__":
    unittest.main()
