import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.iris_join.main import completion_state, compute_requirements


class HermesJoinTests(unittest.TestCase):
    def test_requirements_parsing(self):
        need_kairos, need_web = compute_requirements(verdict_search="YES", kairos_mode="FULL")
        self.assertTrue(need_kairos)
        self.assertTrue(need_web)

    def test_completion_waits_for_needed_steps(self):
        _, _, done = completion_state(need_kairos=True, need_web=True, kairos_status="pending", web_status=None)
        self.assertFalse(done)

    def test_completion_when_skipped(self):
        need_kairos, need_web, done = completion_state(need_kairos=False, need_web=False, kairos_status=None, web_status=None)
        self.assertFalse(need_kairos)
        self.assertFalse(need_web)
        self.assertTrue(done)

    def test_completion_with_failures(self):
        _, _, done = completion_state(need_kairos=True, need_web=True, kairos_status="failed", web_status="failed")
        self.assertTrue(done)


if __name__ == "__main__":
    unittest.main()
