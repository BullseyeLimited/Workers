import unittest


from workers.lib.prompt_builder import _turn_rollover_cutoff


class PromptBuilderTurnRolloverTests(unittest.TestCase):
    def test_turn_rollover_cutoff_math(self):
        # No boundary: no cutoff.
        self.assertEqual(0, _turn_rollover_cutoff(None))

        # Before we have 40 prior turns, cutoff stays 0 (even if an Episode summary exists).
        self.assertEqual(0, _turn_rollover_cutoff(1))   # prior=0
        self.assertEqual(0, _turn_rollover_cutoff(20))  # prior=19
        self.assertEqual(0, _turn_rollover_cutoff(21))  # prior=20
        self.assertEqual(0, _turn_rollover_cutoff(40))  # prior=39

        # At 40 prior turns (boundary=41), consume the oldest 20.
        self.assertEqual(20, _turn_rollover_cutoff(41))  # prior=40
        self.assertEqual(20, _turn_rollover_cutoff(42))  # prior=41
        self.assertEqual(20, _turn_rollover_cutoff(60))  # prior=59

        # At 60 prior turns (boundary=61), consume the oldest 40.
        self.assertEqual(40, _turn_rollover_cutoff(61))  # prior=60

    def test_rollover_keeps_raw_between_20_and_39_once_available(self):
        # Once we've rolled at least once (>=40 prior turns), we should never
        # cut so far that fewer than 20 raw turns remain.
        for boundary in range(41, 200):
            prior = boundary - 1
            cutoff = _turn_rollover_cutoff(boundary)
            remaining = prior - cutoff
            self.assertGreaterEqual(remaining, 20)
            self.assertLessEqual(remaining, 39)


if __name__ == "__main__":
    unittest.main()

