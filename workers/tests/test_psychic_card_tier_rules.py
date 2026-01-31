import importlib
import os
import unittest


from workers.lib.cards import compact_psychic_card, new_base_card


class PsychicCardTierRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # card_patch_applier initializes a Supabase client at import time; give it
        # harmless defaults so pure helpers can be imported in unit tests.
        os.environ.setdefault("SUPABASE_URL", "http://localhost")
        os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test")
        cls.card_patch = importlib.import_module("workers.card_patch_applier.main")

    def test_episode_cannot_edit_season_entry_adds_dispute(self):
        apply_patches_to_card = self.card_patch.apply_patches_to_card

        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "season-1",
                "text": "Likes slow-burn intimacy [likely] [SEASON]",
                "confidence": "likely",
                "origin_tier": "SEASON",
                "tier": "season",
                "created_at": "2020-01-01T00:00:00+00:00",
            }
        ]

        patches = [
            {
                "action": "revise",
                "segment_id": "7",
                "text": "Actually wants it fast",
                "confidence": "possible",
                "reason": "test",
                "target_id": "season-1",
            }
        ]

        updated = apply_patches_to_card(card, summary_id=1, tier="episode", patches=patches)
        entries = updated["segments"]["7"]

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["id"], "season-1")
        self.assertIn("[DISPUTES:SEASON]", entries[1]["text"])
        self.assertIn("[EPISODE]", entries[1]["text"])
        self.assertEqual(entries[1]["origin_tier"], "EPISODE")

    def test_episode_revise_mutates_note_in_place(self):
        apply_patches_to_card = self.card_patch.apply_patches_to_card

        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "episode-1",
                "text": "Likes teasing [possible] [EPISODE]",
                "confidence": "possible",
                "origin_tier": "EPISODE",
                "tier": "episode",
                "created_at": "2020-01-01T00:00:00+00:00",
            }
        ]

        patches = [
            {
                "action": "revise",
                "segment_id": "7",
                "text": "Likes teasing a lot",
                "confidence": "likely",
                "reason": "test",
                "target_id": "episode-1",
            }
        ]

        updated = apply_patches_to_card(card, summary_id=2, tier="episode", patches=patches)
        entries = updated["segments"]["7"]

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["id"], "episode-1")
        self.assertIn("Likes teasing a lot", entries[0]["text"])
        self.assertIn("[EPISODE]", entries[0]["text"])
        self.assertEqual(entries[0]["origin_tier"], "EPISODE")

    def test_season_can_promote_episode_entry_via_supersedes(self):
        apply_patches_to_card = self.card_patch.apply_patches_to_card

        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "episode-1",
                "text": "Likes teasing [possible] [EPISODE]",
                "confidence": "possible",
                "origin_tier": "EPISODE",
                "tier": "episode",
                "created_at": "2020-01-01T00:00:00+00:00",
            }
        ]

        patches = [
            {
                "action": "revise",
                "segment_id": "7",
                "text": "Likes teasing, but needs reassurance",
                "confidence": "likely",
                "reason": "test",
                "target_id": "episode-1",
            }
        ]

        updated = apply_patches_to_card(card, summary_id=3, tier="season", patches=patches)
        entries = updated["segments"]["7"]

        self.assertEqual(len(entries), 2)
        old_entry = next(e for e in entries if e.get("id") == "episode-1")
        new_entry = next(e for e in entries if e.get("origin_tier") == "SEASON")
        self.assertEqual(new_entry.get("supersedes"), "episode-1")
        self.assertEqual(old_entry.get("superseded_by"), new_entry.get("id"))
        self.assertIn("[SEASON]", new_entry.get("text") or "")

    def test_season_cannot_edit_year_entry_adds_dispute(self):
        apply_patches_to_card = self.card_patch.apply_patches_to_card

        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "year-1",
                "text": "Stable preference for slow-burn [confident] [YEAR]",
                "confidence": "confident",
                "origin_tier": "YEAR",
                "tier": "year",
                "created_at": "2020-01-01T00:00:00+00:00",
            }
        ]

        patches = [
            {
                "action": "revise",
                "segment_id": "7",
                "text": "Actually prefers it fast now",
                "confidence": "possible",
                "reason": "test",
                "target_id": "year-1",
            }
        ]

        updated = apply_patches_to_card(card, summary_id=4, tier="season", patches=patches)
        entries = updated["segments"]["7"]

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["id"], "year-1")
        self.assertIn("[DISPUTES:YEAR]", entries[1]["text"])
        self.assertIn("[SEASON]", entries[1]["text"])
        self.assertEqual(entries[1]["origin_tier"], "SEASON")

    def test_episode_delete_is_ignored(self):
        apply_patches_to_card = self.card_patch.apply_patches_to_card

        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "episode-1",
                "text": "Likes teasing [possible] [EPISODE]",
                "confidence": "possible",
                "origin_tier": "EPISODE",
                "tier": "episode",
                "created_at": "2020-01-01T00:00:00+00:00",
            }
        ]

        patches = [
            {
                "action": "delete",
                "segment_id": "7",
                "target_id": "episode-1",
                "reason": "test",
            }
        ]

        updated = apply_patches_to_card(card, summary_id=5, tier="episode", patches=patches)
        entries = updated["segments"]["7"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["id"], "episode-1")
        self.assertFalse(entries[0].get("superseded_by"))

    def test_season_delete_marks_entry_superseded(self):
        apply_patches_to_card = self.card_patch.apply_patches_to_card

        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "episode-1",
                "text": "Likes teasing [possible] [EPISODE]",
                "confidence": "possible",
                "origin_tier": "EPISODE",
                "tier": "episode",
                "created_at": "2020-01-01T00:00:00+00:00",
            }
        ]

        patches = [
            {
                "action": "delete",
                "segment_id": "7",
                "target_id": "episode-1",
                "reason": "test",
            }
        ]

        updated = apply_patches_to_card(card, summary_id=6, tier="season", patches=patches)
        entry = updated["segments"]["7"][0]
        self.assertTrue(str(entry.get("superseded_by") or "").startswith("deleted:"))
        self.assertEqual(entry.get("deleted_by_tier"), "SEASON")

    def test_compact_psychic_card_limits_to_two_tiers(self):
        card = new_base_card()
        card["segments"]["7"] = [
            {
                "id": "episode-1",
                "text": "Episode note [possible] [EPISODE]",
                "confidence": "possible",
                "origin_tier": "EPISODE",
                "created_at": "2020-01-01T00:00:00+00:00",
            },
            {
                "id": "season-1",
                "text": "Season note [likely] [SEASON]",
                "confidence": "likely",
                "origin_tier": "SEASON",
                "created_at": "2020-01-02T00:00:00+00:00",
            },
            {
                "id": "year-1",
                "text": "Year note [confident] [YEAR]",
                "confidence": "confident",
                "origin_tier": "YEAR",
                "created_at": "2020-01-03T00:00:00+00:00",
            },
        ]

        compact = compact_psychic_card(
            card,
            max_entries_per_segment=2,
            drop_superseded=True,
            entry_fields=("id", "origin_tier"),
        )
        self.assertIsInstance(compact, dict)
        segs = compact.get("segments") or {}
        self.assertIn("07.ATTACHMENT_INTIMACY_STYLE", segs)
        picked = segs["07.ATTACHMENT_INTIMACY_STYLE"]
        self.assertEqual(len(picked), 2)
        # Highest tier (YEAR) + second-highest (SEASON)
        self.assertEqual({e.get("origin_tier") for e in picked}, {"YEAR", "SEASON"})


if __name__ == "__main__":
    unittest.main()
