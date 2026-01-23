import os
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.napoleon.main import (  # noqa: E402
    parse_schedule_action,
    _apply_schedule_action_to_plan,
    _flatten_plan_segments,
    _pick_schedule_target,
)


class NapoleonScheduleActionTests(unittest.TestCase):
    def test_parse_returns_none_when_missing_block(self):
        parsed, err = parse_schedule_action("no schedule action here")
        self.assertIsNone(parsed)
        self.assertIsNone(err)

    def test_parse_cancel_with_multiline_reasoning(self):
        raw = """SECTION 1: TACTICAL_PLAN_3TURNS
TURN1_DIRECTIVE: hi
SECTION 2: RETHINK_HORIZONS
STATUS: no
<SCHEDULE_ACTION>
ACTION: CANCEL
REASONING: This is a high-value moment.
We should stay present.
It will pay off.
Do it now.
</SCHEDULE_ACTION>"""
        parsed, err = parse_schedule_action(raw)
        self.assertIsNone(err)
        self.assertEqual("CANCEL", parsed.get("action"))
        self.assertIn("high-value moment", parsed.get("reasoning", ""))
        self.assertIsNone(parsed.get("new_name"))

    def test_parse_edit_requires_new_name(self):
        raw = """<SCHEDULE_ACTION>
ACTION: EDIT
REASONING: Good idea.
</SCHEDULE_ACTION>"""
        parsed, err = parse_schedule_action(raw)
        self.assertIsNone(parsed)
        self.assertEqual("edit_missing_new_name", err)

    def test_apply_cancel_splits_and_cancels_contiguous_event(self):
        plan_json = {
            "hours": [
                {
                    "hour_start_local": "2026-01-23T10:00",
                    "hour_end_local": "2026-01-23T11:00",
                    "segments": [
                        {
                            "start_local": "2026-01-23T10:00",
                            "end_local": "2026-01-23T11:00",
                            "state": "busy",
                            "label": "Gym",
                        }
                    ],
                },
                {
                    "hour_start_local": "2026-01-23T11:00",
                    "hour_end_local": "2026-01-23T12:00",
                    "segments": [
                        {
                            "start_local": "2026-01-23T11:00",
                            "end_local": "2026-01-23T12:00",
                            "state": "busy",
                            "label": "Gym",
                        }
                    ],
                },
            ]
        }
        now_local = datetime.fromisoformat("2026-01-23T10:30").replace(
            tzinfo=ZoneInfo("UTC")
        )
        entries = _flatten_plan_segments(plan_json, "UTC")
        target = _pick_schedule_target(entries, now_local)
        updated, meta = _apply_schedule_action_to_plan(
            plan_json=plan_json,
            tz_name="UTC",
            now_local=now_local,
            target=target,
            action="CANCEL",
            reasoning="Important fan moment.",
            new_name=None,
        )
        self.assertIsInstance(updated, dict)
        self.assertEqual("CANCEL", meta.get("action"))
        self.assertEqual("Gym", meta.get("target_old_label"))
        self.assertEqual(2, meta.get("applied_segments"))

        first_hour_segs = plan_json["hours"][0]["segments"]
        self.assertEqual(2, len(first_hour_segs))
        self.assertEqual("2026-01-23T10:30", first_hour_segs[0]["end_local"])
        self.assertEqual("busy", first_hour_segs[0]["state"])
        self.assertEqual("Gym", first_hour_segs[0]["label"])
        self.assertEqual("2026-01-23T10:30", first_hour_segs[1]["start_local"])
        self.assertEqual("available", first_hour_segs[1]["state"])
        self.assertEqual("Available", first_hour_segs[1]["label"])

        second_hour_seg = plan_json["hours"][1]["segments"][0]
        self.assertEqual("available", second_hour_seg["state"])
        self.assertEqual("Available", second_hour_seg["label"])

    def test_apply_edit_renames_contiguous_event(self):
        plan_json = {
            "hours": [
                {
                    "hour_start_local": "2026-01-23T10:00",
                    "hour_end_local": "2026-01-23T11:00",
                    "segments": [
                        {
                            "start_local": "2026-01-23T10:00",
                            "end_local": "2026-01-23T11:00",
                            "state": "busy",
                            "label": "Gym",
                        }
                    ],
                },
                {
                    "hour_start_local": "2026-01-23T11:00",
                    "hour_end_local": "2026-01-23T12:00",
                    "segments": [
                        {
                            "start_local": "2026-01-23T11:00",
                            "end_local": "2026-01-23T12:00",
                            "state": "busy",
                            "label": "Gym",
                        }
                    ],
                },
            ]
        }
        now_local = datetime.fromisoformat("2026-01-23T09:50").replace(
            tzinfo=ZoneInfo("UTC")
        )
        entries = _flatten_plan_segments(plan_json, "UTC")
        target = _pick_schedule_target(entries, now_local)
        updated, meta = _apply_schedule_action_to_plan(
            plan_json=plan_json,
            tz_name="UTC",
            now_local=now_local,
            target=target,
            action="EDIT",
            reasoning="Rename for believability.",
            new_name="Cardio",
        )
        self.assertIsInstance(updated, dict)
        self.assertEqual("EDIT", meta.get("action"))
        self.assertEqual(2, meta.get("applied_segments"))
        self.assertEqual("Cardio", plan_json["hours"][0]["segments"][0]["label"])
        self.assertEqual("Cardio", plan_json["hours"][1]["segments"][0]["label"])


if __name__ == "__main__":
    unittest.main()

