import unittest


from workers.lib.prompt_builder import latest_plan_fields


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    def __init__(self, client, table_name: str):
        self._client = client
        self._table_name = table_name
        self._single = False

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def single(self):
        self._single = True
        return self

    def execute(self):
        data = self._client._tables.get(self._table_name)
        if self._single and isinstance(data, list):
            data = data[0] if data else {}
        return _FakeResponse(data)


class _FakeClient:
    def __init__(self, *, tables: dict):
        self._tables = tables

    def table(self, table_name: str):
        return _FakeQuery(self, table_name)


class PromptBuilderPlannerIntentTests(unittest.TestCase):
    def test_latest_plan_fields_uses_napoleon_snapshot_for_tactical_plan(self):
        client = _FakeClient(
            tables={
                "threads": {
                    "episode_plan": "episode-thread",
                    "chapter_plan": "chapter-thread",
                    "season_plan": "season-thread",
                    "year_plan": "year-thread",
                    "lifetime_plan": "lifetime-thread",
                },
                "message_ai_details": [
                    {
                        "tactical_plan_3turn": {"TURN1_DIRECTIVE": "do x"},
                        "plan_episode": {"PLAN": "episode-snapshot"},
                        "plan_chapter": {"PLAN": "chapter-snapshot"},
                        "plan_season": {"PLAN": "season-snapshot"},
                        "plan_year": {"PLAN": "year-snapshot"},
                        "plan_lifetime": {"PLAN": "lifetime-snapshot"},
                    }
                ],
            }
        )

        fields = latest_plan_fields(123, client=client)
        self.assertEqual({"TURN1_DIRECTIVE": "do x"}, fields["CREATOR_TACTICAL_PLAN_3TURN"])
        self.assertEqual("episode-thread", fields["CREATOR_EPISODE_PLAN"])
        self.assertEqual("chapter-thread", fields["CREATOR_CHAPTER_PLAN"])
        self.assertEqual("season-thread", fields["CREATOR_SEASON_PLAN"])
        self.assertEqual("year-thread", fields["CREATOR_YEAR_PLAN"])
        self.assertEqual("lifetime-thread", fields["CREATOR_LIFETIME_PLAN"])

    def test_latest_plan_fields_falls_back_to_snapshot_when_thread_plans_empty(self):
        snapshot_episode = {"PLAN": "episode-snapshot"}
        client = _FakeClient(
            tables={
                "threads": {
                    "episode_plan": "",
                    "chapter_plan": "",
                    "season_plan": "",
                    "year_plan": "",
                    "lifetime_plan": "",
                },
                "message_ai_details": [
                    {
                        "tactical_plan_3turn": {"TURN1_DIRECTIVE": "do x"},
                        "plan_episode": snapshot_episode,
                        "plan_chapter": {"PLAN": "chapter-snapshot"},
                        "plan_season": {"PLAN": "season-snapshot"},
                        "plan_year": {"PLAN": "year-snapshot"},
                        "plan_lifetime": {"PLAN": "lifetime-snapshot"},
                    }
                ],
            }
        )

        fields = latest_plan_fields(123, client=client)
        self.assertEqual(snapshot_episode, fields["CREATOR_EPISODE_PLAN"])


if __name__ == "__main__":
    unittest.main()

