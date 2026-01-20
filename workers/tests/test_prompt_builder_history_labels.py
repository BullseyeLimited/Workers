import unittest

import json


from workers.lib.prompt_builder import (
    _ordinal,
    _recency_prefix,
    live_turn_window,
    latest_kairos_json,
    make_block,
)


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    def __init__(self, client, table_name: str):
        self._client = client
        self._table_name = table_name

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def gt(self, *_args, **_kwargs):
        return self

    def gte(self, *_args, **_kwargs):
        return self

    def lt(self, *_args, **_kwargs):
        return self

    def lte(self, *_args, **_kwargs):
        return self

    def neq(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        return _FakeResponse(self._client._tables.get(self._table_name) or [])


class _FakeClient:
    def __init__(self, *, tables: dict):
        self._tables = tables

    def table(self, table_name: str):
        return _FakeQuery(self, table_name)


class PromptBuilderHistoryLabelTests(unittest.TestCase):
    def test_ordinal_suffixes(self):
        self.assertEqual("1st", _ordinal(1))
        self.assertEqual("2nd", _ordinal(2))
        self.assertEqual("3rd", _ordinal(3))
        self.assertEqual("4th", _ordinal(4))
        self.assertEqual("11th", _ordinal(11))
        self.assertEqual("12th", _ordinal(12))
        self.assertEqual("13th", _ordinal(13))
        self.assertEqual("21st", _ordinal(21))

    def test_recency_prefixes(self):
        self.assertEqual("Newest", _recency_prefix(1))
        self.assertEqual("2nd newest", _recency_prefix(2))
        self.assertEqual("3rd newest", _recency_prefix(3))

    def test_make_block_labels_newest_and_orders_oldest_to_newest(self):
        client = _FakeClient(
            tables={
                "summaries": [
                    # Returned newest-first (tier_index desc).
                    {
                        "id": 100,
                        "thread_id": 1,
                        "tier": "episode",
                        "tier_index": 3,
                        "start_turn": 41,
                        "end_turn": 60,
                        "abstract_summary": "A3",
                        "narrative_summary": "N3",
                    },
                    {
                        "id": 99,
                        "thread_id": 1,
                        "tier": "episode",
                        "tier_index": 2,
                        "start_turn": 21,
                        "end_turn": 40,
                        "abstract_summary": "A2",
                        "narrative_summary": "N2",
                    },
                ],
                # Avoid timestamp decoration in labels.
                "messages": [],
            }
        )

        block = make_block(1, "episode", limit=2, client=client)
        self.assertIn("2nd newest Episode – Abstract:\nA2", block)
        self.assertIn("2nd newest Episode – Narrative:\nN2", block)
        self.assertIn("Newest Episode – Abstract:\nA3", block)
        self.assertIn("Newest Episode – Narrative:\nN3", block)
        self.assertLess(
            block.index("2nd newest Episode – Abstract:"),
            block.index("2nd newest Episode – Narrative:"),
        )
        self.assertLess(
            block.index("Newest Episode – Abstract:"),
            block.index("Newest Episode – Narrative:"),
        )
        self.assertLess(
            block.index("2nd newest Episode – Abstract:"),
            block.index("Newest Episode – Abstract:"),
        )

    def test_live_turn_window_numbers_turns_by_recency(self):
        client = _FakeClient(
            tables={
                # Returned newest-first (turn_index desc).
                "messages": [
                    {
                        "id": 3,
                        "turn_index": 3,
                        "sender": "fan",
                        "message_text": "newest",
                        "media_analysis_text": None,
                        "created_at": "2026-01-01T00:00:03Z",
                        "content_id": None,
                    },
                    {
                        "id": 2,
                        "turn_index": 2,
                        "sender": "creator",
                        "message_text": "middle",
                        "media_analysis_text": None,
                        "created_at": "2026-01-01T00:00:02Z",
                        "content_id": None,
                    },
                    {
                        "id": 1,
                        "turn_index": 1,
                        "sender": "fan",
                        "message_text": "oldest",
                        "media_analysis_text": None,
                        "created_at": "2026-01-01T00:00:01Z",
                        "content_id": None,
                    },
                ],
                "content_offers": [],
                "content_deliveries": [],
                "content_items": [],
                "summaries": [],
            }
        )

        rendered = live_turn_window(1, boundary_turn=4, client=client)
        lines = [line for line in rendered.splitlines() if line.strip()]
        self.assertGreaterEqual(len(lines), 3)
        self.assertTrue(lines[0].startswith("Turn 3 @"))
        self.assertTrue(lines[1].startswith("Turn 2 @"))
        self.assertTrue(lines[2].startswith("Turn 1 @"))

    def test_latest_kairos_json_uses_same_message_id(self):
        client = _FakeClient(
            tables={
                "message_ai_details": [
                    {
                        "message_id": 123,
                        "strategic_narrative": "SN",
                        "psychological_levers": "PL",
                        "risks": "R",
                    }
                ]
            }
        )
        raw = latest_kairos_json(1, client=client, message_id=123)
        parsed = json.loads(raw)
        self.assertEqual("SN", parsed.get("STRATEGIC_NARRATIVE"))
        self.assertEqual("PL", parsed.get("PSYCHOLOGICAL_LEVERS"))
        self.assertEqual("R", parsed.get("RISKS"))

    def test_latest_kairos_json_empty_when_missing(self):
        client = _FakeClient(tables={"message_ai_details": []})
        self.assertEqual("", latest_kairos_json(1, client=client, message_id=999))


if __name__ == "__main__":
    unittest.main()
