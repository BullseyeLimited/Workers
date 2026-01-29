import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.iris_join import main as iris_join


class _FakeQuery:
    def __init__(self, sb, table_name: str):
        self._sb = sb
        self._table = table_name
        self._rows = list(sb._rows.get(table_name, []))
        self._insert_payload = None

    def select(self, *args, **kwargs):
        return self

    def eq(self, column, value):
        self._rows = [
            row for row in self._rows if isinstance(row, dict) and row.get(column) == value
        ]
        return self

    def limit(self, n):
        self._rows = self._rows[: int(n)]
        return self

    def insert(self, payload):
        self._insert_payload = payload
        return self

    def execute(self):
        if self._insert_payload is not None:
            self._sb.inserts.append((self._table, self._insert_payload))
            self._sb._rows.setdefault(self._table, []).append(self._insert_payload)
            return SimpleNamespace(data=[self._insert_payload])
        return SimpleNamespace(data=self._rows)


class _FakeSB:
    def __init__(self, *, rows: dict[str, list[dict]] | None = None):
        self._rows = dict(rows or {})
        self.inserts: list[tuple[str, dict]] = []

    def table(self, name: str):
        return _FakeQuery(self, name)


class IrisJoinSeedsMessageAiDetailsTests(unittest.TestCase):
    def test_seeds_minimal_row_when_missing(self):
        fake = _FakeSB(
            rows={
                "message_ai_details": [],
                "messages": [{"id": 123, "thread_id": 999}],
            }
        )
        original_sb = iris_join.SB
        iris_join.SB = fake
        try:
            ok = iris_join.process_job({"message_id": 123})
        finally:
            iris_join.SB = original_sb

        self.assertTrue(ok)
        inserts = [payload for table, payload in fake.inserts if table == "message_ai_details"]
        self.assertEqual(len(inserts), 1)
        seeded = inserts[0]
        self.assertEqual(seeded.get("message_id"), 123)
        self.assertEqual(seeded.get("thread_id"), 999)
        self.assertEqual(seeded.get("sender"), "fan")
        self.assertEqual(seeded.get("kairos_status"), "pending")
        self.assertEqual(seeded.get("extras"), {})
        self.assertTrue(isinstance(seeded.get("raw_hash"), str) and seeded["raw_hash"])


if __name__ == "__main__":
    unittest.main()

