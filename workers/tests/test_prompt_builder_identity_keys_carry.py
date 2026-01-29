import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.lib.prompt_builder import resolve_hermes_identity_keys


class _FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def select(self, *args, **kwargs):
        return self

    def eq(self, column, value):
        self._rows = [
            row for row in self._rows if isinstance(row, dict) and row.get(column) == value
        ]
        return self

    def lt(self, column, value):
        self._rows = [
            row
            for row in self._rows
            if isinstance(row, dict)
            and row.get(column) is not None
            and row.get(column) < value
        ]
        return self

    def order(self, column, desc=False):
        reverse = bool(desc)

        def key(row):
            val = row.get(column) if isinstance(row, dict) else None
            return (val is None, val)

        self._rows = sorted(self._rows, key=key, reverse=reverse)
        return self

    def limit(self, n):
        self._rows = self._rows[: int(n)]
        return self

    def execute(self):
        return SimpleNamespace(data=self._rows)


class _FakeClient:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def table(self, name):
        self.assert_table(name)
        return _FakeQuery(self._rows)

    def assert_table(self, name):
        if name != "message_ai_details":
            raise AssertionError(f"Unexpected table: {name}")


class PromptBuilderIdentityKeysCarryTests(unittest.TestCase):
    def test_uses_current_keys(self):
        keys = resolve_hermes_identity_keys(
            current_keys=["style", "boundaries"],
            current_extras={"iris": {"parsed": {"hermes": "skip"}}},
            thread_id=1,
            message_id=100,
            client=_FakeClient([]),
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(keys, ["style", "boundaries"])

    def test_no_carry_when_hermes_not_skipped(self):
        rows = [
            {
                "thread_id": 1,
                "message_id": 99,
                "extras": {
                    "hermes": {
                        "created_at": datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc).isoformat(),
                        "parsed": {"identity_keys": ["style"]},
                    }
                },
                "hermes_output_raw": "",
            }
        ]
        keys = resolve_hermes_identity_keys(
            current_keys=[],
            current_extras={"iris": {"parsed": {"hermes": "full"}}},
            thread_id=1,
            message_id=100,
            client=_FakeClient(rows),
            now=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(keys, [])

    def test_carry_forward_within_ttl(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        rows = [
            {
                "thread_id": 1,
                "message_id": 99,
                "extras": {
                    "hermes": {
                        "created_at": datetime(2026, 1, 1, 11, 30, tzinfo=timezone.utc).isoformat(),
                        "parsed": {"identity_keys": ["style", "availability"]},
                    }
                },
                "hermes_output_raw": "",
            }
        ]
        keys = resolve_hermes_identity_keys(
            current_keys=[],
            current_extras={"iris": {"parsed": {"hermes": "skip"}}},
            thread_id=1,
            message_id=100,
            client=_FakeClient(rows),
            now=now,
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(keys, ["style", "availability"])

    def test_ttl_expired_returns_empty(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        rows = [
            {
                "thread_id": 1,
                "message_id": 99,
                "extras": {
                    "hermes": {
                        "created_at": datetime(2026, 1, 1, 7, 0, tzinfo=timezone.utc).isoformat(),
                        "parsed": {"identity_keys": ["style"]},
                    }
                },
                "hermes_output_raw": "",
            }
        ]
        keys = resolve_hermes_identity_keys(
            current_keys=[],
            current_extras={"iris": {"parsed": {"hermes": "skip"}}},
            thread_id=1,
            message_id=100,
            client=_FakeClient(rows),
            now=now,
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(keys, [])


if __name__ == "__main__":
    unittest.main()

