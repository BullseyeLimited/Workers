import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.iris_join.main import resolve_content_request_for_pack


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

    def order(self, *args, **kwargs):
        return self

    def limit(self, n):
        self._rows = self._rows[: int(n)]
        return self

    def execute(self):
        return SimpleNamespace(data=self._rows)


class _FakeClient:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def table(self, _name):
        return _FakeQuery(self._rows)


class _ClientShouldNotBeUsed:
    def table(self, _name):
        raise AssertionError("DB should not be queried for this scenario")


class IrisJoinContentRequestCarryTests(unittest.TestCase):
    def test_uses_current_request(self):
        request, inferred = resolve_content_request_for_pack(
            current_request={"zoom": 2, "script_id": "abc"},
            has_iris=True,
            iris_hermes_mode="skip",
            thread_id=1,
            fan_msg_id=100,
            client=_ClientShouldNotBeUsed(),
        )
        self.assertEqual(request.get("zoom"), 2)
        self.assertEqual(request.get("script_id"), "abc")
        self.assertFalse(inferred)

    def test_defaults_to_zoom0_when_hermes_not_skipped(self):
        request, inferred = resolve_content_request_for_pack(
            current_request=None,
            has_iris=True,
            iris_hermes_mode="full",
            thread_id=1,
            fan_msg_id=100,
            client=_ClientShouldNotBeUsed(),
        )
        self.assertEqual(request, {"zoom": 0})
        self.assertFalse(inferred)

    def test_hermes_skipped_no_previous_request(self):
        request, inferred = resolve_content_request_for_pack(
            current_request=None,
            has_iris=True,
            iris_hermes_mode="skip",
            thread_id=1,
            fan_msg_id=100,
            client=_FakeClient([]),
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(request, {"zoom": 0})
        self.assertTrue(inferred)

    def test_carry_forward_within_ttl(self):
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        previous_created_at = datetime(2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        rows = [
            {
                "message_id": 99,
                "thread_id": 1,
                "content_request": {"zoom": 1, "script_id": "s1"},
                "content_pack_created_at": previous_created_at.isoformat(),
            }
        ]
        request, inferred = resolve_content_request_for_pack(
            current_request=None,
            has_iris=True,
            iris_hermes_mode="skip",
            thread_id=1,
            fan_msg_id=100,
            client=_FakeClient(rows),
            now=now,
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(request.get("zoom"), 1)
        self.assertEqual(request.get("script_id"), "s1")
        self.assertTrue(inferred)

    def test_ttl_expired_falls_back_to_zoom0(self):
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        previous_created_at = datetime(2026, 1, 1, 6, 59, 59, tzinfo=timezone.utc)
        rows = [
            {
                "message_id": 99,
                "thread_id": 1,
                "content_request": {"zoom": 2, "script_id": "s2"},
                "content_pack_created_at": previous_created_at.isoformat(),
            }
        ]
        request, inferred = resolve_content_request_for_pack(
            current_request=None,
            has_iris=True,
            iris_hermes_mode="skip",
            thread_id=1,
            fan_msg_id=100,
            client=_FakeClient(rows),
            now=now,
            ttl_seconds=4 * 60 * 60,
        )
        self.assertEqual(request, {"zoom": 0})
        self.assertTrue(inferred)


if __name__ == "__main__":
    unittest.main()

