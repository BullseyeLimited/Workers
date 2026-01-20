import os
import unittest
import datetime
import time

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.lib import simple_queue


class _FakeResponse:
    def __init__(self, data):
        self.data = data


def _parse_dt(value: str) -> datetime.datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(raw)


class _FakeJobQueueQuery:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self._op = None
        self._filters: dict[str, tuple[str, str]] = {}
        self._update_payload: dict | None = None
        self._limit: int | None = None

    def select(self, *_args, **_kwargs):
        self._op = "select"
        return self

    def eq(self, field: str, value: str):
        self._filters[field] = ("eq", str(value))
        return self

    def lte(self, field: str, value: str):
        self._filters[field] = ("lte", str(value))
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, n: int):
        self._limit = int(n)
        return self

    def update(self, payload: dict):
        self._op = "update"
        self._update_payload = payload
        return self

    def delete(self):
        self._op = "delete"
        return self

    def execute(self):
        if self._op == "select":
            queue_name = self._filters.get("queue", ("eq", ""))[1]
            available_lte = self._filters.get("available_at", ("lte", ""))[1]
            threshold = _parse_dt(available_lte) if available_lte else None

            out = []
            for row in self._rows:
                if str(row.get("queue")) != str(queue_name):
                    continue
                if threshold is not None:
                    available_at = row.get("available_at") or "1970-01-01T00:00:00+00:00"
                    if _parse_dt(str(available_at)) > threshold:
                        continue
                out.append({"id": row["id"], "payload": row["payload"]})
                break

            if self._limit is not None:
                out = out[: self._limit]
            return _FakeResponse(out)

        if self._op == "update":
            row_id = int(self._filters.get("id", ("eq", "0"))[1])
            for row in self._rows:
                if int(row.get("id") or 0) == row_id:
                    row.update(self._update_payload or {})
                    break
            return _FakeResponse([])

        if self._op == "delete":
            row_id = int(self._filters.get("id", ("eq", "0"))[1])
            self._rows[:] = [row for row in self._rows if int(row.get("id") or 0) != row_id]
            return _FakeResponse([])

        return _FakeResponse([])


class _FakeClient:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def table(self, table_name: str):
        if table_name != "job_queue":
            raise AssertionError(f"unexpected table: {table_name}")
        return _FakeJobQueueQuery(self._rows)


class SimpleQueueFallbackTests(unittest.TestCase):
    def test_receive_falls_back_when_claim_rpc_raises(self):
        original_sb = simple_queue.SB
        original_use_rpc = simple_queue.USE_RPC
        original_allow_fallback = simple_queue.ALLOW_FALLBACK
        original_receive_via_rpc = simple_queue._receive_via_rpc
        original_last_log_at = simple_queue._LAST_RPC_ERROR_LOG_AT

        rows = [
            {
                "id": 1,
                "queue": "reply.supervise",
                "payload": {"message_id": 123},
                "available_at": "2000-01-01T00:00:00+00:00",
            }
        ]

        try:
            simple_queue.SB = _FakeClient(rows)
            simple_queue.USE_RPC = True
            simple_queue.ALLOW_FALLBACK = False
            # Keep test output clean (rate-limited logger would otherwise print).
            simple_queue._LAST_RPC_ERROR_LOG_AT = time.time()

            def _boom(*_args, **_kwargs):
                raise RuntimeError("rpc down")

            simple_queue._receive_via_rpc = _boom

            job = simple_queue.receive("reply.supervise", 30)
            self.assertIsNotNone(job)
            self.assertEqual(1, job["row_id"])
            self.assertEqual({"message_id": 123}, job["payload"])
        finally:
            simple_queue.SB = original_sb
            simple_queue.USE_RPC = original_use_rpc
            simple_queue.ALLOW_FALLBACK = original_allow_fallback
            simple_queue._receive_via_rpc = original_receive_via_rpc
            simple_queue._LAST_RPC_ERROR_LOG_AT = original_last_log_at


if __name__ == "__main__":
    unittest.main()
