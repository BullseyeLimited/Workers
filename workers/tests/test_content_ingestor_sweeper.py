import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
os.environ.setdefault("RUNPOD_MODEL_NAME", "test-model")

import workers.content_ingestor.main as content_ingestor


class _FakeQuery:
    def __init__(self, parent):
        self._parent = parent
        self._filters = {}

    def select(self, *args, **kwargs):
        return self

    def eq(self, *args, **kwargs):
        if len(args) >= 2:
            self._filters[args[0]] = args[1]
        return self

    def lt(self, *args, **kwargs):
        return self

    def order(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        ingest_status = self._filters.get("ingest_status")
        if ingest_status == "processing":
            data = self._parent.processing_rows
        elif ingest_status == "pending":
            data = self._parent.pending_rows
        else:
            data = []
        return SimpleNamespace(data=data)


class _FakeSB:
    def __init__(self, *, processing_rows=None, pending_rows=None):
        self.processing_rows = processing_rows or []
        self.pending_rows = pending_rows or []

    def table(self, name):
        if name != "content_items":
            raise AssertionError(f"unexpected table {name}")
        return _FakeQuery(self)


class ContentIngestorSweeperTests(unittest.TestCase):
    def test_reset_stale_processing_items_enqueues_jobs(self):
        fake_sb = _FakeSB(processing_rows=[{"id": 1, "ingest_attempts": 2, "ingest_updated_at": "2000-01-01"}])

        with patch.object(content_ingestor, "SB", fake_sb):
            with patch.object(content_ingestor, "_update_ingest_status") as update_status:
                with patch.object(content_ingestor, "send") as send:
                    count = content_ingestor._reset_stale_processing_items(limit=10)

        self.assertEqual(1, count)
        update_status.assert_called()
        self.assertEqual("pending", update_status.call_args.kwargs.get("status"))
        self.assertEqual("stale_processing_reset", update_status.call_args.kwargs.get("error"))
        send.assert_called_once_with(content_ingestor.QUEUE, {"content_id": 1})

    def test_enqueue_missing_pending_ingest_jobs_skips_existing_or_no_url(self):
        pending_rows = [
            {"id": 1, "url_main": "https://example.test/a.jpg", "url_thumb": None, "ingest_status": "pending"},
            {"id": 2, "url_main": None, "url_thumb": None, "ingest_status": "pending"},
            {"id": 3, "url_main": "https://example.test/b.jpg", "url_thumb": None, "ingest_status": "pending"},
        ]
        fake_sb = _FakeSB(processing_rows=[], pending_rows=pending_rows)

        def fake_job_exists(queue, message_id, *, client=None, field="message_id"):
            # Pretend id=3 already has a queued job.
            return str(message_id) == "3"

        with patch.object(content_ingestor, "SB", fake_sb):
            with patch.object(content_ingestor, "job_exists", side_effect=fake_job_exists):
                with patch.object(content_ingestor, "send") as send:
                    count = content_ingestor._enqueue_missing_pending_ingest_jobs(limit=10)

        self.assertEqual(1, count)
        send.assert_called_once_with(content_ingestor.QUEUE, {"content_id": 1})


if __name__ == "__main__":
    unittest.main()
