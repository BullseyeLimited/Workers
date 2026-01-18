import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

import workers.content_ingestor.main as content_ingestor


class _FakeQuery:
    def __init__(self, data):
        self._data = data

    def select(self, *args, **kwargs):
        return self

    def eq(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self._data)


class _FakeSB:
    def __init__(self, rows):
        self._rows = rows

    def table(self, name):
        if name != "content_items":
            raise AssertionError(f"unexpected table {name}")
        return _FakeQuery(self._rows)


class ContentIngestorRunpodGateTests(unittest.TestCase):
    def test_process_job_returns_false_when_runpod_unreachable(self):
        row = {
            "id": 123,
            "ingest_status": "pending",
            "ingest_attempts": 0,
            "media_type": "video",
            "mimetype": "video/mp4",
            "url_main": "https://example.test/video.mp4",
        }

        fake_sb = _FakeSB([row])

        with patch.object(content_ingestor, "SB", fake_sb):
            with patch.object(content_ingestor, "_load_prompt", return_value="prompt"):
                with patch.object(
                    content_ingestor,
                    "_run_model",
                    return_value=("", "runpod_unreachable: connection refused"),
                ):
                    with patch.object(content_ingestor, "_update_ingest_status") as update_status:
                        ok = content_ingestor.process_job({"content_id": 123})

        self.assertFalse(ok)
        self.assertEqual(2, len(update_status.call_args_list))
        first = update_status.call_args_list[0]
        second = update_status.call_args_list[1]

        self.assertEqual(123, first.args[0])
        self.assertEqual("processing", first.kwargs.get("status"))
        self.assertIsNone(first.kwargs.get("error"))

        self.assertEqual(123, second.args[0])
        self.assertEqual("pending", second.kwargs.get("status"))
        self.assertEqual("runpod_unavailable", second.kwargs.get("error"))


if __name__ == "__main__":
    unittest.main()

