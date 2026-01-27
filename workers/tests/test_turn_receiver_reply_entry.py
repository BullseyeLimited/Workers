import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.turn_receiver import main as turn_receiver


class TurnReceiverReplyEntryTests(unittest.TestCase):
    def setUp(self):
        self.sent: list[tuple[str, dict]] = []

        self._orig_send = turn_receiver.send
        self._orig_job_exists = turn_receiver.job_exists
        self._orig_upsert_step = turn_receiver.upsert_step
        self._orig_active_reply_run = turn_receiver._active_reply_run
        self._orig_create_reply_run = turn_receiver._create_reply_run
        self._orig_replied_boundary = turn_receiver._replied_boundary_turn

        turn_receiver.send = lambda queue, payload: self.sent.append((queue, payload))
        turn_receiver.job_exists = lambda *_args, **_kwargs: False
        turn_receiver.upsert_step = lambda **_kwargs: None

    def tearDown(self):
        turn_receiver.send = self._orig_send
        turn_receiver.job_exists = self._orig_job_exists
        turn_receiver.upsert_step = self._orig_upsert_step
        turn_receiver._active_reply_run = self._orig_active_reply_run
        turn_receiver._create_reply_run = self._orig_create_reply_run
        turn_receiver._replied_boundary_turn = self._orig_replied_boundary

    def test_idle_starts_pipeline_without_reply_supervise(self):
        turn_receiver._active_reply_run = lambda _thread_id: None
        turn_receiver._create_reply_run = lambda **_kwargs: "run-1"

        turn_receiver._kick_reply_flow(
            message_id=10,
            thread_id=1,
            turn_index=5,
            has_media=True,
        )

        queues = [q for q, _ in self.sent]
        self.assertIn("argus.analyse", queues)
        self.assertIn("iris.decide", queues)
        self.assertNotIn("reply.supervise", queues)

        iris_payload = next(p for q, p in self.sent if q == "iris.decide")
        self.assertEqual(10, iris_payload.get("message_id"))
        self.assertEqual("run-1", iris_payload.get("run_id"))

    def test_busy_enqueues_interrupt_only(self):
        turn_receiver._active_reply_run = lambda _thread_id: {"run_id": "run-0", "root_fan_message_id": 9}

        turn_receiver._kick_reply_flow(
            message_id=10,
            thread_id=1,
            turn_index=5,
            has_media=False,
        )

        queues = [q for q, _ in self.sent]
        self.assertIn("reply.supervise", queues)
        self.assertNotIn("iris.decide", queues)

    def test_busy_same_root_does_not_interrupt(self):
        turn_receiver._active_reply_run = lambda _thread_id: {"run_id": "run-0", "root_fan_message_id": 10}

        turn_receiver._kick_reply_flow(
            message_id=10,
            thread_id=1,
            turn_index=5,
            has_media=False,
        )

        queues = [q for q, _ in self.sent]
        self.assertNotIn("reply.supervise", queues)
        self.assertIn("iris.decide", queues)

    def test_retry_skips_when_already_replied(self):
        turn_receiver._active_reply_run = lambda _thread_id: None
        turn_receiver._replied_boundary_turn = lambda _thread_id: 10

        turn_receiver._kick_reply_flow(
            message_id=10,
            thread_id=1,
            turn_index=5,
            has_media=False,
            is_retry=True,
        )

        self.assertEqual([], self.sent)


if __name__ == "__main__":
    unittest.main()
