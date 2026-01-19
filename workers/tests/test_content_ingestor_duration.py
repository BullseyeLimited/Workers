import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
os.environ.setdefault("RUNPOD_MODEL_NAME", "test-model")

from workers.content_ingestor.main import _extract_mp4_duration_seconds_from_bytes


class ContentIngestorDurationTests(unittest.TestCase):
    def test_extract_mp4_duration_seconds_from_bytes_version_0(self):
        # mvhd v0: duration/timescale.
        timescale = 1000
        duration_units = 123456  # 123.456s
        mvhd = (
            (28).to_bytes(4, "big")
            + b"mvhd"
            + b"\x00\x00\x00\x00"  # version=0, flags=0
            + (0).to_bytes(4, "big")  # creation
            + (0).to_bytes(4, "big")  # modification
            + timescale.to_bytes(4, "big")
            + duration_units.to_bytes(4, "big")
        )
        self.assertEqual(123, _extract_mp4_duration_seconds_from_bytes(mvhd))

    def test_extract_mp4_duration_seconds_from_bytes_version_1(self):
        timescale = 1000
        duration_units = 123456  # 123.456s
        mvhd = (
            (40).to_bytes(4, "big")
            + b"mvhd"
            + b"\x01\x00\x00\x00"  # version=1, flags=0
            + (0).to_bytes(8, "big")  # creation
            + (0).to_bytes(8, "big")  # modification
            + timescale.to_bytes(4, "big")
            + duration_units.to_bytes(8, "big")
        )
        self.assertEqual(123, _extract_mp4_duration_seconds_from_bytes(mvhd))


if __name__ == "__main__":
    unittest.main()
