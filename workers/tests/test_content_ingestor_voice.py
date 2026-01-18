import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.content_ingestor.main import _infer_media_type, _parse_header_output, _sanitize_update


class ContentIngestorVoiceTests(unittest.TestCase):
    def test_infer_media_type_handles_querystring_extensions(self):
        row = {"media_type": None, "mimetype": "", "url_main": "https://x/y/audio.m4a?token=abc"}
        self.assertEqual("voice", _infer_media_type(row))

        row = {"media_type": None, "mimetype": "", "url_main": "https://x/y/video.mp4?token=abc"}
        self.assertEqual("video", _infer_media_type(row))

        row = {"media_type": None, "mimetype": "", "url_main": "https://x/y/image.jpg?token=abc"}
        self.assertEqual("photo", _infer_media_type(row))

    def test_infer_media_type_supports_opus(self):
        row = {"media_type": None, "mimetype": "", "url_main": "https://x/y/audio.opus?token=abc"}
        self.assertEqual("voice", _infer_media_type(row))

    def test_parse_voice_header_output_and_sanitize(self):
        raw = """Title
Quick Check-In

Short Description
He apologizes for being quiet and asks how her day was.

Long Description
00:00 – [traffic] He starts with a tired greeting.
00:08 – He apologizes and asks how she is.
00:20 – He says he will talk later and ends the note.

Explicitness
sfw

Time of Day

Mood Tags
apologetic, reassuring, tired

Action Tags
voice_note, checking_in, apologizing, asking_question, making_plan

Duration Seconds
28

Voice Transcript
[traffic]
\"Hey… uh, sorry I’ve been quiet.\"
[small exhale]
\"How was your day?\"
"""
        parsed = _parse_header_output(raw)
        self.assertEqual("Quick Check-In", parsed.get("title"))
        self.assertIn("00:00", parsed.get("desc_long", ""))
        self.assertIn("[traffic]", parsed.get("voice_transcript", ""))

        sanitized = _sanitize_update(parsed)
        self.assertEqual("sfw", sanitized.get("explicitness"))
        self.assertEqual(28, sanitized.get("duration_seconds"))
        self.assertEqual(["apologetic", "reassuring", "tired"], sanitized.get("mood_tags"))
        self.assertIn("voice_note", sanitized.get("action_tags", []))
        # Time of day left blank should not be set.
        self.assertIsNone(sanitized.get("time_of_day"))


if __name__ == "__main__":
    unittest.main()

