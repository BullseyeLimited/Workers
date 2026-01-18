import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

from workers.content_ingestor.main import _build_messages


class ContentIngestorVideoPayloadTests(unittest.TestCase):
    def test_build_messages_video_uses_video_url_and_embeds_prompt(self):
        url = "https://example.test/video.mp4"
        prompt = "TEST_PROMPT"

        messages = _build_messages("video", prompt, {}, url)
        self.assertEqual(1, len(messages))

        msg0 = messages[0]
        self.assertEqual("user", msg0.get("role"))

        content = msg0.get("content")
        self.assertIsInstance(content, list)

        text_parts = [item for item in content if isinstance(item, dict) and item.get("type") == "text"]
        self.assertTrue(text_parts)
        self.assertIn(prompt, text_parts[0].get("text", ""))
        self.assertIn("VIDEO_URL", text_parts[0].get("text", ""))
        self.assertIn(url, text_parts[0].get("text", ""))

        video_parts = [item for item in content if isinstance(item, dict) and item.get("type") == "video_url"]
        self.assertTrue(video_parts)
        self.assertEqual(url, video_parts[0].get("video_url", {}).get("url"))


if __name__ == "__main__":
    unittest.main()

