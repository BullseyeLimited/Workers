import unittest

from workers.lib.link_utils import extract_urls


class ExtractUrlsTests(unittest.TestCase):
    def test_extracts_basic_http_and_https(self):
        text = "See http://example.com and https://foo.bar/baz."
        urls = extract_urls(text)
        self.assertIn("http://example.com", urls)
        self.assertIn("https://foo.bar/baz", urls)

    def test_trims_trailing_punctuation_and_deduplicates(self):
        text = "Check https://news.site/article?, also https://news.site/article."
        urls = extract_urls(text)
        self.assertEqual(1, len(urls))
        self.assertEqual("https://news.site/article", urls[0])

    def test_handles_www_prefix(self):
        text = "Visit www.example.org now!"
        urls = extract_urls(text)
        self.assertEqual(["http://www.example.org"], urls)


if __name__ == "__main__":
    unittest.main()
