from __future__ import annotations

import re
from urllib.parse import urlparse

# Broad URL matcher; expects http(s) or bare www.* links.
_URL_RE = re.compile(r"(?i)\b((?:https?://|www\.)[^\s<>()\[\]{}\"]+)")
_TRAILING_PUNCT = ".,;:!?)]\"'"


def _normalize_url(raw: str) -> str | None:
    if not raw:
        return None

    candidate = raw.strip()
    candidate = candidate.rstrip(_TRAILING_PUNCT)
    if candidate.startswith("www."):
        candidate = f"http://{candidate}"

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return candidate


def extract_urls(text: str, limit: int = 10) -> list[str]:
    """
    Extract up to `limit` distinct URLs from freeform text.
    """
    if not text:
        return []

    seen = set()
    urls: list[str] = []
    for match in _URL_RE.finditer(text):
        normalized = _normalize_url(match.group(1))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
        if len(urls) >= limit:
            break
    return urls


__all__ = ["extract_urls"]
