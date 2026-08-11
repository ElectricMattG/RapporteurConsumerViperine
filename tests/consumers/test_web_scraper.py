"""
tests/consumers/test_web_scraper.py

Unit tests for KafkaWebScraperConsumer pure/mockable methods:
  - _is_extraction_valid          word-count + failure-message validation
  - _check_content_quality        sidebar/navigation heuristics
  - _is_cloudflare_bot_page       Cloudflare challenge detection
  - _extract_full_text_content    BeautifulSoup main-content extraction
  - _convert_transcript_to_markdown  YouTube transcript → markdown

Built via __new__ so __init__ (URLResolver, Kafka, etc.) is bypassed.
Live-I/O methods (Playwright, HTTP, cloudscraper) are intentionally skipped.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

bs4 = pytest.importorskip("bs4")
from bs4 import BeautifulSoup  # noqa: E402


def _make_scraper(min_words=None):
    from consumers.kafka_web_scraper import KafkaWebScraperConsumer

    inst = KafkaWebScraperConsumer.__new__(KafkaWebScraperConsumer)
    inst._logger = logging.getLogger("test.scraper")
    # _is_extraction_valid reads self._app_config.get('web_scraper.min_words...')
    app_config = MagicMock()
    app_config.get.return_value = min_words
    inst._app_config = app_config
    return inst


# ── _is_extraction_valid ────────────────────────────────────────────────────────

class TestIsExtractionValid:
    def test_empty_is_invalid(self):
        assert _make_scraper()._is_extraction_valid("") is False

    def test_none_is_invalid(self):
        assert _make_scraper()._is_extraction_valid(None) is False

    def test_sufficient_words_is_valid(self):
        scraper = _make_scraper()
        text = " ".join(["word"] * 50)
        assert scraper._is_extraction_valid(text) is True

    def test_below_min_words_is_invalid(self):
        scraper = _make_scraper()  # default min 10
        assert scraper._is_extraction_valid("only three words") is False

    def test_failure_indicator_is_invalid(self):
        scraper = _make_scraper()
        assert scraper._is_extraction_valid("Page not found") is False

    def test_failure_indicator_prefix_is_invalid(self):
        scraper = _make_scraper()
        assert scraper._is_extraction_valid("access denied - try again later please") is False

    def test_configured_min_words_respected(self):
        scraper = _make_scraper(min_words=3)
        assert scraper._is_extraction_valid("one two three") is True

    def test_invalid_config_falls_back_to_default(self):
        scraper = _make_scraper(min_words="not-an-int")
        assert scraper._is_extraction_valid(" ".join(["w"] * 20)) is True
        assert scraper._is_extraction_valid("too few") is False


class TestYouTubeTranscriptFailure:
    def test_transcript_failure_returns_failure_without_page_fallback(self, tmp_path):
        scraper = _make_scraper()
        scraper.cache_dir = Path(tmp_path)
        scraper._resolve_redirects = lambda url: url
        scraper._extract_youtube_transcript = lambda *args, **kwargs: {
            "success": False,
            "error": "Transcript API blocked the request",
        }
        scraper._extract_with_playwright = lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("YouTube transcript failures must not fall back to Playwright")
        )

        result = asyncio.run(scraper._process_message({
            "uri": "https://www.youtube.com/watch?v=VLCosBKRUxk",
            "title": "Video",
            "session_id": "session-1",
            "request_id": "request-1",
        }))

        assert result["success"] is False
        assert "Transcript API blocked" in result["error"]

    def test_transcript_failure_dlq_record_is_published_once(self):
        scraper = _make_scraper()
        scraper.producer = MagicMock()
        scraper.dead_letter_topic = "viperine.dead-letter-queue"
        scraper.telemetry_enabled = False

        scraper._publish_to_dlq(
            {
                "session_id": "session-1",
                "request_id": "request-1",
                "query_id": "query-1",
                "uri": "https://www.youtube.com/watch?v=VLCosBKRUxk",
            },
            "YouTube transcript extraction failed: Transcript API blocked the request",
        )

        scraper.producer.produce.assert_called_once()
        record = json.loads(scraper.producer.produce.call_args.kwargs["value"])
        assert record["category"] == "youtube_transcript"
        assert "YouTube transcript extraction failed" in record["error_message"]

# ── _check_content_quality ──────────────────────────────────────────────────────

class TestCheckContentQuality:
    def test_empty_is_not_quality(self):
        out = _make_scraper()._check_content_quality("", "Title")
        assert out["is_quality"] is False

    def test_long_article_is_quality(self):
        scraper = _make_scraper()
        text = "\n".join(" ".join(["word"] * 20) for _ in range(20))
        out = scraper._check_content_quality(text, "Real Article")
        assert out["is_quality"] is True

    def test_short_content_with_metadata_flagged(self):
        scraper = _make_scraper()
        out = scraper._check_content_quality("Just a few words here", "Some Title")
        assert out["is_quality"] is False

    def test_date_listing_flagged(self):
        scraper = _make_scraper()
        text = "January 5, 2024\nFebruary 6, 2024\nMarch 7, 2024"
        out = scraper._check_content_quality(text, "Archive")
        assert out["is_quality"] is False

    def test_high_newline_ratio_flagged(self):
        scraper = _make_scraper()
        text = "\n".join(["Home", "About", "Contact", "Blog", "Login", "Signup"])
        out = scraper._check_content_quality(text, "Nav")
        assert out["is_quality"] is False

    def test_reason_present(self):
        out = _make_scraper()._check_content_quality("", "T")
        assert "reason" in out and out["reason"]


# ── _is_cloudflare_bot_page ─────────────────────────────────────────────────────

class TestIsCloudflareBotPage:
    def test_just_a_moment_title(self):
        scraper = _make_scraper()
        assert scraper._is_cloudflare_bot_page("short", "Just a moment...") is True

    def test_attention_required_title(self):
        scraper = _make_scraper()
        assert scraper._is_cloudflare_bot_page("x", "Attention Required! | Cloudflare") is True

    def test_short_content_with_two_indicators(self):
        scraper = _make_scraper()
        text = "Checking your browser. Please enable javascript and cookies."
        assert scraper._is_cloudflare_bot_page(text, "Site") is True

    def test_long_real_article_not_flagged(self):
        scraper = _make_scraper()
        text = "This is a real article. " * 100
        assert scraper._is_cloudflare_bot_page(text, "News") is False

    def test_short_single_indicator_not_flagged(self):
        scraper = _make_scraper()
        assert scraper._is_cloudflare_bot_page("cloudflare powered site", "Site") is False


# ── _is_youtube_blocked_page ───────────────────────────────────────────────────

class TestIsYouTubeBlockedPage:
    def test_private_video_is_blocked(self):
        scraper = _make_scraper()
        assert scraper._is_youtube_blocked_page("This video is private", "YouTube") is True

    def test_bot_challenge_is_blocked(self):
        scraper = _make_scraper()
        assert scraper._is_youtube_blocked_page(
            "Sign in to confirm you're not a bot", "YouTube"
        ) is True

    def test_real_video_page_is_not_blocked(self):
        scraper = _make_scraper()
        text = "A real video page with the title, channel, description, and player controls. " * 5
        assert scraper._is_youtube_blocked_page(text, "Actual video title") is False


# ── _extract_full_text_content ──────────────────────────────────────────────────

class TestExtractFullTextContent:
    def test_extracts_article_text(self):
        scraper = _make_scraper()
        html = "<html><body><article><p>Hello world article body.</p></article></body></html>"
        out = scraper._extract_full_text_content(BeautifulSoup(html, "html.parser"))
        assert "Hello world article body." in out

    def test_strips_script_and_nav(self):
        scraper = _make_scraper()
        html = (
            "<html><body><nav>MENU</nav><script>evil()</script>"
            "<main><p>Real content here.</p></main></body></html>"
        )
        out = scraper._extract_full_text_content(BeautifulSoup(html, "html.parser"))
        assert "Real content here." in out
        assert "MENU" not in out
        assert "evil()" not in out

    def test_falls_back_to_body(self):
        scraper = _make_scraper()
        html = "<html><body><p>Body-only text.</p></body></html>"
        out = scraper._extract_full_text_content(BeautifulSoup(html, "html.parser"))
        assert "Body-only text." in out

    def test_empty_returns_no_readable_content(self):
        scraper = _make_scraper()
        html = "<html><body></body></html>"
        out = scraper._extract_full_text_content(BeautifulSoup(html, "html.parser"))
        assert out == "No readable content found"


# ── _convert_transcript_to_markdown ─────────────────────────────────────────────

def _snippets(*texts):
    return [SimpleNamespace(text=t) for t in texts]


class TestConvertTranscriptToMarkdown:
    def test_header_includes_title_and_video_id(self):
        scraper = _make_scraper()
        out = scraper._convert_transcript_to_markdown(
            _snippets("hello", "world"), video_id="abc123", title="My Video"
        )
        assert "**Title:** My Video" in out
        assert "**Video ID:** abc123" in out

    def test_strips_music_symbols(self):
        scraper = _make_scraper()
        out = scraper._convert_transcript_to_markdown(
            _snippets("♪ music ♪", "lyrics"), video_id="v", title="t"
        )
        assert "♪" not in out

    def test_strips_vtt_tags(self):
        scraper = _make_scraper()
        out = scraper._convert_transcript_to_markdown(
            _snippets("<c>tagged</c> text"), video_id="v", title="t"
        )
        assert "<c>" not in out
        assert "tagged" in out

    def test_joins_segments(self):
        scraper = _make_scraper()
        out = scraper._convert_transcript_to_markdown(
            _snippets("first", "second", "third"), video_id="v", title="t"
        )
        assert "first second third" in out

    def test_optional_metadata_rendered(self):
        scraper = _make_scraper()
        out = scraper._convert_transcript_to_markdown(
            _snippets("x"), video_id="v", title="t",
            channel="My Channel", upload_date="2024-01-01", duration=3661,
        )
        assert "**Channel:** My Channel" in out
        assert "**Upload Date:** 2024-01-01" in out
        assert "1:01:01" in out  # 3661s = 1h 1m 1s

    def test_duration_formats_minutes_seconds(self):
        scraper = _make_scraper()
        out = scraper._convert_transcript_to_markdown(
            _snippets("x"), video_id="v", title="t", duration=125,
        )
        assert "2:05" in out  # 125s = 2m 5s
