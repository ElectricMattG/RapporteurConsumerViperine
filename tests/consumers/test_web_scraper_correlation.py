"""
tests/consumers/test_web_scraper_correlation.py

Phase 1 unit tests — session_id / request_id / origin / url_index / total_urls
propagation through KafkaWebScraperConsumer.

Coverage:
  TestWebScraperInboundCorrelationParsing  — fields read from message; defaults when absent
  TestWebScraperInternalHandleDerivation  — cache-dir handle is per-URL; never leaks to wire
  TestWebScraperOutboundPassthrough        — all 5 fields forwarded verbatim through both
                                             publish methods; Kafka key = request_id:NNN
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.anyio

# ── UUIDs matching the Phase 0.5 fixture shape ────────────────────────────────

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"


# ── Factory helpers ───────────────────────────────────────────────────────────

def _make_scraper(mock_producer, tmp_cache: Path):
    """Build a minimal KafkaWebScraperConsumer without calling __init__."""
    from consumers.kafka_web_scraper import KafkaWebScraperConsumer

    inst = KafkaWebScraperConsumer.__new__(KafkaWebScraperConsumer)
    inst._logger = logging.getLogger("test.scraper.correlation")
    inst.producer = mock_producer
    inst.summarise_document_topic = "viperine.summarise-document"
    inst.output_topic = "viperine.content-extracted"
    inst.telemetry_enabled = False
    inst.cache_dir = tmp_cache

    app_config = MagicMock()
    app_config.get.return_value = None
    inst._app_config = app_config

    return inst


def _decoded_msg(mock_producer, call_index: int = 0) -> dict:
    """Return the decoded JSON body from the Nth produce() call."""
    call_kwargs = mock_producer.produce.call_args_list[call_index]
    value_bytes = call_kwargs[1].get("value") or call_kwargs[0][1]
    return json.loads(value_bytes)


# ══════════════════════════════════════════════════════════════════════════════
# TestWebScraperInboundCorrelationParsing
# ══════════════════════════════════════════════════════════════════════════════

class TestWebScraperInboundCorrelationParsing:
    """
    Verify that _process_message correctly reads the new correlation fields and
    forwards them into extraction_result / published payload.

    Strategy: run _process_message with all network I/O patched out so it takes
    the curl_cffi happy path, then assert the published payload.
    """

    def _base_message_data(self, **overrides) -> dict:
        base = {
            "uri": "https://example.com/article",
            "title": "Test Article",
            "context": [],
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "frontend",
            "url_index": 2,
            "total_urls": 5,
        }
        base.update(overrides)
        return base

    async def _run_with_mocks(self, scraper, message_data: dict) -> None:
        """Patch all I/O and run _process_message, letting it hit the curl_cffi path."""
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.headers = {"content-type": "text/html"}
        fake_response.content = (
            b"<html><body><p>" + (b"word " * 50) + b"</p></body></html>"
        )

        fake_soup_text = "word " * 50  # passes _is_extraction_valid

        scraper.extract_markdown = False
        scraper.web_searcher = MagicMock()
        scraper.web_searcher.session = MagicMock()
        scraper.web_searcher.session.get.return_value = fake_response
        scraper.web_searcher._extract_page_metadata.return_value = {"title": "Test Article"}

        with patch.object(scraper, "_resolve_redirects", return_value="https://example.com/article"), \
             patch.object(scraper, "_extract_full_text_content", return_value=fake_soup_text), \
             patch.object(scraper, "_is_extraction_valid", return_value=True), \
             patch.object(scraper, "_check_content_quality", return_value={"is_quality": True, "reason": ""}), \
             patch.object(scraper, "_save_extraction"):
            await scraper._process_message(message_data)

    async def test_origin_field_forwarded_to_kafka(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        await self._run_with_mocks(scraper, self._base_message_data(origin="frontend"))
        msg = _decoded_msg(mock_producer)
        assert msg["origin"] == "frontend"

    async def test_url_index_forwarded_to_kafka(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        await self._run_with_mocks(scraper, self._base_message_data(url_index=2, total_urls=5))
        msg = _decoded_msg(mock_producer)
        assert msg["url_index"] == 2

    async def test_total_urls_forwarded_to_kafka(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        await self._run_with_mocks(scraper, self._base_message_data(url_index=0, total_urls=7))
        msg = _decoded_msg(mock_producer)
        assert msg["total_urls"] == 7

    async def test_url_index_defaults_to_0_when_missing(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        data = self._base_message_data()
        del data["url_index"]
        await self._run_with_mocks(scraper, data)
        msg = _decoded_msg(mock_producer)
        assert msg["url_index"] == 0

    async def test_total_urls_defaults_to_1_when_neither_field_present(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        data = self._base_message_data()
        data.pop("url_index", None)
        data.pop("total_urls", None)
        data.pop("total_requests", None)
        await self._run_with_mocks(scraper, data)
        msg = _decoded_msg(mock_producer)
        assert msg["total_urls"] == 1

    async def test_total_urls_falls_back_to_total_requests_for_legacy_messages(
        self, mock_producer, tmp_path
    ):
        """Legacy inbound messages have total_requests (not total_urls) — honour them."""
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        data = self._base_message_data()
        data.pop("total_urls", None)
        data["total_requests"] = 4
        await self._run_with_mocks(scraper, data)
        msg = _decoded_msg(mock_producer)
        assert msg["total_urls"] == 4

    async def test_origin_none_when_absent(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        data = self._base_message_data()
        data.pop("origin", None)
        await self._run_with_mocks(scraper, data)
        msg = _decoded_msg(mock_producer)
        assert msg["origin"] is None


# ══════════════════════════════════════════════════════════════════════════════
# TestWebScraperInternalHandleDerivation
# ══════════════════════════════════════════════════════════════════════════════

class TestWebScraperInternalHandleDerivation:
    """
    The cache directory must use a per-URL handle (request_id_url_NNN) so N
    concurrent URLs sharing one request_id don't overwrite each other's files.
    The handle must never appear in the published Kafka payload.
    """

    def test_publish_to_kafka_key_encodes_url_index(self, mock_producer, tmp_path):
        """Kafka key is request_id:NNN — unique per URL within a request."""
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        extraction_result = {
            "url": "https://example.com/",
            "original_url": "https://example.com/",
            "text": "text",
            "metadata": {},
            "extracted_at": "2026-06-22T00:00:00",
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "frontend",
            "url_index": 3,
            "total_urls": 5,
        }
        scraper._publish_to_kafka(extraction_result)
        call_kwargs = mock_producer.produce.call_args_list[0][1]
        key_bytes = call_kwargs.get("key") or mock_producer.produce.call_args_list[0][0][1]
        assert key_bytes == f"{_REQUEST_UUID}:003".encode()

    def test_publish_to_kafka_key_zero_padded(self, mock_producer, tmp_path):
        """url_index is zero-padded to 3 digits in the Kafka key."""
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        extraction_result = {
            "url": "https://example.com/",
            "original_url": "https://example.com/",
            "text": "text",
            "metadata": {},
            "extracted_at": "2026-06-22T00:00:00",
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": None,
            "url_index": 0,
            "total_urls": 1,
        }
        scraper._publish_to_kafka(extraction_result)
        key_bytes = mock_producer.produce.call_args_list[0][1]["key"]
        assert key_bytes == f"{_REQUEST_UUID}:000".encode()

    def test_handle_never_leaks_onto_wire_payload(self, mock_producer, tmp_path):
        """The internal request_handle (request_id_url_NNN) must not appear in the message body."""
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        extraction_result = {
            "url": "https://example.com/",
            "original_url": "https://example.com/",
            "text": "text",
            "metadata": {},
            "extracted_at": "2026-06-22T00:00:00",
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "frontend",
            "url_index": 1,
            "total_urls": 3,
        }
        scraper._publish_to_kafka(extraction_result)
        msg = _decoded_msg(mock_producer)
        expected_handle = f"{_REQUEST_UUID}_url_001"
        assert expected_handle not in json.dumps(msg), (
            f"Internal cache handle {expected_handle!r} must not appear in Kafka payload"
        )

    async def test_cache_dir_uses_handle_not_raw_request_id(self, mock_producer, tmp_path):
        """request_dir must be request_id_url_NNN, not bare request_id."""
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper.extract_markdown = False
        scraper.web_searcher = MagicMock()
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.headers = {"content-type": "text/html"}
        fake_response.content = b"<html><body><p>" + b"word " * 50 + b"</p></body></html>"
        scraper.web_searcher.session.get.return_value = fake_response
        scraper.web_searcher._extract_page_metadata.return_value = {}

        message_data = {
            "uri": "https://example.com/article",
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "frontend",
            "url_index": 2,
            "total_urls": 4,
        }

        with patch.object(scraper, "_resolve_redirects", return_value="https://example.com/article"), \
             patch.object(scraper, "_extract_full_text_content", return_value="word " * 50), \
             patch.object(scraper, "_is_extraction_valid", return_value=True), \
             patch.object(scraper, "_check_content_quality", return_value={"is_quality": True, "reason": ""}), \
             patch.object(scraper, "_save_extraction") as mock_save:
            await scraper._process_message(message_data)

        # _save_extraction must have been called with a path inside request_handle dir
        assert mock_save.called
        saved_path: Path = mock_save.call_args[0][1]
        expected_handle = f"{_REQUEST_UUID}_url_002"
        assert expected_handle in str(saved_path), (
            f"Expected cache path to contain handle {expected_handle!r}, got {saved_path}"
        )
        # Must NOT use bare request_id as dir name
        bare_dir = tmp_path / "cache" / _REQUEST_UUID
        assert not bare_dir.exists(), "Cache dir must NOT be bare request_id"


# ══════════════════════════════════════════════════════════════════════════════
# TestWebScraperOutboundPassthrough
# ══════════════════════════════════════════════════════════════════════════════

class TestWebScraperOutboundPassthrough:
    """
    _publish_to_kafka and _publish_failed_url_for_summary must forward all five
    correlation fields verbatim: session_id, request_id, origin, url_index, total_urls.
    """

    def _full_extraction_result(self, url_index: int = 1, origin: str = "frontend") -> dict:
        return {
            "url": "https://example.com/page",
            "original_url": "https://example.com/page",
            "text": "text content",
            "metadata": {},
            "extracted_at": "2026-06-22T00:00:00",
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": origin,
            "url_index": url_index,
            "total_urls": 5,
        }

    def test_publish_to_kafka_includes_session_id(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_to_kafka(self._full_extraction_result())
        msg = _decoded_msg(mock_producer)
        assert msg["session_id"] == _SESSION_UUID

    def test_publish_to_kafka_includes_request_id(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_to_kafka(self._full_extraction_result())
        msg = _decoded_msg(mock_producer)
        assert msg["request_id"] == _REQUEST_UUID

    def test_publish_to_kafka_includes_origin(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_to_kafka(self._full_extraction_result(origin="direct"))
        msg = _decoded_msg(mock_producer)
        assert msg["origin"] == "direct"

    def test_publish_to_kafka_includes_url_index(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_to_kafka(self._full_extraction_result(url_index=4))
        msg = _decoded_msg(mock_producer)
        assert msg["url_index"] == 4

    def test_publish_to_kafka_includes_total_urls(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_to_kafka(self._full_extraction_result())
        msg = _decoded_msg(mock_producer)
        assert msg["total_urls"] == 5

    def test_per_url_kafka_key_uses_request_id_colon_index_suffix(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_to_kafka(self._full_extraction_result(url_index=3))
        # First produce call is to summarise-document topic
        key = mock_producer.produce.call_args_list[0][1]["key"]
        assert key == f"{_REQUEST_UUID}:003".encode()

    def test_publish_failed_url_includes_origin(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_failed_url_for_summary(
            url="https://example.com/fail",
            original_url="https://example.com/fail",
            title="Fail",
            context=[],
            error_msg="HTTP 403",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            origin="frontend",
            url_index=1,
            total_urls=3,
        )
        msg = _decoded_msg(mock_producer)
        assert msg["origin"] == "frontend"

    def test_publish_failed_url_includes_url_index(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_failed_url_for_summary(
            url="https://example.com/fail",
            original_url="https://example.com/fail",
            title="Fail",
            context=[],
            error_msg="HTTP 403",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            origin=None,
            url_index=2,
            total_urls=6,
        )
        msg = _decoded_msg(mock_producer)
        assert msg["url_index"] == 2
        assert msg["total_urls"] == 6

    def test_publish_failed_url_kafka_key_uses_request_id_colon_index(
        self, mock_producer, tmp_path
    ):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_failed_url_for_summary(
            url="https://example.com/fail",
            original_url="https://example.com/fail",
            title="Fail",
            context=[],
            error_msg="oops",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            origin=None,
            url_index=5,
            total_urls=10,
        )
        key = mock_producer.produce.call_args_list[0][1]["key"]
        assert key == f"{_REQUEST_UUID}:005".encode()

    def test_publish_failed_url_sets_extraction_failed_true(self, mock_producer, tmp_path):
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_failed_url_for_summary(
            url="https://example.com/fail",
            original_url="https://example.com/fail",
            title="Fail",
            context=[],
            error_msg="timeout",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
        )
        msg = _decoded_msg(mock_producer)
        assert msg["extraction_failed"] is True

    def test_origin_absent_defaults_to_none_in_failed_url(self, mock_producer, tmp_path):
        """origin is Optional — absent means None, not 'unknown' or empty string."""
        scraper = _make_scraper(mock_producer, tmp_path / "cache")
        scraper._publish_failed_url_for_summary(
            url="https://example.com/fail",
            original_url="https://example.com/fail",
            title="Fail",
            context=[],
            error_msg="err",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
        )
        msg = _decoded_msg(mock_producer)
        assert msg["origin"] is None
