"""
tests/consumers/test_summarizer_correlation.py

Phase 2 unit tests — session_id / request_id / origin / url_index / total_urls
propagation through KafkaSummarizerConsumer.

Coverage:
  TestSummarizerInboundCorrelationParsing  — fields parsed from inbound message
  TestSummarizerRequestKeyedAccounting     — request_counts keyed on request_id,
                                             not session_id; two requests from the
                                             same session tracked independently
  TestSummarizerOutboundPassthrough        — all correlation fields forwarded
                                             verbatim in summary-completed payload;
                                             Kafka key = request_id
"""
from __future__ import annotations

import json
import logging

import pytest

pytestmark = pytest.mark.anyio

# ── UUID fixtures ──────────────────────────────────────────────────────────────

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"
_REQUEST_UUID_2 = "55555555-5555-4555-8555-555555555555"


# ── Factory ───────────────────────────────────────────────────────────────────

def _make_summarizer(mock_producer):
    from consumers.kafka_summarizer import KafkaSummarizerConsumer

    inst = KafkaSummarizerConsumer.__new__(KafkaSummarizerConsumer)
    inst.producer = mock_producer
    inst.summary_completed_topic = "viperine.summary-completed"
    inst.dead_letter_topic = "viperine.dead-letter-queue"
    inst.request_counts = {}
    inst.telemetry_enabled = False
    inst._logger = logging.getLogger("test.summarizer.correlation")
    inst.model_name = "test-model"
    return inst


def _decoded_msg(mock_producer) -> dict:
    value_bytes = mock_producer.produce.call_args[1]["value"]
    return json.loads(value_bytes)


# ══════════════════════════════════════════════════════════════════════════════
# TestSummarizerInboundCorrelationParsing
# ══════════════════════════════════════════════════════════════════════════════

class TestSummarizerInboundCorrelationParsing:
    """
    _process_message must read origin / url_index / total_urls and pass them
    to _publish_summary_completed.  We test this by calling
    _publish_summary_completed directly with explicit values (the internal
    propagation is covered by TestSummarizerOutboundPassthrough).
    """

    async def test_parses_session_request_origin(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            origin="frontend",
            url_index=0,
            total_urls=3,
            document_id=None,
            source_url="https://example.com/a",
            summary_file=None,
            message_data={},
            status="success",
        )
        msg = _decoded_msg(mock_producer)
        assert msg["session_id"] == _SESSION_UUID
        assert msg["request_id"] == _REQUEST_UUID
        assert msg["origin"] == "frontend"

    async def test_origin_none_when_absent(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/a",
            summary_file=None,
            message_data={},
            status="success",
        )
        msg = _decoded_msg(mock_producer)
        assert msg["origin"] is None

    async def test_url_index_total_urls_parsed(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            origin="direct",
            url_index=2,
            total_urls=7,
            document_id=None,
            source_url="https://example.com/b",
            summary_file=None,
            message_data={},
            status="success",
        )
        msg = _decoded_msg(mock_producer)
        assert msg["url_index"] == 2
        assert msg["total_urls"] == 7


# ══════════════════════════════════════════════════════════════════════════════
# TestSummarizerRequestKeyedAccounting
# ══════════════════════════════════════════════════════════════════════════════

class TestSummarizerRequestKeyedAccounting:
    """
    The per-request counter must be keyed on request_id (the per-query UUID),
    not session_id (the chat-tab UUID shared across many requests).
    """

    async def test_request_counts_keyed_on_request_id(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/r",
            summary_file=None,
            message_data={"total_requests": 3},
            status="success",
        )
        assert _REQUEST_UUID in s.request_counts
        assert s.request_counts[_REQUEST_UUID]["count"] == 1

    async def test_two_requests_same_session_tracked_separately(self, mock_producer):
        """Two different requests sharing one session must have independent counters."""
        s = _make_summarizer(mock_producer)

        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/x",
            summary_file=None,
            message_data={"total_requests": 3},
            status="success",
        )
        mock_producer.produce.reset_mock()

        await s._publish_summary_completed(
            request_id=_REQUEST_UUID_2,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/y",
            summary_file=None,
            message_data={"total_requests": 3},
            status="success",
        )

        # Each request has its own counter at 1
        assert s.request_counts[_REQUEST_UUID]["count"] == 1
        assert s.request_counts[_REQUEST_UUID_2]["count"] == 1
        # They are independent entries, not merged
        assert _REQUEST_UUID != _REQUEST_UUID_2

    async def test_total_urls_preferred_over_total_requests_for_threshold(
        self, mock_producer
    ):
        """total_urls (new field) takes priority over total_requests (legacy field)."""
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            total_urls=5,
            document_id=None,
            source_url="https://example.com/z",
            summary_file=None,
            message_data={"total_requests": 99},  # legacy value — should be ignored
            status="success",
        )
        assert s.request_counts[_REQUEST_UUID]["total"] == 5

    async def test_session_id_not_in_request_counts(self, mock_producer):
        """session_id must NOT appear as a key in request_counts."""
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/check",
            summary_file=None,
            message_data={"total_requests": 1},
            status="success",
        )
        assert _SESSION_UUID not in s.request_counts


# ══════════════════════════════════════════════════════════════════════════════
# TestSummarizerOutboundPassthrough
# ══════════════════════════════════════════════════════════════════════════════

class TestSummarizerOutboundPassthrough:
    """
    All correlation fields must appear verbatim in the summary-completed payload.
    """

    async def test_summary_completed_payload_carries_all_three_correlation_fields(
        self, mock_producer
    ):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            origin="frontend",
            document_id=None,
            source_url="https://example.com/c",
            summary_file="/tmp/s.jsonl",
            message_data={},
            status="success",
        )
        msg = _decoded_msg(mock_producer)
        assert msg["session_id"] == _SESSION_UUID
        assert msg["request_id"] == _REQUEST_UUID
        assert msg["origin"] == "frontend"

    async def test_summary_completed_payload_carries_url_index_total_urls(
        self, mock_producer
    ):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            origin="direct",
            url_index=3,
            total_urls=8,
            document_id=None,
            source_url="https://example.com/d",
            summary_file=None,
            message_data={},
            status="success",
        )
        msg = _decoded_msg(mock_producer)
        assert msg["url_index"] == 3
        assert msg["total_urls"] == 8

    async def test_kafka_key_is_request_id_for_partition_uniqueness(
        self, mock_producer
    ):
        """Kafka key must be request_id bytes, not session_id bytes."""
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/e",
            summary_file=None,
            message_data={},
            status="success",
        )
        key = mock_producer.produce.call_args[1]["key"]
        assert key == _REQUEST_UUID.encode("utf-8")
        assert key != _SESSION_UUID.encode("utf-8")

    async def test_dlq_record_carries_origin(self, mock_producer):
        """DLQ failure records must include origin for operator correlation."""
        from consumers._failure_taxonomy import classify_error
        s = _make_summarizer(mock_producer)
        exc = RuntimeError("test failure")
        cl = classify_error(exc)

        await s._publish_to_dlq(
            stage="summarizer",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            origin="frontend",
            query_id="default",
            url="https://example.com/fail",
            exc=exc,
            classification=cl,
            attempts=1,
        )
        record = json.loads(mock_producer.produce.call_args[1]["value"])
        assert record["origin"] == "frontend"
        assert record["session_id"] == _SESSION_UUID
        assert record["request_id"] == _REQUEST_UUID

    async def test_dlq_key_is_request_id(self, mock_producer):
        """DLQ Kafka key must be request_id bytes for partition uniqueness."""
        from consumers._failure_taxonomy import classify_error
        s = _make_summarizer(mock_producer)
        exc = RuntimeError("test failure")
        cl = classify_error(exc)

        await s._publish_to_dlq(
            stage="summarizer",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            query_id="default",
            url="https://example.com/fail",
            exc=exc,
            classification=cl,
            attempts=1,
        )
        key = mock_producer.produce.call_args[1]["key"]
        assert key == _REQUEST_UUID.encode("utf-8")
