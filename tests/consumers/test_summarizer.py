"""
tests/consumers/test_summarizer.py

Unit tests for KafkaSummaryIndexerConsumer._publish_summary_completed
and _publish_to_dlq.

We test the two methods that were the focus of the Phase A remediation:
  1. _publish_summary_completed — generates doc_id + dedup_key, produces message
  2. _publish_to_dlq           — produces DLQ record with correct schema

KafkaSummaryConsumer is constructed via __new__ so __init__ is bypassed.
All Kafka I/O goes through mock_producer (from conftest.py).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.anyio

# ── UUID fixtures (Phase 2 — replaces legacy query_* strings) ─────────────────

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"


# ── Factory ────────────────────────────────────────────────────────────────────

def _make_summarizer(mock_producer):
    """Build a minimal KafkaSummarizerConsumer without calling __init__."""
    from consumers.kafka_summarizer import KafkaSummarizerConsumer

    inst = KafkaSummarizerConsumer.__new__(KafkaSummarizerConsumer)
    inst.producer = mock_producer
    inst.summary_completed_topic = "viperine.summary-completed"
    inst.dead_letter_topic = "viperine.dead-letter-queue"
    inst.request_counts = {}  # Phase 2: keyed on request_id
    inst.telemetry_enabled = False
    inst._logger = logging.getLogger("test.summarizer")
    inst.model_name = "test-model"
    return inst


# ── TestPayloadSchema ──────────────────────────────────────────────────────────

class TestPayloadSchema:
    """_publish_summary_completed payload must carry required fields."""

    async def test_required_fields_present(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/page",
            summary_file="/tmp/summary.jsonl",
            message_data={"total_requests": 3},
            status="success",
        )

        assert mock_producer.produce.called
        call_kwargs = mock_producer.produce.call_args
        value_bytes = call_kwargs[1]["value"] if call_kwargs[1] else call_kwargs[0][1]
        msg = json.loads(value_bytes)

        for field in ("request_id", "session_id", "origin", "document_id", "dedup_key",
                      "source_url", "summary_file", "status",
                      "current_count", "total_requests", "completed_at"):
            assert field in msg, f"Missing field: {field}"

    async def test_document_id_format_req_prefix(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/page",
            summary_file="/tmp/summary.jsonl",
            message_data={"total_requests": 3},
            status="success",
        )
        value_bytes = mock_producer.produce.call_args[1]["value"]
        msg = json.loads(value_bytes)
        assert msg["document_id"].startswith("req_")

    async def test_dedup_key_is_32_char_hex(self, mock_producer):
        import re
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/page",
            summary_file="/tmp/summary.jsonl",
            message_data={"total_requests": 3},
            status="success",
        )
        value_bytes = mock_producer.produce.call_args[1]["value"]
        msg = json.loads(value_bytes)
        assert re.fullmatch(r"[0-9a-f]{32}", msg["dedup_key"]), (
            f"dedup_key must be 32 lowercase hex: {msg['dedup_key']!r}"
        )

    async def test_status_field_propagated(self, mock_producer):
        s = _make_summarizer(mock_producer)
        for i, status in enumerate(("success", "degraded", "failed")):
            mock_producer.produced.clear()
            mock_producer.produce.reset_mock()
            # Each call uses a distinct request_id to avoid shared counter state
            req_id = f"33333333-3333-4333-8333-{i:012d}"
            await s._publish_summary_completed(
                request_id=req_id,
                session_id=_SESSION_UUID,
                document_id=None,
                source_url="https://example.com/s",
                summary_file=None,
                message_data={"total_requests": 1},
                status=status,
            )
            value_bytes = mock_producer.produce.call_args[1]["value"]
            msg = json.loads(value_bytes)
            assert msg["status"] == status


# ── TestTerminalEvent ──────────────────────────────────────────────────────────

class TestTerminalEvent:
    """_publish_summary_completed must be called for all three outcome paths."""

    async def test_called_once_for_success(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/te",
            summary_file="/tmp/ok.jsonl",
            message_data={"total_requests": 5},
            status="success",
        )
        assert mock_producer.produce.call_count == 1

    async def test_called_once_for_degraded(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/te",
            summary_file="/tmp/deg.jsonl",
            message_data={"total_requests": 5},
            status="degraded",
        )
        assert mock_producer.produce.call_count == 1

    async def test_called_once_for_failed(self, mock_producer):
        s = _make_summarizer(mock_producer)
        await s._publish_summary_completed(
            request_id=_REQUEST_UUID,
            session_id=_SESSION_UUID,
            document_id=None,
            source_url="https://example.com/te",
            summary_file=None,
            message_data={"total_requests": 5},
            status="failed",
        )
        assert mock_producer.produce.call_count == 1

    async def test_request_count_increments(self, mock_producer):
        """Per-request counter must be keyed on request_id, not session_id."""
        s = _make_summarizer(mock_producer)
        for i in range(3):
            # Same request_id — simulates 3 URLs from one request completing sequentially
            await s._publish_summary_completed(
                request_id=_REQUEST_UUID,
                session_id=_SESSION_UUID,
                document_id=None,
                source_url=f"https://example.com/page{i}",
                summary_file="/tmp/f.jsonl",
                message_data={"total_requests": 5},
                status="success",
            )
        assert s.request_counts[_REQUEST_UUID]["count"] == 3


# ── TestDlqPublish ────────────────────────────────────────────────────────────

class TestDlqPublish:
    """_publish_to_dlq must produce to viperine.dead-letter-queue with correct schema."""

    async def test_produces_to_dlq_topic(self, mock_producer):
        s = _make_summarizer(mock_producer)
        from consumers._failure_taxonomy import classify_error
        exc = RuntimeError("some hard failure")
        cl = classify_error(exc)

        await s._publish_to_dlq(
            stage="summarizer",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            query_id="default",
            url="https://example.com/fail",
            exc=exc,
            classification=cl,
            attempts=3,
        )

        assert mock_producer.produce.called
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "viperine.dead-letter-queue"

    async def test_dlq_schema_fields(self, mock_producer):
        s = _make_summarizer(mock_producer)
        from consumers._failure_taxonomy import classify_error
        exc = ConnectionError("timeout")
        cl = classify_error(exc)

        await s._publish_to_dlq(
            stage="summarizer",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            query_id="default",
            url="https://example.com/timeout",
            exc=exc,
            classification=cl,
            attempts=2,
        )

        value_bytes = mock_producer.produce.call_args[1]["value"]
        record = json.loads(value_bytes)

        for field in ("stage", "category", "session_id", "request_id", "origin",
                      "url", "error_class", "error_message",
                      "occurred_at", "attempts", "retryable"):
            assert field in record, f"DLQ schema missing field: {field}"

    async def test_dlq_category_from_classification(self, mock_producer):
        s = _make_summarizer(mock_producer)
        from consumers._failure_taxonomy import classify_error
        exc = ValueError("bad value")
        cl = classify_error(exc)

        await s._publish_to_dlq(
            stage="summarizer",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            query_id="default",
            url="https://example.com/bad",
            exc=exc,
            classification=cl,
            attempts=1,
        )

        value_bytes = mock_producer.produce.call_args[1]["value"]
        record = json.loads(value_bytes)
        assert record["category"] == "permanent_internal"
        assert record["retryable"] is False

    async def test_dlq_stage_field(self, mock_producer):
        s = _make_summarizer(mock_producer)
        from consumers._failure_taxonomy import classify_error
        exc = Exception("generic")
        cl = classify_error(exc)

        await s._publish_to_dlq(
            stage="summarizer",
            session_id=_SESSION_UUID,
            request_id=_REQUEST_UUID,
            query_id="default",
            url="https://example.com/g",
            exc=exc,
            classification=cl,
            attempts=1,
        )

        value_bytes = mock_producer.produce.call_args[1]["value"]
        record = json.loads(value_bytes)
        assert record["stage"] == "summarizer"
