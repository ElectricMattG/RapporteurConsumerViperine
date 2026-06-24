"""
tests/consumers/test_dead_letter_correlation.py

Phase 4 — correlation field coverage for KafkaDeadLetterConsumer.

Tests verify that:
  - `origin` is parsed from inbound DLQ messages (with 'unknown' default)
  - `origin` is logged in the processing prefix block
  - individual failure files and failures.json contain all three correlation fields
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.anyio

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"


def _make_dlq(tmp_path: Path):
    """Build a minimal KafkaDeadLetterConsumer without __init__."""
    from consumers.kafka_dead_letter_consumer import KafkaDeadLetterConsumer

    inst = KafkaDeadLetterConsumer.__new__(KafkaDeadLetterConsumer)
    inst.output_dir = tmp_path / "failures"
    inst.output_dir.mkdir(parents=True, exist_ok=True)
    inst.failures_file = inst.output_dir / "failures.json"
    inst.failures_file.write_text("[]", encoding="utf-8")
    inst._logger = logging.getLogger("test.dlq")
    inst.telemetry_enabled = False
    return inst


def _read_failures(inst) -> list:
    return json.loads(inst.failures_file.read_text(encoding="utf-8"))


def _full_failure(origin: str = "frontend") -> dict:
    return {
        "request_id": _REQUEST_UUID,
        "session_id": _SESSION_UUID,
        "origin": origin,
        "failed_stage": "summarizer",
        "error": "LLM quota exhausted",
        "timestamp": "2026-06-22T10:00:00+00:00",
        "origin_topic": "viperine.summarise-document",
    }


# ── TestDeadLetterInboundCorrelationParsing ───────────────────────────────────

class TestDeadLetterInboundCorrelationParsing:
    async def test_parses_origin_alongside_session_request(self, tmp_path, caplog):
        dlq = _make_dlq(tmp_path)
        with caplog.at_level(logging.INFO, logger="test.dlq"):
            await dlq.process_message(_full_failure(origin="frontend"))
        assert "frontend" in caplog.text

    async def test_origin_defaults_to_unknown_when_absent(self, tmp_path, caplog):
        dlq = _make_dlq(tmp_path)
        msg = _full_failure()
        del msg["origin"]
        with caplog.at_level(logging.INFO, logger="test.dlq"):
            await dlq.process_message(msg)
        assert "unknown" in caplog.text

    async def test_origin_logged_in_prefix_block(self, tmp_path, caplog):
        dlq = _make_dlq(tmp_path)
        with caplog.at_level(logging.INFO, logger="test.dlq"):
            await dlq.process_message(_full_failure(origin="api-direct"))
        # The prefix block must log "Origin:" line
        assert "Origin:" in caplog.text
        assert "api-direct" in caplog.text


# ── TestDeadLetterFailureFileCorrelation ─────────────────────────────────────

class TestDeadLetterFailureFileCorrelation:
    async def test_individual_failure_file_contains_session_id(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        await dlq.process_message(_full_failure())
        files = list(dlq.output_dir.glob("failure_*.json"))
        assert len(files) == 1
        data = json.loads(files[0].read_text(encoding="utf-8"))
        assert data.get("session_id") == _SESSION_UUID

    async def test_individual_failure_file_contains_request_id(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        await dlq.process_message(_full_failure())
        files = list(dlq.output_dir.glob("failure_*.json"))
        data = json.loads(files[0].read_text(encoding="utf-8"))
        assert data.get("request_id") == _REQUEST_UUID

    async def test_individual_failure_file_contains_origin(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        await dlq.process_message(_full_failure(origin="api-direct"))
        files = list(dlq.output_dir.glob("failure_*.json"))
        data = json.loads(files[0].read_text(encoding="utf-8"))
        assert data.get("origin") == "api-direct"

    async def test_failures_json_aggregate_contains_correlation_for_every_entry(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        await dlq.process_message(_full_failure(origin="frontend"))
        await dlq.process_message({
            "request_id": "33333333-3333-4333-8333-333333333333",
            "session_id": _SESSION_UUID,
            "origin": "api-direct",
            "failed_stage": "web_scraper",
            "error": "timeout",
        })
        failures = _read_failures(dlq)
        assert len(failures) == 2
        for record in failures:
            assert "session_id" in record
            assert "request_id" in record
            assert "origin" in record
