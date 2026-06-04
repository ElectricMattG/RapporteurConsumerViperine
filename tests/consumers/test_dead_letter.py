"""
tests/consumers/test_dead_letter.py

Unit tests for KafkaDeadLetterConsumer._append_failure.

Built via __new__ so __init__ (which dials Kafka) is bypassed.
All file I/O is redirected to a tmp_path.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest


def _make_dlq(tmp_path: Path):
    """Build a minimal KafkaDeadLetterConsumer without __init__."""
    from consumers.kafka_dead_letter_consumer import KafkaDeadLetterConsumer

    inst = KafkaDeadLetterConsumer.__new__(KafkaDeadLetterConsumer)
    inst.output_dir = tmp_path / "failures"
    inst.output_dir.mkdir(parents=True, exist_ok=True)
    inst.failures_file = inst.output_dir / "failures.json"
    inst.failures_file.write_text("[]", encoding="utf-8")
    inst._logger = logging.getLogger("test.dlq")
    return inst


def _read_failures(inst) -> list:
    return json.loads(inst.failures_file.read_text(encoding="utf-8"))


class TestAppendFailure:
    def test_appends_to_failures_json(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        record = {"request_id": "req_001", "failed_stage": "summarizer", "error": "boom"}
        dlq._append_failure(record)
        failures = _read_failures(dlq)
        assert len(failures) == 1
        assert failures[0]["request_id"] == "req_001"

    def test_multiple_appends_accumulate(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        dlq._append_failure({"request_id": "req_001", "failed_stage": "scraper"})
        dlq._append_failure({"request_id": "req_002", "failed_stage": "summarizer"})
        failures = _read_failures(dlq)
        assert len(failures) == 2
        assert {f["request_id"] for f in failures} == {"req_001", "req_002"}

    def test_creates_individual_failure_file(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        dlq._append_failure({"request_id": "req_xyz", "failed_stage": "summarizer"})
        individual = list(dlq.output_dir.glob("failure_req_xyz_summarizer_*.json"))
        assert len(individual) == 1

    def test_individual_file_contains_full_record(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        record = {"request_id": "req_z", "failed_stage": "scraper", "url": "https://x.com"}
        dlq._append_failure(record)
        individual = list(dlq.output_dir.glob("failure_req_z_scraper_*.json"))[0]
        data = json.loads(individual.read_text(encoding="utf-8"))
        assert data["url"] == "https://x.com"

    def test_missing_fields_default_to_unknown(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        dlq._append_failure({"error": "no ids here"})
        individual = list(dlq.output_dir.glob("failure_unknown_unknown_*.json"))
        assert len(individual) == 1

    def test_non_ascii_preserved(self, tmp_path):
        dlq = _make_dlq(tmp_path)
        dlq._append_failure({"request_id": "req_u", "failed_stage": "s", "error": "café ☕"})
        failures = _read_failures(dlq)
        assert failures[0]["error"] == "café ☕"

    def test_corrupt_failures_file_is_handled(self, tmp_path):
        """A corrupt failures.json must not raise out of _append_failure."""
        dlq = _make_dlq(tmp_path)
        dlq.failures_file.write_text("NOT JSON", encoding="utf-8")
        # Should swallow the error internally (logged, not raised)
        dlq._append_failure({"request_id": "req_c", "failed_stage": "x"})
