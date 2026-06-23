"""
tests/regression/test_logging_convention.py

Phase 5 regression tests — logging convention sweep invariants.

Validates the three story-mandated logging properties across all 5 consumers:

  1. install_correlation_record_factory() stamps every LogRecord with
     session_id and request_id attributes pulled from ContextVars.
  2. CustomJsonFormatter.add_fields reads the per-record attributes
     dynamically (not a static value from __init__), so JSON logs carry
     per-message IDs.
  3. The sentinel "-" is emitted when no IDs are bound (i.e. the factory
     is installed but bind_correlation_ids has not yet been called).
"""
from __future__ import annotations

import logging

import pytest

from src.monitoring.logging_config import CustomJsonFormatter
from utils.correlation import (bind_correlation_ids,
                               install_correlation_record_factory,
                               reset_correlation_ids)

pytestmark = pytest.mark.regression

_SESSION = "11111111-1111-4111-8111-111111111111"
_REQUEST = "22222222-2222-4222-8222-222222222222"


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_record(logger_name: str = "test", message: str = "hello") -> logging.LogRecord:
    """Create a LogRecord via the current log-record factory (honours the installed chain)."""
    logger = logging.getLogger(logger_name)
    return logger.makeRecord(
        logger_name, logging.INFO, "(test)", 0, message, (), None
    )


def _format_record(record: logging.LogRecord) -> dict:
    """Run the record through CustomJsonFormatter and return parsed fields."""
    import json
    fmt = CustomJsonFormatter(consumer_id="test_consumer")
    output = fmt.format(record)
    return json.loads(output)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestRecordFactoryInstallation:
    """install_correlation_record_factory stamps every LogRecord."""

    def test_factory_stamps_session_id_after_bind(self):
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        try:
            record = _make_record()
            assert record.session_id == _SESSION
        finally:
            reset_correlation_ids(tokens)

    def test_factory_stamps_request_id_after_bind(self):
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        try:
            record = _make_record()
            assert record.request_id == _REQUEST
        finally:
            reset_correlation_ids(tokens)

    def test_factory_stamps_sentinel_when_unbound(self):
        install_correlation_record_factory()
        # No bind_correlation_ids — both must be sentinel
        record = _make_record()
        assert record.session_id == "-"
        assert record.request_id == "-"

    def test_factory_is_idempotent(self):
        """Calling install_correlation_record_factory() twice must not double-wrap."""
        install_correlation_record_factory()
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        try:
            record = _make_record()
            assert record.session_id == _SESSION
            assert record.request_id == _REQUEST
        finally:
            reset_correlation_ids(tokens)

    def test_reset_restores_sentinel(self):
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        reset_correlation_ids(tokens)
        record = _make_record()
        assert record.session_id == "-"
        assert record.request_id == "-"


class TestJsonFormatterDynamicCorrelationIds:
    """CustomJsonFormatter.add_fields reads record attributes, not static __init__ values."""

    def test_json_output_carries_session_id_from_record(self):
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        try:
            fields = _format_record(_make_record())
            assert fields["session_id"] == _SESSION
        finally:
            reset_correlation_ids(tokens)

    def test_json_output_carries_request_id_from_record(self):
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        try:
            fields = _format_record(_make_record())
            assert fields["request_id"] == _REQUEST
        finally:
            reset_correlation_ids(tokens)

    def test_json_output_uses_sentinel_when_unbound(self):
        install_correlation_record_factory()
        fields = _format_record(_make_record())
        assert fields["session_id"] == "-"
        assert fields["request_id"] == "-"

    def test_json_formatter_init_session_id_does_not_override_record(self):
        """The static session_id kwarg in __init__ must NOT bleed into per-record output."""
        install_correlation_record_factory()
        tokens = bind_correlation_ids(_SESSION, _REQUEST)
        try:
            import json
            fmt = CustomJsonFormatter(consumer_id="test", session_id="STATIC_VALUE_SHOULD_NOT_APPEAR")
            record = _make_record()
            fields = json.loads(fmt.format(record))
            # The bound ContextVar value wins; static kwarg is irrelevant
            assert fields["session_id"] == _SESSION
        finally:
            reset_correlation_ids(tokens)

    def test_different_messages_carry_different_ids(self):
        """Two consecutive binds produce different IDs in their respective records."""
        install_correlation_record_factory()

        _SESSION_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        _REQUEST_A = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        _SESSION_B = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        _REQUEST_B = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

        tok_a = bind_correlation_ids(_SESSION_A, _REQUEST_A)
        fields_a = _format_record(_make_record("a"))
        reset_correlation_ids(tok_a)

        tok_b = bind_correlation_ids(_SESSION_B, _REQUEST_B)
        fields_b = _format_record(_make_record("b"))
        reset_correlation_ids(tok_b)

        assert fields_a["session_id"] == _SESSION_A
        assert fields_b["session_id"] == _SESSION_B
        assert fields_a["session_id"] != fields_b["session_id"]
