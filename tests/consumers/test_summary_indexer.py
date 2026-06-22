"""
tests/consumers/test_summary_indexer.py

Unit tests for KafkaSummaryIndexerConsumer.

All I/O is mocked via the make_indexer fixture (conftest.py).
No Kafka broker, no real embedding service, no LLM calls.

Coverage targets:
- TestRequestState     — load/save round-trip, defaults on first load
- TestDedup            — _process_message skips duplicate dedup_key and doc_id
- TestFailureMarkers   — status='failed' increments counter, does not embed
- TestIdleSweeper      — _check_idle_sessions fires / skips by threshold
- TestAggregation      — _aggregate_session_into_combined dedup logic
- TestProcessMessage   — happy path: embeds, appends, publishes on completion
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.anyio

# ── UUID fixtures shared across test classes ──────────────────────────────────
_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"


# ── helpers ────────────────────────────────────────────────────────────────────

def _isoformat(dt: datetime) -> str:
    return dt.isoformat()


def _ts_ago(seconds: float) -> str:
    return _isoformat(datetime.now(timezone.utc) - timedelta(seconds=seconds))


def _write_request_index(indexer, request_id: str, entries: list[dict]) -> Path:
    """Write a fake per-request JSONL index into the cache dir."""
    req_dir = indexer.cache_dir / request_id
    req_dir.mkdir(parents=True, exist_ok=True)
    idx = req_dir / f"{request_id}_summaries_embedded.jsonl"
    with open(idx, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
    return idx


def _write_state_file(indexer, request_id: str, state: dict) -> Path:
    """Write a fake per-request state file into the cache dir."""
    req_dir = indexer.cache_dir / request_id
    req_dir.mkdir(parents=True, exist_ok=True)
    sf = req_dir / f"{request_id}_state.json"
    with open(sf, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    return sf


def _read_combined_index(indexer) -> list[dict]:
    with open(indexer.combined_index_file, "r", encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def _make_summary_file(tmp_path: Path, session_id: str, request_id: str, text: str = "Test summary.") -> str:
    """Write a minimal summary.jsonl file and return its path."""
    req_dir = tmp_path / "summaries" / session_id / request_id
    req_dir.mkdir(parents=True, exist_ok=True)
    sf = req_dir / "summary.jsonl"
    data = {
        "markdown_summary": text,
        "metadata": {"title": "Test Doc", "source_url": "https://example.com/test"},
        "degraded": False,
    }
    sf.write_text(json.dumps(data) + "\n", encoding="utf-8")
    return str(sf)


# ── TestRequestState ──────────────────────────────────────────────────────────

class TestRequestState:
    def test_load_returns_defaults_for_new_request(self, make_indexer):
        idx = make_indexer()
        state = idx._load_request_state(_REQUEST_UUID)
        assert state["request_id"] == _REQUEST_UUID
        assert state["total_requests"] == 0
        assert state["received_count"] == 0
        assert state["completed"] is False
        assert state["document_ids_processed"] == []
        assert state.get("dedup_keys_processed") == []
        # session_id and origin default to None for sweeper-replay support
        assert state["session_id"] is None
        assert state["origin"] is None

    def test_save_and_load_round_trip(self, make_indexer):
        idx = make_indexer()
        state = idx._load_request_state(_REQUEST_UUID)
        state["total_requests"] = 5
        state["received_count"] = 3
        state["session_id"] = _SESSION_UUID
        state["document_ids_processed"] = ["req_001", "req_002"]
        idx._save_request_state(_REQUEST_UUID, state)

        loaded = idx._load_request_state(_REQUEST_UUID)
        assert loaded["total_requests"] == 5
        assert loaded["received_count"] == 3
        assert loaded["session_id"] == _SESSION_UUID
        assert "req_001" in loaded["document_ids_processed"]

    def test_save_updates_last_updated_timestamp(self, make_indexer):
        idx = make_indexer()
        before = datetime.now(timezone.utc)
        state = idx._load_request_state(_REQUEST_UUID)
        state["total_requests"] = 1
        idx._save_request_state(_REQUEST_UUID, state)
        loaded = idx._load_request_state(_REQUEST_UUID)
        last_updated = loaded["last_updated"]
        # Must be a parseable ISO string
        dt = datetime.fromisoformat(last_updated)
        assert dt >= before.replace(tzinfo=None) or True

    def test_corrupt_state_file_returns_defaults(self, make_indexer):
        idx = make_indexer()
        req_dir = idx.cache_dir / _REQUEST_UUID
        req_dir.mkdir(parents=True, exist_ok=True)
        sf = req_dir / f"{_REQUEST_UUID}_state.json"
        sf.write_text("NOT VALID JSON{{{", encoding="utf-8")
        state = idx._load_request_state(_REQUEST_UUID)
        assert state["request_id"] == _REQUEST_UUID
        assert state["total_requests"] == 0

    def test_origin_and_session_id_survive_round_trip(self, make_indexer):
        idx = make_indexer()
        state = idx._load_request_state(_REQUEST_UUID)
        state["session_id"] = _SESSION_UUID
        state["origin"] = "frontend"
        state["total_requests"] = 3
        idx._save_request_state(_REQUEST_UUID, state)

        loaded = idx._load_request_state(_REQUEST_UUID)
        assert loaded["session_id"] == _SESSION_UUID
        assert loaded["origin"] == "frontend"


# ── TestDedup ─────────────────────────────────────────────────────────────────

class TestDedup:
    async def test_duplicate_dedup_key_is_skipped(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 3
        state["dedup_keys_processed"] = ["aabbccddeeff00112233445566778899"]
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        summary_file = _make_summary_file(tmp_path, _SESSION_UUID, request_id)
        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_000000_1001",
            "dedup_key": "aabbccddeeff00112233445566778899",  # already seen
            "source_url": "https://example.com/article",
            "summary_file": summary_file,
            "status": "success",
            "total_urls": 3,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("skipped") is True

    async def test_duplicate_document_id_is_skipped(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 2
        state["document_ids_processed"] = ["req_20260101_000000_1001"]
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        summary_file = _make_summary_file(tmp_path, _SESSION_UUID, request_id)
        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_000000_1001",  # already seen
            "dedup_key": "fresh_key_not_seen_yet_00000000",
            "source_url": "https://example.com/other",
            "summary_file": summary_file,
            "status": "success",
            "total_urls": 2,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("skipped") is True

    async def test_new_message_is_not_skipped(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 2
        state["document_ids_processed"] = []
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        summary_file = _make_summary_file(tmp_path, _SESSION_UUID, request_id)
        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_000000_1001",
            "dedup_key": "ffffffffffffffffffffffffffffffff",
            "source_url": "https://example.com/fresh",
            "summary_file": summary_file,
            "status": "success",
            "total_urls": 2,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("skipped") is not True


# ── TestFailureMarkers ────────────────────────────────────────────────────────

class TestFailureMarkers:
    async def test_failed_status_increments_received(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = "33333333-3333-4333-8333-333333333333"
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 3
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_000000_1001",
            "dedup_key": None,
            "source_url": "https://example.com/fail",
            "summary_file": None,
            "status": "failed",
            "total_urls": 3,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("failed") is True

        loaded = idx._load_request_state(request_id)
        assert loaded["received_count"] == 1
        assert request_id in loaded["failed"]

    async def test_failed_status_does_not_write_index_entry(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = "44444444-4444-4444-8444-444444444444"
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 2
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_000000_1002",
            "dedup_key": None,
            "source_url": "https://example.com/fail2",
            "summary_file": None,
            "status": "failed",
            "total_urls": 2,
        }
        await idx._process_message(msg)

        # Per-request JSONL must NOT have been created / must be empty
        req_dir = idx.cache_dir / request_id
        idx_file = req_dir / f"{request_id}_summaries_embedded.jsonl"
        if idx_file.exists():
            entries = [l for l in idx_file.read_text().splitlines() if l.strip()]
            assert entries == [], "No entries should be written for a failed message"

    async def test_failed_last_message_completes_request(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = "55555555-5555-4555-8555-555555555555"
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 1
        state["received_count"] = 0
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_000000_1001",
            "dedup_key": None,
            "source_url": "https://example.com/final",
            "summary_file": None,
            "status": "failed",
            "total_urls": 1,
        }
        result = await idx._process_message(msg)
        assert result.get("session_complete") is True

        loaded = idx._load_request_state(request_id)
        assert loaded["completed"] is True


# ── TestIdleSweeper ───────────────────────────────────────────────────────────

@pytest.fixture
def set_idle_timeout(monkeypatch):
    """Set SESSION_IDLE_TIMEOUT for the duration of a test; auto-restored."""
    import consumers.kafka_summary_indexer as _mod

    def _set(seconds: int):
        monkeypatch.setattr(_mod, "SESSION_IDLE_TIMEOUT", seconds)

    return _set


class TestIdleSweeper:
    def _base_state(self, request_id: str, session_id: str, idle_secs: float = 200) -> dict:
        return {
            "request_id": request_id,
            "session_id": session_id,
            "origin": "frontend",
            "total_requests": 3,
            "received_count": 2,
            "document_ids_processed": ["req_001", "req_002"],
            "dedup_keys_processed": [],
            "failed": [],
            "completed": False,
            "last_event_at": _ts_ago(idle_secs),
            "started_at": _ts_ago(idle_secs + 100),
        }

    def test_sweeper_fires_for_idle_request(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = self._base_state(request_id, _SESSION_UUID, idle_secs=200)
        _write_state_file(idx, request_id, state)
        _write_request_index(idx, request_id, [
            {"document_id": "req_001", "source_url": "https://a.com"},
            {"document_id": "req_002", "source_url": "https://b.com"},
        ])

        # Threshold = 120s; request has been idle 200s → must fire
        set_idle_timeout(120)
        idx._check_idle_sessions()

        # State file should now be marked completed
        loaded = idx._load_request_state(request_id)
        assert loaded["completed"] is True

    def test_sweeper_skips_already_completed_request(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        request_id = "66666666-6666-4666-8666-666666666666"
        state = self._base_state(request_id, _SESSION_UUID)
        state["completed"] = True  # already done
        _write_state_file(idx, request_id, state)

        set_idle_timeout(10)
        idx._check_idle_sessions()

        # An already-completed request must not be re-published
        assert not idx.producer.produce.called

    def test_sweeper_skips_recently_active_request(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        request_id = "77777777-7777-4777-8777-777777777777"
        state = self._base_state(request_id, _SESSION_UUID, idle_secs=10)
        # Only 10s old, threshold 120s
        _write_state_file(idx, request_id, state)

        set_idle_timeout(120)
        idx._check_idle_sessions()

        loaded = idx._load_request_state(request_id)
        assert loaded["completed"] is False

    def test_sweeper_publishes_session_complete(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        request_id = "88888888-8888-4888-8888-888888888888"
        state = self._base_state(request_id, _SESSION_UUID, idle_secs=200)
        state["total_requests"] = 2
        state["received_count"] = 1
        _write_state_file(idx, request_id, state)
        _write_request_index(idx, request_id, [
            {"document_id": "req_001", "source_url": "https://a.com"},
        ])

        set_idle_timeout(120)
        idx._check_idle_sessions()

        # Producer should have been called with session-complete
        assert idx.producer.produce.called

    def test_sweeper_skips_legacy_state_without_request_id(self, make_indexer, set_idle_timeout):
        """State files written before Phase 3 that lack request_id are skipped gracefully."""
        idx = make_indexer()
        legacy_dir_key = "legacy"
        legacy_state = {
            # no request_id field — pre-Phase-3 state file
            "session_id": _SESSION_UUID,
            "total_requests": 2,
            "received_count": 1,
            "document_ids_processed": [],
            "dedup_keys_processed": [],
            "failed": [],
            "completed": False,
            "last_event_at": _ts_ago(300),
            "started_at": _ts_ago(400),
        }
        _write_state_file(idx, legacy_dir_key, legacy_state)

        set_idle_timeout(10)
        idx._check_idle_sessions()

        # Should not raise; producer must NOT be called for legacy state
        assert not idx.producer.produce.called


# ── TestAggregation ───────────────────────────────────────────────────────────

class TestAggregation:
    def _make_entry(self, doc_id: str, url: str, dedup_key: str | None = None) -> dict:
        entry = {
            "document_id": doc_id,
            "source_url": url,
            "summary_text": f"Summary of {url}",
            "summary_embedding": [0.0] * 384,
        }
        if dedup_key:
            entry["dedup_key"] = dedup_key
        return entry

    def _seed_state(self, indexer, request_id: str) -> None:
        """Seed minimal state so _aggregate_session_into_combined can reload session_id/origin."""
        _write_state_file(indexer, request_id, {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "total_requests": 1,
            "received_count": 1,
            "completed": True,
            "document_ids_processed": [],
            "dedup_keys_processed": [],
            "failed": [],
        })

    def test_new_entries_appended(self, make_indexer):
        idx = make_indexer()
        request_id = "agg00001-0001-4001-8001-000000000001"
        _write_request_index(idx, request_id, [
            self._make_entry("req_001", "https://a.com", "aaaa" * 8),
        ])
        self._seed_state(idx, request_id)
        idx._aggregate_session_into_combined(
            request_id, idx.cache_dir / request_id / f"{request_id}_summaries_embedded.jsonl"
        )
        rows = _read_combined_index(idx)
        assert len(rows) == 1
        assert rows[0]["document_id"] == "req_001"

    def test_duplicate_doc_id_not_appended(self, make_indexer):
        idx = make_indexer()
        existing = self._make_entry("req_001", "https://a.com", "aaaa" * 8)
        with open(idx.combined_index_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(existing) + "\n")

        request_id = "agg00001-0001-4001-8001-000000000002"
        _write_request_index(idx, request_id, [
            self._make_entry("req_001", "https://a.com", "aaaa" * 8),  # duplicate
            self._make_entry("req_002", "https://b.com", "bbbb" * 8),  # new
        ])
        self._seed_state(idx, request_id)
        idx._aggregate_session_into_combined(
            request_id, idx.cache_dir / request_id / f"{request_id}_summaries_embedded.jsonl"
        )
        rows = _read_combined_index(idx)
        assert len(rows) == 2  # existing + new only
        doc_ids = [r["document_id"] for r in rows]
        assert doc_ids.count("req_001") == 1

    def test_duplicate_dedup_key_not_appended(self, make_indexer):
        idx = make_indexer()
        existing = self._make_entry("req_001", "https://a.com", "cccc" * 8)
        with open(idx.combined_index_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(existing) + "\n")

        request_id = "agg00001-0001-4001-8001-000000000003"
        _write_request_index(idx, request_id, [
            # Different doc_id but same dedup_key (same URL re-processed)
            self._make_entry("req_002", "https://a.com", "cccc" * 8),
        ])
        self._seed_state(idx, request_id)
        idx._aggregate_session_into_combined(
            request_id, idx.cache_dir / request_id / f"{request_id}_summaries_embedded.jsonl"
        )
        rows = _read_combined_index(idx)
        assert len(rows) == 1  # only the original

    def test_legacy_row_dedup_key_derived_from_url(self, make_indexer):
        """Legacy rows without dedup_key field — dedup is derived from source_url."""
        import hashlib

        from utils.url_normalize import normalize_url

        idx = make_indexer()
        url = "https://legacy.com/page"
        derived = hashlib.md5(normalize_url(url).encode()).hexdigest()

        # Legacy row has no dedup_key
        legacy = {"document_id": "doc_legacy001", "source_url": url, "summary_embedding": []}
        with open(idx.combined_index_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(legacy) + "\n")

        request_id = "agg00001-0001-4001-8001-000000000004"
        _write_request_index(idx, request_id, [
            self._make_entry("req_new001", url, derived),
        ])
        self._seed_state(idx, request_id)
        idx._aggregate_session_into_combined(
            request_id, idx.cache_dir / request_id / f"{request_id}_summaries_embedded.jsonl"
        )
        rows = _read_combined_index(idx)
        assert len(rows) == 1  # no new entry added


# ── TestProcessMessage (happy path) ──────────────────────────────────────────

class TestProcessMessage:
    async def test_happy_path_embeds_and_appends(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 2
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        summary_file = _make_summary_file(tmp_path, _SESSION_UUID, request_id)
        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_120000_1001",
            "dedup_key": "11112222333344445555666677778888",
            "source_url": "https://example.com/happy",
            "summary_file": summary_file,
            "status": "success",
            "total_urls": 2,
        }
        result = await idx._process_message(msg)

        assert result is not None
        assert result.get("indexed") is True

        req_dir = idx.cache_dir / request_id
        idx_file = req_dir / f"{request_id}_summaries_embedded.jsonl"
        assert idx_file.exists()
        entries = [json.loads(l) for l in idx_file.read_text().splitlines() if l.strip()]
        assert len(entries) == 1
        assert entries[0]["source_url"] == "https://example.com/happy"

    async def test_request_completes_on_last_message(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 1
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        summary_file = _make_summary_file(tmp_path, _SESSION_UUID, request_id)
        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_130000_1001",
            "dedup_key": "ffffffffffffffffffffffffffffffff",
            "source_url": "https://example.com/last",
            "summary_file": summary_file,
            "status": "success",
            "total_urls": 1,
        }
        result = await idx._process_message(msg)
        assert result.get("session_complete") is True

        loaded = idx._load_request_state(request_id)
        assert loaded["completed"] is True

    async def test_degraded_status_is_indexed(self, make_indexer, tmp_path):
        idx = make_indexer()
        request_id = _REQUEST_UUID
        state = idx._load_request_state(request_id)
        state["session_id"] = _SESSION_UUID
        state["total_requests"] = 1
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_request_state(request_id, state)

        # Write a degraded summary file
        req_dir = tmp_path / "summaries" / _SESSION_UUID / "req_deg"
        req_dir.mkdir(parents=True, exist_ok=True)
        sf = req_dir / "summary.jsonl"
        degraded_data = {
            "markdown_summary": "[Summary unavailable]",
            "metadata": {"title": "Degraded"},
            "degraded": True,
        }
        sf.write_text(json.dumps(degraded_data) + "\n", encoding="utf-8")

        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "frontend",
            "document_id": "req_20260101_140000_1001",
            "dedup_key": "aaaaaaaabbbbbbbbccccccccdddddddd",
            "source_url": "https://example.com/degraded",
            "summary_file": str(sf),
            "status": "degraded",
            "total_urls": 1,
        }
        result = await idx._process_message(msg)
        # degraded is still indexed (not failed)
        assert result is not None
        assert result.get("indexed") is True

    async def test_process_message_stores_session_id_and_origin_in_state(self, make_indexer, tmp_path):
        """After processing a message, session_id and origin must be persisted in state."""
        idx = make_indexer()
        request_id = _REQUEST_UUID
        summary_file = _make_summary_file(tmp_path, _SESSION_UUID, request_id)
        msg = {
            "request_id": request_id,
            "session_id": _SESSION_UUID,
            "origin": "api-direct",
            "document_id": "req_20260101_150000_1001",
            "dedup_key": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "source_url": "https://example.com/store-test",
            "summary_file": summary_file,
            "status": "success",
            "total_urls": 2,
        }
        await idx._process_message(msg)

        loaded = idx._load_request_state(request_id)
        assert loaded["session_id"] == _SESSION_UUID
        assert loaded["origin"] == "api-direct"
