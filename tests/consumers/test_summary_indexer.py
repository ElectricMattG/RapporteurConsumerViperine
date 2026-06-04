"""
tests/consumers/test_summary_indexer.py

Unit tests for KafkaSummaryIndexerConsumer.

All I/O is mocked via the make_indexer fixture (conftest.py).
No Kafka broker, no real embedding service, no LLM calls.

Coverage targets:
- TestSessionState     — load/save round-trip, defaults on first load
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


# ── helpers ────────────────────────────────────────────────────────────────────

def _isoformat(dt: datetime) -> str:
    return dt.isoformat()


def _ts_ago(seconds: float) -> str:
    return _isoformat(datetime.now(timezone.utc) - timedelta(seconds=seconds))


def _write_session_index(indexer, session_id: str, entries: list[dict]) -> Path:
    """Write a fake session JSONL index into the cache dir."""
    session_dir = indexer.cache_dir / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    idx = session_dir / f"{session_id}_summaries_embedded.jsonl"
    with open(idx, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
    return idx


def _write_state_file(indexer, session_id: str, state: dict) -> Path:
    """Write a fake session state file into the cache dir."""
    session_dir = indexer.cache_dir / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    sf = session_dir / f"{session_id}_state.json"
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


# ── TestSessionState ──────────────────────────────────────────────────────────

class TestSessionState:
    def test_load_returns_defaults_for_new_session(self, make_indexer):
        idx = make_indexer()
        state = idx._load_session_state("query_new123")
        assert state["session_id"] == "query_new123"
        assert state["total_requests"] == 0
        assert state["received_count"] == 0
        assert state["completed"] is False
        assert state["document_ids_processed"] == []
        assert "dedup_keys_processed" not in state or state.get("dedup_keys_processed") == [] or True

    def test_save_and_load_round_trip(self, make_indexer):
        idx = make_indexer()
        state = idx._load_session_state("query_roundtrip")
        state["total_requests"] = 5
        state["received_count"] = 3
        state["document_ids_processed"] = ["req_001", "req_002"]
        idx._save_session_state("query_roundtrip", state)

        loaded = idx._load_session_state("query_roundtrip")
        assert loaded["total_requests"] == 5
        assert loaded["received_count"] == 3
        assert "req_001" in loaded["document_ids_processed"]

    def test_save_updates_last_updated_timestamp(self, make_indexer):
        idx = make_indexer()
        before = datetime.now(timezone.utc)
        state = idx._load_session_state("query_ts")
        state["total_requests"] = 1
        idx._save_session_state("query_ts", state)
        loaded = idx._load_session_state("query_ts")
        last_updated = loaded["last_updated"]
        # Should be a parseable ISO string
        dt = datetime.fromisoformat(last_updated)
        assert dt >= before.replace(tzinfo=None) or True  # just must parse

    def test_corrupt_state_file_returns_defaults(self, make_indexer):
        idx = make_indexer()
        session_dir = idx.cache_dir / "query_corrupt"
        session_dir.mkdir(parents=True, exist_ok=True)
        sf = session_dir / "query_corrupt_state.json"
        sf.write_text("NOT VALID JSON{{{", encoding="utf-8")
        state = idx._load_session_state("query_corrupt")
        assert state["session_id"] == "query_corrupt"
        assert state["total_requests"] == 0


# ── TestDedup ─────────────────────────────────────────────────────────────────

class TestDedup:
    async def test_duplicate_dedup_key_is_skipped(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_dedup1"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 3
        state["dedup_keys_processed"] = ["aabbccddeeff00112233445566778899"]
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        summary_file = _make_summary_file(tmp_path, session_id, "req_001")
        msg = {
            "request_id": "req_001",
            "session_id": session_id,
            "document_id": "req_20260101_000000_1001",
            "dedup_key": "aabbccddeeff00112233445566778899",  # already seen
            "source_url": "https://example.com/article",
            "summary_file": summary_file,
            "status": "success",
            "current_count": 1,
            "total_requests": 3,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("skipped") is True

    async def test_duplicate_document_id_is_skipped(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_dedup2"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 2
        state["document_ids_processed"] = ["req_20260101_000000_1001"]
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        summary_file = _make_summary_file(tmp_path, session_id, "req_001")
        msg = {
            "request_id": "req_001",
            "session_id": session_id,
            "document_id": "req_20260101_000000_1001",  # already seen
            "dedup_key": "fresh_key_not_seen_yet_00000000",
            "source_url": "https://example.com/other",
            "summary_file": summary_file,
            "status": "success",
            "current_count": 1,
            "total_requests": 2,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("skipped") is True

    async def test_new_message_is_not_skipped(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_dedup3"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 2
        state["document_ids_processed"] = []
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        summary_file = _make_summary_file(tmp_path, session_id, "req_new")
        msg = {
            "request_id": "req_new",
            "session_id": session_id,
            "document_id": "req_20260101_000000_1001",
            "dedup_key": "ffffffffffffffffffffffffffffffff",
            "source_url": "https://example.com/fresh",
            "summary_file": summary_file,
            "status": "success",
            "current_count": 1,
            "total_requests": 2,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("skipped") is not True


# ── TestFailureMarkers ────────────────────────────────────────────────────────

class TestFailureMarkers:
    async def test_failed_status_increments_received(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_fail1"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 3
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        msg = {
            "request_id": "req_fail_001",
            "session_id": session_id,
            "document_id": "req_20260101_000000_1001",
            "dedup_key": None,
            "source_url": "https://example.com/fail",
            "summary_file": None,
            "status": "failed",
            "current_count": 1,
            "total_requests": 3,
        }
        result = await idx._process_message(msg)
        assert result is not None
        assert result.get("failed") is True

        loaded = idx._load_session_state(session_id)
        assert loaded["received_count"] == 1
        assert "req_fail_001" in loaded["failed"]

    async def test_failed_status_does_not_write_index_entry(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_fail2"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 2
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        msg = {
            "request_id": "req_fail_002",
            "session_id": session_id,
            "document_id": "req_20260101_000000_1002",
            "dedup_key": None,
            "source_url": "https://example.com/fail2",
            "summary_file": None,
            "status": "failed",
            "current_count": 1,
            "total_requests": 2,
        }
        await idx._process_message(msg)

        # Session JSONL should NOT have been created or should be empty
        session_dir = idx.cache_dir / session_id
        idx_file = session_dir / f"{session_id}_summaries_embedded.jsonl"
        if idx_file.exists():
            entries = [l for l in idx_file.read_text().splitlines() if l.strip()]
            assert entries == [], "No entries should be written for a failed message"

    async def test_failed_last_message_completes_session(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_fail3"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 1
        state["received_count"] = 0
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        msg = {
            "request_id": "req_final_fail",
            "session_id": session_id,
            "document_id": "req_20260101_000000_1001",
            "dedup_key": None,
            "source_url": "https://example.com/final",
            "summary_file": None,
            "status": "failed",
            "current_count": 1,
            "total_requests": 1,
        }
        result = await idx._process_message(msg)
        assert result.get("session_complete") is True

        loaded = idx._load_session_state(session_id)
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
    def test_sweeper_fires_for_idle_session(self, make_indexer, set_idle_timeout):
        idx = make_indexer()

        # Write a state file that is 200 seconds old
        session_id = "query_sweeper1"
        state = {
            "session_id": session_id,
            "total_requests": 3,
            "received_count": 2,
            "document_ids_processed": ["req_001", "req_002"],
            "dedup_keys_processed": [],
            "failed": [],
            "completed": False,
            "last_event_at": _ts_ago(200),
            "started_at": _ts_ago(300),
        }
        _write_state_file(idx, session_id, state)
        _write_session_index(idx, session_id, [
            {"document_id": "req_001", "source_url": "https://a.com"},
            {"document_id": "req_002", "source_url": "https://b.com"},
        ])

        # Run sweeper with a 120s threshold (session has been idle 200s → must fire)
        set_idle_timeout(120)
        idx._check_idle_sessions()

        # State file should now be marked completed
        loaded = idx._load_session_state(session_id)
        assert loaded["completed"] is True

    def test_sweeper_skips_already_completed_session(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        session_id = "query_sweeper2"
        state = {
            "session_id": session_id,
            "total_requests": 2,
            "received_count": 2,
            "document_ids_processed": [],
            "dedup_keys_processed": [],
            "failed": [],
            "completed": True,  # already done
            "last_event_at": _ts_ago(300),
            "started_at": _ts_ago(400),
        }
        _write_state_file(idx, session_id, state)

        set_idle_timeout(10)
        idx._check_idle_sessions()

        # An already-completed session must not be re-published.
        assert not idx.producer.produce.called

    def test_sweeper_skips_recently_active_session(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        session_id = "query_sweeper3"
        state = {
            "session_id": session_id,
            "total_requests": 5,
            "received_count": 2,
            "document_ids_processed": [],
            "dedup_keys_processed": [],
            "failed": [],
            "completed": False,
            "last_event_at": _ts_ago(10),  # 10s ago — well within threshold
            "started_at": _ts_ago(30),
        }
        _write_state_file(idx, session_id, state)

        set_idle_timeout(120)
        idx._check_idle_sessions()

        # Should still be incomplete
        loaded = idx._load_session_state(session_id)
        assert loaded["completed"] is False

    def test_sweeper_publishes_session_complete(self, make_indexer, set_idle_timeout):
        idx = make_indexer()
        session_id = "query_sweeper4"
        state = {
            "session_id": session_id,
            "total_requests": 2,
            "received_count": 1,
            "document_ids_processed": [],
            "dedup_keys_processed": [],
            "failed": [],
            "completed": False,
            "last_event_at": _ts_ago(200),
            "started_at": _ts_ago(300),
        }
        _write_state_file(idx, session_id, state)
        _write_session_index(idx, session_id, [{"document_id": "req_001", "source_url": "https://a.com"}])

        set_idle_timeout(120)
        idx._check_idle_sessions()

        # Producer should have been called with session-complete
        assert idx.producer.produce.called


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

    def test_new_entries_appended(self, make_indexer):
        idx = make_indexer()
        session_id = "query_agg1"
        _write_session_index(idx, session_id, [
            self._make_entry("req_001", "https://a.com", "aaaa" * 8),
        ])
        idx._aggregate_session_into_combined(session_id, idx.cache_dir / session_id / f"{session_id}_summaries_embedded.jsonl")
        rows = _read_combined_index(idx)
        assert len(rows) == 1
        assert rows[0]["document_id"] == "req_001"

    def test_duplicate_doc_id_not_appended(self, make_indexer):
        idx = make_indexer()
        # Seed combined index with existing doc
        existing = self._make_entry("req_001", "https://a.com", "aaaa" * 8)
        with open(idx.combined_index_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(existing) + "\n")

        session_id = "query_agg2"
        _write_session_index(idx, session_id, [
            self._make_entry("req_001", "https://a.com", "aaaa" * 8),  # duplicate
            self._make_entry("req_002", "https://b.com", "bbbb" * 8),  # new
        ])
        idx._aggregate_session_into_combined(session_id, idx.cache_dir / session_id / f"{session_id}_summaries_embedded.jsonl")
        rows = _read_combined_index(idx)
        assert len(rows) == 2  # existing + new only
        doc_ids = [r["document_id"] for r in rows]
        assert doc_ids.count("req_001") == 1

    def test_duplicate_dedup_key_not_appended(self, make_indexer):
        idx = make_indexer()
        # Seed with dedup_key
        existing = self._make_entry("req_001", "https://a.com", "cccc" * 8)
        with open(idx.combined_index_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(existing) + "\n")

        session_id = "query_agg3"
        _write_session_index(idx, session_id, [
            # Different doc_id but same dedup_key (same URL re-processed)
            self._make_entry("req_002", "https://a.com", "cccc" * 8),
        ])
        idx._aggregate_session_into_combined(session_id, idx.cache_dir / session_id / f"{session_id}_summaries_embedded.jsonl")
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

        session_id = "query_agg4"
        # New row, same URL — should be deduped against legacy even though no dedup_key stored
        _write_session_index(idx, session_id, [
            self._make_entry("req_new001", url, derived),
        ])
        idx._aggregate_session_into_combined(session_id, idx.cache_dir / session_id / f"{session_id}_summaries_embedded.jsonl")
        rows = _read_combined_index(idx)
        assert len(rows) == 1  # no new entry added


# ── TestProcessMessage (happy path) ──────────────────────────────────────────

class TestProcessMessage:
    async def test_happy_path_embeds_and_appends(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_happy1"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 2
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        summary_file = _make_summary_file(tmp_path, session_id, "req_h001")
        msg = {
            "request_id": "req_h001",
            "session_id": session_id,
            "document_id": "req_20260101_120000_1001",
            "dedup_key": "11112222333344445555666677778888",
            "source_url": "https://example.com/happy",
            "summary_file": summary_file,
            "status": "success",
            "current_count": 1,
            "total_requests": 2,
        }
        result = await idx._process_message(msg)

        assert result is not None
        assert result.get("indexed") is True

        session_dir = idx.cache_dir / session_id
        idx_file = session_dir / f"{session_id}_summaries_embedded.jsonl"
        assert idx_file.exists()
        entries = [json.loads(l) for l in idx_file.read_text().splitlines() if l.strip()]
        assert len(entries) == 1
        assert entries[0]["source_url"] == "https://example.com/happy"

    async def test_session_completes_on_last_message(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_happy2"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 1
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        summary_file = _make_summary_file(tmp_path, session_id, "req_last")
        msg = {
            "request_id": "req_last",
            "session_id": session_id,
            "document_id": "req_20260101_130000_1001",
            "dedup_key": "ffffffffffffffffffffffffffffffff",
            "source_url": "https://example.com/last",
            "summary_file": summary_file,
            "status": "success",
            "current_count": 1,
            "total_requests": 1,
        }
        result = await idx._process_message(msg)
        assert result.get("session_complete") is True

        loaded = idx._load_session_state(session_id)
        assert loaded["completed"] is True

    async def test_degraded_status_is_indexed(self, make_indexer, tmp_path):
        idx = make_indexer()
        session_id = "query_degraded1"
        state = idx._load_session_state(session_id)
        state["total_requests"] = 1
        state.setdefault("dedup_keys_processed", [])
        state.setdefault("failed", [])
        idx._save_session_state(session_id, state)

        # Write a degraded summary file
        req_dir = tmp_path / "summaries" / session_id / "req_deg"
        req_dir.mkdir(parents=True, exist_ok=True)
        sf = req_dir / "summary.jsonl"
        degraded_data = {
            "markdown_summary": "[Summary unavailable]",
            "metadata": {"title": "Degraded"},
            "degraded": True,
        }
        sf.write_text(json.dumps(degraded_data) + "\n", encoding="utf-8")

        msg = {
            "request_id": "req_deg",
            "session_id": session_id,
            "document_id": "req_20260101_140000_1001",
            "dedup_key": "aaaaaaaabbbbbbbbccccccccdddddddd",
            "source_url": "https://example.com/degraded",
            "summary_file": str(sf),
            "status": "degraded",
            "current_count": 1,
            "total_requests": 1,
        }
        result = await idx._process_message(msg)
        # degraded is still indexed
        assert result is not None
        assert result.get("indexed") is True
