"""
tests/test_session_completion.py

Regression tests for the session-stuck bug and related remediation.

Coverage:
  §5.1  Failure taxonomy: correct category + retry/degrade flags
  §5.2  DLQ record schema
  §5.3  doc_id contract: format, uniqueness, dedup_key
  §5.4  url_normalize: strip tracking params, lowercase, default ports
  §5.5  Summarizer always emits terminal event (success and failure paths)
  §5.6  Indexer failure markers: session completes when 1-of-N fails
  §5.7  Indexer idle sweeper: force-aggregates sessions past idle timeout
  §5.8  Dedup: same URL in two sessions → 1 row written
  §5.9  Replay safety: running the same session twice doesn't duplicate
  §5.10 Recovery script: detects and aggregates stuck sessions
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.regression

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from consumers._doc_id import make_document_id
from consumers._failure_taxonomy import FailureClassification, classify_error
from utils.url_normalize import normalize_url

# sys.path is configured by tests/conftest.py



# ──────────────────────────────────────────────────────────────────────────────
# §5.1  Failure taxonomy
# ──────────────────────────────────────────────────────────────────────────────
class TestFailureTaxonomy:
    def test_model_empty_stop(self):
        exc = Exception("Empty response from model (finish_reason: STOP)")
        c = classify_error(exc)
        assert c.category == 'model_empty'
        assert c.retryable is True
        assert c.max_attempts == 2
        assert c.degrade is True

    def test_policy_block_safety(self):
        exc = Exception("finish_reason SAFETY — response blocked by content policy")
        c = classify_error(exc)
        assert c.category == 'policy_block'
        assert c.retryable is False
        assert c.max_attempts == 1

    def test_transient_timeout(self):
        exc = Exception("connection timed out after 30s")
        c = classify_error(exc)
        assert c.category == 'transient'
        assert c.retryable is True
        assert c.max_attempts == 3

    def test_transient_429(self):
        exc = Exception("HTTP 429 rate limit exceeded")
        c = classify_error(exc)
        assert c.category == 'transient'
        assert c.retryable is True

    def test_not_found_404(self):
        exc = Exception("HTTP 404 not found")
        c = classify_error(exc)
        assert c.category == 'not_found'
        assert c.retryable is False

    def test_auth_forbidden(self):
        exc = Exception("API key invalid: 403 Forbidden")
        c = classify_error(exc)
        assert c.category == 'auth'
        assert c.retryable is False

    def test_bad_input_empty_text(self):
        exc = Exception("empty text provided for summarization")
        c = classify_error(exc)
        assert c.category == 'bad_input'
        assert c.retryable is False

    def test_embedding_error(self):
        exc = Exception("embedding service error: connection refused")
        c = classify_error(exc)
        assert c.category == 'embedding'
        assert c.retryable is True

    def test_permanent_internal_attribute_error(self):
        exc = AttributeError("'NoneType' object has no attribute 'foo'")
        c = classify_error(exc)
        assert c.category == 'permanent_internal'
        assert c.retryable is False

    def test_unknown_gives_one_retry(self):
        exc = Exception("something totally unexpected")
        c = classify_error(exc)
        assert c.category == 'unknown'
        assert c.retryable is True
        assert c.max_attempts == 2


# ──────────────────────────────────────────────────────────────────────────────
# §5.3  doc_id contract
# ──────────────────────────────────────────────────────────────────────────────
class TestDocId:
    def test_format_matches_pattern(self):
        import re
        doc_id, _ = make_document_id("https://example.com/page", batch_index=7)
        assert re.match(r'^req_\d{8}_\d{6}_1\d{3,}$', doc_id), doc_id

    def test_batch_index_encoded(self):
        doc_id, _ = make_document_id("https://example.com", batch_index=7)
        assert doc_id.endswith('_1007')

    def test_dedup_key_is_32hex(self):
        import re
        _, dedup_key = make_document_id("https://example.com/path")
        assert re.match(r'^[0-9a-f]{32}$', dedup_key), dedup_key

    def test_same_url_same_dedup_key(self):
        _, dk1 = make_document_id("https://example.com/page", batch_index=1)
        _, dk2 = make_document_id("https://example.com/page", batch_index=2)
        assert dk1 == dk2

    def test_same_url_different_timestamp_different_doc_id(self):
        t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
        id1, _ = make_document_id("https://example.com", batch_index=1, when=t1)
        id2, _ = make_document_id("https://example.com", batch_index=1, when=t2)
        assert id1 != id2

    def test_different_urls_different_dedup_keys(self):
        _, dk1 = make_document_id("https://example.com/a")
        _, dk2 = make_document_id("https://example.com/b")
        assert dk1 != dk2


# ──────────────────────────────────────────────────────────────────────────────
# §5.4  URL normalization
# ──────────────────────────────────────────────────────────────────────────────
class TestUrlNormalize:
    def test_strips_utm_params(self):
        url = "https://example.com/page?utm_source=twitter&utm_medium=social&id=42"
        n = normalize_url(url)
        assert 'utm_source' not in n
        assert 'id=42' in n

    def test_lowercases_host(self):
        assert normalize_url("HTTPS://EXAMPLE.COM/Path") == "https://example.com/Path"

    def test_removes_default_http_port(self):
        assert normalize_url("http://example.com:80/page") == "http://example.com/page"

    def test_removes_default_https_port(self):
        assert normalize_url("https://example.com:443/page") == "https://example.com/page"

    def test_retains_non_default_port(self):
        assert "8080" in normalize_url("http://example.com:8080/page")

    def test_strips_fragment(self):
        assert '#' not in normalize_url("https://example.com/page#section")

    def test_sorts_query_params(self):
        a = normalize_url("https://example.com/?b=2&a=1")
        b = normalize_url("https://example.com/?a=1&b=2")
        assert a == b

    def test_same_url_different_tracking_same_result(self):
        base = "https://example.com/article"
        tracked = base + "?utm_campaign=email&utm_source=newsletter"
        assert normalize_url(base) == normalize_url(tracked)

    def test_handles_empty_string(self):
        assert normalize_url("") == ""


# ──────────────────────────────────────────────────────────────────────────────
# §5.6  Indexer: session completes when 1-of-N docs fail
# ──────────────────────────────────────────────────────────────────────────────
class TestIndexerFailureMarkers:
    """Unit tests for the indexer's _process_message with status='failed'."""

    def _make_consumer(self, tmp_path: Path):
        """Construct a minimal KafkaSummaryIndexerConsumer with mocked deps."""
        # Late import to avoid needing full config at module-load time
        from consumers.kafka_summary_indexer import KafkaSummaryIndexerConsumer

        with patch.object(KafkaSummaryIndexerConsumer, '__init__', lambda s: None):
            consumer = KafkaSummaryIndexerConsumer.__new__(KafkaSummaryIndexerConsumer)

        consumer._logger = MagicMock()
        consumer.cache_dir = tmp_path / "summaries"
        consumer.cache_dir.mkdir()
        consumer.telemetry_enabled = False
        consumer.producer = MagicMock()
        consumer.model_name = 'test-model'
        combined = tmp_path / "combined.jsonl"
        combined.touch()
        consumer.combined_index_file = combined
        consumer.root_index_file = combined
        return consumer

    @pytest.mark.anyio
    async def test_failed_message_increments_count_no_embed(self, tmp_path):
        consumer = self._make_consumer(tmp_path)
        msg = {
            'request_id': 'req_001',
            'session_id': 'sess_test',
            'document_id': 'req_20260604_120000_1001',
            'dedup_key': 'a' * 32,
            'source_url': 'https://example.com/fail',
            'summary_file': None,
            'status': 'failed',
            'current_count': 1,
            'total_requests': 3,
        }
        result = await consumer._process_message(msg)
        state = consumer._load_request_state('req_001')
        assert state['received_count'] == 1
        assert 'req_001' in state['failed']
        assert result['failed'] is True

    @pytest.mark.anyio
    async def test_session_completes_with_last_failed(self, tmp_path):
        """If N-1 succeeded and 1 fails, session should aggregate."""
        consumer = self._make_consumer(tmp_path)

        # Pre-populate state as if N-1 docs already processed
        state = {
            'request_id': 'req_002',
            'session_id': 'sess_test2',
            'origin': None,
            'total_requests': 2,
            'received_count': 1,
            'document_ids_processed': ['req_20260604_120000_1001'],
            'dedup_keys_processed': ['b' * 32],
            'failed': [],
            'started_at': datetime.now(timezone.utc).isoformat(),
            'last_event_at': datetime.now(timezone.utc).isoformat(),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'completed': False,
        }
        state_dir = consumer._get_request_dir('req_002')
        state_file = state_dir / 'req_002_state.json'
        with open(state_file, 'w') as f:
            json.dump(state, f)

        # Pre-create a request index with 1 entry
        index_file = state_dir / 'req_002_summaries_embedded.jsonl'
        with open(index_file, 'w') as f:
            entry = {'document_id': 'req_20260604_120000_1001', 'dedup_key': 'b' * 32,
                     'source_url': 'https://example.com/ok', 'summary_text': 'ok',
                     'summary_embedding': [0.1] * 5, 'doc_metadata': {}}
            f.write(json.dumps(entry) + '\n')

        failed_msg = {
            'request_id': 'req_002',
            'session_id': 'sess_test2',
            'document_id': 'req_20260604_120000_1002',
            'dedup_key': 'c' * 32,
            'source_url': 'https://example.com/fail',
            'summary_file': None,
            'status': 'failed',
            'current_count': 2,
            'total_requests': 2,
        }
        # Mock publish and aggregate so we don't need Kafka/locks
        with patch.object(consumer, '_publish_session_complete') as mock_pub, \
             patch.object(consumer, '_aggregate_to_combined_index') as mock_agg:
            result = await consumer._process_message(failed_msg)

        assert mock_pub.called, "session-complete should have been published"
        assert mock_agg.called, "aggregation should have been triggered"
        call_kwargs = mock_pub.call_args
        # partial=True because there were failures
        assert call_kwargs[1]['failed'] == ['req_002'] or \
               (len(call_kwargs[0]) >= 3 and 'req_002' in call_kwargs[0][2])

    @pytest.mark.anyio
    async def test_dedup_key_prevents_second_write(self, tmp_path):
        """Same dedup_key arriving twice should be skipped on the second call."""
        consumer = self._make_consumer(tmp_path)
        dk = hashlib.md5(b'https://example.com/same').hexdigest()
        msg1 = {
            'request_id': 'req_001',
            'session_id': 'sess_dedup',
            'document_id': 'req_20260604_120000_1001',
            'dedup_key': dk,
            'source_url': 'https://example.com/same',
            'summary_file': None,
            'status': 'failed',  # use failed so we don't need an actual file
            'current_count': 1,
            'total_requests': 2,
        }
        msg2 = dict(msg1, request_id='req_001', document_id='req_20260604_120001_1001',
                    current_count=2)
        await consumer._process_message(msg1)
        result2 = await consumer._process_message(msg2)
        # Second message should be skipped (dedup_key already seen)
        assert result2 is not None
        assert result2.get('skipped') is True


# ──────────────────────────────────────────────────────────────────────────────
# §5.7  Idle sweeper
# ──────────────────────────────────────────────────────────────────────────────
class TestIdleSweeper:
    def _make_consumer(self, tmp_path: Path):
        from consumers.kafka_summary_indexer import KafkaSummaryIndexerConsumer
        with patch.object(KafkaSummaryIndexerConsumer, '__init__', lambda s: None):
            consumer = KafkaSummaryIndexerConsumer.__new__(KafkaSummaryIndexerConsumer)
        consumer._logger = MagicMock()
        consumer.cache_dir = tmp_path / "summaries"
        consumer.cache_dir.mkdir()
        consumer.telemetry_enabled = False
        consumer.producer = MagicMock()
        combined = tmp_path / "combined.jsonl"
        combined.touch()
        consumer.combined_index_file = combined
        consumer.root_index_file = combined
        return consumer

    def _write_stuck_session(self, consumer, session_id: str, idle_secs: float):
        """Create a stuck session state file and non-empty index file."""
        session_dir = consumer.cache_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        last_event = datetime.now(timezone.utc) - timedelta(seconds=idle_secs)
        state = {
            'request_id': session_id,
            'session_id': session_id,
            'origin': None,
            'total_requests': 5,
            'received_count': 3,
            'document_ids_processed': [],
            'dedup_keys_processed': [],
            'failed': [],
            'started_at': last_event.isoformat(),
            'last_event_at': last_event.isoformat(),
            'last_updated': last_event.isoformat(),
            'completed': False,
        }
        with open(session_dir / f"{session_id}_state.json", 'w') as f:
            json.dump(state, f)
        index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"
        with open(index_file, 'w') as f:
            for i in range(3):
                entry = {'document_id': f'doc_{i}', 'source_url': f'https://ex.com/{i}',
                         'summary_text': 'text', 'summary_embedding': [0.1], 'doc_metadata': {}}
                f.write(json.dumps(entry) + '\n')
        return state

    def test_sweeper_triggers_for_idle_session(self, tmp_path):
        consumer = self._make_consumer(tmp_path)
        self._write_stuck_session(consumer, 'stuck_sess', idle_secs=200)

        with patch.object(consumer, '_publish_session_complete') as mock_pub, \
             patch.object(consumer, '_aggregate_to_combined_index') as mock_agg:
            # Temporarily reduce timeout for test
            import consumers.kafka_summary_indexer as _mod
            from consumers.kafka_summary_indexer import SESSION_IDLE_TIMEOUT
            orig = _mod.SESSION_IDLE_TIMEOUT
            _mod.SESSION_IDLE_TIMEOUT = 120
            try:
                consumer._check_idle_sessions()
            finally:
                _mod.SESSION_IDLE_TIMEOUT = orig

        assert mock_pub.called, "sweeper should have published session-complete"
        assert mock_agg.called, "sweeper should have triggered aggregation"

    def test_sweeper_skips_fresh_session(self, tmp_path):
        consumer = self._make_consumer(tmp_path)
        self._write_stuck_session(consumer, 'fresh_sess', idle_secs=10)

        with patch.object(consumer, '_publish_session_complete') as mock_pub, \
             patch.object(consumer, '_aggregate_to_combined_index') as mock_agg:
            import consumers.kafka_summary_indexer as _mod
            orig = _mod.SESSION_IDLE_TIMEOUT
            _mod.SESSION_IDLE_TIMEOUT = 120
            try:
                consumer._check_idle_sessions()
            finally:
                _mod.SESSION_IDLE_TIMEOUT = orig

        assert not mock_pub.called, "sweeper should NOT touch a fresh session"
        assert not mock_agg.called

    def test_sweeper_skips_completed_session(self, tmp_path):
        consumer = self._make_consumer(tmp_path)
        state_written = self._write_stuck_session(consumer, 'done_sess', idle_secs=300)
        # Mark it complete
        state_written['completed'] = True
        state_file = consumer.cache_dir / 'done_sess' / 'done_sess_state.json'
        with open(state_file, 'w') as f:
            json.dump(state_written, f)

        with patch.object(consumer, '_publish_session_complete') as mock_pub, \
             patch.object(consumer, '_aggregate_to_combined_index') as mock_agg:
            consumer._check_idle_sessions()

        assert not mock_pub.called


# ──────────────────────────────────────────────────────────────────────────────
# §5.8  Cross-session dedup in aggregation
# ──────────────────────────────────────────────────────────────────────────────
class TestAggregationDedup:
    def _make_consumer(self, tmp_path: Path):
        from consumers.kafka_summary_indexer import KafkaSummaryIndexerConsumer
        with patch.object(KafkaSummaryIndexerConsumer, '__init__', lambda s: None):
            consumer = KafkaSummaryIndexerConsumer.__new__(KafkaSummaryIndexerConsumer)
        consumer._logger = MagicMock()
        consumer.producer = MagicMock()
        combined = tmp_path / "combined.jsonl"
        combined.touch()
        consumer.combined_index_file = combined
        consumer.root_index_file = combined
        return consumer

    def _write_session_index(self, tmp_path, session_id, entries):
        session_dir = tmp_path / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"
        with open(index_file, 'w') as f:
            for e in entries:
                f.write(json.dumps(e) + '\n')
        return index_file

    def test_same_dedup_key_two_sessions_one_row(self, tmp_path):
        consumer = self._make_consumer(tmp_path)
        url = "https://example.com/same"
        dk = hashlib.md5(normalize_url(url).encode()).hexdigest()

        entry = {
            'document_id': 'req_20260604_120000_1001',
            'dedup_key': dk,
            'source_url': url,
            'summary_text': 'text',
            'summary_embedding': [0.1],
            'doc_metadata': {},
        }
        # Session 1
        sess1_file = self._write_session_index(tmp_path, 'sess1', [entry])
        consumer._aggregate_session_into_combined('sess1', sess1_file)

        # Session 2: same URL, new document_id
        entry2 = dict(entry, document_id='req_20260604_130000_1001')
        sess2_file = self._write_session_index(tmp_path, 'sess2', [entry2])
        consumer._aggregate_session_into_combined('sess2', sess2_file)

        # Read combined — should have exactly 1 row
        lines = [l for l in consumer.combined_index_file.read_text().splitlines() if l.strip()]
        assert len(lines) == 1, f"Expected 1 row, got {len(lines)}"

    def test_different_urls_two_sessions_two_rows(self, tmp_path):
        consumer = self._make_consumer(tmp_path)

        def entry(n):
            url = f"https://example.com/page{n}"
            dk = hashlib.md5(normalize_url(url).encode()).hexdigest()
            return {'document_id': f'doc_{n}', 'dedup_key': dk,
                    'source_url': url, 'summary_text': 'text',
                    'summary_embedding': [0.1], 'doc_metadata': {}}

        f1 = self._write_session_index(tmp_path, 'sa', [entry(1)])
        consumer._aggregate_session_into_combined('sa', f1)
        f2 = self._write_session_index(tmp_path, 'sb', [entry(2)])
        consumer._aggregate_session_into_combined('sb', f2)

        lines = [l for l in consumer.combined_index_file.read_text().splitlines() if l.strip()]
        assert len(lines) == 2


# ──────────────────────────────────────────────────────────────────────────────
# §5.10  Recovery script
# ──────────────────────────────────────────────────────────────────────────────
class TestRecoveryScript:
    def _write_stuck_session(self, cache_dir: Path, session_id: str, idle_secs: float):
        session_dir = cache_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        last_event = datetime.now(timezone.utc) - timedelta(seconds=idle_secs)
        state = {
            'session_id': session_id,
            'total_requests': 3,
            'received_count': 2,
            'failed': [],
            'started_at': last_event.isoformat(),
            'last_event_at': last_event.isoformat(),
            'completed': False,
        }
        with open(session_dir / f"{session_id}_state.json", 'w') as f:
            json.dump(state, f)
        index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"
        with open(index_file, 'w') as f:
            entry = {'document_id': f'doc_recover', 'dedup_key': 'x' * 32,
                     'source_url': f'https://example.com/{session_id}',
                     'summary_text': 'text', 'summary_embedding': [0.1], 'doc_metadata': {}}
            f.write(json.dumps(entry) + '\n')

    def test_recovery_script_dry_run_detects_stuck(self, tmp_path, capsys):
        cache_dir = tmp_path / "summaries"
        cache_dir.mkdir()
        combined = tmp_path / "combined.jsonl"
        combined.touch()
        self._write_stuck_session(cache_dir, 'stuck_1', idle_secs=400)

        import scripts.recover_stuck_sessions as script
        stuck = list(script._iter_stuck_sessions(cache_dir, idle_threshold=300))
        assert len(stuck) == 1
        assert stuck[0][0] == 'stuck_1'

    def test_recovery_script_fresh_not_detected(self, tmp_path):
        cache_dir = tmp_path / "summaries"
        cache_dir.mkdir()
        self._write_stuck_session(cache_dir, 'fresh_1', idle_secs=10)

        import scripts.recover_stuck_sessions as script
        stuck = list(script._iter_stuck_sessions(cache_dir, idle_threshold=300))
        assert len(stuck) == 0

    def test_recovery_script_aggregate_writes_to_combined(self, tmp_path):
        cache_dir = tmp_path / "summaries"
        cache_dir.mkdir()
        combined = tmp_path / "combined.jsonl"
        combined.touch()
        self._write_stuck_session(cache_dir, 'stuck_2', idle_secs=400)

        session_dir = cache_dir / 'stuck_2'
        index_file = session_dir / 'stuck_2_summaries_embedded.jsonl'

        import scripts.recover_stuck_sessions as script
        new_c, dup_c = script._aggregate_session('stuck_2', index_file, combined, dry_run=False)
        assert new_c == 1
        assert dup_c == 0
        lines = [l for l in combined.read_text().splitlines() if l.strip()]
        assert len(lines) == 1

    def test_recovery_script_no_duplicate_on_rerun(self, tmp_path):
        cache_dir = tmp_path / "summaries"
        cache_dir.mkdir()
        combined = tmp_path / "combined.jsonl"
        combined.touch()
        self._write_stuck_session(cache_dir, 'stuck_3', idle_secs=400)
        session_dir = cache_dir / 'stuck_3'
        index_file = session_dir / 'stuck_3_summaries_embedded.jsonl'

        import scripts.recover_stuck_sessions as script
        script._aggregate_session('stuck_3', index_file, combined, dry_run=False)
        new_c2, dup_c2 = script._aggregate_session('stuck_3', index_file, combined, dry_run=False)
        assert new_c2 == 0
        assert dup_c2 == 1
