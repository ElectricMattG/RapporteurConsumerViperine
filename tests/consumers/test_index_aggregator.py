"""
tests/consumers/test_index_aggregator.py

Unit tests for KafkaIndexAggregatorConsumer pure/mockable methods:
  - _get_blob_names         blob-name generation + backward-compat default
  - _normalize_qa_pair      flat QA → enriched-chunks schema transform
  - _check_session_timeouts timeout detection + empty-session cleanup

Built via __new__ so __init__ (Kafka + Azure) is bypassed.
"""
from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock

import pytest


def _make_aggregator():
    from consumers.kafka_index_aggregator import KafkaIndexAggregatorConsumer

    inst = KafkaIndexAggregatorConsumer.__new__(KafkaIndexAggregatorConsumer)
    inst._logger = logging.getLogger("test.aggregator")
    return inst


# ── _get_blob_names ────────────────────────────────────────────────────────────

class TestGetBlobNames:
    def test_query_id_used_as_prefix(self):
        agg = _make_aggregator()
        chunks, qa = agg._get_blob_names("israel_pm")
        assert chunks == "israel_pm_chunks_with_embeddings_combined.jsonl"
        assert qa == "israel_pm_qa_pairs_with_embeddings_combined.jsonl"

    def test_none_falls_back_to_research(self):
        agg = _make_aggregator()
        chunks, qa = agg._get_blob_names(None)
        assert chunks.startswith("research_")
        assert qa.startswith("research_")

    def test_default_string_falls_back_to_research(self):
        agg = _make_aggregator()
        chunks, qa = agg._get_blob_names("default")
        assert chunks.startswith("research_")
        assert qa.startswith("research_")

    def test_returns_tuple_of_two(self):
        agg = _make_aggregator()
        result = agg._get_blob_names("x")
        assert isinstance(result, tuple)
        assert len(result) == 2


# ── _normalize_qa_pair ──────────────────────────────────────────────────────────

def _qa_pair(**overrides):
    base = {
        "question": "What happened?",
        "answer": "Many things.",
        "context": "The full source paragraph.",
        "chunk_id": "chunk_abc",
        "qa_index": 2,
        "document_id": "doc_001",
        "source_url": "https://example.com/a",
        "title": "Sample",
        "author": "Jane",
        "combined_embedding": [0.1] * 384,
        "embedding_dimension": 384,
    }
    base.update(overrides)
    return base


class TestNormalizeQaPair:
    def test_returns_nested_structure(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair())
        assert "content" in out
        assert "chunk_metadata" in out
        assert "document_metadata" in out
        assert "embedding_enrichment" in out

    def test_content_includes_question_and_answer(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair())
        assert "QUESTION: What happened?" in out["content"]
        assert "ANSWER: Many things." in out["content"]
        assert "SOURCE CONTEXT:" in out["content"]

    def test_chunk_id_is_namespaced(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair(chunk_id="chunk_abc", qa_index=2))
        assert out["chunk_metadata"]["chunk_id"] == "qa_chunk_abc_2"

    def test_embedding_vector_preserved(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair())
        assert out["embedding_enrichment"]["embedding_vector"] == [0.1] * 384

    def test_word_count_computed(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair())
        assert out["chunk_metadata"]["word_count"] > 0

    def test_section_hierarchy_prepended_to_enriched_text(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair(section_hierarchy=["Ch 1", "Sec A"]))
        assert out["embedding_enrichment"]["enriched_text"].startswith("Ch 1 > Sec A:")

    def test_document_metadata_carries_source_url(self):
        agg = _make_aggregator()
        out = agg._normalize_qa_pair(_qa_pair())
        assert out["document_metadata"]["source_url"] == "https://example.com/a"

    def test_missing_embedding_defaults_to_empty_list(self):
        agg = _make_aggregator()
        qa = _qa_pair()
        del qa["combined_embedding"]
        out = agg._normalize_qa_pair(qa)
        assert out["embedding_enrichment"]["embedding_vector"] == []


# ── _check_session_timeouts ─────────────────────────────────────────────────────

class TestCheckSessionTimeouts:
    def _prime(self, agg, *, idle_seconds, total, received_ids, timeout=120):
        agg.session_timeout = timeout
        agg.session_last_activity = {"sess1": time.time() - idle_seconds}
        agg.session_total_requests = {"sess1": total}
        agg.session_request_ids = {"sess1": set(received_ids)}
        agg.session_chunks_complete = {}
        agg.session_qa_complete = {}
        agg.session_summaries_complete = {}
        agg._aggregate_and_upload_session = MagicMock()

    def test_timed_out_session_with_data_is_aggregated(self):
        agg = _make_aggregator()
        self._prime(agg, idle_seconds=200, total=3, received_ids=["r1", "r2"])
        agg._check_session_timeouts()
        agg._aggregate_and_upload_session.assert_called_once_with("sess1")

    def test_recent_session_is_not_aggregated(self):
        agg = _make_aggregator()
        self._prime(agg, idle_seconds=10, total=3, received_ids=["r1"])
        agg._check_session_timeouts()
        agg._aggregate_and_upload_session.assert_not_called()

    def test_empty_timed_out_session_is_skipped_and_cleaned(self):
        agg = _make_aggregator()
        self._prime(agg, idle_seconds=300, total=0, received_ids=[])
        agg._check_session_timeouts()
        agg._aggregate_and_upload_session.assert_not_called()
        # tracking entry removed
        assert "sess1" not in agg.session_last_activity


# ── TestIndexAggregatorCorrelationStorage (Phase 4) ───────────────────────────

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"


def _make_process_msg_aggregator():
    """Build an aggregator ready for _process_message calls (Kafka msg stub)."""
    from collections import defaultdict
    agg = _make_aggregator()
    agg.session_last_activity = {}
    agg.session_origins = {}
    agg.session_browser_session_ids = {}
    agg.session_summaries_complete = defaultdict(bool)
    agg.session_chunks_complete = defaultdict(bool)
    agg.session_qa_complete = defaultdict(bool)
    agg.session_total_requests = {}
    agg.session_request_ids = defaultdict(set)
    agg.aggregated_sessions = set()
    agg._check_and_aggregate_if_ready = MagicMock()
    return agg


def _fake_msg(data: dict):
    """Return a minimal Kafka message stub whose .value() returns JSON bytes."""
    import json
    msg = MagicMock()
    msg.value.return_value = json.dumps(data).encode()
    msg.topic.return_value = "viperine.session-complete"
    return msg


class TestIndexAggregatorCorrelationStorage:
    def test_session_origin_dict_populated_on_inbound(self):
        agg = _make_process_msg_aggregator()
        msg = _fake_msg({
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "frontend",
            "index_type": "summaries",
            "summary_index_file": "/tmp/idx.jsonl",
            "total_summaries": 5,
        })
        agg._process_message(msg)
        assert agg.session_origins.get(_SESSION_UUID) == "frontend"

    def test_session_origin_persists_until_aggregation(self):
        agg = _make_process_msg_aggregator()
        msg = _fake_msg({
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "api-direct",
            "index_type": "summaries",
            "summary_index_file": "/tmp/idx.jsonl",
            "total_summaries": 3,
        })
        agg._process_message(msg)
        # Aggregation hasn't fired yet — origin must still be readable
        assert _SESSION_UUID in agg.session_origins
        assert agg.session_origins[_SESSION_UUID] == "api-direct"

    def test_browser_session_id_populated_only_when_origin_frontend(self):
        agg = _make_process_msg_aggregator()
        # origin=frontend → browser_session_ids populated
        msg_fe = _fake_msg({
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "origin": "frontend",
            "index_type": "summaries",
            "summary_index_file": "/tmp/idx.jsonl",
            "total_summaries": 1,
        })
        agg._process_message(msg_fe)
        assert _SESSION_UUID in agg.session_browser_session_ids

    def test_browser_session_id_not_populated_when_origin_api_direct(self):
        agg = _make_process_msg_aggregator()
        other_session = "33333333-3333-4333-8333-333333333333"
        msg_api = _fake_msg({
            "session_id": other_session,
            "request_id": _REQUEST_UUID,
            "origin": "api-direct",
            "index_type": "summaries",
            "summary_index_file": "/tmp/idx.jsonl",
            "total_summaries": 1,
        })
        agg._process_message(msg_api)
        assert other_session not in agg.session_browser_session_ids

    def test_none_origin_stored_without_error(self):
        agg = _make_process_msg_aggregator()
        msg = _fake_msg({
            "session_id": _SESSION_UUID,
            "request_id": _REQUEST_UUID,
            "index_type": "summaries",
            "summary_index_file": "/tmp/idx.jsonl",
            "total_summaries": 1,
        })
        agg._process_message(msg)
        assert agg.session_origins.get(_SESSION_UUID) is None
