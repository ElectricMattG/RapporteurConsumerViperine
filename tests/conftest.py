"""
tests/conftest.py

Shared pytest fixtures for the full test suite.
sys.path is configured via pythonpath = [...] in pyproject.toml.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


# ── anyio backend (pin to asyncio; keeps trio out of CI unless explicitly wanted)
@pytest.fixture
def anyio_backend():
    return "asyncio"


# ── filesystem helpers ─────────────────────────────────────────────────────────

@pytest.fixture
def tmp_cache(tmp_path):
    """Empty cache/summaries directory."""
    d = tmp_path / "cache" / "summaries"
    d.mkdir(parents=True)
    return d


@pytest.fixture
def combined_index(tmp_path):
    """Empty combined-index JSONL file."""
    f = tmp_path / "research_summaries_combined.jsonl"
    f.touch()
    return f


@pytest.fixture
def populated_combined_index(tmp_path):
    """Combined-index JSONL seeded with two docs (one with dedup_key, one legacy)."""
    f = tmp_path / "research_summaries_combined.jsonl"
    rows = [
        {
            "document_id": "req_20260101_000000_1001",
            "source_url": "https://example.com/a",
            "dedup_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
            "summary_text": "Summary A",
            "summary_embedding": [0.1] * 384,
        },
        {
            # legacy row — no dedup_key field
            "document_id": "doc_legacy001",
            "source_url": "https://example.com/b",
            "summary_text": "Summary B",
            "summary_embedding": [0.2] * 384,
        },
    ]
    with open(f, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    return f


# ── mock Kafka producer ────────────────────────────────────────────────────────

@pytest.fixture
def mock_producer():
    """
    Confluent-style producer stub.
    Records (topic, value, kwargs) on mock_producer.produced list.
    """
    prod = MagicMock()
    prod.produced = []

    def _produce(topic, *, value, **kwargs):
        prod.produced.append({"topic": topic, "value": value, "kwargs": kwargs})

    prod.produce.side_effect = _produce
    prod.flush.return_value = None
    return prod


# ── mock embedding client ──────────────────────────────────────────────────────

@pytest.fixture
def mock_embedder():
    """Async embedding client returning deterministic 384-dim vector."""
    import numpy as np

    emb = AsyncMock()
    emb.embed.return_value = [np.zeros(384, dtype=float)]
    return emb


# ── KafkaSummaryIndexerConsumer factory ───────────────────────────────────────

@pytest.fixture
def make_indexer(tmp_path, mock_producer, mock_embedder):
    """
    Factory that builds a KafkaSummaryIndexerConsumer with all I/O mocked.

    Uses __new__ so __init__ (which dials Kafka) is never called.
    Caller can pass keyword overrides to replace individual attributes.
    """
    def _factory(**overrides):
        from consumers.kafka_summary_indexer import KafkaSummaryIndexerConsumer

        inst = KafkaSummaryIndexerConsumer.__new__(KafkaSummaryIndexerConsumer)

        # filesystem
        inst.cache_dir = tmp_path / "cache" / "summaries"
        inst.cache_dir.mkdir(parents=True, exist_ok=True)
        inst.combined_index_file = tmp_path / "research_summaries_combined.jsonl"
        inst.combined_index_file.touch()
        inst.root_index_file = inst.combined_index_file   # alias used by reload msg

        # kafka
        inst.producer = mock_producer
        inst.output_topic = "session-complete"

        # embedding
        inst.embedding_client = mock_embedder
        inst.model_name = "test-embedding-model"

        # telemetry off
        inst.telemetry_enabled = False

        # minimal logger
        import logging
        inst._logger = logging.getLogger("test.indexer")

        for k, v in overrides.items():
            setattr(inst, k, v)

        return inst

    return _factory
