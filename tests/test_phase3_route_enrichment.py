"""
Phase 3 integration tests — RAG service route enrichment wiring.

Covers:
- SearchRequest / QueryLLMRequest / GroundingPreviewRequest model validation
  (enrich fields present, enrich_types validator rejects unknown values)
- EnrichmentRetriever wired onto ServiceState at startup
- /search returns enrichment_results when enrich=True
- /query_llm (ask mode) includes enrichment in prompt and response
- /grounding_preview returns enrichment_results and enrichment_preview
- All routes default to no enrichment when enrich=False (default)

Tests run against the FastAPI app with state manually populated — no
external services (embedding client, LLM client, Kafka) are called.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
_RAG_DIR = str(Path(__file__).parent.parent / "src" / "services" / "rag")
if _RAG_DIR not in sys.path:
    sys.path.insert(0, _RAG_DIR)

from src.services.rag.enrichment_retriever import (  # noqa: E402
    EnrichmentRetriever, _normalise_url)
# ---------------------------------------------------------------------------
# Import the app and state AFTER path bootstrap
# ---------------------------------------------------------------------------
from src.services.rag.main import GroundingPreviewRequest  # noqa: E402
from src.services.rag.main import QueryLLMRequest, SearchRequest, app, state

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

LINKED_RECORD = {
    "time_window": "2025-01-01→2025-01-15",
    "event_key": "abcd1234",
    "event_descriptor": "Pete Hegseth sworn in as SecDef",
    "eo_identifier": "EO 14347",
    "author": "Staff Writer",
    "publisher": "www.war.gov",
    "date_publication": "2025-01-21",
    "consumer_salience": 0.99,
    "link": "https://www.war.gov/News/Article/419819/",
    "media_framing": {
        "factualness": "Accurate",
        "spin": "None detected",
        "convergence_divergence": "Converges",
        "supportive_unsupportive": "Neutral",
        "linked_to_dow": "Yes",
        "risk": {"rating": "LOW", "reasoning": "Official government communication."},
    },
}

UNLINKED_RECORD = {
    "time_window": "2025-01-01→2025-01-15",
    "author": "Jane Doe",
    "publisher": "cnn.com",
    "date_publication": "2025-01-22",
    "consumer_salience": 0.72,
    "link": "https://cnn.com/politics/hegseth-confirmed",
    "media_framing": {
        "factualness": "Mostly factual",
        "spin": "Slight negative framing",
        "convergence_divergence": "Diverges",
        "supportive_unsupportive": "Unsupportive",
        "linked_to_dow": "Indirectly",
        "risk": None,
    },
}

LINKED_KEY = _normalise_url(LINKED_RECORD["link"])
UNLINKED_KEY = _normalise_url(UNLINKED_RECORD["link"])

# Minimal summary document whose source_url maps to LINKED_RECORD
MOCK_SUMMARY = {
    "document_id": "doc-001",
    "source_url": LINKED_RECORD["link"],
    "summary_text": "A summary about SecDef appointment.",
    "doc_metadata": {
        "publication_date": "2025-01-21",
        "author": "Staff Writer",
    },
}

MOCK_SUMMARY_UNLINKED = {
    "document_id": "doc-002",
    "source_url": UNLINKED_RECORD["link"],
    "summary_text": "CNN coverage of confirmation.",
    "doc_metadata": {
        "publication_date": "2025-01-22",
        "author": "Jane Doe",
    },
}


def _mock_search_result(summary: dict, similarity: float = 0.88) -> dict:
    return {"summary": summary, "similarity": similarity}


def _build_retriever() -> EnrichmentRetriever:
    return EnrichmentRetriever(
        linked_map={LINKED_KEY: [LINKED_RECORD]},
        unlinked_map={UNLINKED_KEY: [UNLINKED_RECORD]},
    )


# ---------------------------------------------------------------------------
# Model validation tests (no HTTP needed)
# ---------------------------------------------------------------------------

class TestRequestModelValidation:
    def test_search_request_default_no_enrich(self):
        req = SearchRequest(query="test")
        assert req.enrich is False
        assert req.enrich_types == ["all"]
        assert req.get_linked is False

    def test_search_request_enrich_types_valid(self):
        req = SearchRequest(query="test", enrich=True, enrich_types=["events", "risk"])
        assert req.enrich_types == ["events", "risk"]

    def test_search_request_enrich_types_rejects_unknown(self):
        with pytest.raises(Exception):  # Pydantic ValidationError
            SearchRequest(query="test", enrich_types=["unknown_type"])

    def test_query_llm_request_inherits_enrich_fields(self):
        req = QueryLLMRequest(query="test", enrich=True, enrich_types=["media"])
        assert req.enrich is True
        assert req.enrich_types == ["media"]

    def test_grounding_preview_request_has_enrich_fields(self):
        req = GroundingPreviewRequest(query="test", enrich=True, get_linked=True)
        assert req.enrich is True
        assert req.get_linked is True

    def test_grounding_preview_enrich_types_rejects_unknown(self):
        with pytest.raises(Exception):
            GroundingPreviewRequest(query="test", enrich_types=["bad"])


# ---------------------------------------------------------------------------
# /search endpoint enrichment
# ---------------------------------------------------------------------------

class TestSearchEnrichment:
    @pytest.fixture(autouse=True)
    def setup_state(self):
        """Populate state with index docs and retriever."""
        state.index_loaded = True
        state.index_path = "/fake/summaries.jsonl"
        state.enrichment_retriever = _build_retriever()
        yield
        state.enrichment_retriever = None
        state.index_loaded = False

    def test_search_without_enrich_returns_no_enrichment_results(self):
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post("/search", json={"query": "SecDef appointment"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["enrichment_results"] is None

    def test_search_with_enrich_true_returns_records(self):
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post(
                "/search",
                json={"query": "SecDef appointment", "enrich": True, "get_linked": True},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["enrichment_results"] is not None
        assert len(body["enrichment_results"]) >= 1
        assert body["enrichment_results"][0]["event_descriptor"] == "Pete Hegseth sworn in as SecDef"

    def test_search_enrich_type_filter_applied(self):
        """Requesting only 'risk' type should return only records with a risk rating."""
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[
                _mock_search_result(MOCK_SUMMARY),
                _mock_search_result(MOCK_SUMMARY_UNLINKED),
            ],
        ):
            client = TestClient(app)
            resp = client.post(
                "/search",
                json={
                    "query": "SecDef",
                    "enrich": True,
                    "get_linked": True,
                    "enrich_types": ["risk"],
                },
            )
        assert resp.status_code == 200
        results = resp.json()["enrichment_results"]
        assert all(
            r.get("media_framing", {}) and r["media_framing"].get("risk")
            for r in results
        )

    def test_search_invalid_enrich_types_returns_422(self):
        client = TestClient(app)
        resp = client.post(
            "/search",
            json={"query": "test", "enrich": True, "enrich_types": ["invalid"]},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /query_llm endpoint enrichment
# ---------------------------------------------------------------------------

class TestQueryLLMEnrichment:
    @pytest.fixture(autouse=True)
    def setup_state(self):
        state.index_loaded = True
        state.index_path = "/fake/summaries.jsonl"
        state.enrichment_retriever = _build_retriever()
        state.llm_client = MagicMock()
        state.llm_client.generate_content = AsyncMock(return_value="LLM answer with grounding.")
        yield
        state.enrichment_retriever = None
        state.index_loaded = False
        state.llm_client = None

    def test_query_llm_without_enrich_returns_null_enrichment(self):
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post(
                "/query_llm",
                json={"query": "Who is SecDef?", "mode": "ask"},
            )
        assert resp.status_code == 200
        assert resp.json()["enrichment_results"] is None

    def test_query_llm_with_enrich_returns_records(self):
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post(
                "/query_llm",
                json={"query": "Who is SecDef?", "mode": "ask", "enrich": True, "get_linked": True},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["enrichment_results"] is not None
        assert len(body["enrichment_results"]) >= 1

    def test_query_llm_enrichment_text_appended_to_prompt(self):
        """When enrich=True, the prompt returned must contain the enrichment header."""
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post(
                "/query_llm",
                json={"query": "Who is SecDef?", "mode": "ask", "enrich": True, "get_linked": True},
            )
        assert resp.status_code == 200
        prompt = resp.json()["prompt"]
        assert "Enrichment Context" in prompt


# ---------------------------------------------------------------------------
# /grounding_preview endpoint enrichment
# ---------------------------------------------------------------------------

class TestGroundingPreviewEnrichment:
    @pytest.fixture(autouse=True)
    def setup_state(self):
        state.index_loaded = True
        state.index_path = "/fake/summaries.jsonl"
        state.enrichment_retriever = _build_retriever()
        yield
        state.enrichment_retriever = None
        state.index_loaded = False

    def test_preview_without_enrich_returns_nulls(self):
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post("/grounding_preview", json={"query": "SecDef"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["enrichment_results"] is None
        assert body["enrichment_preview"] is None

    def test_preview_with_enrich_returns_results_and_preview(self):
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY)],
        ):
            client = TestClient(app)
            resp = client.post(
                "/grounding_preview",
                json={"query": "SecDef", "enrich": True, "get_linked": True},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["enrichment_results"] is not None
        assert len(body["enrichment_results"]) >= 1
        assert body["enrichment_preview"] is not None
        assert "Enrichment Context" in body["enrichment_preview"]

    def test_preview_unlinked_fallback(self):
        """URL only in unlinked map should appear in results without get_linked."""
        with patch(
            "src.services.rag.main.search_index",
            new_callable=AsyncMock,
            return_value=[_mock_search_result(MOCK_SUMMARY_UNLINKED)],
        ):
            client = TestClient(app)
            resp = client.post(
                "/grounding_preview",
                json={"query": "Hegseth confirmation", "enrich": True},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["enrichment_results"] is not None
        assert body["enrichment_results"][0]["publisher"] == "cnn.com"
