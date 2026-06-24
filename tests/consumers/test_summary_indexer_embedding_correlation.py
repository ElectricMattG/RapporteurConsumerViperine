"""
tests/consumers/test_summary_indexer_embedding_correlation.py

Integration tests for Phase 3b — prove that
KafkaSummaryIndexerConsumer._process_message threads the bound
session_id / request_id ContextVars all the way down through
_embed_summary -> EmbeddingServiceClient.embed -> _post_with_retry,
landing as X-Session-Id / X-Request-Id headers on the captured
httpx request.

The companion unit tests in
tests/clients/test_embedding_client_correlation_headers.py prove the
embedding client *itself* injects the headers when a ContextVar is
bound.  These tests prove the indexer is the *binding site* — the
boundary at which inbound Kafka payload becomes outbound HTTP header.

Strategy:
- Build a *real* EmbeddingServiceClient with httpx.MockTransport so the
  HTTP layer is exercised end-to-end but no network is touched.
- Override the make_indexer fixture's mock embedder with the real client.
- Drive the indexer's _process_message with synthetic message_data.
- Assert the captured httpx.Request carries the expected headers.

No correlation kwarg is ever passed explicitly — propagation is entirely
via the ContextVar bound at the top of _process_message.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import httpx
import pytest

pytestmark = [pytest.mark.anyio]

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"
_OTHER_SESSION_UUID = "33333333-3333-4333-8333-333333333333"
_OTHER_REQUEST_UUID = "44444444-4444-4444-8444-444444444444"


# ── helpers ───────────────────────────────────────────────────────────────────

def _fake_embed_response(n: int = 1, dim: int = 4) -> httpx.Response:
    body = json.dumps({
        "embeddings": [[0.1] * dim for _ in range(n)],
        "shape": [n, dim],
        "processing_time": 0.01,
    }).encode()
    return httpx.Response(200, content=body, headers={"content-type": "application/json"})


def _make_real_embedding_client(captured_requests: List[httpx.Request]):
    """Build a *real* EmbeddingServiceClient wired to httpx.MockTransport.

    Bypasses __init__ (which reads config and dials network) by using __new__
    and stamping the minimal attributes _post_with_retry / embed need.
    """
    from clients.embedding_client import EmbeddingServiceClient

    def _handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        n = len(json.loads(request.content)["texts"])
        return _fake_embed_response(n=n)

    transport = httpx.MockTransport(_handler)

    with patch.object(EmbeddingServiceClient, '__init__', lambda s: None):
        client = EmbeddingServiceClient.__new__(EmbeddingServiceClient)

    client._logger = MagicMock()
    client.service_url = "http://embed-svc"
    client.api_key = "test-key"
    client.timeout = httpx.Timeout(30.0)
    client.limits = httpx.Limits(max_connections=1)
    client.max_retries = 1
    client.retry_delay = 0.0
    client.max_texts_per_request = 64
    client._embedding_dim = None

    _cfg = MagicMock()
    _cfg.get.return_value = None
    client._app_config = _cfg

    client._client = httpx.AsyncClient(
        base_url="http://embed-svc",
        transport=transport,
        timeout=httpx.Timeout(30.0),
    )
    client._client_loop = asyncio.get_event_loop()

    return client


def _write_summary_file(tmp_path: Path, session_id: str, request_id: str,
                       text: str = "Test summary.") -> str:
    """Write a minimal summary JSON file the indexer's _load_summary_file can read."""
    req_dir = tmp_path / "summaries" / session_id / request_id
    req_dir.mkdir(parents=True, exist_ok=True)
    sf = req_dir / "summary.json"
    data = {
        "markdown_summary": text,
        "metadata": {"title": "Test Doc", "source_url": "https://example.com/test"},
        "degraded": False,
    }
    sf.write_text(json.dumps(data), encoding="utf-8")
    return str(sf)


def _build_message(session_id, request_id, summary_file, *,
                  origin="frontend", document_id=None,
                  total_urls=1, url_index=0) -> dict:
    return {
        "session_id": session_id,
        "request_id": request_id,
        "origin": origin,
        "document_id": document_id or f"req_20260624_120000_1{url_index:03d}",
        "dedup_key": f"dedup_{url_index}",
        "source_url": "https://example.com/test",
        "summary_file": summary_file,
        "current_count": url_index + 1,
        "total_urls": total_urls,
        "url_index": url_index,
        "status": "success",
    }


# ── TestSummaryIndexerForwardsCorrelationToEmbed ─────────────────────────────

class TestSummaryIndexerForwardsCorrelationToEmbed:
    """Inbound Kafka message_data IDs land as X-Session-Id / X-Request-Id on
    the captured httpx request to the embedding service."""

    async def test_process_message_propagates_session_id_to_embed_http_header(
        self, make_indexer, tmp_path
    ):
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        summary_file = _write_summary_file(tmp_path, _SESSION_UUID, _REQUEST_UUID)
        msg = _build_message(_SESSION_UUID, _REQUEST_UUID, summary_file)

        await idx._process_message(msg)

        assert len(captured) == 1, "Embedding service must be called exactly once"
        assert captured[0].headers.get("x-session-id") == _SESSION_UUID

    async def test_process_message_propagates_request_id_to_embed_http_header(
        self, make_indexer, tmp_path
    ):
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        summary_file = _write_summary_file(tmp_path, _SESSION_UUID, _REQUEST_UUID)
        msg = _build_message(_SESSION_UUID, _REQUEST_UUID, summary_file)

        await idx._process_message(msg)

        assert captured[0].headers.get("x-request-id") == _REQUEST_UUID

    async def test_process_message_sends_no_x_origin_header(
        self, make_indexer, tmp_path
    ):
        """origin lives on the Kafka payload only — never on outbound HTTP."""
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        summary_file = _write_summary_file(tmp_path, _SESSION_UUID, _REQUEST_UUID)
        msg = _build_message(_SESSION_UUID, _REQUEST_UUID, summary_file, origin="frontend")

        await idx._process_message(msg)

        assert "x-origin" not in captured[0].headers

    async def test_process_message_with_missing_session_id_omits_only_session_header(
        self, make_indexer, tmp_path
    ):
        """No session_id on the Kafka payload → X-Session-Id is suppressed but
        X-Request-Id still flows.  (Indexer requires request_id to operate;
        the bare-no-IDs case is excluded as a sender-side bug, not a
        propagation concern.)"""
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        summary_file = _write_summary_file(tmp_path, "no_session", _REQUEST_UUID)
        msg = _build_message(None, _REQUEST_UUID, summary_file)

        await idx._process_message(msg)

        assert "x-session-id" not in captured[0].headers
        assert captured[0].headers.get("x-request-id") == _REQUEST_UUID

    async def test_process_message_with_missing_request_id_returns_none_no_embed(
        self, make_indexer, tmp_path
    ):
        """Missing request_id → early return with descriptive log, no crash,
        no embedding call.  Guards the fix for BUG_INDEXER_NULL_REQUEST_ID."""
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        summary_file = _write_summary_file(tmp_path, _SESSION_UUID, "no_request")
        msg = _build_message(_SESSION_UUID, None, summary_file)

        result = await idx._process_message(msg)

        assert result is None
        assert len(captured) == 0, "No embedding call must be made when request_id is absent"

    async def test_process_message_with_both_ids_missing_returns_none_no_embed(
        self, make_indexer, tmp_path
    ):
        """Both IDs missing → same early-return path; no TypeError."""
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        msg = _build_message(None, None, "/nonexistent/summary.json")

        result = await idx._process_message(msg)

        assert result is None
        assert len(captured) == 0


# ── TestProcessMessageResetsContextVar ───────────────────────────────────────

class TestProcessMessageResetsContextVar:
    """Token-based reset in _process_message's finally block prevents bleed."""

    async def test_context_var_returns_to_sentinel_after_process_message(
        self, make_indexer, tmp_path
    ):
        from utils.correlation import get_request_id, get_session_id

        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        summary_file = _write_summary_file(tmp_path, _SESSION_UUID, _REQUEST_UUID)
        msg = _build_message(_SESSION_UUID, _REQUEST_UUID, summary_file)

        # Pre-state must be sentinel for this test to be meaningful
        assert get_session_id() == "-"
        assert get_request_id() == "-"

        await idx._process_message(msg)

        # Post-state must also be sentinel — the finally block reset
        assert get_session_id() == "-"
        assert get_request_id() == "-"

    async def test_sequential_messages_with_different_ids_do_not_leak(
        self, make_indexer, tmp_path
    ):
        """Two back-to-back _process_message calls each tag THEIR OWN headers."""
        captured: List[httpx.Request] = []
        client = _make_real_embedding_client(captured)
        idx = make_indexer(embedding_client=client)

        # Message 1
        sf1 = _write_summary_file(tmp_path, _SESSION_UUID, _REQUEST_UUID, text="Doc 1")
        msg1 = _build_message(_SESSION_UUID, _REQUEST_UUID, sf1)
        await idx._process_message(msg1)

        # Message 2 — different IDs, different request_id (new state file)
        sf2 = _write_summary_file(tmp_path, _OTHER_SESSION_UUID, _OTHER_REQUEST_UUID, text="Doc 2")
        msg2 = _build_message(_OTHER_SESSION_UUID, _OTHER_REQUEST_UUID, sf2)
        await idx._process_message(msg2)

        assert len(captured) == 2
        assert captured[0].headers.get("x-session-id") == _SESSION_UUID
        assert captured[0].headers.get("x-request-id") == _REQUEST_UUID
        assert captured[1].headers.get("x-session-id") == _OTHER_SESSION_UUID
        assert captured[1].headers.get("x-request-id") == _OTHER_REQUEST_UUID

    async def test_exception_in_embed_still_resets_context_var(
        self, make_indexer, tmp_path
    ):
        """If the embedding call raises, the finally block still resets ContextVars."""
        # Build a client whose transport raises
        from clients.embedding_client import EmbeddingServiceClient
        from utils.correlation import get_request_id, get_session_id

        def _raising_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("simulated network failure")

        transport = httpx.MockTransport(_raising_handler)
        with patch.object(EmbeddingServiceClient, '__init__', lambda s: None):
            client = EmbeddingServiceClient.__new__(EmbeddingServiceClient)
        client._logger = MagicMock()
        client.service_url = "http://embed-svc"
        client.api_key = "k"
        client.timeout = httpx.Timeout(5.0)
        client.limits = httpx.Limits(max_connections=1)
        client.max_retries = 1
        client.retry_delay = 0.0
        client.max_texts_per_request = 64
        client._embedding_dim = None
        _cfg = MagicMock()
        _cfg.get.return_value = None
        client._app_config = _cfg
        client._client = httpx.AsyncClient(base_url="http://embed-svc", transport=transport)
        client._client_loop = asyncio.get_event_loop()

        idx = make_indexer(embedding_client=client)
        summary_file = _write_summary_file(tmp_path, _SESSION_UUID, _REQUEST_UUID)
        msg = _build_message(_SESSION_UUID, _REQUEST_UUID, summary_file)

        # _process_message may swallow or propagate — either way, finally must run
        try:
            await idx._process_message(msg)
        except Exception:
            pass

        assert get_session_id() == "-", "session_id ContextVar leaked after exception"
        assert get_request_id() == "-", "request_id ContextVar leaked after exception"
