"""
tests/clients/test_embedding_client_correlation_headers.py

Unit tests for Phase 3b — X-Session-Id / X-Request-Id header propagation
through EmbeddingServiceClient._post_with_retry.

All tests use httpx.MockTransport so no real network is involved.
ContextVars are bound via bind_correlation_ids() and always reset in
teardown to avoid cross-test contamination.

Test names mirror the companion RAG 0.2.0
tests/test_embedding_client_headers.py for cross-repo readability.
"""
from __future__ import annotations

import json
from typing import List
from unittest.mock import MagicMock, patch

import httpx
import numpy as np
import pytest

pytestmark = pytest.mark.anyio

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"
_SENTINEL = "-"


# ── helpers ───────────────────────────────────────────────────────────────────

def _fake_embed_response(n: int = 1, dim: int = 4) -> httpx.Response:
    """Return a minimal /embeddings JSON response for n texts."""
    embeddings = [[0.1] * dim for _ in range(n)]
    body = json.dumps({
        "embeddings": embeddings,
        "shape": [n, dim],
        "processing_time": 0.01,
    }).encode()
    return httpx.Response(200, content=body, headers={"content-type": "application/json"})


def _make_client(captured_requests: list, api_key: str | None = "test-key") -> "EmbeddingServiceClient":
    """
    Build an EmbeddingServiceClient that uses a MockTransport.
    Captured requests are appended to the `captured_requests` list.
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
    client.api_key = api_key
    client.timeout = httpx.Timeout(30.0)
    client.limits = httpx.Limits(max_connections=1)
    client.max_retries = 1
    client.retry_delay = 0.0
    client.max_texts_per_request = 64
    client._embedding_dim = None
    client._client_loop = None
    # Provide a minimal _app_config stub so embed() can read config keys
    _cfg = MagicMock()
    _cfg.get.return_value = None
    client._app_config = _cfg

    # Inject MockTransport into the httpx client
    import asyncio
    real_client = httpx.AsyncClient(
        base_url="http://embed-svc",
        transport=transport,
        timeout=httpx.Timeout(30.0),
    )
    client._client = real_client
    client._client_loop = asyncio.get_event_loop()

    return client


# ── TestEmbeddingClientHeaderInjection ───────────────────────────────────────

class TestEmbeddingClientHeaderInjection:
    """Header builder in _post_with_retry — pure unit, no network round-trip needed."""

    async def test_forwards_session_and_request_headers_when_both_bound(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        assert len(captured) == 1
        headers = captured[0].headers
        assert headers.get("x-session-id") == _SESSION_UUID
        assert headers.get("x-request-id") == _REQUEST_UUID

    async def test_omits_both_headers_when_both_are_sentinel(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SENTINEL, _SENTINEL)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        headers = captured[0].headers
        assert "x-session-id" not in headers
        assert "x-request-id" not in headers

    async def test_omits_session_header_when_only_session_is_sentinel(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SENTINEL, _REQUEST_UUID)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        headers = captured[0].headers
        assert "x-session-id" not in headers
        assert headers.get("x-request-id") == _REQUEST_UUID

    async def test_omits_request_header_when_only_request_is_sentinel(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SESSION_UUID, _SENTINEL)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        headers = captured[0].headers
        assert headers.get("x-session-id") == _SESSION_UUID
        assert "x-request-id" not in headers

    async def test_no_x_origin_header_is_ever_sent(self):
        """Parity guard with RAG 0.2.0 — origin stays on Kafka wire, not in HTTP headers."""
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        headers = captured[0].headers
        assert "x-origin" not in headers

    async def test_no_none_string_leaks_into_any_header_value(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(None, None)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        headers = captured[0].headers
        for v in headers.values():
            assert v != "None", f"Literal 'None' leaked into header: {dict(headers)}"

    async def test_no_sentinel_dash_leaks_into_any_header_value(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids("-", "-")
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        assert "x-session-id" not in captured[0].headers
        assert "x-request-id" not in captured[0].headers

    async def test_x_api_key_header_still_present_alongside_correlation(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured, api_key="my-secret-key")

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            await client.embed(["hello"])
        finally:
            reset_correlation_ids(tokens)

        headers = captured[0].headers
        assert headers.get("x-api-key") == "my-secret-key"
        assert headers.get("x-session-id") == _SESSION_UUID
        assert headers.get("x-request-id") == _REQUEST_UUID


# ── TestEmbeddingClientHeaderRoundTripThroughEmbed ───────────────────────────

class TestEmbeddingClientHeaderRoundTripThroughEmbed:
    """End-to-end through the public embed() API — no correlation kwarg passed."""

    async def test_embed_forwards_headers_single_request_branch(self):
        """Single-request path (<=max_texts_per_request) propagates headers."""
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            result = await client.embed(["text one", "text two"])
        finally:
            reset_correlation_ids(tokens)

        assert result.shape[0] == 2
        assert len(captured) == 1
        assert captured[0].headers.get("x-session-id") == _SESSION_UUID
        assert captured[0].headers.get("x-request-id") == _REQUEST_UUID

    async def test_embed_forwards_headers_chunked_batch_branch(self):
        """Chunked path (>max_texts_per_request) propagates headers on every chunk request."""
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)

        captured: list = []
        client = _make_client(captured)
        client.max_texts_per_request = 2  # Force chunking with 5 texts

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            result = await client.embed(["t1", "t2", "t3", "t4", "t5"])
        finally:
            reset_correlation_ids(tokens)

        assert result.shape[0] == 5
        assert len(captured) == 3  # ceil(5/2) = 3 chunks
        for req in captured:
            assert req.headers.get("x-session-id") == _SESSION_UUID
            assert req.headers.get("x-request-id") == _REQUEST_UUID

    async def test_embed_without_bound_context_sends_no_correlation_headers(self):
        """Unbound context (default sentinel) must produce no correlation headers."""
        # Reset to clean sentinel state — no bind_correlation_ids call
        from utils.correlation import get_request_id, get_session_id
        assert get_session_id() == "-"
        assert get_request_id() == "-"

        captured: list = []
        client = _make_client(captured)
        await client.embed(["unbound text"])

        headers = captured[0].headers
        assert "x-session-id" not in headers
        assert "x-request-id" not in headers


# ── TestEmbeddingClientHeaderValuesAreOpaque ─────────────────────────────────

class TestEmbeddingClientHeaderValuesAreOpaque:
    """Story §AC.5 — IDs are opaque pass-throughs. No validation of UUID format."""

    async def test_non_uuid_session_id_string_forwarded_verbatim(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids("query_1773614994", _REQUEST_UUID)
        try:
            await client.embed(["text"])
        finally:
            reset_correlation_ids(tokens)

        assert captured[0].headers.get("x-session-id") == "query_1773614994"

    async def test_non_uuid_request_id_string_forwarded_verbatim(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)
        captured: list = []
        client = _make_client(captured)

        tokens = bind_correlation_ids(_SESSION_UUID, "req_20260622_000000_1001")
        try:
            await client.embed(["text"])
        finally:
            reset_correlation_ids(tokens)

        assert captured[0].headers.get("x-request-id") == "req_20260622_000000_1001"
