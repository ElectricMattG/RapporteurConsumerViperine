"""
tests/regression/test_outbound_correlation_header_invariants.py

Phase 3b / Phase 6 invariant regression — guards future drift.

Three guard suites:

1. TestThirdPartyScrapeHeaderLeakGuard
   Runtime test: bind correlation IDs, drive the web-scraper's
   third-party redirect resolver, assert no X-Session-Id /
   X-Request-Id / X-Origin headers reach the captured outbound
   `requests.get` call.

2. TestEmbeddingHeadersAreTheOnlyOutboundCorrelationSurface
   Filesystem invariant: across `src/`, the literal `X-Session-Id`
   and `X-Request-Id` strings appear (as quoted dict keys or HTTP
   header assignments) ONLY in the two embedding clients.

3. TestNoXOriginHeaderAnywhere
   Filesystem invariant: across `src/` and `tests/`, no code path
   ever stamps an `X-Origin` HTTP header.  Comments mentioning the
   string are allowed; quoted-key assignments are not.

These are deliberately static + runtime checks. They will fail loudly
if a future commit:
  - adds correlation headers to a third-party scrape call
  - introduces correlation headers in another client module
  - re-introduces X-Origin (which RAG 0.2.0 explicitly removed)
"""
from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.regression

# Repository root: tests/regression/ -> tests/ -> repo root
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SRC = _REPO_ROOT / "src"
_TESTS = _REPO_ROOT / "tests"

_SESSION_UUID = "11111111-1111-4111-8111-111111111111"
_REQUEST_UUID = "22222222-2222-4222-8222-222222222222"

# Quoted-key or dict-literal forms (case-insensitive on the X-… header name).
# Matches things like:
#     headers['X-Session-Id'] = sid     -> closing quote, then ']', then '='
#     {'X-Session-Id': sid, ...}        -> closing quote, then ':'
#     ("x-session-id", sid)             -> closing quote, then ','
# Does NOT match free-text strings like "the X-Session-Id header" because
# the closing quote there does not sit immediately after the header name.
_QUOTED_HEADER = lambda name: re.compile(
    r"""['"]""" + re.escape(name) + r"""['"]\s*\]?\s*[:=,]""",
    re.IGNORECASE,
)


def _walk_py(root: Path):
    """Yield every .py file under `root`, skipping __pycache__ and .pyc."""
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


# ── TestThirdPartyScrapeHeaderLeakGuard ──────────────────────────────────────

class TestThirdPartyScrapeHeaderLeakGuard:
    """The web-scraper's outbound requests.get to third-party URLs must
    never carry our correlation IDs.  External servers have no business
    seeing our session_id / request_id; the only sanctioned outbound
    surface is the embedding service."""

    def _make_scraper(self):
        """Build a KafkaWebScraperConsumer with __init__ bypassed."""
        from consumers.kafka_web_scraper import KafkaWebScraperConsumer
        with patch.object(KafkaWebScraperConsumer, '__init__', lambda s: None):
            scraper = KafkaWebScraperConsumer.__new__(KafkaWebScraperConsumer)
        scraper._logger = MagicMock()
        return scraper

    def test_resolve_redirects_headers_only_contain_user_agent(self):
        """Static guarantee: the headers dict at the requests.get call site
        contains only User-Agent.  Captured via monkey-patched requests.get."""
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)

        scraper = self._make_scraper()
        captured = {}

        def _fake_get(url, headers=None, allow_redirects=True, timeout=10):
            captured['url'] = url
            captured['headers'] = dict(headers or {})
            resp = MagicMock()
            resp.url = url
            return resp

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            with patch("consumers.kafka_web_scraper.requests.get", side_effect=_fake_get):
                scraper._resolve_redirects("https://example.com/path")
        finally:
            reset_correlation_ids(tokens)

        assert 'headers' in captured, "_resolve_redirects did not call requests.get"
        assert set(captured['headers'].keys()) == {'User-Agent'}, (
            f"Unexpected headers on third-party scrape: {captured['headers']}"
        )

    def test_no_x_session_id_header_in_third_party_scrape(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)

        scraper = self._make_scraper()
        captured = {}

        def _fake_get(url, headers=None, **kw):
            captured['headers'] = dict(headers or {})
            resp = MagicMock()
            resp.url = url
            return resp

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            with patch("consumers.kafka_web_scraper.requests.get", side_effect=_fake_get):
                scraper._resolve_redirects("https://example.com/path")
        finally:
            reset_correlation_ids(tokens)

        # Case-insensitive header key check
        lowered = {k.lower() for k in captured['headers']}
        assert 'x-session-id' not in lowered

    def test_no_x_request_id_header_in_third_party_scrape(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)

        scraper = self._make_scraper()
        captured = {}

        def _fake_get(url, headers=None, **kw):
            captured['headers'] = dict(headers or {})
            resp = MagicMock()
            resp.url = url
            return resp

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            with patch("consumers.kafka_web_scraper.requests.get", side_effect=_fake_get):
                scraper._resolve_redirects("https://example.com/path")
        finally:
            reset_correlation_ids(tokens)

        lowered = {k.lower() for k in captured['headers']}
        assert 'x-request-id' not in lowered

    def test_no_x_origin_header_in_third_party_scrape(self):
        from utils.correlation import (bind_correlation_ids,
                                       reset_correlation_ids)

        scraper = self._make_scraper()
        captured = {}

        def _fake_get(url, headers=None, **kw):
            captured['headers'] = dict(headers or {})
            resp = MagicMock()
            resp.url = url
            return resp

        tokens = bind_correlation_ids(_SESSION_UUID, _REQUEST_UUID)
        try:
            with patch("consumers.kafka_web_scraper.requests.get", side_effect=_fake_get):
                scraper._resolve_redirects("https://example.com/path")
        finally:
            reset_correlation_ids(tokens)

        lowered = {k.lower() for k in captured['headers']}
        assert 'x-origin' not in lowered


# ── TestEmbeddingHeadersAreTheOnlyOutboundCorrelationSurface ─────────────────

class TestEmbeddingHeadersAreTheOnlyOutboundCorrelationSurface:
    """Story §"Header parity": X-Session-Id and X-Request-Id appear, as
    quoted dict keys or assignments, ONLY in embedding_client.py and
    vision_embedding_client.py.  Comments mentioning the strings (e.g.
    correlation.py's docstring) are intentionally allowed."""

    _ALLOWED = {
        _SRC / "clients" / "embedding_client.py",
        _SRC / "clients" / "vision_embedding_client.py",
    }

    def _scan_for_quoted_header(self, name: str) -> set[Path]:
        rx = _QUOTED_HEADER(name)
        hits: set[Path] = set()
        for path in _walk_py(_SRC):
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if rx.search(text):
                hits.add(path)
        return hits

    def test_x_session_id_quoted_key_appears_only_in_allowed_clients(self):
        hits = self._scan_for_quoted_header("X-Session-Id")
        unexpected = hits - self._ALLOWED
        assert not unexpected, (
            "X-Session-Id appears as a quoted key outside the allowed embedding "
            f"clients: {sorted(p.relative_to(_REPO_ROOT) for p in unexpected)}"
        )

    def test_x_request_id_quoted_key_appears_only_in_allowed_clients(self):
        hits = self._scan_for_quoted_header("X-Request-Id")
        unexpected = hits - self._ALLOWED
        assert not unexpected, (
            "X-Request-Id appears as a quoted key outside the allowed embedding "
            f"clients: {sorted(p.relative_to(_REPO_ROOT) for p in unexpected)}"
        )

    def test_both_embedding_clients_do_carry_the_headers(self):
        """Counterpart to the negative assertions — guards against a refactor
        that silently *removes* header injection from one of the clients."""
        for name in ("X-Session-Id", "X-Request-Id"):
            hits = self._scan_for_quoted_header(name)
            assert self._ALLOWED.issubset(hits), (
                f"{name} missing from one of the embedding clients. "
                f"Found in: {sorted(p.relative_to(_REPO_ROOT) for p in hits)}"
            )

    def test_no_third_party_client_carries_correlation_headers(self):
        """geocoding_client, weather_client, llm_client(s), cached_*: none
        should ever stamp X-Session-Id or X-Request-Id."""
        third_party_clients = [
            _SRC / "clients" / "geocoding_client.py",
            _SRC / "clients" / "weather_client.py",
            _SRC / "clients" / "llm_client.py",
            _SRC / "clients" / "llm_openai_client.py",
            _SRC / "clients" / "llm_gcp_client.py",
            _SRC / "clients" / "llm_cache_client.py",
            _SRC / "clients" / "cached_vision_client.py",
            _SRC / "clients" / "cached_vision_embedding_client.py",
        ]
        for path in third_party_clients:
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for name in ("X-Session-Id", "X-Request-Id"):
                rx = _QUOTED_HEADER(name)
                assert not rx.search(text), (
                    f"{path.relative_to(_REPO_ROOT)} contains a quoted "
                    f"{name} header — third-party clients must not leak "
                    f"correlation IDs."
                )


# ── TestNoXOriginHeaderAnywhere ──────────────────────────────────────────────

class TestNoXOriginHeaderAnywhere:
    """RAG 0.2.0 explicitly removed X-Origin from the outbound header set.
    Viperine's parity guarantee: no module — production or test — ever
    stamps an X-Origin header.  Comments mentioning the string are fine."""

    def test_no_x_origin_quoted_key_in_any_src_file(self):
        rx = _QUOTED_HEADER("X-Origin")
        offenders = []
        for path in _walk_py(_SRC):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if rx.search(text):
                offenders.append(path.relative_to(_REPO_ROOT))
        assert not offenders, (
            f"X-Origin header drift detected in src/: {offenders}"
        )

    def test_no_x_origin_quoted_key_in_any_test_file(self):
        """Tests must not assert X-Origin is present either — that would
        invert the invariant."""
        rx = _QUOTED_HEADER("X-Origin")
        offenders = []
        for path in _walk_py(_TESTS):
            # Self-reference: this test file naturally contains the string
            # in its negative-assertion strings ("'x-origin' not in ..."),
            # which the regex would match because of the `:` after the key.
            # Skip self, and also skip the embedding_client_correlation_headers
            # test which has analogous negative assertions.
            if path.name in (
                "test_outbound_correlation_header_invariants.py",
                "test_embedding_client_correlation_headers.py",
                "test_summary_indexer_embedding_correlation.py",
            ):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if rx.search(text):
                offenders.append(path.relative_to(_REPO_ROOT))
        assert not offenders, (
            f"X-Origin header drift detected in tests/: {offenders}"
        )

    def test_x_origin_suppression_is_documented_in_embedding_clients(self):
        """Soft guard: both embedding clients carry a comment explaining why
        X-Origin is deliberately NOT sent.  Catches a future commit that
        quietly removes the documentation alongside (or instead of) the code."""
        for path in (
            _SRC / "clients" / "embedding_client.py",
            _SRC / "clients" / "vision_embedding_client.py",
        ):
            text = path.read_text(encoding="utf-8")
            assert "X-Origin" in text, (
                f"{path.relative_to(_REPO_ROOT)} no longer mentions X-Origin "
                "in a comment — the design rationale that prevents future drift "
                "may have been lost."
            )
