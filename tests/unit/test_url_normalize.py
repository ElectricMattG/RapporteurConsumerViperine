"""
tests/unit/test_url_normalize.py

Full coverage of src/utils/url_normalize.py

Contracts verified:
- scheme and host are lowercased
- default ports (80 http, 443 https) are removed
- non-default ports are retained
- fragment (#...) is always stripped
- tracking params (utm_*, gclid, fbclid, …) are removed
- remaining query params are sorted for stability
- empty / unparseable URLs round-trip without raising
"""
from __future__ import annotations

import pytest

from utils.url_normalize import normalize_url


class TestNormalize:
    def test_lowercase_scheme(self):
        assert normalize_url("HTTP://example.com/").startswith("http://")

    def test_lowercase_host(self):
        assert "example.com" in normalize_url("https://EXAMPLE.COM/path")

    def test_removes_default_http_port(self):
        assert ":80" not in normalize_url("http://example.com:80/page")

    def test_removes_default_https_port(self):
        assert ":443" not in normalize_url("https://example.com:443/page")

    def test_keeps_non_default_port(self):
        result = normalize_url("http://example.com:8080/page")
        assert ":8080" in result

    def test_strips_fragment(self):
        assert "#" not in normalize_url("https://example.com/page#section")

    def test_strips_utm_source(self):
        result = normalize_url("https://example.com/?utm_source=twitter")
        assert "utm_source" not in result

    def test_strips_utm_medium(self):
        result = normalize_url("https://example.com/?utm_medium=email")
        assert "utm_medium" not in result

    def test_strips_utm_campaign(self):
        result = normalize_url("https://example.com/?utm_campaign=spring")
        assert "utm_campaign" not in result

    def test_strips_gclid(self):
        result = normalize_url("https://example.com/?gclid=abc123")
        assert "gclid" not in result

    def test_strips_fbclid(self):
        result = normalize_url("https://example.com/?fbclid=xyz")
        assert "fbclid" not in result

    def test_preserves_semantic_params(self):
        result = normalize_url("https://example.com/?q=search&page=2")
        assert "q=search" in result
        assert "page=2" in result

    def test_sorts_query_params(self):
        url_a = "https://example.com/?z=last&a=first"
        url_b = "https://example.com/?a=first&z=last"
        assert normalize_url(url_a) == normalize_url(url_b)

    def test_tracking_params_mixed_with_semantic_params(self):
        """utm_* stripped, real param kept."""
        result = normalize_url("https://example.com/?q=test&utm_source=social")
        assert "utm_source" not in result
        assert "q=test" in result

    def test_all_tracking_params_combined(self):
        url = (
            "https://example.com/page"
            "?utm_source=a&utm_medium=b&utm_campaign=c"
            "&gclid=d&fbclid=e&msclkid=f&_ga=g"
        )
        result = normalize_url(url)
        for param in ("utm_source", "utm_medium", "utm_campaign",
                      "gclid", "fbclid", "msclkid", "_ga"):
            assert param not in result

    def test_path_is_preserved_case_sensitive(self):
        """Paths are case-sensitive on most servers — must not lowercase them."""
        result = normalize_url("https://example.com/Path/To/Resource")
        assert "/Path/To/Resource" in result

    def test_idempotent(self):
        """normalize(normalize(url)) == normalize(url)"""
        url = "https://example.com/path?b=2&a=1&utm_source=x#frag"
        once = normalize_url(url)
        twice = normalize_url(once)
        assert once == twice


class TestEdgeCases:
    def test_empty_string_returns_empty(self):
        assert normalize_url("") == ""

    def test_no_query_string(self):
        result = normalize_url("https://example.com/page")
        assert result == "https://example.com/page"

    def test_no_path(self):
        result = normalize_url("https://example.com")
        assert "example.com" in result

    def test_returns_string_type(self):
        assert isinstance(normalize_url("https://example.com"), str)

    def test_malformed_url_does_not_raise(self):
        """Unparseable input should return the original, not raise."""
        bad = "not a url at all!!!%%"
        result = normalize_url(bad)
        assert isinstance(result, str)

    def test_parse_failure_returns_original(self, monkeypatch):
        """If urlparse raises internally, the original URL is returned unchanged."""
        import utils.url_normalize as mod

        def _boom(*args, **kwargs):
            raise ValueError("forced parse failure")

        monkeypatch.setattr(mod, "urlparse", _boom)
        original = "https://example.com/path?a=1"
        assert normalize_url(original) == original
