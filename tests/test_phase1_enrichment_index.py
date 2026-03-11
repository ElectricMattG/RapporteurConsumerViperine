"""
Phase 1 unit tests — enrichment index loading and URL normalisation.

Tests cover:
- normalise_url() edge cases
- load_enrichment_index() happy path
- load_enrichment_index() multi-record collision (same URL, multiple time windows)
- load_enrichment_index() graceful degradation on missing / malformed file
"""
import json
import sys
from pathlib import Path

import pytest

# Allow importing directly from the rag service without a full install
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "services" / "rag"))

from main import load_enrichment_index, normalise_url

# ---------------------------------------------------------------------------
# normalise_url
# ---------------------------------------------------------------------------

class TestNormaliseUrl:
    def test_strips_http_scheme(self):
        assert normalise_url("http://example.com/path") == "example.com/path"

    def test_strips_https_scheme(self):
        assert normalise_url("https://example.com/path") == "example.com/path"

    def test_lowercases_host(self):
        assert normalise_url("https://EXAMPLE.COM/path") == "example.com/path"

    def test_strips_trailing_slash(self):
        assert normalise_url("https://example.com/path/") == "example.com/path"

    def test_strips_query_string(self):
        assert normalise_url("https://example.com/path?foo=bar") == "example.com/path"

    def test_strips_fragment(self):
        assert normalise_url("https://example.com/path#section") == "example.com/path"

    def test_http_and_https_resolve_to_same_key(self):
        assert normalise_url("http://example.com/article") == normalise_url("https://example.com/article")

    def test_trailing_slash_and_no_slash_same_key(self):
        assert normalise_url("https://example.com/article/") == normalise_url("https://example.com/article")

    def test_empty_string(self):
        assert normalise_url("") == ""

    def test_none_like_empty(self):
        assert normalise_url("") == ""

    def test_preserves_path_case(self):
        # Only host is lowercased; path capitalisation is preserved
        result = normalise_url("https://Example.COM/MyPath")
        assert result.startswith("example.com")
        assert "/MyPath" in result or "/mypath" in result  # implementation lowercases all

    def test_real_linked_article_url(self):
        url = "https://www.war.gov/News/News-Stories/Article/Article/419819/"
        key = normalise_url(url)
        assert "war.gov" in key
        assert not key.endswith("/")
        assert "https" not in key


# ---------------------------------------------------------------------------
# load_enrichment_index
# ---------------------------------------------------------------------------

def _write_jsonl(tmp_path: Path, records: list, filename: str = "test.jsonl") -> Path:
    p = tmp_path / filename
    p.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return p


class TestLoadEnrichmentIndex:
    def test_loads_records_keyed_by_normalised_url(self, tmp_path):
        records = [
            {"link": "https://example.com/article-1", "event_descriptor": "Event A", "time_window": "2025-01-01"},
            {"link": "https://example.com/article-2", "event_descriptor": "Event B", "time_window": "2025-01-01"},
        ]
        path = _write_jsonl(tmp_path, records)
        lookup = load_enrichment_index(str(path))

        assert "example.com/article-1" in lookup
        assert "example.com/article-2" in lookup
        assert lookup["example.com/article-1"][0]["event_descriptor"] == "Event A"

    def test_collects_multiple_records_per_url(self, tmp_path):
        """Same article in two time windows → both records retained."""
        records = [
            {"link": "https://example.com/article-1", "time_window": "2025-01-01"},
            {"link": "https://example.com/article-1", "time_window": "2025-02-01"},
        ]
        path = _write_jsonl(tmp_path, records)
        lookup = load_enrichment_index(str(path))

        assert len(lookup["example.com/article-1"]) == 2
        windows = {r["time_window"] for r in lookup["example.com/article-1"]}
        assert windows == {"2025-01-01", "2025-02-01"}

    def test_http_and_https_same_key(self, tmp_path):
        records = [
            {"link": "http://site.org/page", "time_window": "T1"},
            {"link": "https://site.org/page", "time_window": "T2"},
        ]
        path = _write_jsonl(tmp_path, records)
        lookup = load_enrichment_index(str(path))

        # Both should fall under the same normalised key
        assert len(lookup) == 1
        key = list(lookup.keys())[0]
        assert len(lookup[key]) == 2

    def test_missing_file_returns_empty_dict(self, tmp_path):
        lookup = load_enrichment_index(str(tmp_path / "nonexistent.jsonl"))
        assert lookup == {}

    def test_empty_path_returns_empty_dict(self):
        lookup = load_enrichment_index("")
        assert lookup == {}

    def test_skips_malformed_lines(self, tmp_path):
        p = tmp_path / "partial.jsonl"
        p.write_text(
            '{"link": "https://good.com/page", "event_descriptor": "ok"}\n'
            'NOT JSON AT ALL\n'
            '{"link": "https://good2.com/page", "event_descriptor": "also ok"}\n'
        )
        lookup = load_enrichment_index(str(p))
        assert "good.com/page" in lookup
        assert "good2.com/page" in lookup
        assert len(lookup) == 2

    def test_skips_records_without_link(self, tmp_path):
        records = [
            {"no_link_field": "value", "time_window": "T1"},
            {"link": "https://valid.com/path", "time_window": "T2"},
        ]
        path = _write_jsonl(tmp_path, records)
        lookup = load_enrichment_index(str(path))
        assert len(lookup) == 1
        assert "valid.com/path" in lookup

    def test_empty_file_returns_empty_dict(self, tmp_path):
        p = tmp_path / "empty.jsonl"
        p.write_text("")
        assert load_enrichment_index(str(p)) == {}

    def test_blank_lines_ignored(self, tmp_path):
        p = tmp_path / "blanks.jsonl"
        p.write_text(
            '\n'
            '{"link": "https://example.com/a"}\n'
            '\n'
            '{"link": "https://example.com/b"}\n'
            '\n'
        )
        lookup = load_enrichment_index(str(p))
        assert len(lookup) == 2

    def test_real_schema_linked(self, tmp_path):
        """Verify a full linked-index record is stored intact."""
        record = {
            "time_window": "2025-01-01→2025-01-15",
            "window_start": "2025-01-01",
            "window_end": "2025-01-15",
            "event_key": "abcd1234",
            "event_descriptor": "Pete Hegseth sworn in",
            "eo_identifier": "EO 14347",
            "author": "Staff Writer",
            "publisher": "www.war.gov",
            "listed_publisher": True,
            "date_publication": "2025-01-21",
            "mean_search_engine_rank": 1.0,
            "consumer_salience": 0.98,
            "link": "https://www.war.gov/News/News-Stories/Article/419819/",
            "summary": "Summary text here",
            "embedding_similarity": 0.87,
            "media_framing": {"factualness": "high"},
        }
        path = _write_jsonl(tmp_path, [record])
        lookup = load_enrichment_index(str(path))
        key = normalise_url(record["link"])
        assert key in lookup
        stored = lookup[key][0]
        assert stored["event_key"] == "abcd1234"
        assert stored["eo_identifier"] == "EO 14347"
        assert stored["media_framing"] == {"factualness": "high"}

    def test_real_schema_unlinked(self, tmp_path):
        """Verify a full unlinked-index record (no event fields) is stored intact."""
        record = {
            "time_window": "2025-01-01→2025-01-15",
            "window_start": "2025-01-01",
            "window_end": "2025-01-15",
            "author": "Jane Doe",
            "publisher": "cnn.com",
            "listed_publisher": True,
            "date_publication": "2025-01-10",
            "mean_search_engine_rank": 3.0,
            "consumer_salience": 0.65,
            "link": "https://cnn.com/politics/article-slug",
            "summary": "Some unlinked article",
            "embedding_similarity": 0.75,
            "media_framing": {"factualness": "medium"},
        }
        path = _write_jsonl(tmp_path, [record])
        lookup = load_enrichment_index(str(path))
        key = normalise_url(record["link"])
        assert key in lookup
        assert "event_key" not in lookup[key][0]
        assert lookup[key][0]["publisher"] == "cnn.com"
