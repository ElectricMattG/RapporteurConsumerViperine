"""
Phase 2 unit tests — EnrichmentRetriever

Covers:
- lookup(): linked-hit, unlinked-hit, no-hit, linked-priority-over-unlinked
- _dedup(): same URL from multiple primary docs, same URL in different time windows
- filter_by_type(): each of the 5 type tokens, multi-type, unknown type ignored
- format_for_grounding(): linked record, unlinked record, empty list, risk block
"""
import sys
from pathlib import Path

import pytest

# Project root needed for src.config_handler (Base)
_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
# Rag service directory for direct module import
_RAG_DIR = str(Path(__file__).parent.parent / "src" / "services" / "rag")
if _RAG_DIR not in sys.path:
    sys.path.insert(0, _RAG_DIR)

from enrichment_retriever import EnrichmentRetriever, _normalise_url

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

LINKED_RECORD = {
    "time_window": "2025-01-01→2025-01-15",
    "window_start": "2025-01-01",
    "window_end": "2025-01-15",
    "event_key": "abcd1234",
    "event_descriptor": "Pete Hegseth sworn in as SecDef",
    "eo_identifier": "EO 14347",
    "author": "Staff Writer",
    "publisher": "www.war.gov",
    "listed_publisher": True,
    "date_publication": "2025-01-21",
    "mean_search_engine_rank": 1.0,
    "consumer_salience": 0.99,
    "link": "https://www.war.gov/News/Article/419819/",
    "summary": "SecDef sworn in.",
    "embedding_similarity": 0.91,
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
    "window_start": "2025-01-01",
    "window_end": "2025-01-15",
    "author": "Jane Doe",
    "publisher": "cnn.com",
    "listed_publisher": True,
    "date_publication": "2025-01-22",
    "mean_search_engine_rank": 2.5,
    "consumer_salience": 0.72,
    "link": "https://cnn.com/politics/hegseth-confirmed",
    "summary": "Senate confirms Hegseth.",
    "embedding_similarity": 0.84,
    "media_framing": {
        "factualness": "Mostly factual",
        "spin": "Slight negative framing",
        "convergence_divergence": "Diverges on tone",
        "supportive_unsupportive": "Unsupportive",
        "linked_to_dow": "Indirectly linked",
        "risk": None,
    },
}

LINKED_KEY = _normalise_url(LINKED_RECORD["link"])
UNLINKED_KEY = _normalise_url(UNLINKED_RECORD["link"])


def _make_retriever(linked=None, unlinked=None):
    linked_map = linked if linked is not None else {LINKED_KEY: [LINKED_RECORD]}
    unlinked_map = unlinked if unlinked is not None else {UNLINKED_KEY: [UNLINKED_RECORD]}
    return EnrichmentRetriever(linked_map, unlinked_map)


# ---------------------------------------------------------------------------
# lookup()
# ---------------------------------------------------------------------------

class TestLookup:
    def test_returns_linked_record_when_url_in_linked_map(self):
        r = _make_retriever()
        results = r.lookup([LINKED_RECORD["link"]], get_linked=True)
        assert len(results) == 1
        assert results[0]["event_key"] == "abcd1234"

    def test_get_linked_false_suppresses_linked_records(self):
        """Without get_linked=True, URLs only in the linked map return nothing."""
        r = _make_retriever()
        results = r.lookup([LINKED_RECORD["link"]])  # default get_linked=False
        assert results == []

    def test_falls_back_to_unlinked_when_not_in_linked(self):
        r = _make_retriever(linked={})
        results = r.lookup([UNLINKED_RECORD["link"]])
        assert len(results) == 1
        assert results[0]["publisher"] == "cnn.com"

    def test_prefers_linked_over_unlinked_for_same_url(self):
        """Linked record should be returned, not unlinked, when both have the URL."""
        shared_url = "https://shared.example.com/article"
        key = _normalise_url(shared_url)
        linked_rec = {**LINKED_RECORD, "link": shared_url, "event_descriptor": "Linked event"}
        unlinked_rec = {**UNLINKED_RECORD, "link": shared_url}
        r = EnrichmentRetriever(
            linked_map={key: [linked_rec]},
            unlinked_map={key: [unlinked_rec]},
        )
        results = r.lookup([shared_url], get_linked=True)
        assert len(results) == 1
        assert results[0]["event_descriptor"] == "Linked event"

    def test_no_hit_returns_empty_list(self):
        r = _make_retriever()
        results = r.lookup(["https://unknown.example.com/no-match"])
        assert results == []

    def test_empty_url_list_returns_empty(self):
        r = _make_retriever()
        assert r.lookup([]) == []

    def test_multiple_urls_collected(self):
        r = _make_retriever()
        results = r.lookup([LINKED_RECORD["link"], UNLINKED_RECORD["link"]], get_linked=True)
        assert len(results) == 2

    def test_http_and_https_resolved_to_same_record(self):
        """A primary URL with http:// should hit a record stored under https://."""
        r = _make_retriever()
        http_url = LINKED_RECORD["link"].replace("https://", "http://")
        results = r.lookup([http_url], get_linked=True)
        assert len(results) == 1

    def test_get_linked_expands_to_all_event_articles(self):
        """Matching one URL in an event should return ALL linked articles for that event."""
        url_a = "https://news.example.com/article-a"
        url_b = "https://news.example.com/article-b"
        url_c = "https://news.example.com/article-c"
        key_a = _normalise_url(url_a)
        key_b = _normalise_url(url_b)
        key_c = _normalise_url(url_c)
        shared_event = "event-shared-99"
        rec_a = {**LINKED_RECORD, "link": url_a, "event_key": shared_event}
        rec_b = {**LINKED_RECORD, "link": url_b, "event_key": shared_event}
        rec_c = {**LINKED_RECORD, "link": url_c, "event_key": shared_event}
        r = EnrichmentRetriever(
            linked_map={key_a: [rec_a], key_b: [rec_b], key_c: [rec_c]},
            unlinked_map={},
        )
        # Query with only url_a — expect all three event articles returned
        results = r.lookup([url_a], get_linked=True)
        returned_urls = {res["link"] for res in results}
        assert returned_urls == {url_a, url_b, url_c}

    def test_trailing_slash_variation_hits(self):
        r = _make_retriever()
        stripped = LINKED_RECORD["link"].rstrip("/")
        results = r.lookup([stripped], get_linked=True)
        assert len(results) == 1


# ---------------------------------------------------------------------------
# _dedup()
# ---------------------------------------------------------------------------

class TestDedup:
    def test_same_record_from_two_primary_urls_is_deduped(self):
        """Two different primary doc URLs both map to the same enrichment record."""
        r = _make_retriever()
        raw = [LINKED_RECORD, LINKED_RECORD]  # exact duplicate
        deduped = r._dedup(raw)
        assert len(deduped) == 1

    def test_same_url_different_time_windows_both_kept(self):
        # Same article, same event_key, but different time windows → keep both
        rec_a = {**LINKED_RECORD, "time_window": "2025-01-01"}
        rec_b = {**LINKED_RECORD, "time_window": "2025-02-01"}
        r = _make_retriever()
        deduped = r._dedup([rec_a, rec_b])
        assert len(deduped) == 2

    def test_same_url_different_event_keys_both_kept(self):
        # Same article linked to two different events → keep both
        rec_a = {**LINKED_RECORD, "event_key": "aaaa0001", "event_descriptor": "Event A"}
        rec_b = {**LINKED_RECORD, "event_key": "bbbb0002", "event_descriptor": "Event B"}
        r = _make_retriever()
        deduped = r._dedup([rec_a, rec_b])
        assert len(deduped) == 2

    def test_different_urls_both_kept(self):
        r = _make_retriever()
        deduped = r._dedup([LINKED_RECORD, UNLINKED_RECORD])
        assert len(deduped) == 2

    def test_empty_list(self):
        r = _make_retriever()
        assert r._dedup([]) == []


# ---------------------------------------------------------------------------
# filter_by_type()
# ---------------------------------------------------------------------------

class TestFilterByType:
    def test_all_returns_everything(self):
        r = _make_retriever()
        results = r.filter_by_type([LINKED_RECORD, UNLINKED_RECORD], ["all"])
        assert len(results) == 2

    def test_events_returns_only_linked(self):
        r = _make_retriever()
        results = r.filter_by_type([LINKED_RECORD, UNLINKED_RECORD], ["events"])
        assert len(results) == 1
        assert results[0]["event_descriptor"] is not None

    def test_executive_orders_filters_correctly(self):
        r = _make_retriever()
        results = r.filter_by_type([LINKED_RECORD, UNLINKED_RECORD], ["executive_orders"])
        assert len(results) == 1
        assert results[0]["eo_identifier"] == "EO 14347"

    def test_media_returns_records_with_media_framing(self):
        r = _make_retriever()
        no_framing = {**UNLINKED_RECORD, "link": "https://other.com/a", "media_framing": None}
        results = r.filter_by_type([LINKED_RECORD, UNLINKED_RECORD, no_framing], ["media"])
        assert all(r_.get("media_framing") for r_ in results)
        assert len(results) == 2

    def test_risk_returns_only_records_with_non_null_risk(self):
        r = _make_retriever()
        # LINKED has risk={rating: LOW}, UNLINKED has risk=None
        results = r.filter_by_type([LINKED_RECORD, UNLINKED_RECORD], ["risk"])
        assert len(results) == 1
        assert results[0]["media_framing"]["risk"]["rating"] == "LOW"

    def test_multi_type_union(self):
        """Requesting both events and risk returns union of matching records."""
        r = _make_retriever()
        # Add a record with risk but no event
        risk_only = {
            **UNLINKED_RECORD,
            "link": "https://risk-only.example.com/a",
            "media_framing": {**UNLINKED_RECORD["media_framing"], "risk": {"rating": "HIGH", "reasoning": "Serious"}},
        }
        results = r.filter_by_type([LINKED_RECORD, UNLINKED_RECORD, risk_only], ["events", "risk"])
        links = {res["link"] for res in results}
        assert LINKED_RECORD["link"] in links   # matches events
        assert risk_only["link"] in links       # matches risk
        assert UNLINKED_RECORD["link"] not in links  # matches neither

    def test_empty_records_returns_empty(self):
        r = _make_retriever()
        assert r.filter_by_type([], ["all"]) == []

    def test_unknown_type_is_ignored_gracefully(self):
        """An unknown type string should simply match nothing — no exception."""
        r = _make_retriever()
        results = r.filter_by_type([LINKED_RECORD], ["nonexistent_type"])
        assert results == []


# ---------------------------------------------------------------------------
# format_for_grounding()
# ---------------------------------------------------------------------------

class TestFormatForGrounding:
    def test_empty_list_returns_empty_string(self):
        r = _make_retriever()
        assert r.format_for_grounding([]) == ""

    def test_output_contains_header(self):
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD])
        assert "### Enrichment Context" in text

    def test_linked_record_labelled_correctly(self):
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD])
        assert "Event-Linked Article" in text

    def test_unlinked_record_labelled_correctly(self):
        r = _make_retriever()
        text = r.format_for_grounding([UNLINKED_RECORD])
        assert "Unlinked Media Article" in text

    def test_event_descriptor_present(self):
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD])
        assert "Pete Hegseth sworn in" in text

    def test_eo_identifier_present(self):
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD])
        assert "EO 14347" in text

    def test_risk_rating_present(self):
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD])
        assert "LOW" in text

    def test_null_risk_not_shown(self):
        r = _make_retriever()
        text = r.format_for_grounding([UNLINKED_RECORD])
        assert "Risk Rating" not in text

    def test_publisher_and_author_present(self):
        r = _make_retriever()
        text = r.format_for_grounding([UNLINKED_RECORD])
        assert "cnn.com" in text
        assert "Jane Doe" in text

    def test_multiple_records_numbered(self):
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD, UNLINKED_RECORD])
        assert "#### 1." in text
        assert "#### 2." in text

    def test_summary_not_rendered(self):
        """Summary field is suppressed in grounding output to save tokens."""
        r = _make_retriever()
        text = r.format_for_grounding([LINKED_RECORD])
        assert "SecDef sworn in" not in text
        assert "Summary" not in text
