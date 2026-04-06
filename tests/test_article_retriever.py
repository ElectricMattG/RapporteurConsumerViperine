"""Tests for ArticleRetriever: BM25 + fuzzy matching over tagged articles."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.components.analysis.article_retriever import (ArticleQuery,
                                                       ArticleRetriever,
                                                       RAGReranker, _tokenize)

# ===================================================================
# Fixtures
# ===================================================================

def _make_article(
    url: str = "https://example.com/a",
    title: str = "Default Title",
    source: str = "unlinked",
    topics: list[str] | None = None,
    persons: list[str] | None = None,
    countries: list[str] | None = None,
    operations: list[str] | None = None,
    media: list[str] | None = None,
    content: str = "",
) -> dict:
    return {
        "url": url,
        "title": title,
        "source": source,
        "tags": {
            "topic": topics or [],
            "person": persons or [],
            "country": countries or [],
            "operation": operations or [],
            "media": media or [],
        },
        "content": content,
    }


ARTICLES = [
    _make_article(
        url="https://ex.com/hegseth-dod",
        title="Hegseth Overhauls Pentagon Culture Amid Controversy",
        topics=["culture_and_media", "national_security_domestic"],
        persons=["Pete Hegseth", "Donald Trump"],
        countries=["United States"],
        operations=[],
        content="## Summary\nPentagon overhaul of DEI policies.",
    ),
    _make_article(
        url="https://ex.com/ai-exec-order",
        title="DoW AI-First Agenda: Executive Order on Artificial Intelligence",
        topics=["artificial_intelligence"],
        persons=["Elon Musk", "Sam Altman"],
        countries=["United States"],
    ),
    _make_article(
        url="https://ex.com/border-ops",
        title="Southern Border Crackdown on Drug Cartels",
        topics=["national_security_domestic"],
        persons=["Tom Homan"],
        countries=["Mexico", "United States"],
        operations=["Illegal Immigration", "Southern Border"],
    ),
    _make_article(
        url="https://ex.com/russia-ukraine",
        title="Russia Brushes Off Trump's Threats Over Ukraine",
        topics=["national_security_foreign"],
        persons=["Donald Trump", "Vladimir Putin", "Volodymyr Zelensky"],
        countries=["Russia", "Ukraine", "United States"],
    ),
    _make_article(
        url="https://ex.com/tariff-china",
        title="New Tariffs Target Chinese Electronics Imports",
        topics=["federal_procurement"],
        persons=["Donald Trump"],
        countries=["China", "United States"],
    ),
    _make_article(
        url="https://ex.com/energy-canada",
        title="Canada Retaliates Against US Energy Export Restrictions",
        topics=["energy", "federal_procurement"],
        countries=["Canada", "United States"],
        source="linked",
    ),
    _make_article(
        url="https://ex.com/culture-media-dod",
        title="Pentagon Press Conference on Media Policy",
        topics=["culture_and_media"],
        persons=["Pete Hegseth"],
        countries=["United States"],
        media=["Fox News", "CNN"],
    ),
]


@pytest.fixture
def retriever() -> ArticleRetriever:
    return ArticleRetriever(ARTICLES)


# ===================================================================
# _tokenize
# ===================================================================

class TestTokenize:
    def test_basic(self):
        assert _tokenize("Hello World") == ["hello", "world"]

    def test_punctuation(self):
        assert _tokenize("U.S. policy — quick!") == ["u", "s", "policy", "quick"]

    def test_empty(self):
        assert _tokenize("") == []

    def test_none(self):
        assert _tokenize(None) == []


# ===================================================================
# ArticleRetriever construction
# ===================================================================

class TestRetrieverConstruction:
    def test_article_count(self, retriever: ArticleRetriever):
        assert retriever.article_count == len(ARTICLES)

    def test_source_counts(self, retriever: ArticleRetriever):
        counts = retriever.source_counts()
        assert counts["unlinked"] == 6
        assert counts["linked"] == 1


# ===================================================================
# Basic queries
# ===================================================================

class TestQuery:
    def test_topic_query_returns_results(self, retriever: ArticleRetriever):
        results = retriever.query(topic="culture_and_media")
        assert len(results) >= 1
        for r in results:
            assert "_score" in r
            assert r["_score"] > 0

    def test_topic_culture_matches_expected(self, retriever: ArticleRetriever):
        results = retriever.query(topic="culture_and_media", top_k=10)
        urls = {r["url"] for r in results}
        assert "https://ex.com/hegseth-dod" in urls
        assert "https://ex.com/culture-media-dod" in urls

    def test_person_query(self, retriever: ArticleRetriever):
        results = retriever.query(person="Elon Musk")
        assert any("ai" in r["url"] for r in results)

    def test_country_query(self, retriever: ArticleRetriever):
        results = retriever.query(country="Russia")
        assert any("russia" in r["url"] for r in results)

    def test_title_query(self, retriever: ArticleRetriever):
        results = retriever.query(title="Cartels Border")
        assert len(results) >= 1
        assert results[0]["url"] == "https://ex.com/border-ops"

    def test_operation_query(self, retriever: ArticleRetriever):
        results = retriever.query(operation="Illegal Immigration")
        assert any(r["url"] == "https://ex.com/border-ops" for r in results)

    def test_multi_field_query(self, retriever: ArticleRetriever):
        results = retriever.query(
            topic="national_security_foreign",
            person=["Trump", "Putin"],
            country=["Russia", "Ukraine"],
        )
        assert len(results) >= 1
        # Russia-Ukraine article should score highest
        assert results[0]["url"] == "https://ex.com/russia-ukraine"

    def test_empty_results_for_nonsense(self, retriever: ArticleRetriever):
        results = retriever.query(title="zzzyyyxxx_nonexistent_term_12345")
        assert results == []


# ===================================================================
# Scoring & field weights
# ===================================================================

class TestScoring:
    def test_scores_descending(self, retriever: ArticleRetriever):
        results = retriever.query(topic="national_security_domestic", top_k=10)
        scores = [r["_score"] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_topic_weight_amplifies(self, retriever: ArticleRetriever):
        """Topic has weight 3.0 — a topic-matched article should score
        higher via topic field than the same article via title field alone."""
        topic_res = retriever.query(topic="artificial_intelligence", top_k=1)
        # The AI article should score from topic query
        assert topic_res
        assert topic_res[0]["url"] == "https://ex.com/ai-exec-order"

    def test_field_hits_count(self, retriever: ArticleRetriever):
        results = retriever.query(
            topic="culture_and_media",
            person="Pete Hegseth",
            top_k=5,
        )
        # At least one result should hit both fields
        multi_hit = [r for r in results if r["_field_hits"] >= 2]
        assert len(multi_hit) >= 1


# ===================================================================
# require_all (AND mode)
# ===================================================================

class TestRequireAll:
    def test_require_all_filters_partial(self, retriever: ArticleRetriever):
        # Without require_all: border article + AI article both score
        results_or = retriever.query(
            topic="national_security_domestic",
            operation="Illegal Immigration",
            require_all=False,
            top_k=10,
        )
        # With require_all: only articles matching BOTH fields survive
        results_and = retriever.query(
            topic="national_security_domestic",
            operation="Illegal Immigration",
            require_all=True,
            top_k=10,
        )
        assert len(results_and) <= len(results_or)
        for r in results_and:
            assert r["_field_hits"] >= 2

    def test_require_all_no_results_when_impossible(self, retriever: ArticleRetriever):
        results = retriever.query(
            topic="energy",
            operation="Illegal Immigration",
            require_all=True,
        )
        assert results == []


# ===================================================================
# source_filter
# ===================================================================

class TestSourceFilter:
    def test_filter_linked_only(self, retriever: ArticleRetriever):
        results = retriever.query(topic="energy", source_filter="linked")
        for r in results:
            assert r["source"] == "linked"

    def test_filter_unlinked_only(self, retriever: ArticleRetriever):
        results = retriever.query(topic="culture_and_media", source_filter="unlinked")
        for r in results:
            assert r["source"] == "unlinked"


# ===================================================================
# Fuzzy matching
# ===================================================================

class TestFuzzyMatching:
    def test_fuzzy_person_partial_name(self, retriever: ArticleRetriever):
        # "Hegseth" should fuzzy-match "Pete Hegseth"
        results = retriever.query(person="Hegseth")
        assert any("hegseth" in r["url"] or "culture" in r["url"] for r in results)

    def test_fuzzy_country_match(self, retriever: ArticleRetriever):
        # "Ukraine" is an exact substring — should score
        results = retriever.query(country="Ukraine")
        assert any(r["url"] == "https://ex.com/russia-ukraine" for r in results)


# ===================================================================
# query_from_spec (ArticleQuery)
# ===================================================================

class TestArticleQuery:
    def test_query_from_spec(self, retriever: ArticleRetriever):
        spec = ArticleQuery(
            title="tariff",
            country=["China"],
            topic="federal_procurement",
            top_k=5,
        )
        results = retriever.query_from_spec(spec)
        assert len(results) >= 1
        assert results[0]["url"] == "https://ex.com/tariff-china"

    def test_query_defaults(self):
        spec = ArticleQuery()
        assert spec.top_k == 50
        assert spec.require_all is False
        assert spec.source_filter is None


# ===================================================================
# query_category convenience
# ===================================================================

class TestQueryCategory:
    def test_category_basic(self, retriever: ArticleRetriever):
        results = retriever.query_category("artificial_intelligence", top_k=5)
        assert len(results) >= 1
        assert any("ai" in r["url"] for r in results)

    def test_category_with_extra_person(self, retriever: ArticleRetriever):
        results = retriever.query_category(
            "culture_and_media",
            extra_person=["Hegseth"],
            top_k=5,
        )
        assert len(results) >= 1


# ===================================================================
# format_for_grounding
# ===================================================================

class TestFormatForGrounding:
    def test_returns_string(self, retriever: ArticleRetriever):
        results = retriever.query(topic="culture_and_media", top_k=3)
        text = retriever.format_for_grounding(results)
        assert isinstance(text, str)
        assert "BM25 Retrieved" in text

    def test_includes_article_details(self, retriever: ArticleRetriever):
        results = retriever.query(topic="culture_and_media", top_k=3)
        text = retriever.format_for_grounding(results)
        assert "Score:" in text
        assert "Topics:" in text
        assert "URL:" in text

    def test_empty_results(self, retriever: ArticleRetriever):
        text = retriever.format_for_grounding([])
        assert text == ""

    def test_max_chars_respected(self, retriever: ArticleRetriever):
        results = retriever.query(topic="national_security_domestic", top_k=10)
        text = retriever.format_for_grounding(results, max_chars=500)
        assert len(text) <= 600  # some overhead from header

    def test_summary_snippet_included(self, retriever: ArticleRetriever):
        # Hegseth article has content with "## Summary"
        results = retriever.query(topic="culture_and_media", person="Pete Hegseth", top_k=5)
        text = retriever.format_for_grounding(results)
        assert "Summary:" in text


# ===================================================================
# from_tagged_files
# ===================================================================

class TestFromTaggedFiles:
    def test_load_from_tmp_files(self, tmp_path: Path):
        unlinked_path = tmp_path / "unlinked.jsonl"
        linked_path = tmp_path / "linked.jsonl"

        unlinked_records = [
            {
                "url": "https://test.com/u1",
                "title": "Test unlinked article",
                "domain": "test.com",
                "date": "2025-03-01",
                "publisher": "Test Pub",
                "content": "body text",
                "tags": {"topic": ["energy"], "person": [], "country": ["Norway"],
                         "operation": [], "media": []},
            },
        ]
        linked_records = [
            {
                "record_id": "r1",
                "article_url": "https://test.com/l1",
                "article_title": "Test linked article",
                "article_domain": "test.com",
                "article_date": "2025-03-02",
                "publisher": "X Corp",
                "tags": {"topic": ["federal_procurement"], "person": ["Alice"],
                         "country": ["Sweden"], "operation": [], "media": []},
                "linkage_rationale": "Matched via EO 12345",
                "linked_eos": ["EO 12345"],
                "confidence": 0.9,
            },
        ]

        with open(unlinked_path, "w") as f:
            for rec in unlinked_records:
                f.write(json.dumps(rec) + "\n")
        with open(linked_path, "w") as f:
            for rec in linked_records:
                f.write(json.dumps(rec) + "\n")

        retriever = ArticleRetriever.from_tagged_files(unlinked_path, linked_path)
        assert retriever.article_count == 2
        counts = retriever.source_counts()
        assert counts["unlinked"] == 1
        assert counts["linked"] == 1

        # Verify searchable
        results = retriever.query(country="Norway")
        assert len(results) == 1
        assert results[0]["url"] == "https://test.com/u1"

    def test_missing_files_ok(self, tmp_path: Path):
        retriever = ArticleRetriever.from_tagged_files(
            tmp_path / "nope1.jsonl", tmp_path / "nope2.jsonl"
        )
        assert retriever.article_count == 0


# ===================================================================
# RAGReranker (unit-level, no real RAG server)
# ===================================================================

class TestRAGReranker:
    def test_instantiation(self, retriever: ArticleRetriever):
        reranker = RAGReranker(retriever, rag_url="http://localhost:9999")
        assert reranker._cutoff == 0.35

    def test_falls_back_to_bm25_when_rag_unavailable(self, retriever: ArticleRetriever):
        """When RAG endpoint is unreachable, RAGReranker returns BM25-only
        results (those above the cutoff)."""
        reranker = RAGReranker(
            retriever,
            rag_url="http://localhost:1",  # won't connect
            relevance_cutoff=0.0,  # accept everything for test
        )
        results = reranker.query(
            search_query="Pentagon culture",
            topic="culture_and_media",
            bm25_top_k=5,
        )
        # Should get results from BM25 fallback (normalised BM25 * 0.4 ≥ 0.0)
        assert len(results) >= 1
        for r in results:
            assert "_bm25_score" in r
            assert "_combined_score" in r
