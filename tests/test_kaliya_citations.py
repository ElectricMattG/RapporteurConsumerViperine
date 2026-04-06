"""Tests for Kaliya citation pipeline: supplementary packages, citation markers,
post-processing, and KaliyaHandoff integration.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest

from src.components.analysis.media_linkage import KaliyaHandoff
from src.demo.demo_media_linkage import (_build_kaliya_supplementary,
                                         _resolve_kaliya_citations)

# ===================================================================
# Fixtures
# ===================================================================

def _make_articles(n: int = 5) -> list[dict]:
    """Create N sample articles with incrementing metadata."""
    articles = []
    for i in range(1, n + 1):
        articles.append({
            "url": f"https://example.com/article-{i}",
            "author": f"Author {i}",
            "publisher": f"Publisher {i}",
            "date_publication": f"2025-0{min(i, 9):d}-{10 + i:02d}",
            "title": f"Article {i} Title",
            "source_type": "linked",
        })
    return articles


def _make_articles_varied() -> list[dict]:
    """Articles with varied metadata completeness for edge-case testing."""
    return [
        {   # Full metadata
            "url": "https://nytimes.com/article-1",
            "author": "Jane Doe",
            "publisher": "New York Times",
            "date_publication": "2025-03-15",
        },
        {   # No author, fallback to doc_metadata.authors
            "url": "https://reuters.com/article-2",
            "author": "",
            "publisher": "Reuters",
            "date_publication": "2025-02-20",
            "doc_metadata": {"authors": ["John Smith"]},
        },
        {   # No author at all, fallback to publisher
            "url": "https://apnews.com/article-3",
            "author": "",
            "publisher": "Associated Press",
            "date_publication": "2025-01-10",
        },
        {   # No URL — citation should be left unresolved
            "url": "",
            "author": "Ghost Writer",
            "publisher": "Unknown Press",
            "date_publication": "2025-04-01",
        },
        {   # Year-only date
            "url": "https://bbc.com/article-5",
            "author": "BBC Reporter",
            "publisher": "BBC",
            "date_publication": "2025",
        },
        {   # Year-month date
            "url": "https://guardian.com/article-6",
            "author": "Guardian Writer",
            "publisher": "The Guardian",
            "date_publication": "2025-06",
        },
    ]


def _make_cluster_results(n_clusters: int = 3, n_articles: int = 5) -> list[dict]:
    """Create cluster_results list in Phase 5 output format."""
    clusters = []
    for i in range(n_clusters):
        letter = chr(ord("A") + i)
        arts = _make_articles(n_articles)
        for a in arts:
            a["url"] = f"https://example.com/cluster-{letter}/{a['url'].split('/')[-1]}"
        clusters.append({
            "cluster_id": f"cluster_{letter}",
            "cluster_number": i + 1,
            "cluster_description": f"Test cluster {letter} description",
            "cluster_text": f"Cluster {letter} analysis text",
            "article_count": len(arts),
            "articles": arts,
        })
    return [{"category": "test_category", "clusters": clusters}]


# ===================================================================
# _resolve_kaliya_citations
# ===================================================================


class TestResolveKaliyaCitations:
    """Tests for the Kaliya citation post-processor."""

    def test_basic_resolution(self):
        articles = _make_articles(3)
        text = "Some claim (art_1, kaliya) and another (art_2, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 2
        assert "(art_" not in result
        assert "[Author 1, January 2025]" in result
        assert "(https://example.com/article-1)" in result
        assert "[Author 2, February 2025]" in result

    def test_author_fallback_to_doc_metadata(self):
        articles = _make_articles_varied()
        text = "Claim (art_2, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 1
        assert "[John Smith" in result

    def test_author_fallback_to_publisher(self):
        articles = _make_articles_varied()
        text = "Claim (art_3, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 1
        assert "[Associated Press" in result

    def test_no_url_left_unresolved(self):
        articles = _make_articles_varied()
        text = "Claim (art_4, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 0
        assert "(art_4, kaliya)" in result  # preserved

    def test_out_of_range_preserved(self):
        articles = _make_articles(3)
        text = "Claim (art_999, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 0
        assert "(art_999, kaliya)" in result

    def test_year_only_date(self):
        articles = _make_articles_varied()
        text = "Claim (art_5, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 1
        assert "January 2025" in result  # datetime.strptime("2025", "%Y") → Jan

    def test_year_month_date(self):
        articles = _make_articles_varied()
        text = "Claim (art_6, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 1
        assert "June 2025" in result

    def test_adjacent_citations(self):
        articles = _make_articles(5)
        text = "Multiple sources(art_1, kaliya)(art_3, kaliya)(art_5, kaliya)."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 3
        assert "(art_" not in result

    def test_preserves_req_markers(self):
        """req_* markers are the canonical citation format and must be preserved."""
        articles = _make_articles(2)
        text = (
            "Claim (req_20260303_082125_009, summary) and "
            "another (doc_0a6cbf8e56c8, summary)."
        )
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 0
        assert "(req_20260303_082125_009, summary)" in result  # preserved
        assert "(doc_" not in result  # doc_* still scrubbed

    def test_mixed_art_and_req_markers(self):
        articles = _make_articles(3)
        text = (
            "Claim (art_1, kaliya) supported by (req_20260303_082125_009, summary) "
            "and (art_2, kaliya)."
        )
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 2
        assert "(art_" not in result
        assert "(req_20260303_082125_009, summary)" in result  # req_* preserved

    def test_empty_articles(self):
        text = "No articles (art_1, kaliya)."
        result, count = _resolve_kaliya_citations(text, [])
        assert count == 0
        assert "(art_1, kaliya)" in result

    def test_empty_text(self):
        articles = _make_articles(3)
        result, count = _resolve_kaliya_citations("", articles)
        assert count == 0
        assert result == ""

    def test_no_markers(self):
        articles = _make_articles(3)
        text = "Plain text with no citations at all."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 0
        assert result == text

    def test_hyperlink_format(self):
        articles = [
            {
                "url": "https://nytimes.com/test",
                "author": "Jane Doe",
                "date_publication": "2025-03-15",
            },
        ]
        text = "Claim (art_1, kaliya)."
        result, _ = _resolve_kaliya_citations(text, articles)
        assert "[Jane Doe, March 2025](https://nytimes.com/test)" in result

    def test_does_not_resolve_phase4_markers(self):
        """(art_N, cluster) markers belong to Phase 4 — should NOT be touched."""
        articles = _make_articles(3)
        text = "Phase 4 citation (art_1, cluster) not for Kaliya."
        result, count = _resolve_kaliya_citations(text, articles)
        assert count == 0
        assert "(art_1, cluster)" in result


# ===================================================================
# _build_kaliya_supplementary
# ===================================================================


class TestBuildKaliyaSupplementary:
    """Tests for the supplementary grounding package builder."""

    @pytest.fixture(autouse=True)
    def _patch_working_dir(self, tmp_path):
        """Redirect WORKING_DIR and KALIYA_CACHE_DIR to temp paths for tests."""
        self._tmp = tmp_path
        with patch("src.demo.demo_media_linkage.WORKING_DIR", tmp_path), \
             patch("src.demo.demo_media_linkage.KALIYA_CACHE_DIR", tmp_path):
            yield

    def test_creates_supplementary_files(self):
        results = _make_cluster_results(3, 5)
        supp = _build_kaliya_supplementary("test_category", results, None)
        assert len(supp) == 3
        for cid, path in supp.items():
            assert Path(path).exists()
            assert Path(path).stat().st_size > 0

    def test_supp_dir_naming(self):
        results = _make_cluster_results(1, 5)
        supp = _build_kaliya_supplementary("culture_and_media", results, None)
        path = Path(list(supp.values())[0])
        assert "kaliya_supplementary_culture_and_media" in str(path.parent)

    def test_contains_citation_rules(self):
        results = _make_cluster_results(1, 3)
        supp = _build_kaliya_supplementary("test", results, None)
        content = Path(list(supp.values())[0]).read_text()
        assert "Citation Rules" in content
        assert "(req_" in content
        assert "(document_id, summary)" in content
        assert "(art_1)" in content  # in the "NEVER use" rule
        assert "(doc_1)" in content  # in the "NEVER use" rule

    def test_contains_article_metadata_table(self):
        results = _make_cluster_results(1, 3)
        supp = _build_kaliya_supplementary("test", results, None)
        content = Path(list(supp.values())[0]).read_text()
        assert "Article Metadata" in content
        assert "| art_1 |" in content
        assert "| art_2 |" in content
        assert "| art_3 |" in content

    def test_cluster_id_normalization(self):
        """cluster_id='cluster_A' looks up Phase 4 section keyed by 'A'."""
        results = _make_cluster_results(2, 3)
        # Write a fake Phase 4 markdown that has Cluster A and Cluster B sections
        phase4_md = (
            "## Cluster A — Test topic alpha\n"
            "Deep analysis of alpha cluster.\n\n"
            "## Cluster B — Test topic beta\n"
            "Deep analysis of beta cluster.\n\n"
        )
        phase4_path = self._tmp / "phase4_test_category_20260330_000000.md"
        phase4_path.write_text(phase4_md)

        supp = _build_kaliya_supplementary("test_category", results, None)
        content_a = Path(supp["cluster_A"]).read_text()
        assert "Cluster Analysis (from Phase 4" in content_a
        assert "Deep analysis of alpha cluster" in content_a

    def test_grounding_events_included(self):
        results = _make_cluster_results(1, 3)
        events = [
            {
                "eo_number": "14148",
                "eo_title": "Test EO",
                "action_type": "signing",
                "action_description": "Enacted a test",
                "official": "Test Official",
                "date": "2025-01-20",
                "url": "https://whitehouse.gov/eo/14148",
            },
        ]
        supp = _build_kaliya_supplementary("test", results, events)
        content = Path(list(supp.values())[0]).read_text()
        assert "Grounding Events" in content
        assert "14148" in content
        assert "Test EO" in content

    def test_no_grounding_events(self):
        results = _make_cluster_results(1, 3)
        supp = _build_kaliya_supplementary("test", results, None)
        content = Path(list(supp.values())[0]).read_text()
        assert "Grounding Events" not in content

    def test_fallback_to_cluster_text(self):
        """When no Phase 4 markdown, uses cluster_text from parse_result."""
        results = _make_cluster_results(1, 3)
        results[0]["clusters"][0]["cluster_text"] = "Fallback cluster summary"
        supp = _build_kaliya_supplementary("test", results, None)
        content = Path(list(supp.values())[0]).read_text()
        assert "Cluster Summary" in content
        assert "Fallback cluster summary" in content

    def test_returns_correct_keys(self):
        results = _make_cluster_results(3, 5)
        supp = _build_kaliya_supplementary("test", results, None)
        assert set(supp.keys()) == {"cluster_A", "cluster_B", "cluster_C"}


# ===================================================================
# KaliyaHandoff — supplementary & citation fields
# ===================================================================


class TestKaliyaHandoffCitationFields:
    """Tests for citation and supplementary fields in KaliyaHandoff."""

    @staticmethod
    def _sample_cluster() -> dict:
        return {
            "cluster_id": "cluster_A",
            "cluster_number": 1,
            "cluster_description": "Test cluster",
            "cluster_text": "Analysis text",
            "article_count": 3,
            "articles": [
                {"url": "https://a.com/1", "author": "A"},
                {"url": "https://b.com/2", "author": "B"},
                {"url": "https://c.com/3", "author": "C"},
            ],
        }

    def test_build_handoff_has_supplementary_instructions(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", self._sample_cluster())
        assert "supplementary_instructions" in result
        assert "CITATION RULES" in result["supplementary_instructions"]
        assert "(document_id, summary)" in result["supplementary_instructions"]

    def test_build_handoff_has_imagery_instructions(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", self._sample_cluster())
        assert "IMAGERY INSTRUCTIONS" in result["supplementary_instructions"]
        assert "geospatial" in result["supplementary_instructions"]

    def test_build_handoff_has_store_reasoning(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", self._sample_cluster())
        assert result["store_reasoning"] is True

    def test_build_handoff_supplementary_file(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff(
            "test", self._sample_cluster(),
            supplementary_file="/tmp/supp.md",
        )
        assert result["supplementary_file"] == "/tmp/supp.md"

    def test_build_handoff_no_supplementary_file(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", self._sample_cluster())
        assert "supplementary_file" not in result

    def test_build_all_handoffs_passes_supplementary_files(self):
        handoff = KaliyaHandoff()
        cat_result = {
            "category": "test",
            "clusters": [self._sample_cluster()],
        }
        supp = {"cluster_A": "/tmp/supp_A.md"}
        results = handoff.build_all_handoffs(
            [cat_result], min_articles=0, supplementary_files=supp,
        )
        assert len(results) == 1
        assert results[0]["supplementary_file"] == "/tmp/supp_A.md"

    def test_citation_rules_forbid_art_doc(self):
        """Supplementary instructions explicitly forbid art_N and doc_N markers."""
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", self._sample_cluster())
        instructions = result["supplementary_instructions"]
        assert "(doc_1)" in instructions or "doc_1" in instructions
        assert "(art_1)" in instructions or "art_1" in instructions
        assert "NEVER" in instructions
