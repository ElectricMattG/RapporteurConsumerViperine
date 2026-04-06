"""Tests for the analytics_conclude module."""
from __future__ import annotations

import json
# Ensure project root on sys.path
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.components.publisher.analytics_conclude import (
    AnalyticsConcludeStage, _calculate_cost, _deduplicate_chunks,
    _format_article, _parse_article_numbers, _strip_code_fencing)

# ---------------------------------------------------------------------------
# _parse_article_numbers
# ---------------------------------------------------------------------------

class TestParseArticleNumbers:
    def test_simple_range(self):
        assert _parse_article_numbers("1-5") == [1, 2, 3, 4, 5]

    def test_single_numbers(self):
        assert _parse_article_numbers("14, 48, 58") == [14, 48, 58]

    def test_mixed(self):
        result = _parse_article_numbers("1-5, 14, 16-17, 48")
        assert result == [1, 2, 3, 4, 5, 14, 16, 17, 48]

    def test_large_range(self):
        result = _parse_article_numbers("100-105, 200")
        assert result == [100, 101, 102, 103, 104, 105, 200]

    def test_empty_string(self):
        assert _parse_article_numbers("") == []

    def test_spaces(self):
        assert _parse_article_numbers(" 1 - 3 , 5 ") == [1, 2, 3, 5]

    def test_dedup(self):
        result = _parse_article_numbers("1-3, 2-4")
        assert result == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# _parse_cluster_map
# ---------------------------------------------------------------------------

class TestParseClusterMap:
    SAMPLE_MD = textwrap.dedent("""\
        # Phase 4 — Threat Cluster Analysis

        ## Cluster Map

        | Cluster | Theme | Articles |
        |---------|-------|----------|
        | A | Nomination and confirmation | 1-5, 14 |
        | B | Anti-DEI implementation | 6-13, 15 |
        | C | Department of War renaming | 19, 24-30 |

        ---

        ## Official baseline events
    """)

    def test_basic_map(self):
        result = AnalyticsConcludeStage._parse_cluster_map(self.SAMPLE_MD)
        assert "A" in result
        assert "B" in result
        assert "C" in result
        assert result["A"] == [1, 2, 3, 4, 5, 14]
        assert result["B"] == [6, 7, 8, 9, 10, 11, 12, 13, 15]
        assert result["C"] == [19, 24, 25, 26, 27, 28, 29, 30]

    def test_no_cluster_map(self):
        assert AnalyticsConcludeStage._parse_cluster_map("# No cluster map here") == {}


# ---------------------------------------------------------------------------
# _deduplicate_chunks
# ---------------------------------------------------------------------------

class TestDeduplicateChunks:
    def test_removes_title_prefix(self):
        text = "My Title: First chunk content. My Title: Second chunk content."
        result = _deduplicate_chunks(text, "My Title")
        assert result.count("My Title: ") == 0
        assert "First chunk content." in result
        assert "Second chunk content." in result

    def test_deduplicates_long_paragraphs(self):
        long_para = "x" * 60
        text = f"My Title: {long_para}\nshort\nMy Title: {long_para}\nmore"
        result = _deduplicate_chunks(text, "My Title")
        assert result.count(long_para) == 1
        assert "short" in result
        assert "more" in result

    def test_no_title_noop(self):
        text = "Some text without title prefixes"
        assert _deduplicate_chunks(text, "Other Title") == text

    def test_empty_inputs(self):
        assert _deduplicate_chunks("", "Title") == ""
        assert _deduplicate_chunks("content", "") == "content"


# ---------------------------------------------------------------------------
# _format_article
# ---------------------------------------------------------------------------

class TestFormatArticle:
    def test_basic_format(self):
        article = {
            "url": "https://example.com/article",
            "title": "Test Article",
            "author": "John Doe",
            "publisher": "Example News",
            "date_publication": "2025-01-15",
            "full_text": "This is the article body.",
        }
        result = _format_article(14, article)
        assert "### Article 14: Test Article" in result
        assert "**Author:** John Doe" in result
        assert "**Publisher:** Example News" in result
        assert "(bundle_014, summary)" in result
        assert "This is the article body." in result

    def test_missing_fields(self):
        article = {"url": "https://example.com", "full_text": "Body"}
        result = _format_article(1, article)
        assert "### Article 1: (untitled)" in result
        assert "(bundle_001, summary)" in result

    def test_doc_metadata_fallback(self):
        article = {
            "url": "https://example.com",
            "title": "Title",
            "author": "",
            "publisher": "",
            "doc_metadata": {
                "authors": ["Jane Smith", "Bob Lee"],
                "publisher": "Metro News",
            },
            "full_text": "Text",
        }
        result = _format_article(5, article)
        assert "Jane Smith, Bob Lee" in result
        assert "Metro News" in result

    def test_req_document_id_citation(self):
        article = {
            "url": "https://example.com/article",
            "title": "Test Article",
            "document_id": "req_20260302_220748_005",
            "full_text": "Body.",
        }
        result = _format_article(7, article)
        assert "(req_20260302_220748_005, summary)" in result
        assert "bundle_" not in result

    def test_empty_document_id_falls_back(self):
        article = {
            "url": "https://example.com/article",
            "title": "Test Article",
            "document_id": "",
            "full_text": "Body.",
        }
        result = _format_article(3, article)
        assert "(bundle_003, summary)" in result


# ---------------------------------------------------------------------------
# _build_fixed_grounding
# ---------------------------------------------------------------------------

class TestBuildFixedGrounding:
    def test_includes_all_parts(self):
        phase4_md = "# Phase 4 analysis\n\nSome analysis.\n\n# APPENDIX: Events"
        narratives = {"A": "Cluster A narrative", "B": "Cluster B narrative"}
        eo_summaries = [
            {
                "eo_number": "14148",
                "title": "Test EO",
                "url": "https://gov.example.com/eo",
                "summary_text": "Summary of the EO.",
            }
        ]

        result = AnalyticsConcludeStage._build_fixed_grounding(
            phase4_md, narratives, eo_summaries, "culture_and_media",
        )

        assert "PART 1 — PHASE 4 CLUSTER ANALYSIS" in result
        assert "Some analysis." in result
        # Appendix should be excluded
        assert "APPENDIX: Events" not in result
        assert "PART 2 — KALIYA MEDIA AGENDA ANALYSIS" in result
        assert "Cluster A narrative" in result
        assert "Cluster B narrative" in result
        assert "PART 3 — EXECUTIVE ORDER POLICY SUMMARIES" in result
        assert "Executive Order 14148" in result
        assert "Summary of the EO." in result

    def test_no_narratives(self):
        result = AnalyticsConcludeStage._build_fixed_grounding(
            "Analysis", {}, [], "test_cat",
        )
        assert "PART 1" in result
        assert "PART 2" not in result
        assert "PART 3" not in result


# ---------------------------------------------------------------------------
# _strip_code_fencing
# ---------------------------------------------------------------------------

class TestStripCodeFencing:
    def test_markdown_fencing(self):
        text = "```markdown\n# Hello\nWorld\n```"
        assert _strip_code_fencing(text) == "# Hello\nWorld"

    def test_no_fencing(self):
        assert _strip_code_fencing("Normal text") == "Normal text"

    def test_empty(self):
        assert _strip_code_fencing("") == ""


# ---------------------------------------------------------------------------
# _calculate_cost
# ---------------------------------------------------------------------------

class TestCalculateCost:
    def test_gpt54(self):
        cost = _calculate_cost("gpt-5.4", 1_000_000, 100_000)
        assert cost == pytest.approx(2.50 + 1.00, rel=1e-4)

    def test_unknown_model(self):
        # Falls back to gpt-5.4 pricing
        cost = _calculate_cost("unknown-model", 1_000_000, 0)
        assert cost == pytest.approx(2.50, rel=1e-4)


# ---------------------------------------------------------------------------
# _load_phase4_analysis (filesystem)
# ---------------------------------------------------------------------------

class TestLoadPhase4Analysis:
    def test_finds_latest(self, tmp_path: Path):
        (tmp_path / "phase4_culture_and_media_20260328_181000.md").write_text("old")
        (tmp_path / "phase4_culture_and_media_20260328_183618.md").write_text("new")
        (tmp_path / "phase4_culture_and_media_20260328_183618.bundle.jsonl").write_text("{}")

        text, path = AnalyticsConcludeStage._load_phase4_analysis(
            tmp_path, "culture_and_media",
        )
        assert text == "new"
        assert "183618" in path.name

    def test_no_files(self, tmp_path: Path):
        text, _ = AnalyticsConcludeStage._load_phase4_analysis(tmp_path, "nope")
        assert text == ""


# ---------------------------------------------------------------------------
# _load_kaliya_narratives
# ---------------------------------------------------------------------------

class TestLoadKaliyaNarratives:
    def test_finds_latest_per_cluster(self, tmp_path: Path):
        (tmp_path / "kaliya-culture_and_media-cluster_A-20260330_214056_old_narrative.md").write_text("old A")
        (tmp_path / "kaliya-culture_and_media-cluster_A-20260331_093559_new_narrative.md").write_text("new A")
        (tmp_path / "kaliya-culture_and_media-cluster_B-20260331_093559_abc_narrative.md").write_text("B text")

        result = AnalyticsConcludeStage._load_kaliya_narratives(
            tmp_path, "culture_and_media",
        )
        assert set(result.keys()) == {"A", "B"}
        assert result["A"] == "new A"
        assert result["B"] == "B text"


# ---------------------------------------------------------------------------
# _inject_document_ids
# ---------------------------------------------------------------------------

class TestInjectDocumentIds:
    def test_injects_matching_ids(self):
        articles = [
            {"url": "https://example.com/a", "title": "A"},
            {"url": "https://example.com/b", "title": "B"},
            {"url": "https://example.com/c", "title": "C"},
        ]
        url_to_docid = {
            "https://example.com/a": "req_20260302_220748_001",
            "https://example.com/c": "req_20260302_220748_003",
        }
        AnalyticsConcludeStage._inject_document_ids(articles, url_to_docid)
        assert articles[0]["document_id"] == "req_20260302_220748_001"
        assert articles[1]["document_id"] == ""
        assert articles[2]["document_id"] == "req_20260302_220748_003"


# ---------------------------------------------------------------------------
# _find_bundle
# ---------------------------------------------------------------------------

class TestFindBundle:
    def test_finds_latest(self, tmp_path: Path):
        (tmp_path / "phase4_culture_and_media_20260328_181239.bundle.jsonl").write_text("{}")
        (tmp_path / "phase4_culture_and_media_20260328_183618.bundle.jsonl").write_text("{}")

        result = AnalyticsConcludeStage._find_bundle(tmp_path, "culture_and_media")
        assert result is not None
        assert "183618" in result.name

    def test_not_found(self, tmp_path: Path):
        assert AnalyticsConcludeStage._find_bundle(tmp_path, "nope") is None


# ---------------------------------------------------------------------------
# _sample_articles (token budgeting)
# ---------------------------------------------------------------------------

class TestSampleArticles:
    def _make_enc(self):
        """Create a mock tokenizer that counts words as tokens."""
        enc = MagicMock()
        enc.encode = lambda text: text.split()
        return enc

    def test_samples_within_budget(self):
        enc = self._make_enc()
        articles = [
            {
                "url": f"https://example.com/{i}",
                "title": f"Article {i}",
                "author": "Author",
                "publisher": "Pub",
                "date_publication": "2025-01-01",
                "full_text": "word " * 100,  # ~100 tokens per article
            }
            for i in range(1, 21)
        ]
        cluster_map = {
            "A": list(range(1, 11)),   # articles 1-10
            "B": list(range(11, 21)),  # articles 11-20
        }

        text, count = AnalyticsConcludeStage._sample_articles(
            articles, cluster_map, 2000, enc,
        )
        assert count > 0
        assert count <= 20
        # Should have articles from both clusters
        assert "Cluster A" in text
        assert "Cluster B" in text

    def test_empty_articles(self):
        enc = self._make_enc()
        text, count = AnalyticsConcludeStage._sample_articles(
            [], {}, 50000, enc,
        )
        assert text == ""
        assert count == 0

    def test_cross_cluster_dedup(self):
        """Articles in multiple clusters should only be emitted once."""
        enc = self._make_enc()
        articles = [
            {
                "url": f"https://example.com/{i}",
                "title": f"Article {i}",
                "author": "Author",
                "publisher": "Pub",
                "date_publication": "2025-01-01",
                "full_text": "word " * 50,
            }
            for i in range(1, 6)
        ]
        # Article 3 appears in both clusters
        cluster_map = {
            "A": [1, 2, 3],
            "B": [3, 4, 5],
        }
        text, count = AnalyticsConcludeStage._sample_articles(
            articles, cluster_map, 100000, enc,
        )
        # Article 3 heading should appear exactly once
        assert text.count("### Article 3:") == 1
        assert count == 5  # 5 unique articles, not 6
