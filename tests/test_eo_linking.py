"""Tests for EO linking core classes: ArticleRecord, GovMilExtractor,
IndexLoader, EORegistry, TokenBudgetBatcher, LinkageExtractor,
EOImplementationCache, CostTracker, ConversationBuilder, and helper functions.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from src.components.analysis.eo_linking import (ArticleRecord,
                                                ConversationBuilder,
                                                CostTracker, EOEntry,
                                                EOImplementationCache,
                                                EORegistry, GovMilExtractor,
                                                IndexLoader, LinkageExtractor,
                                                TokenBudgetBatcher,
                                                _clean_json_markdown, _eo_name,
                                                _normalise_date,
                                                _normalise_url,
                                                _parse_date_loose, _truncate,
                                                filter_precedence_graph)

# ===================================================================
# ArticleRecord
# ===================================================================


class TestArticleRecord:
    def test_basic_attributes(self):
        raw = {
            "document_id": "d1",
            "source_url": "https://www.state.gov/report",
            "summary_text": "Summary text",
            "doc_metadata": {"title": "Report Title", "publisher": "State"},
        }
        rec = ArticleRecord(raw, "x_indices_merged")
        assert rec.document_id == "d1"
        assert rec.source_url == "https://www.state.gov/report"
        assert rec.summary_text == "Summary text"
        assert rec.index_folder == "x_indices_merged"
        assert rec.raw is raw

    def test_title_property(self):
        rec = ArticleRecord({"doc_metadata": {"title": "My Title"}}, "idx")
        assert rec.title == "My Title"

    def test_title_missing_metadata(self):
        rec = ArticleRecord({}, "idx")
        assert rec.title == ""

    def test_domain_property(self):
        rec = ArticleRecord({"source_url": "https://www.defense.gov/path"}, "idx")
        assert rec.domain == "www.defense.gov"

    def test_domain_empty_url(self):
        rec = ArticleRecord({"source_url": ""}, "idx")
        assert rec.domain == ""

    def test_domain_missing_url(self):
        rec = ArticleRecord({}, "idx")
        assert rec.domain == ""


# ===================================================================
# _normalise_url
# ===================================================================


class TestNormaliseUrl:
    def test_strips_scheme_and_trailing_slash(self):
        assert _normalise_url("https://example.gov/path/") == "example.gov/path"

    def test_strips_fragment_and_query(self):
        assert _normalise_url("https://a.gov/p?q=1#frag") == "a.gov/p"

    def test_empty(self):
        assert _normalise_url("") == ""

    def test_http_scheme(self):
        assert _normalise_url("http://a.gov/p") == "a.gov/p"


# ===================================================================
# GovMilExtractor
# ===================================================================


class TestGovMilExtractor:
    def _make_article(self, url: str, summary: str = "x") -> ArticleRecord:
        return ArticleRecord(
            {"source_url": url, "summary_text": summary, "document_id": url},
            "idx",
        )

    def test_extract_gov_domain(self):
        ext = GovMilExtractor()
        catalog = [
            self._make_article("https://www.state.gov/report"),
            self._make_article("https://www.nytimes.com/article"),
        ]
        result = ext.extract(catalog)
        assert len(result) == 1
        assert result[0].domain == "www.state.gov"

    def test_extract_mil_domain(self):
        ext = GovMilExtractor()
        catalog = [self._make_article("https://www.defense.mil/info")]
        assert len(ext.extract(catalog)) == 1

    def test_exclude_domain(self):
        ext = GovMilExtractor(domain_exclude=["www.state.gov"])
        catalog = [self._make_article("https://www.state.gov/report")]
        assert len(ext.extract(catalog)) == 0

    def test_dedup_by_url(self):
        ext = GovMilExtractor()
        catalog = [
            self._make_article("https://www.state.gov/same", "short"),
            self._make_article("https://www.state.gov/same", "longer summary"),
        ]
        result = ext.extract(catalog)
        assert len(result) == 1
        assert result[0].summary_text == "longer summary"

    def test_custom_include(self):
        ext = GovMilExtractor(domain_include=["example.com"])
        catalog = [
            self._make_article("https://example.com/page"),
            self._make_article("https://www.state.gov/page"),
        ]
        result = ext.extract(catalog)
        assert len(result) == 1
        assert result[0].domain == "example.com"

    def test_is_included_suffix_match(self):
        ext = GovMilExtractor()
        assert ext._is_included("sub.state.gov") is True

    def test_is_included_exact(self):
        ext = GovMilExtractor(domain_include=["dirco.gov.za"])
        assert ext._is_included("dirco.gov.za") is True

    def test_is_excluded_suffix(self):
        ext = GovMilExtractor(domain_exclude=[".test.gov"])
        assert ext._is_excluded("sub.test.gov") is True
        assert ext._is_excluded("other.gov") is False

    def test_empty_catalog(self):
        ext = GovMilExtractor()
        assert ext.extract([]) == []


# ===================================================================
# IndexLoader
# ===================================================================


class TestIndexLoader:
    def test_load_jsonl_files(self, tmp_path):
        folder = tmp_path / "x_indices_test"
        folder.mkdir()
        records = [
            {"document_id": "d1", "source_url": "https://a.gov/1", "summary_text": "s1"},
            {"document_id": "d2", "source_url": "https://b.gov/2", "summary_text": "s2"},
        ]
        jl = folder / "research_summaries_combined.jsonl"
        jl.write_text(
            "\n".join(json.dumps(r) for r in records), encoding="utf-8"
        )
        loader = IndexLoader()
        catalog = loader.load(["x_indices_test"], base_dir=tmp_path)
        assert len(catalog) == 2
        assert catalog[0].document_id == "d1"
        assert catalog[0].index_folder == "x_indices_test"

    def test_load_missing_folder(self, tmp_path):
        loader = IndexLoader()
        catalog = loader.load(["nonexistent"], base_dir=tmp_path)
        assert catalog == []

    def test_load_skip_bad_json(self, tmp_path):
        folder = tmp_path / "x_test"
        folder.mkdir()
        jl = folder / "research_summaries.jsonl"
        jl.write_text('bad json\n{"document_id":"d1"}\n', encoding="utf-8")
        loader = IndexLoader()
        catalog = loader.load(["x_test"], base_dir=tmp_path)
        assert len(catalog) == 1


# ===================================================================
# EORegistry
# ===================================================================


class TestEORegistry:
    def _write_chapter(self, d: Path, name: str, content: str) -> None:
        (d / name).write_text(content, encoding="utf-8")

    def test_load_chapters_and_supercessions(self, tmp_path):
        book_dir = tmp_path / "book"
        book_dir.mkdir()
        self._write_chapter(
            book_dir,
            "01-eo.md",
            "# Exec. Order No. 14148 (2025): Initial Rescissions\n\n"
            "This order rescinded Exec. Order No. 13985 and revoked Exec. Order No. 14004.\n",
        )
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        reg = EORegistry.load(book_dir, idx_dir)
        assert "14148" in reg.entries
        assert "14148" in reg.entries["14148"].title
        assert "13985" in reg.supercessions.get("14148", [])
        assert "14004" in reg.supercessions.get("14148", [])

    def test_load_events_jsonl(self, tmp_path):
        book_dir = tmp_path / "book"
        book_dir.mkdir()
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        events = [{"eo_identifier": "EO 14148", "eo_title": "Test", "eo_date": "2025"}]
        (idx_dir / "eo_events_combined.jsonl").write_text(
            "\n".join(json.dumps(e) for e in events), encoding="utf-8"
        )
        reg = EORegistry.load(book_dir, idx_dir)
        assert "14148" in reg.entries
        assert len(reg.entries["14148"].events) == 1

    def test_load_summaries_matched(self, tmp_path):
        book_dir = tmp_path / "book"
        book_dir.mkdir()
        self._write_chapter(
            book_dir,
            "01-eo.md",
            "# Exec. Order No. 14148 (2025): Test\n\nBody.\n",
        )
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        summaries = [
            {"summary_text": "This is about Executive Order 14148 provisions."},
        ]
        (idx_dir / "research_summaries_combined.jsonl").write_text(
            "\n".join(json.dumps(s) for s in summaries), encoding="utf-8"
        )
        reg = EORegistry.load(book_dir, idx_dir)
        assert len(reg.entries["14148"].summaries) == 1

    def test_full_grounding_text(self, tmp_path):
        book_dir = tmp_path / "book"
        book_dir.mkdir()
        self._write_chapter(book_dir, "01-ch.md", "Content A")
        self._write_chapter(book_dir, "02-ch.md", "Content B")
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        reg = EORegistry.load(book_dir, idx_dir)
        text = reg.full_grounding_text
        assert "01-ch.md" in text
        assert "Content A" in text
        assert "Content B" in text

    def test_skip_index_md(self, tmp_path):
        book_dir = tmp_path / "book"
        book_dir.mkdir()
        self._write_chapter(book_dir, "index.md", "# Index\n\nShould be skipped")
        self._write_chapter(
            book_dir,
            "01-eo.md",
            "# Exec. Order No. 14148 (2025): Test\n\n",
        )
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        reg = EORegistry.load(book_dir, idx_dir)
        filenames = [f for f, _ in reg.chapter_texts]
        assert "index.md" not in filenames

    def test_empty_dirs(self, tmp_path):
        book_dir = tmp_path / "book"
        book_dir.mkdir()
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        reg = EORegistry.load(book_dir, idx_dir)
        assert len(reg.entries) == 0


# ===================================================================
# TokenBudgetBatcher
# ===================================================================


class TestTokenBudgetBatcher:
    def _articles(self, n: int, folder: str = "idx") -> list[ArticleRecord]:
        return [
            ArticleRecord(
                {"document_id": f"d{i}", "source_url": f"https://a.gov/{i}"},
                folder,
            )
            for i in range(n)
        ]

    def test_single_batch(self):
        batcher = TokenBudgetBatcher(batch_size=10)
        batches = batcher.batch(self._articles(5))
        assert len(batches) == 1
        assert len(batches[0]) == 5

    def test_multiple_batches(self):
        batcher = TokenBudgetBatcher(batch_size=3)
        batches = batcher.batch(self._articles(7))
        assert len(batches) == 3
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 1

    def test_empty(self):
        batcher = TokenBudgetBatcher()
        assert batcher.batch([]) == []

    def test_groups_by_folder(self):
        batcher = TokenBudgetBatcher(batch_size=100)
        arts = self._articles(2, "a") + self._articles(2, "b")
        batches = batcher.batch(arts)
        assert len(batches) == 1
        # articles grouped by folder, sorted by folder name
        folders = [a.index_folder for a in batches[0]]
        assert folders == ["a", "a", "b", "b"]


# ===================================================================
# CostTracker
# ===================================================================


class TestCostTracker:
    def test_record_and_summary(self):
        tracker = CostTracker()
        tracker.record([
            {"execution_time_ms": 1000, "llm_params": {"input_tokens": 500, "output_tokens": 100}},
            {"execution_time_ms": 2000, "llm_params": {"input_tokens": 300, "output_tokens": 50}},
        ])
        s = tracker.summary("gemini-2.5-flash")
        assert s["total_calls"] == 2
        assert s["total_input_tokens"] == 800
        assert s["total_output_tokens"] == 150
        assert s["total_time_s"] == 3.0
        assert s["estimated_cost_usd"] > 0

    def test_empty_summary(self):
        tracker = CostTracker()
        s = tracker.summary()
        assert s["total_calls"] == 0
        assert s["estimated_cost_usd"] == 0


# ===================================================================
# ConversationBuilder
# ===================================================================


class TestConversationBuilder:
    def test_build_and_save(self, tmp_path):
        builder = ConversationBuilder(tmp_path)
        chat_results = [
            {
                "article_url": "https://a.gov/1",
                "article_title": "Title 1",
                "document_id": "d1",
                "publisher": "Pub",
                "date": "2025-01-01",
                "domain": "a.gov",
                "index_folder": "idx",
                "article_content": "Content here",
            },
        ]
        out = builder.build_and_save(0, chat_results)
        assert out.exists()
        data = json.loads(out.read_text())
        assert len(data) == 1
        assert data[0]["metadata"]["article_url"] == "https://a.gov/1"

    def test_creates_batch_dir(self, tmp_path):
        builder = ConversationBuilder(tmp_path)
        builder.build_and_save(5, [{"article_content": "x"}])
        assert (tmp_path / "batch_5" / "conversation.json").exists()


# ===================================================================
# _clean_json_markdown
# ===================================================================


class TestCleanJsonMarkdown:
    def test_strip_json_fence(self):
        assert _clean_json_markdown('```json\n{"a":1}\n```') == '{"a":1}'

    def test_strip_bare_fence(self):
        assert _clean_json_markdown("```\ndata\n```") == "data"

    def test_no_fence(self):
        assert _clean_json_markdown('{"a": 1}') == '{"a": 1}'


# ===================================================================
# filter_precedence_graph
# ===================================================================


class TestFilterPrecedenceGraph:
    def test_removes_excluded_top_level(self):
        graph = {
            "14148": {"rescinds": ["13985"], "derived_from": [], "read_with": []},
            "13985": {"rescinds": [], "derived_from": [], "read_with": []},
        }
        filtered = filter_precedence_graph(graph, {"13985"})
        assert "13985" not in filtered
        assert filtered["14148"]["rescinds"] == []

    def test_empty_excludes(self):
        graph = {"14148": {"rescinds": ["13985"], "derived_from": [], "read_with": []}}
        assert filter_precedence_graph(graph, set()) is graph

    def test_removes_from_all_lists(self):
        graph = {
            "14148": {
                "rescinds": ["13985"],
                "derived_from": ["13985"],
                "read_with": ["13985"],
            },
        }
        filtered = filter_precedence_graph(graph, {"13985"})
        assert filtered["14148"] == {"rescinds": [], "derived_from": [], "read_with": []}


# ===================================================================
# LinkageExtractor
# ===================================================================


class TestLinkageExtractor:
    def test_extract_format_b_executive_orders(self):
        """Schema B: {"executive_orders": {"Exec. Order No. 14148": {...}}}"""
        reflect_json = json.dumps({
            "executive_orders": {
                "Exec. Order No. 14148": {
                    "title": "Initial Rescissions",
                    "rescinds_or_supersedes": ["Exec. Order No. 13985"],
                    "derived_from_or_read_together_with": [],
                    "communications": [
                        {
                            "url": "https://www.state.gov/comm1",
                            "communication_title": "State Comm",
                            "date": "2025-02-01",
                            "action_decision_or_event": "Implements rescission",
                            "responsible_official_or_department": "State Dept",
                            "implementation_status": "implements",
                            "confidence_score": 0.9,
                        }
                    ],
                }
            }
        })
        ext = LinkageExtractor()
        records = ext.extract(reflect_json, "batch_0")
        assert len(records) == 1
        assert records[0]["eo_number"] == "14148"
        assert records[0]["communication"]["url"] == "https://www.state.gov/comm1"
        assert records[0]["precedence"]["rescinds"] == ["13985"]

    def test_extract_no_comms_eo_only(self):
        reflect_json = json.dumps({
            "executive_orders": {
                "Exec. Order No. 14179": {
                    "title": "Death Penalty",
                    "rescinds_or_supersedes": [],
                    "derived_from_or_read_together_with": ["Exec. Order No. 14148"],
                    "communications": [],
                }
            }
        })
        ext = LinkageExtractor()
        records = ext.extract(reflect_json, "batch_1")
        assert len(records) == 1
        assert records[0]["communication"] is None
        assert records[0]["precedence"]["derived_from"] == ["14148"]

    def test_extract_format_a_direct_keys(self):
        """Schema A: direct EO keys at top level."""
        reflect_json = json.dumps({
            "Exec. Order No. 14148 (2025)": {
                "title": "Test",
                "communications": [
                    {
                        "url": "https://a.gov/x",
                        "date": "2025-01-21",
                        "action_decision_or_event": "Does something",
                        "responsible_official_or_department": "WH",
                        "implementation_status": "implements",
                        "confidence_score": 0.8,
                    }
                ],
                "rescinds_or_supersedes": [],
                "derived_from_or_read_together_with": [],
            }
        })
        ext = LinkageExtractor()
        records = ext.extract(reflect_json, "batch_2")
        assert len(records) == 1
        assert records[0]["eo_number"] == "14148"

    def test_extract_format_c_list(self):
        """Schema C: top-level list."""
        reflect_json = json.dumps([
            {
                "eo_number": "14192",
                "title": "Deregulation",
                "communications": [],
                "legal_precedence": {
                    "rescinds_or_supersedes": [],
                    "derived_from_or_read_together_with": ["Exec. Order No. 14148"],
                },
            }
        ])
        ext = LinkageExtractor()
        records = ext.extract(reflect_json, "batch_3")
        assert len(records) == 1
        assert records[0]["eo_number"] == "14192"
        assert records[0]["precedence"]["derived_from"] == ["14148"]

    def test_extract_json_in_markdown_fence(self):
        text = '```json\n{"executive_orders": {"Exec. Order No. 14148": {"title": "T", "communications": [], "rescinds_or_supersedes": [], "derived_from_or_read_together_with": []}}}\n```'
        ext = LinkageExtractor()
        records = ext.extract(text, "batch_4")
        assert len(records) == 1

    def test_extract_eo_nums_varied_formats(self):
        ext = LinkageExtractor()
        items = [
            "Exec. Order No. 13985",
            "Executive Order No 14004",
            "EO 14148",
        ]
        result = ext._extract_eo_nums(items)
        assert "13985" in result
        assert "14004" in result
        assert "14148" in result

    def test_extract_eo_nums_empty(self):
        ext = LinkageExtractor()
        assert ext._extract_eo_nums([]) == []
        assert ext._extract_eo_nums(None) == []

    def test_extract_domain(self):
        assert LinkageExtractor._extract_domain("https://www.state.gov/page") == "www.state.gov"
        assert LinkageExtractor._extract_domain("") == ""

    def test_flatten_eo_entry_metadata(self):
        entry = {
            "metadata": {"title": "Test Title", "date_signed": "2025"},
            "communications": [],
        }
        flat = LinkageExtractor._flatten_eo_entry(entry)
        assert flat["title"] == "Test Title"
        assert flat["eo_date"] == "2025"
        assert "metadata" not in flat

    def test_flatten_eo_entry_legal_precedence(self):
        entry = {
            "legal_precedence": {
                "rescinds_or_supersedes": ["Exec. Order No. 13985"],
                "derived_from_or_read_together_with": [],
            },
            "communications": [],
        }
        flat = LinkageExtractor._flatten_eo_entry(entry)
        assert flat["rescinds_or_supersedes"] == ["Exec. Order No. 13985"]

    def test_normalise_comm_field_names(self):
        entry = {
            "communications": [
                {
                    "source_url": "https://a.gov/x",
                    "action_decision_event": "did thing",
                    "responsible_official_department": "DoD",
                }
            ],
        }
        flat = LinkageExtractor._flatten_eo_entry(entry)
        comm = flat["communications"][0]
        assert "url" in comm
        assert "action_decision_or_event" in comm
        assert "responsible_official_or_department" in comm

    def test_empty_reflect_text(self):
        ext = LinkageExtractor()
        assert ext.extract("no json here at all", "batch_x") == []


# ===================================================================
# EOImplementationCache
# ===================================================================


class TestEOImplementationCache:
    def _make_records(self) -> list[dict]:
        return [
            {
                "eo_number": "14148",
                "eo_title": "Test",
                "communication": {"url": "https://a.gov/1", "domain": "a.gov"},
                "precedence": {"rescinds": ["13985"], "derived_from": [], "read_with": []},
                "record_id": "rec1",
            },
            {
                "eo_number": "14192",
                "eo_title": "Dereg",
                "communication": {"url": "https://b.gov/2", "domain": "b.gov"},
                "precedence": {"rescinds": [], "derived_from": ["14148"], "read_with": []},
                "record_id": "rec2",
            },
        ]

    def test_append_and_load(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        added = cache.append(self._make_records())
        assert added == 2
        loaded = cache.load()
        assert len(loaded) == 2

    def test_dedup_on_append(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        recs = self._make_records()
        cache.append(recs)
        added = cache.append(recs)  # same records again
        assert added == 0
        assert len(cache.load()) == 2

    def test_by_eo(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        cache.append(self._make_records())
        eo14148 = cache.by_eo("14148")
        assert len(eo14148) == 1
        assert eo14148[0]["eo_number"] == "14148"

    def test_by_domain(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        cache.append(self._make_records())
        result = cache.by_domain("a.gov")
        assert len(result) == 1

    def test_precedence_graph(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        cache.append(self._make_records())
        graph = cache.precedence_graph()
        assert "14148" in graph
        assert "13985" in graph["14148"]["rescinds"]
        assert "14148" in graph["14192"]["derived_from"]

    def test_validate_precedence_circular(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        cache.append([
            {
                "eo_number": "14148",
                "precedence": {"rescinds": ["14192"], "derived_from": [], "read_with": []},
                "record_id": "r1",
            },
            {
                "eo_number": "14192",
                "precedence": {"rescinds": ["14148"], "derived_from": [], "read_with": []},
                "record_id": "r2",
            },
        ])
        warnings = cache.validate_precedence()
        assert any("Circular" in w for w in warnings)

    def test_validate_no_circulars(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        cache.append(self._make_records())
        assert cache.validate_precedence() == []

    def test_summary(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        cache.append(self._make_records())
        s = cache.summary()
        assert s["total_records"] == 2
        assert s["unique_eos"] == 2
        assert s["records_with_comms"] == 2

    def test_persistence(self, tmp_path):
        path = tmp_path / "cache.jsonl"
        cache1 = EOImplementationCache(path)
        cache1.append(self._make_records())
        # Re-instantiate — should load from file
        cache2 = EOImplementationCache(path)
        assert len(cache2.load()) == 2

    def test_skip_records_without_id(self, tmp_path):
        cache = EOImplementationCache(tmp_path / "cache.jsonl")
        added = cache.append([{"eo_number": "14148"}])  # no record_id
        assert added == 0


# ===================================================================
# Helper functions
# ===================================================================


class TestParseDateLoose:
    def test_standard_formats(self):
        assert _parse_date_loose("January 27, 2025") == datetime(2025, 1, 27)
        assert _parse_date_loose("2025-01-27") == datetime(2025, 1, 27)
        assert _parse_date_loose("01/27/2025") == datetime(2025, 1, 27)
        assert _parse_date_loose("2025") == datetime(2025, 1, 1)

    def test_empty(self):
        assert _parse_date_loose("") is None
        assert _parse_date_loose("not a date") is None


class TestNormaliseDate:
    def test_full_date(self):
        assert _normalise_date("January 27, 2025") == "2025-01-27"

    def test_year_only(self):
        assert _normalise_date("2025") == "2025"

    def test_empty(self):
        assert _normalise_date("") == "—"

    def test_unparseable(self):
        assert _normalise_date("unknown") == "unknown"


class TestEoName:
    def test_from_name_lookup(self):
        from src.components.analysis.eo_linking import EONameLookup

        lookup = EONameLookup.__new__(EONameLookup)
        lookup._names = {"14148": "Initial Rescissions"}
        assert _eo_name("14148", name_lookup=lookup) == "Initial Rescissions"

    def test_from_registry(self):
        reg = EORegistry.__new__(EORegistry)
        reg.entries = {"14148": EOEntry("14148", title="Test Title")}
        assert _eo_name("14148", registry=reg) == "Test Title"

    def test_fallback_empty(self):
        assert _eo_name("99999") == ""


class TestTruncate:
    def test_short_text(self):
        assert _truncate("hello", 10) == "hello"

    def test_long_text(self):
        result = _truncate("a" * 200, 120)
        assert len(result) == 120
        assert result.endswith("…")

    def test_empty(self):
        assert _truncate("") == ""
        assert _truncate(None) == ""
