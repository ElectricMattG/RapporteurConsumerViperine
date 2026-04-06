"""Tests for media_linking Phase 1 components: MediaExtractor, MediaLinkageCache.

Phase 2-3-4 components: MediaLinkReflectDriver, MediaLinkageExtractor,
_format_eo_cache_as_grounding, prompts.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.components.analysis.eo_linking import ArticleRecord
from src.components.analysis.media_linking import (
    _MEDIA_LINK_PROMPT, _MEDIA_REFLECT_PROMPT, MediaExtractor,
    MediaLinkageCache, MediaLinkageExtractor, MediaLinkReflectDriver,
    _format_eo_cache_as_grounding, _media_record_id)

# ===================================================================
# Helpers
# ===================================================================

def _make_article(
    url: str,
    summary: str = "short",
    index_folder: str = "x_indices_merged",
    title: str = "Test Article",
) -> ArticleRecord:
    return ArticleRecord(
        raw={
            "document_id": f"doc-{hash(url) % 10000}",
            "source_url": url,
            "summary_text": summary,
            "doc_metadata": {"title": title},
        },
        index_folder=index_folder,
    )


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ===================================================================
# MediaExtractor
# ===================================================================


class TestMediaExtractorDomainFiltering:
    """Verifies that gov/mil domains are excluded and media kept."""

    def test_excludes_dot_gov(self):
        catalog = [
            _make_article("https://www.whitehouse.gov/eo14148"),
            _make_article("https://www.nytimes.com/article"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert result[0].source_url == "https://www.nytimes.com/article"

    def test_excludes_dot_mil(self):
        catalog = [
            _make_article("https://www.defense.mil/release"),
            _make_article("https://www.bbc.com/news/story"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert "bbc.com" in result[0].source_url

    def test_excludes_dvids(self):
        catalog = [
            _make_article("https://www.dvidshub.net/video/123"),
            _make_article("https://dvids.net/news/456"),
            _make_article("https://reuters.com/article/x"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert "reuters.com" in result[0].source_url

    def test_excludes_gov_il(self):
        catalog = [
            _make_article("https://www.gov.il/en/departments/news/x"),
            _make_article("https://www.timesofisrael.com/article"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert "timesofisrael.com" in result[0].source_url

    def test_excludes_dirco_gov_za(self):
        catalog = [
            _make_article("https://dirco.gov.za/statement"),
            _make_article("https://www.news24.com/article"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert "news24.com" in result[0].source_url

    def test_keeps_all_media(self):
        catalog = [
            _make_article("https://www.cnn.com/politics"),
            _make_article("https://www.foxnews.com/politics"),
            _make_article("https://apnews.com/article/123"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 3

    def test_excludes_all_gov_mil_variants(self):
        catalog = [
            _make_article("https://www.state.gov/press-release"),
            _make_article("https://www.army.mil/news"),
            _make_article("https://www.dvidshub.net/image"),
            _make_article("https://www.gov.il/en/press"),
            _make_article("https://dirco.gov.za/docs"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 0

    def test_empty_catalog(self):
        result = MediaExtractor().extract([])
        assert result == []

    def test_no_url_articles_skipped(self):
        catalog = [_make_article("")]
        result = MediaExtractor().extract(catalog)
        assert result == []

    def test_custom_domain_exclude(self):
        extractor = MediaExtractor(domain_exclude=[".edu"])
        catalog = [
            _make_article("https://harvard.edu/paper"),
            _make_article("https://www.washingtonpost.com/story"),
            _make_article("https://www.whitehouse.gov/eo"),
        ]
        result = extractor.extract(catalog)
        # .edu excluded, .gov NOT excluded (custom list replaces default)
        assert len(result) == 2
        domains = {a.domain for a in result}
        assert "harvard.edu" not in domains


class TestMediaExtractorDedup:
    """Verifies dedup by normalised URL — longest summary wins."""

    def test_dedup_keeps_longest_summary(self):
        catalog = [
            _make_article("https://www.nytimes.com/article", summary="short"),
            _make_article(
                "https://www.nytimes.com/article",
                summary="a much longer summary text with more detail",
                index_folder="x_indices_merged",
            ),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert result[0].summary_text == "a much longer summary text with more detail"

    def test_dedup_normalises_url(self):
        """Same article with scheme/query/fragment differences collapses to one."""
        catalog = [
            _make_article("https://www.cnn.com/story", summary="short"),
            _make_article("http://www.cnn.com/story", summary="longer version"),
            _make_article("https://www.cnn.com/story?ref=homepage", summary="x"),
            _make_article("https://www.cnn.com/story#section", summary="y"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert result[0].summary_text == "longer version"

    def test_dedup_trailing_slash(self):
        catalog = [
            _make_article("https://bbc.com/news/", summary="abc"),
            _make_article("https://bbc.com/news", summary="abcdef"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert result[0].summary_text == "abcdef"

    def test_cross_index_dedup(self):
        """Same URL from two different indices — longest wins regardless of index."""
        catalog = [
            _make_article(
                "https://reuters.com/article/x",
                summary="brief",
                index_folder="x_indices_merged",
            ),
            _make_article(
                "https://reuters.com/article/x",
                summary="this is a much more detailed and enriched summary from hegseth",
                index_folder="x_indices_merged",
            ),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 1
        assert result[0].index_folder == "x_indices_merged"


class TestMediaExtractorMixedCatalog:
    """End-to-end filter + dedup on a realistic mixed catalog."""

    def test_mixed_catalog(self):
        catalog = [
            # gov/mil — excluded
            _make_article("https://www.whitehouse.gov/eo14148"),
            _make_article("https://www.defense.mil/release"),
            _make_article("https://www.dvidshub.net/video/1"),
            # media — kept
            _make_article("https://www.nytimes.com/article/a", summary="nyt short"),
            _make_article("https://www.nytimes.com/article/a", summary="nyt longer version"),
            _make_article("https://www.foxnews.com/politics/b"),
            _make_article("https://www.washingtonpost.com/c"),
        ]
        result = MediaExtractor().extract(catalog)
        assert len(result) == 3
        urls = {a.source_url for a in result}
        assert "https://www.whitehouse.gov/eo14148" not in urls
        assert "https://www.defense.mil/release" not in urls
        # nyt deduped to longer version
        nyt = [a for a in result if "nytimes" in a.source_url]
        assert len(nyt) == 1
        assert nyt[0].summary_text == "nyt longer version"


# ===================================================================
# MediaLinkageCache
# ===================================================================


def _make_linkage_record(
    url: str = "https://nytimes.com/article/1",
    eo_number: str = "14148",
    factual: bool = True,
    spin: bool = False,
    convergence: str = "converges",
) -> dict:
    rid = _media_record_id(url, eo_number)
    return {
        "record_id": rid,
        "article_url": url,
        "article_title": "Test Article",
        "article_date": "2026-03-20",
        "article_domain": url.split("/")[2].replace("www.", ""),
        "publisher": "Test Publisher",
        "author": "Test Author",
        "linked_eos": [eo_number],
        "communication": {
            "url": url,
            "title": "Test Article",
            "date": "2026-03-20",
            "domain": url.split("/")[2].replace("www.", ""),
            "action_type": "reports",
            "action_description": "Media coverage of EO implementation",
        },
        "factualness": {"factual": factual, "rationale": "Matches official record"},
        "spin": {"spin": spin, "description": ""},
        "convergence_divergence": {"label": convergence, "key_delta": ""},
        "risk": {"bbao_rating": "low", "text": "Minimal risk"},
        "confidence": 0.85,
        "batch_id": "batch_0",
    }


class TestMediaLinkageCacheBasic:

    def test_append_and_load(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        records = [_make_linkage_record()]
        added = cache.append(records)
        assert added == 1
        loaded = cache.load()
        assert len(loaded) == 1
        assert loaded[0]["article_url"] == "https://nytimes.com/article/1"

    def test_dedup_by_record_id(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        rec = _make_linkage_record()
        cache.append([rec])
        cache.append([rec])  # same record_id
        assert len(cache.load()) == 1

    def test_different_eos_different_records(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        rec1 = _make_linkage_record(eo_number="14148")
        rec2 = _make_linkage_record(eo_number="14150")
        cache.append([rec1, rec2])
        assert len(cache.load()) == 2

    def test_persistence_across_instances(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache1 = MediaLinkageCache(cache_path)
        cache1.append([_make_linkage_record()])
        # New instance reads from disk
        cache2 = MediaLinkageCache(cache_path)
        assert len(cache2.load()) == 1

    def test_empty_record_id_skipped(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        cache.append([{"record_id": "", "article_url": "x"}])
        assert len(cache.load()) == 0

    def test_upsert_overwrites(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        rec = _make_linkage_record()
        cache.append([rec])
        rec["confidence"] = 0.95
        added = cache.append([rec])
        assert added == 0  # not new
        loaded = cache.load()
        assert len(loaded) == 1
        assert loaded[0]["confidence"] == 0.95


class TestMediaLinkageCacheQueries:

    def test_by_eo(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        cache.append([
            _make_linkage_record(url="https://cnn.com/a", eo_number="14148"),
            _make_linkage_record(url="https://bbc.com/b", eo_number="14150"),
            _make_linkage_record(url="https://fox.com/c", eo_number="14148"),
        ])
        eo_14148 = cache.by_eo("14148")
        assert len(eo_14148) == 2
        eo_14150 = cache.by_eo("14150")
        assert len(eo_14150) == 1
        eo_99999 = cache.by_eo("99999")
        assert len(eo_99999) == 0

    def test_by_domain(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        cache.append([
            _make_linkage_record(url="https://cnn.com/a"),
            _make_linkage_record(url="https://cnn.com/b", eo_number="14150"),
            _make_linkage_record(url="https://bbc.com/c"),
        ])
        cnn = cache.by_domain("cnn.com")
        assert len(cnn) == 2
        bbc = cache.by_domain("bbc.com")
        assert len(bbc) == 1

    def test_summary(self, tmp_path):
        cache_path = tmp_path / "media_cache.jsonl"
        cache = MediaLinkageCache(cache_path)
        cache.append([
            _make_linkage_record(url="https://cnn.com/a", eo_number="14148"),
            _make_linkage_record(url="https://bbc.com/b", eo_number="14150"),
        ])
        s = cache.summary()
        assert s["total_records"] == 2
        assert s["unique_eos"] == 2
        assert s["unique_domains"] == 2
        assert "14148" in s["eo_numbers"]
        assert "14150" in s["eo_numbers"]


class TestMediaRecordId:

    def test_deterministic(self):
        id1 = _media_record_id("https://cnn.com/a", "14148")
        id2 = _media_record_id("https://cnn.com/a", "14148")
        assert id1 == id2

    def test_different_eo_different_id(self):
        id1 = _media_record_id("https://cnn.com/a", "14148")
        id2 = _media_record_id("https://cnn.com/a", "14150")
        assert id1 != id2

    def test_different_url_different_id(self):
        id1 = _media_record_id("https://cnn.com/a", "14148")
        id2 = _media_record_id("https://bbc.com/b", "14148")
        assert id1 != id2

    def test_length(self):
        rid = _media_record_id("https://cnn.com/a", "14148")
        assert len(rid) == 16


# ===================================================================
# MediaLinkingConfig import validation
# ===================================================================


class TestMediaLinkingConfig:

    def test_import(self):
        from src.models.publisher import MediaLinkingConfig
        cfg = MediaLinkingConfig()
        assert cfg.enabled is False
        assert cfg.analysis_provider == "openai"
        assert cfg.analysis_model == "gpt-5.4"

    def test_default_domain_exclude(self):
        from src.models.publisher import MediaLinkingConfig
        cfg = MediaLinkingConfig()
        assert ".gov" in cfg.domain_exclude
        assert ".mil" in cfg.domain_exclude
        assert "dvids.net" in cfg.domain_exclude

    def test_pipeline_config_has_field(self):
        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig()
        assert hasattr(cfg, "media_linking")
        assert cfg.media_linking.enabled is False

    def test_eo_cache_path_default(self):
        from src.models.publisher import MediaLinkingConfig
        cfg = MediaLinkingConfig()
        assert "eo_implementation_cache" in cfg.eo_cache_path


# ===================================================================
# Phase 3 — Prompt Templates
# ===================================================================


class TestMediaPrompts:

    def test_link_prompt_has_placeholders(self):
        assert "{eo_grounding}" in _MEDIA_LINK_PROMPT
        assert "{eo_linkage_grounding}" in _MEDIA_LINK_PROMPT
        assert "{batch_conversations}" in _MEDIA_LINK_PROMPT

    def test_reflect_prompt_has_placeholders(self):
        assert "{eo_grounding}" in _MEDIA_REFLECT_PROMPT
        assert "{eo_linkage_grounding}" in _MEDIA_REFLECT_PROMPT
        assert "{link_output}" in _MEDIA_REFLECT_PROMPT
        assert "{batch_conversations}" in _MEDIA_REFLECT_PROMPT

    def test_link_prompt_format(self):
        """Prompt formats without error when all placeholders filled."""
        result = _MEDIA_LINK_PROMPT.format(
            eo_grounding="EO text",
            eo_linkage_grounding="EO linkage text",
            batch_conversations="article text",
        )
        assert "EO text" in result
        assert "EO linkage text" in result
        assert "article text" in result

    def test_reflect_prompt_format(self):
        result = _MEDIA_REFLECT_PROMPT.format(
            eo_grounding="EO text",
            eo_linkage_grounding="EO linkage text",
            link_output="link analysis",
            batch_conversations="article text",
        )
        assert "link analysis" in result

    def test_reflect_prompt_specifies_schema(self):
        assert "article_url" in _MEDIA_REFLECT_PROMPT
        assert "linked_eos" in _MEDIA_REFLECT_PROMPT
        assert "linkage_rationale" in _MEDIA_REFLECT_PROMPT
        assert "confidence" in _MEDIA_REFLECT_PROMPT


# ===================================================================
# Phase 3 — MediaLinkReflectDriver
# ===================================================================


class TestMediaLinkReflectDriver:

    def test_init_defaults(self):
        driver = MediaLinkReflectDriver()
        assert driver.model_name == "gpt-5.4"
        assert driver.provider == "openai"
        assert driver.reasoning_effort == "high"
        assert driver.total_usage == {"input": 0, "output": 0, "thinking": 0}

    def test_init_custom(self):
        driver = MediaLinkReflectDriver(
            model_name="gemini-2.5-pro",
            provider="gemini",
            reasoning_effort="medium",
            timeout=300,
        )
        assert driver.model_name == "gemini-2.5-pro"
        assert driver.provider == "gemini"
        assert driver.timeout == 300

    def test_checkpoint_skip(self, tmp_path):
        """Completed batches are skipped via status.json checkpoint."""
        driver = MediaLinkReflectDriver(output_dir=tmp_path)
        batch_dir = tmp_path / "batch_0"
        batch_dir.mkdir()
        status = {"status": "complete", "batch": 0}
        (batch_dir / "status.json").write_text(json.dumps(status))
        result = driver.process_batch(
            batch_index=0,
            eo_grounding="",
            batch_conversations="",
        )
        assert result["status"] == "complete"

    def test_format_conversations_missing_file(self, tmp_path):
        driver = MediaLinkReflectDriver(output_dir=tmp_path)
        result = driver.format_conversations_for_prompt(
            tmp_path / "nonexistent.json",
        )
        assert result == ""

    def test_format_conversations(self, tmp_path):
        driver = MediaLinkReflectDriver(output_dir=tmp_path)
        conv = [
            {
                "user_query": "Describe this article.",
                "assistant_response": "This article covers EO 14148.",
                "metadata": {
                    "article_url": "https://cnn.com/article/1",
                    "article_title": "CNN Coverage",
                    "domain": "cnn.com",
                    "publisher": "CNN",
                    "date": "2026-03-20",
                },
            },
        ]
        conv_path = tmp_path / "conversation.json"
        conv_path.write_text(json.dumps(conv))
        result = driver.format_conversations_for_prompt(conv_path)
        assert "CNN Coverage" in result
        assert "https://cnn.com/article/1" in result
        assert "cnn.com" in result

    def test_format_conversations_truncates_long_response(self, tmp_path):
        driver = MediaLinkReflectDriver(output_dir=tmp_path)
        conv = [
            {
                "user_query": "q",
                "assistant_response": "x" * 3000,
                "metadata": {
                    "article_url": "https://cnn.com/a",
                    "article_title": "T",
                    "domain": "cnn.com",
                    "publisher": "CNN",
                    "date": "2026-03-20",
                },
            },
        ]
        conv_path = tmp_path / "conv.json"
        conv_path.write_text(json.dumps(conv))
        result = driver.format_conversations_for_prompt(conv_path)
        assert "[...]" in result
        # Should be truncated to ~2000+extras
        assert len(result) < 3500


# ===================================================================
# Phase 4 — MediaLinkageExtractor
# ===================================================================


_VALID_REFLECT_JSON = json.dumps({
    "media_linkages": [
        {
            "article_url": "https://cnn.com/article/1",
            "article_title": "CNN on EO 14148",
            "article_date": "2026-03-20",
            "article_domain": "cnn.com",
            "publisher": "CNN",
            "author": "J. Smith",
            "linked_eos": [
                {"eo_number": "14148", "eo_name": "Border Security"},
                {"eo_number": "14150", "eo_name": "Trade Enforcement"},
            ],
            "relevant_events": [
                {
                    "eo_number": "14148",
                    "event_date": "2026-03-18",
                    "event_source_url": "https://whitehouse.gov/comm/1",
                    "description": "WH Statement on border",
                },
            ],
            "linkage_rationale": "Article covers EO 14148 border provisions",
            "confidence": 0.92,
        },
        {
            "article_url": "https://bbc.com/article/2",
            "article_title": "BBC Analysis",
            "article_date": "2026-03-21",
            "article_domain": "bbc.com",
            "publisher": "BBC",
            "author": "",
            "linked_eos": [
                {"eo_number": "14148", "eo_name": "Border Security"},
            ],
            "relevant_events": [],
            "linkage_rationale": "BBC analysis of border EO impact",
            "confidence": 0.78,
        },
    ],
})


class TestMediaLinkageExtractor:

    def test_extract_valid_json(self):
        ext = MediaLinkageExtractor()
        records = ext.extract(_VALID_REFLECT_JSON, "batch_0")
        assert len(records) == 2
        urls = {r["article_url"] for r in records}
        assert "https://cnn.com/article/1" in urls
        assert "https://bbc.com/article/2" in urls

    def test_record_fields(self):
        ext = MediaLinkageExtractor()
        records = ext.extract(_VALID_REFLECT_JSON, "batch_0")
        cnn_rec = next(r for r in records if "cnn.com" in r["article_url"])
        assert cnn_rec["article_title"] == "CNN on EO 14148"
        assert cnn_rec["linked_eos"] == [
            {"eo_number": "14148", "eo_name": "Border Security"},
            {"eo_number": "14150", "eo_name": "Trade Enforcement"},
        ]
        assert cnn_rec["linkage_rationale"] == "Article covers EO 14148 border provisions"
        assert cnn_rec["confidence"] == 0.92
        assert cnn_rec["batch_id"] == "batch_0"
        assert len(cnn_rec["record_id"]) == 16

    def test_extract_with_code_fences(self):
        text = "```json\n" + _VALID_REFLECT_JSON + "\n```"
        ext = MediaLinkageExtractor()
        records = ext.extract(text, "batch_0")
        assert len(records) == 2

    def test_no_json_returns_empty(self):
        ext = MediaLinkageExtractor()
        records = ext.extract("No JSON here at all.", "batch_0")
        assert records == []

    def test_no_linked_eos_skips_record(self):
        data = json.dumps({
            "media_linkages": {
                "https://cnn.com/a": {
                    "article_title": "T",
                    "linked_eos": [],
                    "linkage_rationale": "",
                    "confidence": 0.5,
                },
            },
        })
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0")
        assert len(records) == 0

    def test_fallback_wrapper_key(self):
        """Handles LLM using 'linkages' instead of 'media_linkages'."""
        data = json.dumps({
            "linkages": {
                "https://cnn.com/a": {
                    "article_title": "T",
                    "linked_eos": ["14148"],
                    "linkage_rationale": "Test",
                    "confidence": 0.5,
                },
            },
        })
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0")
        assert len(records) == 1

    def test_url_fallback_keys(self):
        """Handles top-level URL keys without wrapper."""
        data = json.dumps({
            "https://cnn.com/a": {
                "article_title": "T",
                "linked_eos": ["14148"],
                "linkage_rationale": "Test",
                "confidence": 0.5,
            },
        })
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0")
        assert len(records) == 1

    def test_eo_numbers_normalized(self):
        """EO numbers like 'Exec. Order No. 14148' are normalized to 5-digit."""
        data = json.dumps({
            "media_linkages": {
                "https://x.com/a": {
                    "article_title": "T",
                    "linked_eos": [
                        "Exec. Order No. 14148",
                        "14150",
                    ],
                    "linkage_rationale": "Test",
                    "confidence": 0.8,
                },
            },
        })
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0")
        eo_nums = [e["eo_number"] for e in records[0]["linked_eos"]]
        assert eo_nums == ["14148", "14150"]

    def test_hydrate_from_article_lookup(self):
        data = json.dumps({
            "media_linkages": {
                "https://cnn.com/a": {
                    "article_title": "",
                    "linked_eos": ["14148"],
                    "linkage_rationale": "Test",
                    "confidence": 0.5,
                },
            },
        })
        lookup = {
            "https://cnn.com/a": {
                "title": "CNN Updated Title",
                "date": "2026-03-22",
                "domain": "cnn.com",
                "publisher": "CNN",
            },
        }
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0", article_lookup=lookup)
        assert records[0]["article_title"] == "CNN Updated Title"
        assert records[0]["article_date"] == "2026-03-22"

    def test_domain_extracted_from_url(self):
        data = json.dumps({
            "media_linkages": {
                "https://reuters.com/article/x": {
                    "article_title": "T",
                    "linked_eos": ["14148"],
                    "linkage_rationale": "Test",
                    "confidence": 0.5,
                },
            },
        })
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0")
        assert records[0]["article_domain"] == "reuters.com"

    def test_relevant_events_extracted(self):
        """Relevant events are captured in output records."""
        data = json.dumps({
            "media_linkages": [
                {
                    "article_url": "https://x.com/a",
                    "article_title": "T",
                    "linked_eos": [{"eo_number": "14148", "eo_name": "Border"}],
                    "relevant_events": [
                        {"eo_number": "14148", "event_date": "2026-03-01",
                         "event_source_url": "", "description": "Event desc"},
                    ],
                    "linkage_rationale": "Covers border policy",
                    "confidence": 0.5,
                },
            ],
        })
        ext = MediaLinkageExtractor()
        records = ext.extract(data, "batch_0")
        assert len(records[0]["relevant_events"]) == 1
        assert records[0]["relevant_events"][0]["description"] == "Event desc"


# ===================================================================
# EO Cache Grounding Formatter
# ===================================================================


class TestFormatEoCacheAsGrounding:

    def test_formats_cache(self, tmp_path):
        cache_path = tmp_path / "eo_cache.jsonl"
        records = [
            {
                "eo_number": "14148",
                "eo_title": "Border Security",
                "communication": {
                    "url": "https://whitehouse.gov/stmt/1",
                    "date": "2025-01-22",
                    "action_description": "Signed EO 14148",
                    "official": "President Trump",
                },
            },
            {
                "eo_number": "14148",
                "eo_title": "Border Security",
                "communication": {
                    "url": "https://dhs.gov/action/1",
                    "date": "2025-02-01",
                    "action_description": "DHS implements border closure",
                    "official": "DHS Secretary",
                },
            },
            {
                "eo_number": "14150",
                "eo_title": "Energy Independence",
                "communication": {
                    "url": "https://energy.gov/notice/1",
                    "date": "2025-01-25",
                    "action_description": "DoE reopens drilling permits",
                    "official": "Energy Secretary",
                },
            },
        ]
        _write_jsonl(cache_path, records)

        result = _format_eo_cache_as_grounding(str(cache_path))
        assert "EO 14148: Border Security" in result
        assert "EO 14150: Energy Independence" in result
        assert "Signed EO 14148" in result
        assert "DHS implements border closure" in result
        assert "https://whitehouse.gov/stmt/1" in result

    def test_empty_cache(self, tmp_path):
        cache_path = tmp_path / "empty_cache.jsonl"
        cache_path.write_text("")
        result = _format_eo_cache_as_grounding(str(cache_path))
        assert result == ""

    def test_missing_file(self, tmp_path):
        result = _format_eo_cache_as_grounding(
            str(tmp_path / "nonexistent.jsonl"),
        )
        assert result == ""

    def test_skips_records_without_communication(self, tmp_path):
        cache_path = tmp_path / "eo_cache.jsonl"
        records = [
            {
                "eo_number": "14148",
                "eo_title": "Test EO",
                "communication": None,
            },
            {
                "eo_number": "14150",
                "eo_title": "Test EO 2",
                "communication": {
                    "url": "https://gov.example/x",
                    "date": "2025-01-22",
                    "action_description": "Action X",
                    "official": "Official Y",
                },
            },
        ]
        _write_jsonl(cache_path, records)
        result = _format_eo_cache_as_grounding(str(cache_path))
        assert "EO 14148" in result  # heading still appears
        assert "EO 14150" in result
        assert "Action X" in result


# ===================================================================
# MediaLinkStage import validation
# ===================================================================


class TestMediaLinkStageImport:

    def test_import(self):
        from src.components.analysis.media_linking_stage import MediaLinkStage
        assert MediaLinkStage is not None

    def test_disabled_skips(self):
        from src.components.analysis.media_linking_stage import MediaLinkStage
        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig()
        cfg.media_linking.enabled = False
        stage = MediaLinkStage(cfg)
        # Should not raise — just log and return
        stage.run()

    def test_missing_eo_cache_aborts(self, tmp_path):
        from src.components.analysis.media_linking_stage import MediaLinkStage
        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig()
        cfg.media_linking.enabled = True
        cfg.media_linking.eo_cache_path = str(
            tmp_path / "nonexistent_cache.jsonl",
        )
        stage = MediaLinkStage(cfg)
        # Should not raise — logs error and returns
        stage.run()
