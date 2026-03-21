"""Tests for GeminiLLMClient pure helper methods.

Covers _jaccard_similarity and _check_query_novelty without
requiring GCP credentials or network access.
"""

import logging

import pytest

from src.clients.llm_gcp_client import GeminiLLMClient


def _make_client():
    """Create GeminiLLMClient without Base.__init__ or GCP auth."""
    client = object.__new__(GeminiLLMClient)
    client._logger = logging.getLogger("test.gemini")
    return client


# ── _jaccard_similarity ──────────────────────────────────────────────


class TestJaccardSimilarity:
    def setup_method(self):
        self.c = _make_client()

    def test_identical_strings(self):
        assert self.c._jaccard_similarity("hello world", "hello world") == 1.0

    def test_completely_disjoint(self):
        assert self.c._jaccard_similarity("alpha beta", "gamma delta") == 0.0

    def test_partial_overlap(self):
        # {"the", "cat", "sat"} & {"the", "dog", "sat"} = {"the", "sat"} / {"the","cat","sat","dog"} = 2/4
        score = self.c._jaccard_similarity("the cat sat", "the dog sat")
        assert score == pytest.approx(0.5)

    def test_case_insensitive(self):
        assert self.c._jaccard_similarity("Hello World", "hello world") == 1.0

    def test_apostrophe_stripped(self):
        # "don't" → "dont"
        assert self.c._jaccard_similarity("don't stop", "dont stop") == 1.0

    def test_empty_first(self):
        assert self.c._jaccard_similarity("", "hello") == 0.0

    def test_empty_second(self):
        assert self.c._jaccard_similarity("hello", "") == 0.0

    def test_both_empty(self):
        assert self.c._jaccard_similarity("", "") == 0.0

    def test_single_word_match(self):
        assert self.c._jaccard_similarity("test", "test") == 1.0

    def test_superset(self):
        # {"a","b","c"} & {"a","b"} = 2 / {"a","b","c"} = 2/3
        score = self.c._jaccard_similarity("a b c", "a b")
        assert score == pytest.approx(2 / 3)


# ── _check_query_novelty ────────────────────────────────────────────


class TestCheckQueryNovelty:
    def setup_method(self):
        self.c = _make_client()

    def test_novel_queries_accepted(self):
        assert self.c._check_query_novelty(
            "executive orders",
            ["tariff impacts on trade", "federal regulation economics"],
        ) is True

    def test_too_similar_rejected(self):
        assert self.c._check_query_novelty(
            "executive orders signed",
            ["executive orders signed today", "executive orders signed yesterday"],
            similarity_threshold=0.5,
        ) is False

    def test_empty_list_rejected(self):
        assert self.c._check_query_novelty("anything", []) is False

    def test_threshold_boundary(self):
        # One similar, one novel → 1/2 not > 1 (half) → accepted
        result = self.c._check_query_novelty(
            "alpha beta",
            ["alpha beta gamma", "completely different words"],
            similarity_threshold=0.4,
        )
        assert result is True

    def test_all_identical_rejected(self):
        assert self.c._check_query_novelty(
            "same words here",
            ["same words here", "same words here"],
        ) is False

    def test_single_novel_query(self):
        assert self.c._check_query_novelty(
            "executive orders",
            ["completely unrelated topic"],
        ) is True

    def test_custom_high_threshold(self):
        # With threshold=1.0 only exact duplicates rejected
        assert self.c._check_query_novelty(
            "alpha beta",
            ["alpha beta gamma"],
            similarity_threshold=1.0,
        ) is True
