"""Tests for OpenAILLMClient pure helper methods.

Covers _jaccard_similarity, _check_query_novelty, get_model_info,
and is_reasoning_model without requiring OpenAI credentials.
"""

import logging

import pytest

from src.clients.llm_openai_client import OpenAILLMClient


def _make_client(model_name="gpt-4o-mini"):
    """Create OpenAILLMClient without Base.__init__ or API auth."""
    client = object.__new__(OpenAILLMClient)
    client._logger = logging.getLogger("test.openai")
    client.model_name = model_name
    client.is_reasoning_model = "gpt-5" in model_name.lower()
    return client


# ── _jaccard_similarity ──────────────────────────────────────────────


class TestJaccardSimilarity:
    def setup_method(self):
        self.c = _make_client()

    def test_identical(self):
        assert self.c._jaccard_similarity("hello world", "hello world") == 1.0

    def test_disjoint(self):
        assert self.c._jaccard_similarity("alpha beta", "gamma delta") == 0.0

    def test_partial_overlap(self):
        score = self.c._jaccard_similarity("the cat sat", "the dog sat")
        assert score == pytest.approx(0.5)

    def test_case_insensitive(self):
        assert self.c._jaccard_similarity("Hello World", "hello world") == 1.0

    def test_both_empty(self):
        assert self.c._jaccard_similarity("", "") == 0.0


# ── _check_query_novelty ────────────────────────────────────────────


class TestCheckQueryNovelty:
    def setup_method(self):
        self.c = _make_client()

    def test_novel_accepted(self):
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


# ── is_reasoning_model ───────────────────────────────────────────────


class TestIsReasoningModel:
    def test_gpt5_is_reasoning(self):
        c = _make_client("gpt-5.1")
        assert c.is_reasoning_model is True

    def test_gpt5_mini_is_reasoning(self):
        c = _make_client("gpt-5-mini")
        assert c.is_reasoning_model is True

    def test_gpt4o_not_reasoning(self):
        c = _make_client("gpt-4o")
        assert c.is_reasoning_model is False

    def test_gpt35_not_reasoning(self):
        c = _make_client("gpt-3.5-turbo")
        assert c.is_reasoning_model is False


# ── get_model_info ───────────────────────────────────────────────────


class TestGetModelInfo:
    def test_known_model(self):
        c = _make_client("gpt-5.1")
        info = c.get_model_info()
        assert info["name"] == "gpt-5.1"
        assert info["input_token_limit"] == 200000
        assert info["output_token_limit"] == 100000
        assert info["is_reasoning_model"] is True
        assert "description" in info

    def test_gpt4o_mini(self):
        c = _make_client("gpt-4o-mini")
        info = c.get_model_info()
        assert info["input_token_limit"] == 128000
        assert info["is_reasoning_model"] is False

    def test_unknown_model_fallback(self):
        c = _make_client("mystery-model-99")
        info = c.get_model_info()
        assert info["name"] == "mystery-model-99"
        assert info["input_token_limit"] == "Unknown"
        assert info["description"] == "Unknown model"
