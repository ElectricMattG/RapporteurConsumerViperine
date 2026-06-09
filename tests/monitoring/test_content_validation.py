"""
Tests for validate_summary_content() and _MODEL_ERROR_OUTPUT_PAT.

Validates:
- Known LLM error/refusal patterns are correctly rejected
- Valid markdown summaries pass through
- Edge cases (empty, very short, borderline) are handled
"""
import pytest

from src.consumers._failure_taxonomy import (_MODEL_ERROR_OUTPUT_PAT,
                                             validate_summary_content)


class TestModelErrorOutputPattern:
    """The regex matches known LLM error-as-content patterns."""

    @pytest.mark.parametrize("text", [
        "# Error\n\nFailed to generate summary for citation.",
        "# Error\n\nI was unable to access the provided URL.",
        "  # Error\nSomething went wrong.",
        "Failed to generate summary for the given content.",
        "I'm unable to generate a summary for this citation.",
        "I am unable to provide a summary because the URL is inaccessible.",
        "I'm not able to summarize this content as it requires access.",
        "I cannot access the provided URL to generate a summary.",
        "I cannot summarize this page because it requires authentication.",
        "As an AI language model, I don't have the ability to browse the web.",
        "As an AI, I don't have access to external URLs.",
        "I don't have access to browse websites or URLs.",
    ])
    def test_rejects_known_error_patterns(self, text):
        assert _MODEL_ERROR_OUTPUT_PAT.search(text) is not None
        assert validate_summary_content(text) is False

    @pytest.mark.parametrize("text", [
        "# Summary\n\nThis article discusses the impact of artificial intelligence on healthcare.",
        "## Key Findings\n\nThe research demonstrates a 30% improvement in diagnostic accuracy.",
        "The paper presents a novel approach to natural language processing using transformer architectures.",
        "# Analysis of Market Trends\n\nGlobal semiconductor demand increased by 15% in Q3 2025.",
        "This comprehensive review covers recent advances in quantum computing hardware.",
    ])
    def test_accepts_valid_summaries(self, text):
        assert _MODEL_ERROR_OUTPUT_PAT.search(text) is None
        assert validate_summary_content(text) is True


class TestValidateSummaryContent:
    """validate_summary_content() handles edge cases."""

    def test_rejects_none_equivalent(self):
        assert validate_summary_content("") is False
        assert validate_summary_content("   ") is False

    def test_rejects_very_short_text(self):
        assert validate_summary_content("Too short") is False
        assert validate_summary_content("x" * 19) is False

    def test_accepts_minimum_length_valid(self):
        assert validate_summary_content("x" * 20) is True

    def test_rejects_error_in_longer_text(self):
        """Error pattern at the beginning of a longer text should still be caught."""
        text = "# Error\n\nFailed to generate summary for citation.\n\n" + "x" * 100
        assert validate_summary_content(text) is False

    def test_accepts_text_with_error_word_in_context(self):
        """The word 'error' in a legitimate summary context should pass."""
        text = "The system reported a 5% error rate in the classification task, which is within acceptable bounds for production deployment."
        assert validate_summary_content(text) is True

    def test_accepts_text_discussing_failures(self):
        """A summary that discusses failures academically should pass."""
        text = "The study found that models failed to generate accurate predictions when input data was noisy, suggesting the need for better preprocessing."
        assert validate_summary_content(text) is True
