"""
Tests for error_categories module.

Validates:
- All category values are unique strings
- ErrorCategory.validate() accepts valid and rejects invalid categories
- TAXONOMY_TO_ERROR_CATEGORY maps all _failure_taxonomy categories
- No duplicate values in the enum
"""
import pytest

from src.monitoring.error_categories import (TAXONOMY_TO_ERROR_CATEGORY,
                                             ErrorCategory)


class TestErrorCategoryValues:
    """Ensure all category constants are valid, unique strings."""

    def test_all_categories_are_strings(self):
        for cat in ErrorCategory.all_categories():
            assert isinstance(cat, str), f"Category {cat!r} is not a string"

    def test_all_categories_are_non_empty(self):
        for cat in ErrorCategory.all_categories():
            assert len(cat) > 0, "Found empty category string"

    def test_no_duplicate_values(self):
        cats = list(ErrorCategory.all_categories())
        assert len(cats) == len(set(cats)), "Duplicate category values found"

    def test_minimum_category_count(self):
        """Ensure we haven't accidentally emptied the enum."""
        assert len(ErrorCategory.all_categories()) >= 20

    def test_categories_are_lowercase_snake_case(self):
        import re
        pattern = re.compile(r'^[a-z][a-z0-9_]*$')
        for cat in ErrorCategory.all_categories():
            assert pattern.match(cat), f"Category {cat!r} is not lowercase snake_case"


class TestValidation:
    """ErrorCategory.validate() correctly accepts/rejects."""

    def test_validate_accepts_valid_category(self):
        result = ErrorCategory.validate(ErrorCategory.SCRAPING_TIMEOUT)
        assert result == ErrorCategory.SCRAPING_TIMEOUT

    def test_validate_rejects_freeform_string(self):
        with pytest.raises(ValueError, match="Invalid error category"):
            ErrorCategory.validate("some_random_error_string")

    def test_validate_rejects_empty_string(self):
        with pytest.raises(ValueError):
            ErrorCategory.validate("")

    def test_validate_rejects_python_exception_name(self):
        """This is what the old code used — must be rejected now."""
        with pytest.raises(ValueError):
            ErrorCategory.validate("TimeoutError")

    def test_validate_rejects_old_freeform_values(self):
        """Legacy freeform strings that were previously used."""
        old_values = [
            'json_decode_error',   # old web scraper
            'task_exception',      # old summarizer
            'processing_error',    # old aggregator
            'consumer_error',      # old aggregator
        ]
        for val in old_values:
            with pytest.raises(ValueError):
                ErrorCategory.validate(val)


class TestTaxonomyMapping:
    """TAXONOMY_TO_ERROR_CATEGORY covers all _failure_taxonomy categories."""

    def test_all_taxonomy_categories_mapped(self):
        expected_categories = {
            'transient', 'model_empty', 'policy_block', 'auth',
            'not_found', 'bad_input', 'permanent_internal', 'embedding', 'unknown'
        }
        assert set(TAXONOMY_TO_ERROR_CATEGORY.keys()) == expected_categories

    def test_all_mapped_values_are_valid(self):
        for taxonomy_cat, error_cat in TAXONOMY_TO_ERROR_CATEGORY.items():
            ErrorCategory.validate(error_cat)  # Should not raise


class TestClassConstants:
    """Verify specific categories exist for each consumer domain."""

    def test_scraper_categories_exist(self):
        expected = {
            ErrorCategory.SCRAPING_403,
            ErrorCategory.SCRAPING_TIMEOUT,
            ErrorCategory.SCRAPING_DNS,
            ErrorCategory.SCRAPING_SSL,
            ErrorCategory.SCRAPING_CONNECTION,
            ErrorCategory.SCRAPING_BLOCKED,
        }
        assert expected.issubset(ErrorCategory.all_categories())

    def test_summarizer_categories_exist(self):
        expected = {
            ErrorCategory.SUMMARIZATION_POLICY_BLOCK,
            ErrorCategory.SUMMARIZATION_MODEL_EMPTY,
            ErrorCategory.SUMMARIZATION_MODEL_ERROR_OUTPUT,
            ErrorCategory.SUMMARIZATION_AUTH,
            ErrorCategory.SUMMARIZATION_TIMEOUT,
            ErrorCategory.SUMMARIZATION_TRANSIENT,
        }
        assert expected.issubset(ErrorCategory.all_categories())

    def test_kafka_categories_exist(self):
        expected = {
            ErrorCategory.KAFKA_PRODUCE_ERROR,
            ErrorCategory.KAFKA_CONSUME_ERROR,
            ErrorCategory.KAFKA_DESERIALIZE,
        }
        assert expected.issubset(ErrorCategory.all_categories())
