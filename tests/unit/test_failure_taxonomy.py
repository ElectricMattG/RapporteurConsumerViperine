"""
tests/unit/test_failure_taxonomy.py

Full coverage of src/consumers/_failure_taxonomy.py

Contracts verified:
- every category fires on its canonical trigger phrase
- retryable / max_attempts / degrade flags match spec
- embedding is checked BEFORE transient (priority ordering)
- permanent_internal fires on AttributeError etc. exception type
- unknown fires for everything else with one retry
- FailureClassification is a NamedTuple (destructurable)
"""
from __future__ import annotations

import pytest

from consumers._failure_taxonomy import FailureClassification, classify_error


def _exc(msg: str, type_: type = Exception) -> Exception:
    """Quick factory: raise-then-catch so exc has a real traceback type."""
    exc = type_(msg)
    return exc


class TestCategories:
    def test_model_empty_finish_reason_stop(self):
        fc = classify_error(_exc("finish_reason STOP with empty body"))
        assert fc.category == "model_empty"

    def test_model_empty_explicit_phrase(self):
        fc = classify_error(_exc("empty response from model"))
        assert fc.category == "model_empty"

    def test_policy_block_safety(self):
        fc = classify_error(_exc("finish_reason SAFETY — content not allowed"))
        assert fc.category == "policy_block"

    def test_policy_block_recitation(self):
        fc = classify_error(_exc("finish_reason RECITATION — content blocked"))
        assert fc.category == "policy_block"

    def test_policy_block_content_policy(self):
        fc = classify_error(_exc("violates content policy"))
        assert fc.category == "policy_block"

    def test_auth_api_key_invalid(self):
        fc = classify_error(_exc("API key invalid"))
        assert fc.category == "auth"

    def test_auth_quota_exceeded(self):
        fc = classify_error(_exc("quota exceeded"))
        assert fc.category == "auth"

    def test_not_found_404(self):
        fc = classify_error(_exc("HTTP 404 not found"))
        assert fc.category == "not_found"

    def test_not_found_phrase(self):
        fc = classify_error(_exc("No such file or directory"))
        assert fc.category == "not_found"

    def test_bad_input_empty_text(self):
        fc = classify_error(_exc("empty text supplied"))
        assert fc.category == "bad_input"

    def test_bad_input_encoding_error(self):
        fc = classify_error(_exc("encoding error in payload"))
        assert fc.category == "bad_input"

    def test_embedding_service(self):
        fc = classify_error(_exc("embedding service timeout"))
        assert fc.category == "embedding"

    def test_embedding_embed_word(self):
        fc = classify_error(_exc("failed to embed document"))
        assert fc.category == "embedding"

    def test_transient_timeout(self):
        fc = classify_error(_exc("request timed out"))
        assert fc.category == "transient"

    def test_transient_429(self):
        fc = classify_error(_exc("rate.limit hit — 429"))
        assert fc.category == "transient"

    def test_transient_503(self):
        fc = classify_error(_exc("service 503 temporarily unavailable"))
        assert fc.category == "transient"

    def test_permanent_internal_attribute_error(self):
        fc = classify_error(AttributeError("'NoneType' has no attribute 'foo'"))
        assert fc.category == "permanent_internal"

    def test_permanent_internal_assertion(self):
        fc = classify_error(AssertionError("invariant violated"))
        assert fc.category == "permanent_internal"

    def test_permanent_internal_type_error(self):
        fc = classify_error(TypeError("unsupported operand"))
        assert fc.category == "permanent_internal"

    def test_permanent_internal_value_error(self):
        fc = classify_error(ValueError("invalid literal"))
        assert fc.category == "permanent_internal"

    def test_unknown_generic_exception(self):
        fc = classify_error(Exception("something weird happened"))
        assert fc.category == "unknown"

    def test_unknown_custom_exception(self):
        class MyError(Exception):
            pass
        fc = classify_error(MyError("unexpected condition"))
        assert fc.category == "unknown"


class TestRetryFlags:
    """Verify retryable / max_attempts / degrade per spec."""

    def test_model_empty_retryable_degrade(self):
        fc = classify_error(_exc("empty response from model"))
        assert fc.retryable is True
        assert fc.max_attempts == 2
        assert fc.degrade is True

    def test_policy_block_no_retry(self):
        fc = classify_error(_exc("finish_reason SAFETY"))
        assert fc.retryable is False
        assert fc.max_attempts == 1
        assert fc.degrade is False

    def test_auth_no_retry(self):
        fc = classify_error(_exc("API key invalid"))
        assert fc.retryable is False
        assert fc.max_attempts == 1

    def test_not_found_no_retry(self):
        fc = classify_error(_exc("404 not found"))
        assert fc.retryable is False
        assert fc.max_attempts == 1

    def test_bad_input_no_retry(self):
        fc = classify_error(_exc("empty text"))
        assert fc.retryable is False
        assert fc.max_attempts == 1

    def test_embedding_retry_3(self):
        fc = classify_error(_exc("embedding service down"))
        assert fc.retryable is True
        assert fc.max_attempts == 3
        assert fc.degrade is False

    def test_transient_retry_3(self):
        fc = classify_error(_exc("connection reset by peer"))
        assert fc.retryable is True
        assert fc.max_attempts == 3

    def test_permanent_internal_no_retry(self):
        fc = classify_error(AttributeError("oops"))
        assert fc.retryable is False
        assert fc.max_attempts == 1

    def test_unknown_one_retry(self):
        fc = classify_error(Exception("mystery"))
        assert fc.retryable is True
        assert fc.max_attempts == 2


class TestOrdering:
    """embedding must be classified BEFORE transient (it matches both)."""

    def test_embedding_before_transient(self):
        """An 'embedding timeout' message matches both patterns.
        embedding should win because it is checked first."""
        fc = classify_error(_exc("embedding request timed out"))
        assert fc.category == "embedding", (
            "embedding pattern must be checked before transient"
        )

    def test_named_tuple_destructurable(self):
        fc = classify_error(_exc("empty response from model"))
        category, retryable, max_attempts, degrade = fc
        assert category == "model_empty"
        assert retryable is True
        assert max_attempts == 2
        assert degrade is True

    def test_is_named_tuple(self):
        fc = classify_error(_exc("anything"))
        assert isinstance(fc, FailureClassification)
        assert hasattr(fc, "_fields")
