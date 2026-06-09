"""
Canonical error categories for Prometheus labels.

All consumers MUST use these values as the error_type label.
Freeform strings are forbidden — they cause cardinality explosion.

Usage:
    from src.monitoring.error_categories import ErrorCategory

    # In metric recording:
    metrics.increment_failures(topic, ErrorCategory.SCRAPING_TIMEOUT)

    # Validate before use:
    ErrorCategory.validate('scraping_timeout')  # OK
    ErrorCategory.validate('random_string')     # raises ValueError
"""
from __future__ import annotations


class ErrorCategory:
    """Fixed enum of error categories for all Viperine consumers."""

    # ── Web Scraper ──────────────────────────────────────────────────────────
    SCRAPING_403 = "scraping_403"
    SCRAPING_TIMEOUT = "scraping_timeout"
    SCRAPING_DNS = "scraping_dns"
    SCRAPING_SSL = "scraping_ssl"
    SCRAPING_CONNECTION = "scraping_connection"
    SCRAPING_BLOCKED = "scraping_blocked"

    # ── Shared deserialization ────────────────────────────────────────────────
    JSON_PARSE = "json_parse"
    JSON_DECODE = "json_decode"

    # ── Summarizer (aligned with _failure_taxonomy.py categories) ─────────────
    SUMMARIZATION_POLICY_BLOCK = "summarization_policy_block"
    SUMMARIZATION_MODEL_EMPTY = "summarization_model_empty"
    SUMMARIZATION_MODEL_ERROR_OUTPUT = "summarization_model_error_output"
    SUMMARIZATION_AUTH = "summarization_auth"
    SUMMARIZATION_TIMEOUT = "summarization_timeout"
    SUMMARIZATION_TRANSIENT = "summarization_transient"

    # ── Embedding ─────────────────────────────────────────────────────────────
    EMBEDDING_TIMEOUT = "embedding_timeout"
    EMBEDDING_CONNECTION = "embedding_connection"

    # ── Azure ─────────────────────────────────────────────────────────────────
    AZURE_UPLOAD = "azure_upload"
    AZURE_DOWNLOAD = "azure_download"
    AZURE_AUTH = "azure_auth"

    # ── Index / file operations ───────────────────────────────────────────────
    INDEX_FILE_LOCK = "index_file_lock"
    INDEX_DEDUP_ERROR = "index_dedup_error"
    INDEX_WRITE_ERROR = "index_write_error"

    # ── Kafka ─────────────────────────────────────────────────────────────────
    KAFKA_PRODUCE_ERROR = "kafka_produce_error"
    KAFKA_CONSUME_ERROR = "kafka_consume_error"
    KAFKA_DESERIALIZE = "kafka_deserialize"

    # ── Catch-all (use sparingly) ─────────────────────────────────────────────
    UNKNOWN = "unknown"

    # ── All valid values (for validation) ─────────────────────────────────────
    _ALL_CATEGORIES: frozenset[str] | None = None

    @classmethod
    def all_categories(cls) -> frozenset[str]:
        """Return frozenset of all valid category strings."""
        if cls._ALL_CATEGORIES is None:
            cls._ALL_CATEGORIES = frozenset(
                v for k, v in vars(cls).items()
                if not k.startswith('_') and isinstance(v, str) and k.isupper()
            )
        return cls._ALL_CATEGORIES

    @classmethod
    def validate(cls, category: str) -> str:
        """
        Validate that a category string is in the allowed set.

        Args:
            category: The error category to validate

        Returns:
            The category string (pass-through for chaining)

        Raises:
            ValueError: If category is not a valid ErrorCategory
        """
        if category not in cls.all_categories():
            raise ValueError(
                f"Invalid error category: {category!r}. "
                f"Must be one of: {sorted(cls.all_categories())}"
            )
        return category


# ── Mapping from _failure_taxonomy categories to ErrorCategory ────────────────
TAXONOMY_TO_ERROR_CATEGORY: dict[str, str] = {
    'transient': ErrorCategory.SUMMARIZATION_TRANSIENT,
    'model_empty': ErrorCategory.SUMMARIZATION_MODEL_EMPTY,
    'policy_block': ErrorCategory.SUMMARIZATION_POLICY_BLOCK,
    'auth': ErrorCategory.SUMMARIZATION_AUTH,
    'not_found': ErrorCategory.SCRAPING_403,  # closest match
    'bad_input': ErrorCategory.JSON_PARSE,
    'permanent_internal': ErrorCategory.UNKNOWN,
    'embedding': ErrorCategory.EMBEDDING_TIMEOUT,
    'unknown': ErrorCategory.UNKNOWN,
}
