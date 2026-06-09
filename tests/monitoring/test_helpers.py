"""
Tests for Phase 6 helper methods on PrometheusMetrics.

Validates:
- record_message_processed(topic) is backward-compatible (timestamp only, no counter)
- record_message_processed(topic, duration) increments consumed + observes histogram
- record_message_processed(topic, duration, status) uses the provided status label
- record_failure(topic, error_category) validates category then increments
- record_failure() raises ValueError for freeform strings
- record_dead_letter(topic, reason) increments the dead letter counter
"""
import uuid

import pytest

from src.monitoring.error_categories import ErrorCategory
from src.monitoring.prometheus_metrics import PrometheusMetrics


@pytest.fixture
def metrics():
    ns = f"test_{uuid.uuid4().hex[:8]}"
    return PrometheusMetrics(consumer_id='test_helpers', namespace=ns)


class TestRecordMessageProcessed:

    def test_without_duration_updates_timestamp(self, metrics):
        import time
        before = time.time()
        metrics.record_message_processed('test-topic')
        val = metrics.last_message_processed_timestamp.labels(
            consumer_id='test_helpers', topic='test-topic'
        )._value.get()
        assert val >= before

    def test_backward_compat_no_counter_increment(self, metrics):
        """Calling without duration must NOT increment messages_consumed."""
        before = metrics.messages_consumed.labels(
            consumer_id='test_helpers', topic='test-topic'
        )._value.get()
        metrics.record_message_processed('test-topic')
        after = metrics.messages_consumed.labels(
            consumer_id='test_helpers', topic='test-topic'
        )._value.get()
        assert after == before

    def test_with_duration_increments_consumed(self, metrics):
        before = metrics.messages_consumed.labels(
            consumer_id='test_helpers', topic='test-topic'
        )._value.get()
        metrics.record_message_processed('test-topic', duration=0.5)
        after = metrics.messages_consumed.labels(
            consumer_id='test_helpers', topic='test-topic'
        )._value.get()
        assert after == before + 1

    def test_with_duration_observes_histogram(self, metrics):
        metrics.record_message_processed('test-topic', duration=1.2)
        total = metrics.processing_duration.labels(
            consumer_id='test_helpers', topic='test-topic', status='success'
        )._sum.get()
        assert total >= 1.2

    def test_custom_status_label(self, metrics):
        metrics.record_message_processed('test-topic', duration=0.3, status='failure')
        total = metrics.processing_duration.labels(
            consumer_id='test_helpers', topic='test-topic', status='failure'
        )._sum.get()
        assert total >= 0.3

    def test_with_duration_also_updates_timestamp(self, metrics):
        import time
        before = time.time()
        metrics.record_message_processed('test-topic', duration=0.1)
        val = metrics.last_message_processed_timestamp.labels(
            consumer_id='test_helpers', topic='test-topic'
        )._value.get()
        assert val >= before


class TestRecordFailure:

    def test_valid_category_increments(self, metrics):
        before = metrics.processing_failures.labels(
            consumer_id='test_helpers', topic='test-topic',
            error_type=ErrorCategory.SCRAPING_TIMEOUT
        )._value.get()
        metrics.record_failure('test-topic', ErrorCategory.SCRAPING_TIMEOUT)
        after = metrics.processing_failures.labels(
            consumer_id='test_helpers', topic='test-topic',
            error_type=ErrorCategory.SCRAPING_TIMEOUT
        )._value.get()
        assert after == before + 1

    def test_invalid_category_raises(self, metrics):
        with pytest.raises(ValueError, match="Invalid error category"):
            metrics.record_failure('test-topic', 'random_freeform_error_yo')

    def test_empty_string_raises(self, metrics):
        with pytest.raises(ValueError):
            metrics.record_failure('test-topic', '')

    def test_multiple_valid_calls_accumulate(self, metrics):
        for _ in range(3):
            metrics.record_failure('test-topic', ErrorCategory.SCRAPING_TIMEOUT)
        count = metrics.processing_failures.labels(
            consumer_id='test_helpers', topic='test-topic',
            error_type=ErrorCategory.SCRAPING_TIMEOUT
        )._value.get()
        assert count == 3


class TestRecordDeadLetter:

    def test_increments_counter(self, metrics):
        before = metrics.dead_letter_messages.labels(
            consumer_id='test_helpers', topic='test-topic', reason='max_retries'
        )._value.get()
        metrics.record_dead_letter('test-topic', 'max_retries')
        after = metrics.dead_letter_messages.labels(
            consumer_id='test_helpers', topic='test-topic', reason='max_retries'
        )._value.get()
        assert after == before + 1

    def test_different_reasons_tracked_separately(self, metrics):
        metrics.record_dead_letter('test-topic', 'parse_error')
        metrics.record_dead_letter('test-topic', 'timeout')
        parse = metrics.dead_letter_messages.labels(
            consumer_id='test_helpers', topic='test-topic', reason='parse_error'
        )._value.get()
        timeout = metrics.dead_letter_messages.labels(
            consumer_id='test_helpers', topic='test-topic', reason='timeout'
        )._value.get()
        assert parse == 1
        assert timeout == 1
