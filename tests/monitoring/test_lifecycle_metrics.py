"""
Tests for Phase 2 lifecycle metrics (startup_timestamp, last_message_processed, sessions_in_progress).

Validates:
- record_startup() sets a positive Unix timestamp
- record_message_processed() updates the last-processed gauge
- set_sessions_in_progress() correctly tracks session count
- consumer_info Info metric populates correctly
"""
import time
import uuid

import pytest

from src.monitoring.prometheus_metrics import PrometheusMetrics


@pytest.fixture
def metrics():
    """Create a PrometheusMetrics instance with a unique namespace."""
    ns = f"test_{uuid.uuid4().hex[:8]}"
    return PrometheusMetrics(consumer_id='test_lifecycle', namespace=ns)


class TestStartupTimestamp:
    """record_startup() sets a plausible Unix timestamp."""

    def test_sets_positive_timestamp(self, metrics):
        before = time.time()
        metrics.record_startup()
        after = time.time()

        value = metrics.startup_timestamp.labels(consumer_id='test_lifecycle')._value.get()
        assert before <= value <= after

    def test_idempotent_overwrites(self, metrics):
        metrics.record_startup()
        first = metrics.startup_timestamp.labels(consumer_id='test_lifecycle')._value.get()
        time.sleep(0.01)
        metrics.record_startup()
        second = metrics.startup_timestamp.labels(consumer_id='test_lifecycle')._value.get()
        assert second >= first


class TestLastMessageProcessed:
    """record_message_processed() updates the gauge per topic."""

    def test_updates_timestamp(self, metrics):
        before = time.time()
        metrics.record_message_processed('test-topic')
        after = time.time()

        value = metrics.last_message_processed_timestamp.labels(
            consumer_id='test_lifecycle', topic='test-topic'
        )._value.get()
        assert before <= value <= after

    def test_per_topic_isolation(self, metrics):
        metrics.record_message_processed('topic-a')
        time.sleep(0.01)
        metrics.record_message_processed('topic-b')

        val_a = metrics.last_message_processed_timestamp.labels(
            consumer_id='test_lifecycle', topic='topic-a'
        )._value.get()
        val_b = metrics.last_message_processed_timestamp.labels(
            consumer_id='test_lifecycle', topic='topic-b'
        )._value.get()
        assert val_b > val_a


class TestSessionsInProgress:
    """set_sessions_in_progress() correctly tracks session count."""

    def test_initial_zero(self, metrics):
        metrics.set_sessions_in_progress(0)
        value = metrics.sessions_in_progress.labels(consumer_id='test_lifecycle')._value.get()
        assert value == 0

    def test_increment_and_decrement(self, metrics):
        metrics.set_sessions_in_progress(3)
        value = metrics.sessions_in_progress.labels(consumer_id='test_lifecycle')._value.get()
        assert value == 3

        metrics.set_sessions_in_progress(1)
        value = metrics.sessions_in_progress.labels(consumer_id='test_lifecycle')._value.get()
        assert value == 1


class TestConsumerInfo:
    """set_info() populates the Info metric."""

    def test_sets_info_labels(self, metrics):
        metrics.set_info({
            'consumer_name': 'test_consumer',
            'consumer_group': 'test-group',
            'input_topic': 'test-input',
            'output_topic': 'test-output',
        })

        # Info metric exposes as a dict of label values
        samples = list(metrics.consumer_info.collect())
        assert len(samples) > 0
        # The Info metric adds '_info' suffix and exposes as gauge with value 1.0
        found = False
        for sample_family in samples:
            for sample in sample_family.samples:
                if sample.labels.get('consumer_name') == 'test_consumer':
                    found = True
                    assert sample.labels['consumer_group'] == 'test-group'
                    assert sample.labels['input_topic'] == 'test-input'
                    assert sample.labels['output_topic'] == 'test-output'
                    assert sample.value == 1.0
        assert found, "consumer_info metric not found with expected labels"
