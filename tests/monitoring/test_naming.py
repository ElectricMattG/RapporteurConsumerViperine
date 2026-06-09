"""
Tests for Phase 6: KafkaMetricsWrapper produces clean metric names (no double-prefix).

Before the fix, passing namespace=namespace to Gauge/Counter/Histogram constructors
caused prometheus_client to prepend the namespace twice:
    Gauge(f'{ns}_lag_messages', ..., namespace=ns) → kafka_consumer_kafka_consumer_lag_messages

After the fix the name argument already contains the prefix, so no namespace kwarg:
    Gauge(f'{ns}_lag_messages', ...) → kafka_consumer_lag_messages
"""
import uuid

import pytest

from src.monitoring.kafka_metrics import KafkaMetricsWrapper


@pytest.fixture
def ns():
    return f"test_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_metrics(ns):
    return KafkaMetricsWrapper(consumer_id='test_naming', namespace=ns)


class TestNoDoublePrefix:
    """No KafkaMetricsWrapper metric name should repeat the namespace prefix."""

    def _assert_no_double(self, metric, ns):
        name = metric._name
        double = f'{ns}_{ns}'
        assert double not in name, f"Double-prefix detected in '{name}'"

    def test_lag_messages(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.consumer_lag_messages, ns)

    def test_lag_seconds(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.consumer_lag_seconds, ns)

    def test_poll_duration(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.poll_duration, ns)

    def test_batch_size(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.batch_size, ns)

    def test_rebalances_total(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.rebalances_total, ns)

    def test_offset_commit_duration(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.offset_commit_duration, ns)

    def test_offset_commit_failures(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.offset_commit_failures, ns)

    def test_messages_in_flight(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.messages_in_flight, ns)

    def test_last_successful_poll(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.last_successful_poll, ns)

    def test_message_age(self, kafka_metrics, ns):
        self._assert_no_double(kafka_metrics.message_age, ns)

    def test_all_metrics_have_correct_prefix(self, kafka_metrics, ns):
        """Every metric starts with namespace exactly once."""
        attrs = [
            'consumer_lag_messages', 'consumer_lag_seconds', 'poll_duration',
            'batch_size', 'rebalances_total', 'offset_commit_duration',
            'offset_commit_failures', 'messages_in_flight',
            'last_successful_poll', 'message_age',
        ]
        for attr in attrs:
            name = getattr(kafka_metrics, attr)._name
            assert name.startswith(f'{ns}_'), f"'{name}' is missing the namespace prefix"
            assert f'{ns}_{ns}' not in name, f"Double-prefix detected in '{name}'"
