"""
Tests for produce tracking metrics (messages_published_total, publish_duration_seconds).

Validates:
- KafkaMetricsWrapper.record_publish() increments correct counter labels
- Duration is observed in the histogram
- Both 'success' and 'error' results are tracked
"""
import pytest
from prometheus_client import REGISTRY, CollectorRegistry

from src.monitoring.kafka_metrics import KafkaMetricsWrapper


@pytest.fixture
def kafka_metrics():
    """Create a KafkaMetricsWrapper with a clean registry for testing."""
    # Use a unique namespace to avoid collisions between tests
    import uuid
    ns = f"test_{uuid.uuid4().hex[:8]}"
    return KafkaMetricsWrapper(consumer_id='test_consumer', namespace=ns)


class TestRecordPublish:
    """record_publish() correctly updates counters and histograms."""

    def test_increments_success_counter(self, kafka_metrics):
        kafka_metrics.record_publish(topic='test-topic', result='success', duration=0.05)

        sample = kafka_metrics.messages_published.labels(
            consumer_id='test_consumer',
            topic='test-topic',
            result='success'
        )._value.get()
        assert sample == 1.0

    def test_increments_error_counter(self, kafka_metrics):
        kafka_metrics.record_publish(topic='test-topic', result='error', duration=0.0)

        sample = kafka_metrics.messages_published.labels(
            consumer_id='test_consumer',
            topic='test-topic',
            result='error'
        )._value.get()
        assert sample == 1.0

    def test_observes_duration(self, kafka_metrics):
        kafka_metrics.record_publish(topic='test-topic', result='success', duration=0.123)

        # Histogram _sum should contain the observed value
        sample = kafka_metrics.publish_duration.labels(
            consumer_id='test_consumer',
            topic='test-topic'
        )._sum.get()
        assert abs(sample - 0.123) < 0.001

    def test_multiple_publishes_accumulate(self, kafka_metrics):
        kafka_metrics.record_publish(topic='output', result='success', duration=0.1)
        kafka_metrics.record_publish(topic='output', result='success', duration=0.2)
        kafka_metrics.record_publish(topic='output', result='error', duration=0.0)

        success_count = kafka_metrics.messages_published.labels(
            consumer_id='test_consumer',
            topic='output',
            result='success'
        )._value.get()
        error_count = kafka_metrics.messages_published.labels(
            consumer_id='test_consumer',
            topic='output',
            result='error'
        )._value.get()

        assert success_count == 2.0
        assert error_count == 1.0

    def test_different_topics_tracked_separately(self, kafka_metrics):
        kafka_metrics.record_publish(topic='topic-a', result='success', duration=0.1)
        kafka_metrics.record_publish(topic='topic-b', result='success', duration=0.1)

        count_a = kafka_metrics.messages_published.labels(
            consumer_id='test_consumer',
            topic='topic-a',
            result='success'
        )._value.get()
        count_b = kafka_metrics.messages_published.labels(
            consumer_id='test_consumer',
            topic='topic-b',
            result='success'
        )._value.get()

        assert count_a == 1.0
        assert count_b == 1.0


class TestMetricDeclarations:
    """Verify metric objects are correctly declared."""

    def test_messages_published_exists(self, kafka_metrics):
        assert hasattr(kafka_metrics, 'messages_published')
        assert kafka_metrics.messages_published._type == 'counter'

    def test_publish_duration_exists(self, kafka_metrics):
        assert hasattr(kafka_metrics, 'publish_duration')
        assert kafka_metrics.publish_duration._type == 'histogram'

    def test_messages_published_has_correct_labels(self, kafka_metrics):
        assert kafka_metrics.messages_published._labelnames == ('consumer_id', 'topic', 'result')

    def test_publish_duration_has_correct_labels(self, kafka_metrics):
        assert kafka_metrics.publish_duration._labelnames == ('consumer_id', 'topic')
