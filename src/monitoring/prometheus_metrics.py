"""
Base Prometheus Metrics Class

Provides common metrics and utilities for all consumers.

Common metrics exposed:
- messages_consumed_total: Counter with labels (consumer_id, topic)
- message_processing_duration_seconds: Histogram with labels (consumer_id, topic, status)
- processing_failures_total: Counter with labels (consumer_id, topic, error_type)
- consumer_state: Gauge (0=stopped, 1=running, 2=error)

Usage:
    from monitoring.prometheus_metrics import PrometheusMetrics

    metrics = PrometheusMetrics(consumer_id='web_scraper_pod_abc123')

    with metrics.time_operation('scrape_url', topic='scraped-urls'):
        result = scrape_url(url)

    metrics.increment_messages_consumed(topic='scraped-urls')
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from prometheus_client import Counter, Gauge, Histogram, Info

from src.monitoring.error_categories import ErrorCategory

logger = logging.getLogger(__name__)


class PrometheusMetrics:
    """
    Base class for Prometheus metrics collection.

    Provides common metrics that all consumers share, with support for
    custom labels and timing operations.
    """

    def __init__(self, consumer_id: str, namespace: str = "kafka_consumer"):
        """
        Initialize Prometheus metrics.

        Args:
            consumer_id: Unique identifier for this consumer instance
            namespace: Metric namespace prefix (default: kafka_consumer)
        """
        self.consumer_id = consumer_id
        self.namespace = namespace

        # Common metrics
        self.messages_consumed = Counter(
            f'{namespace}_messages_consumed_total',
            'Total number of messages consumed',
            ['consumer_id', 'topic']
        )

        self.processing_duration = Histogram(
            f'{namespace}_message_processing_duration_seconds',
            'Time spent processing messages',
            ['consumer_id', 'topic', 'status'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
        )

        self.processing_failures = Counter(
            f'{namespace}_processing_failures_total',
            'Total number of processing failures',
            ['consumer_id', 'topic', 'error_type']
        )

        self.consumer_state = Gauge(
            f'{namespace}_state',
            'Current consumer state (0=stopped, 1=running, 2=error)',
            ['consumer_id']
        )

        self.consumer_info = Info(
            f'{namespace}_info',
            'Consumer information'
        )

        # Error tracking by type
        self.deserialization_errors = Counter(
            f'{namespace}_deserialization_errors_total',
            'Total deserialization errors',
            ['consumer_id', 'topic', 'error_type']
        )

        self.retry_attempts = Counter(
            f'{namespace}_retry_attempts_total',
            'Total retry attempts for failed messages',
            ['consumer_id', 'topic']
        )

        self.dead_letter_messages = Counter(
            f'{namespace}_dead_letter_messages_total',
            'Messages sent to dead letter queue',
            ['consumer_id', 'topic', 'reason']
        )

        self.connection_errors = Counter(
            f'{namespace}_connection_errors_total',
            'Total connection errors',
            ['consumer_id', 'error_type']
        )

        self.timeout_errors = Counter(
            f'{namespace}_timeout_errors_total',
            'Total timeout errors',
            ['consumer_id', 'topic', 'operation']
        )

        self.model_error_outputs = Counter(
            f'{namespace}_model_error_outputs_total',
            'LLM returned HTTP 200 but content is an error/refusal message',
            ['consumer_id', 'model']
        )

        # ── Lifecycle metrics (Phase 2) ──────────────────────────────────────
        self.startup_timestamp = Gauge(
            f'{namespace}_startup_timestamp_seconds',
            'Unix timestamp when consumer started',
            ['consumer_id']
        )

        self.last_message_processed_timestamp = Gauge(
            f'{namespace}_last_message_processed_timestamp',
            'Unix timestamp of last successfully processed message',
            ['consumer_id', 'topic']
        )

        self.sessions_in_progress = Gauge(
            f'{namespace}_sessions_in_progress',
            'Number of sessions currently being tracked',
            ['consumer_id']
        )

        # Set initial state
        self.consumer_state.labels(consumer_id=consumer_id).set(0)

        logger.info(f"Initialized Prometheus metrics for consumer: {consumer_id}")

    def increment_messages_consumed(self, topic: str, count: int = 1):
        """Increment the messages consumed counter."""
        self.messages_consumed.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).inc(count)

    def increment_failures(self, topic: str, error_type: str, count: int = 1):
        """Increment the processing failures counter."""
        self.processing_failures.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            error_type=error_type
        ).inc(count)

    def increment_deserialization_errors(self, topic: str, error_type: str, count: int = 1):
        """Increment deserialization error counter."""
        self.deserialization_errors.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            error_type=error_type
        ).inc(count)

    def increment_retries(self, topic: str, count: int = 1):
        """Increment retry attempts counter."""
        self.retry_attempts.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).inc(count)

    def increment_dead_letter(self, topic: str, reason: str, count: int = 1):
        """Increment dead letter queue counter."""
        self.dead_letter_messages.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            reason=reason
        ).inc(count)

    def increment_connection_errors(self, error_type: str, count: int = 1):
        """Increment connection error counter."""
        self.connection_errors.labels(
            consumer_id=self.consumer_id,
            error_type=error_type
        ).inc(count)

    def increment_timeout_errors(self, topic: str, operation: str, count: int = 1):
        """Increment timeout error counter."""
        self.timeout_errors.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            operation=operation
        ).inc(count)

    def set_state(self, state: int):
        """
        Set the consumer state.

        Args:
            state: 0=stopped, 1=running, 2=error
        """
        self.consumer_state.labels(consumer_id=self.consumer_id).set(state)

    def set_info(self, info: Dict[str, str]):
        """Set consumer information metadata."""
        self.consumer_info.info(info)

    def record_startup(self):
        """Record consumer startup timestamp (call once at boot)."""
        self.startup_timestamp.labels(consumer_id=self.consumer_id).set(time.time())

    def record_message_processed(self, topic: str, duration: Optional[float] = None, status: str = 'success'):
        """Update last-message-processed timestamp and optionally record processing metrics.

        When duration is provided, also increments messages_consumed and observes
        the processing_duration histogram, consolidating three call-sites into one.
        Backward-compatible: calling without duration only updates the timestamp.
        """
        self.last_message_processed_timestamp.labels(
            consumer_id=self.consumer_id, topic=topic
        ).set(time.time())
        if duration is not None:
            self.increment_messages_consumed(topic)
            self.processing_duration.labels(
                consumer_id=self.consumer_id, topic=topic, status=status
            ).observe(duration)

    def record_failure(self, topic: str, error_category: str):
        """Increment processing failures after validating category against ErrorCategory.

        Raises ValueError for freeform strings that aren't registered categories,
        preventing invalid label values from reaching Prometheus.
        """
        ErrorCategory.validate(error_category)
        self.processing_failures.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            error_type=error_category
        ).inc()

    def record_dead_letter(self, topic: str, reason: str):
        """Increment the dead letter queue counter."""
        self.dead_letter_messages.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            reason=reason
        ).inc()

    def set_sessions_in_progress(self, count: int):
        """Set the number of sessions currently being tracked."""
        self.sessions_in_progress.labels(consumer_id=self.consumer_id).set(count)

    @contextmanager
    def time_operation(self, topic: str, status: str = 'success'):
        """
        Context manager for timing operations.

        Args:
            topic: Kafka topic name
            status: Operation status (success/failure)

        Usage:
            with metrics.time_operation('my-topic', 'success'):
                do_work()
        """
        start_time = time.time()
        try:
            yield
        except Exception as e:
            status = 'failure'
            raise
        finally:
            duration = time.time() - start_time
            self.processing_duration.labels(
                consumer_id=self.consumer_id,
                topic=topic,
                status=status
            ).observe(duration)

    def record_duration(self, topic: str, duration: float, status: str = 'success'):
        """
        Record a duration metric manually.

        Args:
            topic: Kafka topic name
            duration: Duration in seconds
            status: Operation status
        """
        self.processing_duration.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            status=status
        ).observe(duration)
