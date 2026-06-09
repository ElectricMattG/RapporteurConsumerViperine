"""
Kafka-specific metrics for consumer monitoring.
Tracks consumer lag, poll performance, rebalances, and offset commits.
"""

import time
from contextlib import contextmanager
from typing import Dict, List, Optional

from prometheus_client import Counter, Gauge, Histogram


class KafkaMetricsWrapper:
    """
    Wrapper for Kafka consumer with Prometheus metrics tracking.
    Monitors consumer lag, poll duration, batch sizes, and rebalances.
    """

    def __init__(self, consumer_id: str, namespace: str = 'kafka_consumer'):
        """
        Initialize Kafka metrics.

        Args:
            consumer_id: Unique identifier for this consumer
            namespace: Prometheus namespace prefix
        """
        self.consumer_id = consumer_id
        self.namespace = namespace

        # Consumer lag metrics
        self.consumer_lag_messages = Gauge(
            f'{namespace}_lag_messages',
            'Number of messages behind latest offset',
            ['consumer_id', 'topic', 'partition'],
            namespace=namespace
        )

        self.consumer_lag_seconds = Gauge(
            f'{namespace}_lag_seconds',
            'Time lag based on message timestamps',
            ['consumer_id', 'topic', 'partition'],
            namespace=namespace
        )

        # Poll performance
        self.poll_duration = Histogram(
            f'{namespace}_poll_duration_seconds',
            'Time spent polling Kafka for messages',
            ['consumer_id', 'topic'],
            namespace=namespace,
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
        )

        self.batch_size = Histogram(
            f'{namespace}_batch_size',
            'Number of messages per poll batch',
            ['consumer_id', 'topic'],
            namespace=namespace,
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000]
        )

        # Rebalance tracking
        self.rebalances_total = Counter(
            f'{namespace}_rebalances_total',
            'Total number of partition rebalances',
            ['consumer_id'],
            namespace=namespace
        )

        # Offset commit tracking
        self.offset_commit_duration = Histogram(
            f'{namespace}_offset_commit_duration_seconds',
            'Time spent committing offsets',
            ['consumer_id', 'topic'],
            namespace=namespace,
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0]
        )

        self.offset_commit_failures = Counter(
            f'{namespace}_offset_commit_failures_total',
            'Total offset commit failures',
            ['consumer_id', 'topic', 'error_type'],
            namespace=namespace
        )

        # Messages in flight
        self.messages_in_flight = Gauge(
            f'{namespace}_messages_in_flight',
            'Messages currently being processed',
            ['consumer_id', 'topic'],
            namespace=namespace
        )

        # Last successful poll timestamp
        self.last_successful_poll = Gauge(
            f'{namespace}_last_successful_poll_timestamp',
            'Unix timestamp of last successful poll',
            ['consumer_id'],
            namespace=namespace
        )

        # Message age
        self.message_age = Histogram(
            f'{namespace}_message_age_seconds',
            'Time from message production to consumption',
            ['consumer_id', 'topic'],
            namespace=namespace,
            buckets=[1, 5, 10, 30, 60, 300, 600, 1800, 3600]
        )

        # ── Produce tracking ─────────────────────────────────────────────────
        self.messages_published = Counter(
            f'{namespace}_messages_published_total',
            'Messages published to Kafka topics',
            ['consumer_id', 'topic', 'result']
        )

        self.publish_duration = Histogram(
            f'{namespace}_publish_duration_seconds',
            'Time spent producing messages (produce + flush)',
            ['consumer_id', 'topic'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
        )

    def record_poll(self, topic: str, duration: float, message_count: int):
        """
        Record metrics for a Kafka poll operation.

        Args:
            topic: Topic being polled
            duration: Time spent polling in seconds
            message_count: Number of messages received
        """
        self.poll_duration.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).observe(duration)

        if message_count > 0:
            self.batch_size.labels(
                consumer_id=self.consumer_id,
                topic=topic
            ).observe(message_count)

        self.last_successful_poll.labels(
            consumer_id=self.consumer_id
        ).set(time.time())

    def record_lag(self, topic: str, partition: int, lag_messages: int, lag_seconds: Optional[float] = None):
        """
        Record consumer lag for a partition.

        Args:
            topic: Topic name
            partition: Partition number
            lag_messages: Number of messages behind
            lag_seconds: Time lag in seconds (optional)
        """
        self.consumer_lag_messages.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            partition=str(partition)
        ).set(lag_messages)

        if lag_seconds is not None:
            self.consumer_lag_seconds.labels(
                consumer_id=self.consumer_id,
                topic=topic,
                partition=str(partition)
            ).set(lag_seconds)

    def record_message_age(self, topic: str, message_timestamp: float):
        """
        Record the age of a message (time from production to consumption).

        Args:
            topic: Topic name
            message_timestamp: Message timestamp in seconds (Unix epoch)
        """
        age = time.time() - message_timestamp
        self.message_age.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).observe(age)

    def increment_rebalances(self):
        """Record a partition rebalance event."""
        self.rebalances_total.labels(
            consumer_id=self.consumer_id
        ).inc()

    def record_offset_commit(self, topic: str, duration: float, success: bool = True, error_type: Optional[str] = None):
        """
        Record offset commit metrics.

        Args:
            topic: Topic name
            duration: Time spent committing in seconds
            success: Whether commit was successful
            error_type: Type of error if failed
        """
        self.offset_commit_duration.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).observe(duration)

        if not success and error_type:
            self.offset_commit_failures.labels(
                consumer_id=self.consumer_id,
                topic=topic,
                error_type=error_type
            ).inc()

    def set_messages_in_flight(self, topic: str, count: int):
        """
        Set the number of messages currently being processed.

        Args:
            topic: Topic name
            count: Number of messages in flight
        """
        self.messages_in_flight.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).set(count)

    def increment_messages_in_flight(self, topic: str):
        """Increment messages in flight counter."""
        self.messages_in_flight.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).inc()

    def decrement_messages_in_flight(self, topic: str):
        """Decrement messages in flight counter."""
        self.messages_in_flight.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).dec()

    @contextmanager
    def track_message_processing(self, topic: str):
        """
        Context manager to track message processing with automatic in-flight tracking.

        Args:
            topic: Topic name

        Usage:
            with kafka_metrics.track_message_processing('web-scraping'):
                process_message(msg)
        """
        self.increment_messages_in_flight(topic)
        try:
            yield
        finally:
            self.decrement_messages_in_flight(topic)

    def record_publish(self, topic: str, result: str, duration: float):
        """
        Record a Kafka produce operation outcome.

        Args:
            topic: Destination topic
            result: 'success' or 'error'
            duration: Time spent on produce+flush in seconds
        """
        # instrumentation - messages_published_total
        self.messages_published.labels(
            consumer_id=self.consumer_id,
            topic=topic,
            result=result
        ).inc()

        # instrumentation - publish_duration_seconds
        self.publish_duration.labels(
            consumer_id=self.consumer_id,
            topic=topic
        ).observe(duration)
