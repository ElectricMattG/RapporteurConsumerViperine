#!/usr/bin/env python3
"""
Kafka Dead Letter Queue Consumer

Consumes failed messages from the dead-letter-queue topic and logs them
to a failures.json file with detailed error information.

This consumer tracks failures across all pipeline stages:
- web_scraper: Failed to extract content
- summarizer: Failed to generate summary
- chunker: Failed to chunk document
- citation_extractor: Failed to extract citations
- chunk_embedder: Failed to generate embeddings
- qa_generator: Failed to generate Q&A pairs

Message Format:
    {
        "original_message": {...},  # The original message that failed
        "failed_stage": "stage_name",  # Which stage failed
        "error": "error message",
        "timestamp": "2025-12-03T...",
        "request_id": "req_...",
        "session_id": "session_..."
    }

Output:
    failures.json: Append-only JSON array with all failures

Usage:
    python run_kafka_dead_letter_consumer.py
"""

import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from confluent_kafka import Consumer, KafkaError

# Telemetry imports with fallback
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.logging_config import configure_logging
    TELEMETRY_AVAILABLE = True
except ImportError:
    # Fallback for environments where telemetry is not available
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
        from monitoring.logging_config import configure_logging
        TELEMETRY_AVAILABLE = True
    except ImportError:
        TELEMETRY_AVAILABLE = False
        print("⚠️ Telemetry modules not available - running without metrics")

# Add the src directory to the path
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from config_handler import Base


class KafkaDeadLetterConsumer(Base):
    """Consumer for dead letter queue messages"""

    def __init__(self):
        """Initialize the dead letter queue consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaDeadLetterConsumer")

        # Initialize telemetry components if available
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"dead_letter_{os.getpid()}"
            configure_logging(
                format='json',
                level='INFO',
                consumer_id=consumer_id
            )

            # Initialize metrics collectors
            self.metrics = PrometheusMetrics(consumer_id)
            self.system_metrics = SystemMetricsCollector(consumer_id)
            self.kafka_metrics = KafkaMetricsWrapper(consumer_id)

            # Dead letter queue-specific metrics
            from prometheus_client import Counter, Gauge, Histogram

            self.dlq_messages_processed = Counter(
                'kafka_consumer_dlq_messages_processed_total',
                'Total dead letter queue messages processed',
                ['consumer_id', 'failed_stage']
            )

            self.retry_attempts = Counter(
                'kafka_consumer_dlq_retry_attempts_total',
                'Total retry attempts for failed messages',
                ['consumer_id', 'failed_stage']
            )

            self.messages_reprocessed = Counter(
                'kafka_consumer_dlq_messages_reprocessed_total',
                'Total messages successfully reprocessed',
                ['consumer_id', 'failed_stage']
            )

            self.messages_discarded = Counter(
                'kafka_consumer_dlq_messages_discarded_total',
                'Total messages permanently discarded',
                ['consumer_id', 'failed_stage']
            )

            self.failures_by_stage = Gauge(
                'kafka_consumer_dlq_failures_by_stage',
                'Current count of failures by pipeline stage',
                ['consumer_id', 'stage']
            )

            self.dlq_processing_duration = Histogram(
                'kafka_consumer_dlq_processing_duration_seconds',
                'Time spent processing DLQ messages',
                ['consumer_id', 'status'],
                buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
            )

            self.dlq_messages_by_origin = Counter(
                'kafka_consumer_dlq_messages_by_origin_total',
                'Total DLQ messages by the topic that originated the failure',
                ['consumer_id', 'origin_topic']
            )

            self.dlq_age_seconds = Histogram(
                'kafka_consumer_dlq_age_seconds',
                'Time between failure timestamp and DLQ processing (message age in DLQ)',
                ['consumer_id'],
                buckets=[1, 10, 30, 60, 300, 600, 1800, 3600, 86400]
            )

            # Create metrics server with health checks
            self.metrics_server = MetricsServer(
                port=8009,
                consumer_name='dead_letter',
                health_check=lambda: self.running,
                ready_check=lambda: self.consumer is not None and self.running
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8009")
        else:
            self.metrics = None
            self.system_metrics = None
            self.kafka_metrics = None
            self.metrics_server = None

        # Initialize Kafka consumer (will be set in _load_config)
        self.consumer = None
        self.running = False

        # Load configuration and create consumer
        self._load_config()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self):
        """Load configuration"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers')
            if not bootstrap_servers:
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            consumer_group_id = self._app_config.get('dead_letter.consumer_group_id') or 'viperine-dead-letter-v2'
            self.input_topic = self._app_config.get('dead_letter.input_topic') or 'viperine.dead-letter-queue'

            # Output configuration
            output_dir = self._app_config.get('dead_letter.output_directory') or 'cache/failures'
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)

            self.failures_file = self.output_dir / 'failures.json'

            # Initialize failures file if it doesn't exist
            if not self.failures_file.exists():
                with open(self.failures_file, 'w', encoding='utf-8') as f:
                    json.dump([], f)
                self._logger.info(f"   📝 Created new failures file: {self.failures_file}")

            # Log configuration
            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {consumer_group_id}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Failures file: {self.failures_file}")

            # Initialize Kafka consumer
            self.consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': consumer_group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            })

            # Subscribe to topic
            self.consumer.subscribe([self.input_topic])
            self._logger.info(f"📡 Subscribed to topic: {self.input_topic}")

            self.running = True

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._logger.info(f"⚠️ Received signal {signum}, shutting down...")
        self.running = False

    def _append_failure(self, failure_data: Dict[str, Any]):
        """Append failure to failures.json file.

        Args:
            failure_data: Failure message from dead letter queue
        """
        try:
            # Read existing failures
            with open(self.failures_file, 'r', encoding='utf-8') as f:
                failures = json.load(f)

            # Append new failure
            failures.append(failure_data)

            # Write back
            with open(self.failures_file, 'w', encoding='utf-8') as f:
                json.dump(failures, f, indent=2, ensure_ascii=False)

            # Also create individual failure file
            request_id = failure_data.get('request_id', 'unknown')
            failed_stage = failure_data.get('failed_stage', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            individual_file = self.output_dir / f"failure_{request_id}_{failed_stage}_{timestamp}.json"
            with open(individual_file, 'w', encoding='utf-8') as f:
                json.dump(failure_data, f, indent=2, ensure_ascii=False)

            self._logger.info(f"   💾 Saved failure to {self.failures_file}")
            self._logger.info(f"   💾 Individual failure file: {individual_file}")

        except Exception as e:
            self._logger.error(f"   ❌ Error saving failure: {e}")

    async def process_message(self, message_data: Dict[str, Any]):
        """Process a dead letter queue message.

        Args:
            message_data: Failure message containing original_message, failed_stage, error
        """
        # Track DLQ processing start time
        dlq_start = time.time() if self.telemetry_enabled else None

        failed_stage = message_data.get('failed_stage', 'unknown')
        request_id = message_data.get('request_id', 'unknown')
        session_id = message_data.get('session_id', 'unknown')
        error = message_data.get('error', 'Unknown error')
        timestamp = message_data.get('timestamp', datetime.now().isoformat())
        origin_topic = message_data.get('origin_topic') or message_data.get('source_topic', 'unknown')

        self._logger.info("=" * 80)
        self._logger.info("⚠️  PIPELINE FAILURE DETECTED")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Failed Stage: {failed_stage}")
        self._logger.info(f"   Error: {error[:200]}...")
        self._logger.info(f"   Timestamp: {timestamp}")

        # Save failure
        self._append_failure(message_data)

        # Track DLQ processing metrics
        if self.telemetry_enabled:
            consumer_id_label = f"dead_letter_{os.getpid()}"

            # Increment DLQ messages processed counter
            self.dlq_messages_processed.labels(
                consumer_id=consumer_id_label,
                failed_stage=failed_stage
            ).inc()

            # Track message age in the DLQ
            if hasattr(self, 'dlq_age_seconds'):
                try:
                    from datetime import timezone as _tz
                    failure_ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    if failure_ts.tzinfo is None:
                        failure_ts = failure_ts.replace(tzinfo=_tz.utc)
                    age = (datetime.now(_tz.utc) - failure_ts).total_seconds()
                    if age >= 0:
                        self.dlq_age_seconds.labels(consumer_id=consumer_id_label).observe(age)
                except Exception:
                    pass

            # Track origin topic
            if hasattr(self, 'dlq_messages_by_origin'):
                self.dlq_messages_by_origin.labels(
                    consumer_id=consumer_id_label,
                    origin_topic=origin_topic
                ).inc()

        # Log statistics
        try:
            with open(self.failures_file, 'r', encoding='utf-8') as f:
                failures = json.load(f)

            # Count by stage
            stage_counts = {}
            for failure in failures:
                stage = failure.get('failed_stage', 'unknown')
                stage_counts[stage] = stage_counts.get(stage, 0) + 1

            self._logger.info(f"   📊 Total failures: {len(failures)}")
            self._logger.info(f"   📊 Failures by stage: {stage_counts}")

            # Update failures_by_stage gauge
            if self.telemetry_enabled:
                consumer_id_label = f"dead_letter_{os.getpid()}"
                for stage, count in stage_counts.items():
                    self.failures_by_stage.labels(
                        consumer_id=consumer_id_label,
                        stage=stage
                    ).set(count)

        except Exception as e:
            self._logger.error(f"   ⚠️ Error calculating statistics: {e}")

        # Track DLQ processing duration
        if self.telemetry_enabled and dlq_start:
            dlq_duration = time.time() - dlq_start
            self.dlq_processing_duration.labels(
                consumer_id=consumer_id_label,
                status='success'
            ).observe(dlq_duration)

        self._logger.info("=" * 80)

    async def consume_messages(self):
        """Main consumer loop"""
        self._logger.info("🚀 Starting to consume dead letter queue messages...")

        # Start telemetry services if available
        if self.telemetry_enabled:
            try:
                self._logger.info("📊 Starting telemetry services...")
                self.system_metrics.start()
                self.metrics_server.start()
                self.metrics.set_state(1)  # Set state to running
                self._logger.info("✅ Telemetry services started")
            except Exception as e:
                self._logger.error(f"⚠️ Failed to start telemetry services: {e}")

        try:
            self.running = True

            while self.running:
                # Track poll start time
                poll_start = time.time() if self.telemetry_enabled else None

                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                # Record poll metrics
                if self.telemetry_enabled and poll_start:
                    poll_duration = time.time() - poll_start
                    message_count = 1 if msg and not msg.error() else 0
                    self.kafka_metrics.record_poll(
                        self.input_topic,
                        poll_duration,
                        message_count
                    )

                if msg is None:
                    # No message available
                    await asyncio.sleep(0.1)
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        continue
                    else:
                        self._logger.error(f"❌ Kafka error: {msg.error()}")
                        # Track connection errors
                        if self.telemetry_enabled:
                            self.metrics.increment_connection_errors(str(msg.error().code()))
                        continue

                try:
                    # Track message age
                    if self.telemetry_enabled:
                        timestamp_type, timestamp_ms = msg.timestamp()
                        if timestamp_type != -1 and timestamp_ms > 0:
                            message_timestamp = timestamp_ms / 1000.0
                            self.kafka_metrics.record_message_age(self.input_topic, message_timestamp)

                    # Calculate consumer lag
                    if self.telemetry_enabled:
                        try:
                            from confluent_kafka import TopicPartition
                            topic = msg.topic()
                            partition = msg.partition()
                            current_offset = msg.offset()

                            tp = TopicPartition(topic, partition)
                            _, high_water_mark = self.consumer.get_watermark_offsets(tp)

                            lag_messages = high_water_mark - current_offset - 1
                            if lag_messages > 0:
                                lag_seconds = lag_messages / 10.0
                                self.kafka_metrics.record_lag(topic, partition, lag_messages, lag_seconds)
                        except Exception as e:
                            self._logger.debug(f"Could not calculate lag: {e}")

                    # Decode message
                    message_str = msg.value().decode('utf-8')
                    message_data = json.loads(message_str)

                    # Increment messages consumed and in flight
                    if self.telemetry_enabled:
                        self.metrics.increment_messages_consumed(self.input_topic)
                        self.kafka_metrics.increment_messages_in_flight(self.input_topic)

                    # Track processing start time
                    processing_start = time.time() if self.telemetry_enabled else None

                    # Process the message
                    await self.process_message(message_data)

                    # Record processing duration and decrement messages in flight
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                        if processing_start:
                            duration = time.time() - processing_start
                            self.metrics.record_duration(self.input_topic, duration, 'success')

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                    if self.telemetry_enabled:
                        self.metrics.increment_deserialization_errors(self.input_topic, 'json_decode_error')
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)
                    if self.telemetry_enabled:
                        self.metrics.increment_failures(self.input_topic, 'processing_error')
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

        except KeyboardInterrupt:
            self._logger.info("\n⚠️  Shutting down consumer...")
        finally:
            # Shutdown telemetry on exit
            if self.telemetry_enabled:
                try:
                    self._logger.info("🛑 Shutting down telemetry services...")
                    self.system_metrics.stop()
                    self.metrics_server.stop()
                    self.metrics.set_state(0)
                    self._logger.info("✅ Telemetry services stopped")
                except Exception as e:
                    self._logger.error(f"⚠️ Error shutting down telemetry: {e}")

            if self.consumer:
                self.consumer.close()
            self._logger.info("👋 Consumer closed")

    def run(self):
        """Run the consumer"""
        asyncio.run(self.consume_messages())


def main():
    """Main entry point"""
    print("=" * 80)
    print("⚠️  Kafka Dead Letter Queue Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaDeadLetterConsumer()

        print("✅ Consumer initialized successfully")
        print()

        consumer.run()

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
