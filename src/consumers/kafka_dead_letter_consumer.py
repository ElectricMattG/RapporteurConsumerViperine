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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from confluent_kafka import Consumer, KafkaError

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

        # Load configuration
        self._load_config()

        # Initialize Kafka consumer
        self.consumer = None
        self.running = False

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self):
        """Load configuration"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers') or 'localhost:9092'
            consumer_group_id = self._app_config.get('dead_letter.consumer_group_id') or 'dead-letter-consumer'
            self.input_topic = self._app_config.get('dead_letter.input_topic') or 'dead-letter-queue'

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
        failed_stage = message_data.get('failed_stage', 'unknown')
        request_id = message_data.get('request_id', 'unknown')
        session_id = message_data.get('session_id', 'unknown')
        error = message_data.get('error', 'Unknown error')
        timestamp = message_data.get('timestamp', datetime.now().isoformat())

        self._logger.info("=" * 80)
        self._logger.info("⚠️  PIPELINE FAILURE DETECTED")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Failed Stage: {failed_stage}")
        self._logger.info(f"   Error: {error[:200]}...")
        self._logger.info(f"   Timestamp: {timestamp}")

        # Save failure
        self._append_failure(message_data)

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

        except Exception as e:
            self._logger.error(f"   ⚠️ Error calculating statistics: {e}")

        self._logger.info("=" * 80)

    async def consume_messages(self):
        """Main consumer loop"""
        self._logger.info("🚀 Starting to consume dead letter queue messages...")

        try:
            self.running = True

            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

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
                        continue

                try:
                    # Decode message
                    message_str = msg.value().decode('utf-8')
                    message_data = json.loads(message_str)

                    # Process the message
                    await self.process_message(message_data)

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            self._logger.info("\n⚠️  Shutting down consumer...")
        finally:
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
