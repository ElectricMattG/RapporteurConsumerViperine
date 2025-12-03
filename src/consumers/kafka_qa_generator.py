#!/usr/bin/env python3
"""
Kafka Q&A Pair Generator Consumer

Wraps the PDF Q&A Generator component to generate question-answer pairs from
enriched embedded chunks. This consumer simply receives Kafka messages and
delegates processing to the sophisticated PDFQAGenerator.

Pipeline Flow:
  5. Chunk Embedder → generates embeddings
  6. Q&A Generator (this) → creates Q&A pairs from chunks

Message Format (Input - from chunk embedder):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "embedded_chunks_file": "path/to/chunked_document_enriched_embedded.jsonl",
        "document_id": "doc-id",
        "source_url": "original-url",
        "chunk_count": 4
    }

Usage:
    python run_kafka_qa_generator.py
"""

import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Import components
try:
    from ..components.q_and_a.pdf_qa_generator import PDFQAGenerator
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from components.q_and_a.pdf_qa_generator import PDFQAGenerator
    from config_handler import Base


class KafkaQAGeneratorConsumer(Base):
    """Kafka consumer that wraps PDFQAGenerator for Q&A pair generation"""

    def __init__(self):
        """Initialize the Kafka Q&A generator consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaQAGeneratorConsumer")

        # Load configuration
        self._load_config()

        # Initialize Kafka consumer
        self.consumer = None
        self.producer = None
        self.running = False

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._logger.info("✅ KafkaQAGeneratorConsumer initialized successfully")

    def _load_config(self):
        """Load configuration for Q&A generator"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers')
            if not bootstrap_servers:
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            self.kafka_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': self._app_config.get('qa_generator.consumer_group_id') or 'qa-generator-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 30000,
                'max.poll.interval.ms': 600000,  # 10 minutes for Q&A generation
            }

            # Topic configuration
            self.input_topic = self._app_config.get('qa_generator.input_topic') or 'qa-generation'
            # Output topic is optional
            try:
                self.output_topic = self._app_config.get('qa_generator.output_topic')
            except (KeyError, AttributeError):
                self.output_topic = None

            # Q&A generation configuration
            self.cache_dir = Path(self._app_config.get('qa_generator.cache_directory') or 'cache/qa_pairs')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic or 'None (terminal stage)'}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _create_consumer(self):
        """Create Kafka consumer instance"""
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.input_topic])
            self._logger.info(f"✅ Subscribed to topic: {self.input_topic}")

            # Create producer for DLQ if needed
            if self.output_topic:
                self.producer = Producer({'bootstrap.servers': self.kafka_config['bootstrap.servers']})
                self._logger.info(f"✅ Producer created for output topic: {self.output_topic}")

        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._logger.info(f"⚠️ Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _shutdown(self):
        """Shutdown consumer gracefully"""
        self._logger.info("🛑 Shutting down consumer...")
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
        self._logger.info("✅ Consumer shut down successfully")

    def _publish_to_dlq(self, original_message: Dict[str, Any], stage: str, error: str):
        """Publish failed message to dead letter queue"""
        try:
            from datetime import datetime

            from confluent_kafka import Producer

            dlq_producer = Producer({'bootstrap.servers': self.kafka_config['bootstrap.servers']})

            dlq_message = {
                'original_message': original_message,
                'failed_stage': stage,
                'error': error,
                'timestamp': datetime.now().isoformat(),
                'request_id': original_message.get('request_id', 'unknown'),
                'session_id': original_message.get('session_id', 'unknown')
            }

            dlq_producer.produce(
                topic='dead-letter-queue',
                value=json.dumps(dlq_message).encode('utf-8')
            )
            dlq_producer.flush()
            self._logger.info(f"   📮 Published failure to dead-letter-queue")
        except Exception as e:
            self._logger.error(f"   ⚠️ Failed to publish to DLQ: {e}")

    def _process_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a Q&A generation message using PDFQAGenerator.

        Args:
            message_data: Message containing embedded chunks file path

        Returns:
            Result dictionary with generation status
        """
        session_id = message_data.get('session_id', 'unknown')
        request_id = message_data.get('request_id', f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        embedded_chunks_file = message_data.get('embedded_chunks_file')
        document_id = message_data.get('document_id', 'unknown')
        source_url = message_data.get('source_url', 'unknown')
        chunk_count = message_data.get('chunk_count', 0)

        self._logger.info("=" * 80)
        self._logger.info(f"❓ Processing Q&A generation request")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   Document ID: {document_id}")
        self._logger.info(f"   Embedded chunks file: {embedded_chunks_file}")
        self._logger.info(f"   Source URL: {source_url}")
        self._logger.info(f"   Chunk count: {chunk_count}")

        result = {
            'session_id': session_id,
            'request_id': request_id,
            'document_id': document_id,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'chunks_processed': 0,
            'qa_pairs_generated': 0,
            'qa_file': None,
            'error': None
        }

        # Validate embedded chunks file exists
        if not embedded_chunks_file or not Path(embedded_chunks_file).exists():
            error_msg = f"Embedded chunks file not found: {embedded_chunks_file}"
            self._logger.error(f"   ❌ {error_msg}")
            result['error'] = error_msg
            self._publish_to_dlq(message_data, 'qa_generator', error_msg)
            return result

        try:
            # Determine output directory (same directory as input file)
            chunks_path = Path(embedded_chunks_file)
            output_dir = chunks_path.parent

            # Set incremental output for monitoring
            incremental_file = output_dir / f"{request_id}_qa_pairs_incremental.jsonl"

            self._logger.info(f"   📂 Output directory: {output_dir}")
            self._logger.info(f"   📝 Incremental output: {incremental_file}")

            # Initialize PDFQAGenerator
            # This handles all the sophisticated prompt engineering and processing
            qa_generator = PDFQAGenerator(
                output_dir=str(output_dir),
                incremental_output=str(incremental_file)
            )

            self._logger.info(f"   🤖 Starting Q&A generation with PDFQAGenerator...")

            # Process enriched chunks
            # This calls the full pipeline: load chunks, group by section, route to processors,
            # generate Q&A with batched LLM calls, save results
            generation_results = qa_generator.process_enriched_chunks(str(embedded_chunks_file))

            # Extract results
            qa_pairs = generation_results.get('qa_pairs', [])
            stats = generation_results.get('stats', {})
            qa_file = generation_results.get('qa_file')

            self._logger.info(f"   ✅ Q&A generation complete!")
            self._logger.info(f"   📊 Generated {len(qa_pairs)} Q&A pairs")
            self._logger.info(f"   📄 Output file: {qa_file}")
            self._logger.info(f"   📈 Stats: {stats.get('chunks_processed', 0)} chunks processed, "
                            f"{stats.get('processing_errors', 0)} errors")

            # Update result
            result['success'] = True
            result['chunks_processed'] = stats.get('chunks_processed', 0)
            result['qa_pairs_generated'] = len(qa_pairs)
            result['qa_file'] = qa_file
            result['stats'] = stats

            # Publish to output topic if configured (for Q&A embedding consumer)
            if self.output_topic and self.producer:
                try:
                    output_message = {
                        'request_id': request_id,
                        'session_id': session_id,
                        'document_id': document_id,
                        'source_url': source_url,
                        'qa_pairs_file': qa_file,  # Path to Q&A pairs JSONL file
                        'qa_count': len(qa_pairs),
                        'chunks_processed': stats.get('chunks_processed', 0),
                        'processing_time': stats.get('duration_minutes', 0)
                    }

                    self.producer.produce(
                        topic=self.output_topic,
                        value=json.dumps(output_message).encode('utf-8')
                    )
                    self.producer.flush()
                    self._logger.info(f"   📤 Published to output topic: {self.output_topic}")
                except Exception as e:
                    self._logger.error(f"   ⚠️ Failed to publish to output topic: {e}")

            self._logger.info("=" * 80)
            return result

        except Exception as e:
            error_msg = f"Error generating Q&A pairs: {e}"
            self._logger.error(f"   ❌ {error_msg}", exc_info=True)
            result['error'] = error_msg
            self._publish_to_dlq(message_data, 'qa_generator', error_msg)
            self._logger.info("=" * 80)
            return result

    def consume_messages(self):
        """Main consume loop"""
        self._logger.info("🚀 Starting Q&A generation consumer...")
        self._create_consumer()
        self.running = True

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self._logger.error(f"❌ Consumer error: {msg.error()}")
                        continue

                try:
                    # Parse message
                    message_data = json.loads(msg.value().decode('utf-8'))

                    # Process message (calls PDFQAGenerator)
                    result = self._process_message(message_data)

                    if result['success']:
                        self._logger.info(f"✅ Successfully generated {result['qa_pairs_generated']} Q&A pairs")
                    else:
                        self._logger.warning(f"⚠️ Q&A generation failed: {result.get('error', 'Unknown error')}")

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            self._logger.info("⚠️ Interrupted by user")
        finally:
            self._shutdown()

    def run(self):
        """Run the consumer"""
        self.consume_messages()


def main():
    """Main entry point"""
    consumer = KafkaQAGeneratorConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
