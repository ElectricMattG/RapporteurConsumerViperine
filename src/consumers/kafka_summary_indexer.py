#!/usr/bin/env python3
"""
Kafka Summary Indexer Consumer

Generates embeddings for document summaries incrementally and creates a searchable summary index.
Works as part of the processing pipeline:
  1. Web Scraper → extracts content
  2. Summarizer → generates summaries → summary-completed
  3. Summary Indexer (this) → embeds summaries → session-complete (summaries)
  4. Index Aggregator → aggregates summary index separately

Message Format (Input - from Summarizer):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "document_id": "doc-id",
        "source_url": "original-url",
        "summary_file": "path/to/doc_summary.json",
        "current_count": 3,
        "total_requests": 15
    }

Output Message (to Index Aggregator):
    {
        "session_id": "session-id",
        "summary_index_file": "path/to/session_summaries_embedded.jsonl",
        "total_summaries": 15,
        "index_type": "summaries"
    }

Output File Schema (JSONL - one entry per document):
    {
        "document_id": "doc-123",
        "source_url": "https://...",
        "summary_text": "Full summary content...",
        "summary_embedding": [0.123, ...],  # 384-dim
        "doc_metadata": {
            "title": "...",
            "author": "...",
            "creation_date": "...",
            "word_count": 5000
        },
        "embedding_metadata": {
            "embedding_id": "sum_doc-123",
            "embedding_dim": 384,
            "embedding_model": "all-MiniLM-L6-v2",
            "embedded_at": "..."
        },
        "indexed_at": "..."
    }

Usage:
    python run_kafka_summary_indexer.py
"""

import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Telemetry imports with fallback
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.logging_config import configure_logging
    TELEMETRY_AVAILABLE = True
except ImportError:
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
        from monitoring.logging_config import configure_logging
        TELEMETRY_AVAILABLE = True
    except ImportError:
        TELEMETRY_AVAILABLE = False
        print("⚠️ Telemetry modules not available - running without metrics")

# Import components
try:
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base

# Import embedding client
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../clients'))
from embedding_client import EmbeddingServiceClient


class KafkaSummaryIndexerConsumer(Base):
    """Kafka consumer for indexing and embedding document summaries"""

    def __init__(self):
        """Initialize the Kafka summary indexer consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaSummaryIndexerConsumer")

        # Load configuration
        self._load_config()

        # Initialize telemetry components if available
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"summary_indexer_{os.getpid()}"
            configure_logging(
                format='json',
                level='INFO',
                consumer_id=consumer_id
            )

            # Initialize metrics collectors
            self.metrics = PrometheusMetrics(consumer_id)
            self.system_metrics = SystemMetricsCollector(consumer_id)
            self.kafka_metrics = KafkaMetricsWrapper(consumer_id)

            # Create metrics server with health checks
            self.metrics_server = MetricsServer(
                port=8006,
                consumer_name='summary_indexer',
                health_check=lambda: self.running,
                ready_check=lambda: self.consumer is not None and self.running
            )

            # Summary indexer-specific metrics
            from prometheus_client import Counter, Gauge, Histogram

            self.summaries_indexed = Counter(
                'kafka_consumer_summaries_indexed_total',
                'Total summaries indexed',
                ['consumer_id']
            )

            self.sessions_completed = Counter(
                'kafka_consumer_summary_sessions_completed_total',
                'Total summary indexing sessions completed',
                ['consumer_id']
            )

            self.summary_embedding_duration = Histogram(
                'kafka_consumer_summary_embedding_duration_seconds',
                'Time spent generating summary embeddings',
                ['consumer_id', 'status'],
                buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
            )

            self.summaries_per_session = Histogram(
                'kafka_consumer_summaries_per_session',
                'Number of summaries per session',
                ['consumer_id'],
                buckets=[0, 5, 10, 20, 50, 100, 200, 500]
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8006")
        else:
            self.metrics = None
            self.system_metrics = None
            self.kafka_metrics = None
            self.metrics_server = None

        # Initialize Kafka consumer and producer
        self.consumer = None
        self.producer = None
        self.running = False

        # Initialize embedding client
        self.embedding_client = None

        # Session state tracking
        self.session_states = {}  # {session_id: {count, total, started_at, document_ids}}

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._logger.info("✅ KafkaSummaryIndexerConsumer initialized successfully")

    def _load_config(self):
        """Load configuration for summary indexer"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers')
            if not bootstrap_servers:
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            self.kafka_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': self._app_config.get('summary_indexer.consumer_group_id') or 'summary-indexer-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 45000,
                'heartbeat.interval.ms': 3000,
                'max.poll.interval.ms': 300000,  # 5 minutes
            }

            # Topic configuration
            self.input_topic = self._app_config.get('summary_indexer.input_topic') or 'summary-completed'
            self.output_topic = self._app_config.get('summary_indexer.output_topic') or 'session-complete'

            # Cache directory
            self.cache_dir = Path(self._app_config.get('summary_indexer.cache_directory') or 'cache/summaries')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            # Combined index file path (in cache directory)
            try:
                combined_index_filename = self._app_config.get('summary_indexer.combined_index_file')
            except (KeyError, AttributeError):
                combined_index_filename = None

            if not combined_index_filename:
                combined_index_filename = 'research_summaries_with_embeddings_combined.jsonl'

            # Store combined index in cache directory
            self.combined_index_file = self.cache_dir / combined_index_filename

            # Embedding configuration
            self.model_name = self._app_config.get('summary_indexer.model_name') or 'all-MiniLM-L6-v2'

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Combined index: {self.combined_index_file}")
            self._logger.info(f"   Model: {self.model_name}")

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._logger.info(f"🛑 Received signal {signum}, shutting down...")
        self.running = False

    def _create_producer(self):
        """Create Kafka producer for publishing results"""
        try:
            producer_config = {
                'bootstrap.servers': self.kafka_config['bootstrap.servers']
            }
            self.producer = Producer(producer_config)
            self._logger.info(f"✅ Kafka producer created")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka producer: {e}")
            raise

    def _get_session_dir(self, session_id: str) -> Path:
        """Get directory for session files"""
        session_dir = self.cache_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        return session_dir

    def _load_session_state(self, session_id: str) -> Dict[str, Any]:
        """Load session state from file"""
        state_file = self._get_session_dir(session_id) / f"{session_id}_state.json"

        if state_file.exists():
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self._logger.warning(f"   ⚠️ Error loading state file: {e}")

        return {
            'session_id': session_id,
            'total_requests': 0,
            'received_count': 0,
            'document_ids_processed': [],
            'started_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'completed': False
        }

    def _save_session_state(self, session_id: str, state: Dict[str, Any]):
        """Save session state to file"""
        state_file = self._get_session_dir(session_id) / f"{session_id}_state.json"
        state['last_updated'] = datetime.now().isoformat()

        try:
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self._logger.error(f"   ⚠️ Error saving state file: {e}")

    def _load_summary_file(self, summary_file: str) -> Dict[str, Any]:
        """Load summary data from file"""
        summary_path = Path(summary_file)

        if not summary_path.exists():
            raise FileNotFoundError(f"Summary file not found: {summary_file}")

        with open(summary_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async def _embed_summary(self, summary_text: str) -> List[float]:
        """Generate embedding for summary text"""
        if not self.embedding_client:
            self.embedding_client = EmbeddingServiceClient()

        embeddings = await self.embedding_client.embed(
            [summary_text],
            batch_size=1,
            show_progress=False
        )

        return embeddings[0].tolist()

    def _append_to_index(self, session_id: str, entry: Dict[str, Any]):
        """Append summary entry to session's JSONL index file"""
        session_dir = self._get_session_dir(session_id)
        index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"

        with open(index_file, 'a', encoding='utf-8') as f:
            json.dump(entry, f, ensure_ascii=False)
            f.write('\n')

    def _publish_session_complete(self, session_id: str, total_summaries: int):
        """Publish session-complete message to Index Aggregator"""
        if not self.producer:
            self._create_producer()

        session_dir = self._get_session_dir(session_id)
        index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"

        message = {
            'session_id': session_id,
            'summary_index_file': str(index_file),
            'total_summaries': total_summaries,
            'index_type': 'summaries',
            'completed_at': datetime.now().isoformat()
        }

        try:
            self.producer.produce(
                self.output_topic,
                key=session_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()

            self._logger.info(f"   📤 Published session-complete to {self.output_topic}")

            # Track session completion
            if self.telemetry_enabled:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                self.sessions_completed.labels(consumer_id=consumer_id_label).inc()
                self.summaries_per_session.labels(consumer_id=consumer_id_label).observe(total_summaries)

        except Exception as e:
            self._logger.error(f"   ❌ Error publishing session-complete: {e}")
            raise

    def _aggregate_to_combined_index(self, session_id: str):
        """
        Aggregate session index into the combined master index.
        Handles deduplication by document_id.
        """
        try:
            session_dir = self._get_session_dir(session_id)
            session_index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"

            if not session_index_file.exists():
                self._logger.warning(f"   ⚠️ Session index file not found: {session_index_file}")
                return

            self._logger.info(f"   📚 Aggregating session index to combined index...")
            self._logger.info(f"   Source: {session_index_file}")
            self._logger.info(f"   Target: {self.combined_index_file}")

            # Ensure parent directory exists for combined index file
            self.combined_index_file.parent.mkdir(parents=True, exist_ok=True)

            # Load existing document IDs from combined index to avoid duplicates
            existing_doc_ids = set()
            if self.combined_index_file.exists():
                with open(self.combined_index_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            try:
                                entry = json.loads(line)
                                doc_id = entry.get('document_id')
                                if doc_id:
                                    existing_doc_ids.add(doc_id)
                            except json.JSONDecodeError:
                                continue

            self._logger.info(f"   📊 Found {len(existing_doc_ids)} existing documents in combined index")

            # Read session index and append new entries
            new_count = 0
            duplicate_count = 0
            new_additions = []  # Track new summaries for frontend message

            with open(session_index_file, 'r', encoding='utf-8') as source:
                with open(self.combined_index_file, 'a', encoding='utf-8') as target:
                    for line in source:
                        if line.strip():
                            try:
                                entry = json.loads(line)
                                doc_id = entry.get('document_id')

                                if doc_id and doc_id not in existing_doc_ids:
                                    # New document, append to combined index
                                    json.dump(entry, target, ensure_ascii=False)
                                    target.write('\n')
                                    existing_doc_ids.add(doc_id)
                                    new_count += 1

                                    # Track new addition for frontend message
                                    new_additions.append({
                                        'url': entry.get('source_url', ''),
                                        'title': entry.get('doc_metadata', {}).get('title', 'Untitled')
                                    })
                                else:
                                    duplicate_count += 1

                            except json.JSONDecodeError as e:
                                self._logger.warning(f"   ⚠️ Invalid JSON in session index: {e}")
                                continue

            self._logger.info(f"   ✅ Aggregation complete:")
            self._logger.info(f"      New entries: {new_count}")
            self._logger.info(f"      Duplicates skipped: {duplicate_count}")
            self._logger.info(f"      Total in combined index: {len(existing_doc_ids)}")

            # Publish frontend message with aggregation results
            if new_count > 0:
                self._publish_frontend_message(session_id, new_additions, len(existing_doc_ids))

        except Exception as e:
            self._logger.error(f"   ❌ Error aggregating to combined index: {e}", exc_info=True)

    def _publish_frontend_message(self, session_id: str, new_additions: List[Dict[str, str]], total_summaries: int):
        """
        Publish a message to the frontend-message topic with aggregation results.

        Args:
            session_id: The session ID that was aggregated
            new_additions: List of dicts with 'url' and 'title' for each new summary
            total_summaries: Total number of summaries in the combined index
        """
        if not self.producer:
            self._create_producer()

        message = {
            'type': 'summary_aggregation',
            'session_id': session_id,
            'new_summaries_count': len(new_additions),
            'total_summaries_count': total_summaries,
            'new_summaries': new_additions,
            'timestamp': datetime.now().isoformat()
        }

        try:
            self.producer.produce(
                'frontend-message',
                key=session_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()

            self._logger.info(f"   📢 Published frontend message: {len(new_additions)} new summaries, {total_summaries} total")

        except Exception as e:
            self._logger.error(f"   ❌ Error publishing frontend message: {e}")

    async def _process_message(self, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single summary-completed message"""
        try:
            # Extract message fields
            request_id = message_data.get('request_id')
            session_id = message_data.get('session_id')
            document_id = message_data.get('document_id')
            source_url = message_data.get('source_url')
            summary_file = message_data.get('summary_file')
            current_count = message_data.get('current_count', 1)
            total_requests = message_data.get('total_requests', 1)

            self._logger.info("="*80)
            self._logger.info(f"📝 Processing Summary Indexing")
            self._logger.info(f"   Session ID: {session_id}")
            self._logger.info(f"   Document ID: {document_id}")
            self._logger.info(f"   Progress: {current_count}/{total_requests}")
            self._logger.info(f"   Summary file: {summary_file}")

            # Load session state
            state = self._load_session_state(session_id)

            # Update total if not set
            if state['total_requests'] == 0:
                state['total_requests'] = total_requests

            # Check if already processed (idempotency)
            if document_id in state['document_ids_processed']:
                self._logger.info(f"   ⏭️  Document {document_id} already processed, skipping")
                return None

            # Track embedding start time
            embed_start = time.time() if self.telemetry_enabled else None

            # Load summary data
            try:
                summary_data = self._load_summary_file(summary_file)
            except FileNotFoundError as e:
                self._logger.error(f"   ❌ {e}")
                return None

            # Extract summary text and metadata
            summary_text = summary_data.get('markdown_summary', '')
            doc_metadata = summary_data.get('metadata', {})

            if not summary_text:
                self._logger.warning(f"   ⚠️ No summary text found for {document_id}")
                return None

            self._logger.info(f"   📄 Summary length: {len(summary_text)} chars")

            # Generate embedding
            self._logger.info(f"   🧠 Generating embedding...")
            embedding = await self._embed_summary(summary_text)
            self._logger.info(f"   ✅ Embedding generated: {len(embedding)}-dim")

            # Create index entry
            index_entry = {
                'document_id': document_id,
                'source_url': source_url,
                'summary_text': summary_text,
                'summary_embedding': embedding,
                'doc_metadata': doc_metadata,
                'embedding_metadata': {
                    'embedding_id': f"sum_{document_id}",
                    'embedding_dim': len(embedding),
                    'embedding_model': self.model_name,
                    'embedded_at': datetime.now().isoformat()
                },
                'indexed_at': datetime.now().isoformat()
            }

            # Append to index file
            self._append_to_index(session_id, index_entry)
            self._logger.info(f"   💾 Appended to index")

            # Update state
            state['received_count'] += 1
            state['document_ids_processed'].append(document_id)
            self._save_session_state(session_id, state)

            # Track metrics
            if self.telemetry_enabled and embed_start:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                embed_duration = time.time() - embed_start

                self.summary_embedding_duration.labels(
                    consumer_id=consumer_id_label,
                    status='success'
                ).observe(embed_duration)

                self.summaries_indexed.labels(
                    consumer_id=consumer_id_label
                ).inc()

            # Check if session is complete
            if state['received_count'] >= state['total_requests'] and not state.get('completed', False):
                self._logger.info(f"   🎉 Session complete: {state['received_count']}/{state['total_requests']}")
                state['completed'] = True
                self._save_session_state(session_id, state)

                # Publish session-complete message
                self._publish_session_complete(session_id, state['received_count'])

                # Aggregate session index to combined master index
                self._aggregate_to_combined_index(session_id)

            elif state.get('completed', False):
                self._logger.info(f"   ℹ️  Session already marked complete, skipping signal")
            else:
                self._logger.info(f"   ⏳ Session progress: {state['received_count']}/{state['total_requests']}")

            self._logger.info("="*80)

            return {
                'session_id': session_id,
                'document_id': document_id,
                'indexed': True,
                'session_complete': state['completed']
            }

        except Exception as e:
            self._logger.error(f"❌ Error processing message: {e}", exc_info=True)

            # Track failed embedding
            if self.telemetry_enabled and 'embed_start' in locals() and embed_start:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                embed_duration = time.time() - embed_start

                self.summary_embedding_duration.labels(
                    consumer_id=consumer_id_label,
                    status='failure'
                ).observe(embed_duration)

            return None

    async def consume_messages(self):
        """Main consumer loop"""
        try:
            # Create Kafka consumer
            from confluent_kafka import TopicPartition

            self.consumer = Consumer(self.kafka_config)

            # Manual partition assignment
            cluster_metadata = self.consumer.list_topics(self.input_topic, timeout=10)
            topic_metadata = cluster_metadata.topics.get(self.input_topic)

            if topic_metadata:
                partitions = [TopicPartition(self.input_topic, p) for p in topic_metadata.partitions.keys()]
                self.consumer.assign(partitions)
                self._logger.info(f"✅ Manually assigned {len(partitions)} partitions for topic: {self.input_topic}")
            else:
                self._logger.error(f"❌ Topic {self.input_topic} not found")
                raise RuntimeError(f"Topic {self.input_topic} not found")

            self._logger.info("🚀 Starting to consume summary-completed messages...")

            # Start telemetry services if available
            if self.telemetry_enabled:
                try:
                    self.metrics_server.start()
                    self.system_metrics.start()
                    self._logger.info("✅ Telemetry services started")
                except Exception as e:
                    self._logger.warning(f"⚠️ Failed to start telemetry: {e}")

            self.running = True

            # Set consumer state to running
            if self.telemetry_enabled:
                self.metrics.set_state(1)

            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self._logger.error(f"❌ Kafka error: {msg.error()}")
                        continue

                try:
                    # Parse message
                    message_data = json.loads(msg.value().decode('utf-8'))

                    # Track Kafka metrics
                    if self.telemetry_enabled:
                        self.kafka_metrics.increment_messages_in_flight(self.input_topic)

                    # Process message
                    result = await self._process_message(message_data)

                    # Track processing metrics
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

        except Exception as e:
            self._logger.error(f"❌ Consumer error: {e}", exc_info=True)
            raise
        finally:
            # Shutdown telemetry on exit
            if self.telemetry_enabled:
                try:
                    self.system_metrics.stop()
                    self.metrics_server.stop()
                    self._logger.info("✅ Telemetry services stopped")
                except Exception as e:
                    self._logger.warning(f"⚠️ Error stopping telemetry: {e}")

            if self.consumer:
                self.consumer.close()
                self._logger.info("✅ Consumer closed")
            if self.producer:
                self.producer.flush()
                self._logger.info("✅ Producer flushed")

    def run(self):
        """Run the consumer"""
        try:
            asyncio.run(self.consume_messages())
        except KeyboardInterrupt:
            self._logger.info("🛑 Interrupted by user")
        except Exception as e:
            self._logger.error(f"❌ Fatal error: {e}", exc_info=True)
            sys.exit(1)


def main():
    """Main entry point"""
    print("=" * 80)
    print("📚 Kafka Summary Indexer Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaSummaryIndexerConsumer()

        print("✅ Consumer initialized successfully")
        print()

        consumer.run()

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
