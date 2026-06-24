#!/usr/bin/env python3
"""
Kafka Index Aggregator Consumer

Aggregates embedded chunks and Q&A pairs from research sessions into
unified research indices stored in Azure Blob Storage.

Features:
- Consumes from qa-generation and qa-embedding topics
- Accumulates documents by session_id
- Downloads existing research indices from Azure
- Normalizes Q&A pairs to enriched chunks schema
- Deduplicates by document_id and chunk_id
- Uploads updated indices to Azure
- Archives processed files with 7-day retention
- Cleans up local cache directories

Output:
- research_chunks_with_embeddings_combined.jsonl
- research_qa_pairs_with_embeddings_combined.jsonl

TODO: TELEMETRY & OBSERVABILITY
- [ ] Add Prometheus metrics (port 8008)
- [ ] Track chunks_aggregated_total counter
- [ ] Track qa_pairs_aggregated_total counter
- [ ] Track chunks_duplicates_filtered counter
- [ ] Track qa_pairs_duplicates_filtered counter
- [ ] Track azure_upload_duration_seconds histogram
- [ ] Track session_backup_duration_seconds histogram
- [ ] Track azure_storage_bytes gauge (per container)
- [ ] Track messages_consumed_total counter
- [ ] Track message_processing_duration_seconds histogram
- [ ] Add structured JSON logging with trace_id
- [ ] Expose /metrics endpoint on port 8008
- [ ] Add /health and /ready endpoints
"""

import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from pathlib import Path
from typing import Dict, List, Optional, Set

from azure.storage.blob import BlobServiceClient, ContentSettings
from confluent_kafka import Consumer, KafkaError, KafkaException

# Telemetry imports with fallback
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.error_categories import ErrorCategory
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

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from config_handler import Base
from utils.correlation import bind_correlation_ids, reset_correlation_ids
from utils.index_adapter import transform_qa_index_to_chunks


class KafkaIndexAggregatorConsumer(Base):
    """
    Kafka consumer that aggregates embedded documents into research indices.

    Consumes from:
    - qa-generation: Enriched document chunks with embeddings (from chunk embedder)
    - qa-embedding: Q&A pairs with embeddings (from qa embedder, normalized before aggregation)

    Uploads to Azure:
    - research_chunks_with_embeddings_combined.jsonl
    - research_qa_pairs_with_embeddings_combined.jsonl
    """

    def __init__(self):
        """Initialize the index aggregator consumer."""
        super().__init__()
        self._logger.info("Initializing KafkaIndexAggregatorConsumer")

        # Initialize telemetry components if available
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"index_aggregator_{os.getpid()}"
            configure_logging(
                format='text',
                level='INFO',
                consumer_id=consumer_id
            )

            # Initialize metrics collectors
            self.metrics = PrometheusMetrics(consumer_id)
            self.system_metrics = SystemMetricsCollector(consumer_id, cache_mount_path='./cache')
            self.kafka_metrics = KafkaMetricsWrapper(consumer_id)

            # Index aggregator-specific metrics
            from prometheus_client import Counter, Gauge, Histogram

            self.chunks_aggregated = Counter(
                'kafka_consumer_chunks_aggregated_total',
                'Total chunks aggregated into indices',
                ['consumer_id']
            )

            self.qa_pairs_aggregated = Counter(
                'kafka_consumer_qa_pairs_aggregated_total',
                'Total Q&A pairs aggregated into indices',
                ['consumer_id']
            )

            self.chunks_duplicates_filtered = Counter(
                'kafka_consumer_chunks_duplicates_filtered_total',
                'Total duplicate chunks filtered',
                ['consumer_id']
            )

            self.qa_duplicates_filtered = Counter(
                'kafka_consumer_qa_duplicates_filtered_total',
                'Total duplicate Q&A pairs filtered',
                ['consumer_id']
            )

            self.azure_upload_duration = Histogram(
                'kafka_consumer_azure_upload_duration_seconds',
                'Time spent uploading to Azure Blob Storage',
                ['consumer_id', 'blob_type', 'status'],
                buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]
            )

            self.session_backup_duration = Histogram(
                'kafka_consumer_session_backup_duration_seconds',
                'Time spent backing up session data',
                ['consumer_id', 'status'],
                buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
            )

            self.azure_storage_bytes = Gauge(
                'kafka_consumer_azure_storage_bytes',
                'Size of uploaded blobs in bytes',
                ['consumer_id', 'container', 'blob_name']
            )

            self.sessions_aggregated = Counter(
                'kafka_consumer_sessions_aggregated_total',
                'Total research sessions aggregated',
                ['consumer_id']
            )

            self.azure_upload_failures = Counter(
                'kafka_consumer_azure_upload_failures_total',
                'Total Azure Blob upload failures (after all retries)',
                ['consumer_id', 'index_type']
            )

            # Create metrics server with health checks
            self.metrics_server = MetricsServer(
                port=8008,
                consumer_name='index_aggregator',
                health_check=lambda: True,  # Always healthy if running
                ready_check=lambda: self.consumer is not None
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8008")
        else:
            self.metrics = None
            self.system_metrics = None
            self.kafka_metrics = None
            self.metrics_server = None

        # Load configuration from centralized config
        # Note: CustConf nested configs are accessed as attributes or via data dict
        aggregator_config = self._app_config.data['index_aggregator']

        # Kafka configuration
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.group_id = aggregator_config.consumer_group_id
        self.input_topic = aggregator_config.input_topic
        self.session_complete_topic = aggregator_config.session_complete_topic

        # Azure Blob Storage configuration
        self.connection_string = (
            os.getenv('AZURE_STORAGE_CONNECTION_STRING') or
            self._app_config.get('azure_blob.connection_string')
        )
        if not self.connection_string:
            self._logger.warning("⚠️  AZURE_STORAGE_CONNECTION_STRING not set - index aggregator will not upload to Azure")
            self._logger.warning("⚠️  Files will be saved locally only")
            self.upload_to_azure = False
        else:
            self.upload_to_azure = aggregator_config.upload_to_azure

        self.container_name = aggregator_config.azure_container_indices
        self.archive_container = aggregator_config.azure_container_archive
        self.chunks_blob_name = aggregator_config.azure_chunks_blob_name
        self.qa_blob_name = aggregator_config.azure_qa_pairs_blob_name

        # Session accumulation
        self.session_chunks: Dict[str, List[Dict]] = defaultdict(list)
        self.session_qa_pairs: Dict[str, List[Dict]] = defaultdict(list)
        self.session_request_ids: Dict[str, Set[str]] = defaultdict(set)
        self.session_last_activity: Dict[str, float] = {}
        self.session_total_requests: Dict[str, int] = {}  # Expected total requests per session
        self.session_query_id: Dict[str, str] = {}  # Track query_id per session for index naming
        # Correlation pass-through — stored until upload so any future notification can replay them.
        # session_origins keyed on session_id; overwritten on each signal (idempotent).
        self.session_origins: Dict[str, Optional[str]] = {}         # wire origin field
        self.session_browser_session_ids: Dict[str, Optional[str]] = {}  # session_id for origin="frontend" only

        # Pipeline completion tracking - all three must complete before aggregation
        self.session_chunks_complete: Dict[str, bool] = defaultdict(bool)
        self.session_qa_complete: Dict[str, bool] = defaultdict(bool)
        self.session_summaries_complete: Dict[str, bool] = defaultdict(bool)

        # Track sessions that have been fully aggregated to ignore duplicate signals
        self.aggregated_sessions: Set[str] = set()

        # Directory configuration
        self.cache_dir = Path(aggregator_config.cache_dir)
        self.session_backup_dir = Path(aggregator_config.session_backup_dir)
        self.archive_dir = Path(aggregator_config.archive_dir)
        self.indices_dir = Path(aggregator_config.indices_dir)

        # Behavioral configuration
        self.session_timeout = aggregator_config.session_timeout
        self.cleanup_after_upload = aggregator_config.cleanup_after_upload
        self.archive_before_cleanup = aggregator_config.archive_before_cleanup

        # Initialize Azure Blob Service Client (if credentials available)
        if self.upload_to_azure:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            # Ensure containers exist
            self._ensure_containers_exist()
        else:
            self.blob_service_client = None

        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,  # 45 seconds
            'heartbeat.interval.ms': 3000,  # 3 seconds
            'max.poll.interval.ms': 600000,  # 10 minutes
        })

        # Initialize Kafka producer for session-complete signals
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

        self._logger.info("✅ Configuration loaded:")
        self._logger.info(f"   Kafka servers: {self.bootstrap_servers}")
        self._logger.info(f"   Consumer group: {self.group_id}")
        self._logger.info(f"   Input topic: {self.input_topic}")
        self._logger.info(f"   Azure container: {self.container_name}")
        self._logger.info(f"   Archive container: {self.archive_container}")
        self._logger.info(f"   Chunks blob: {self.chunks_blob_name}")
        self._logger.info(f"   Q&A blob: {self.qa_blob_name}")
        self._logger.info(f"   Session timeout: {self.session_timeout}s")
        self._logger.info("✅ KafkaIndexAggregatorConsumer initialized successfully")

    def _ensure_containers_exist(self):
        """Ensure Azure containers exist."""
        try:
            # List of required containers
            required_containers = [
                (self.container_name, "Indices container for aggregated research data"),
                (self.archive_container, "Archive container (7-day lifecycle)"),
                ("webscrapes", "Raw web scrape data"),
                ("session-backups", "Session backup data"),
                ("archive", "Archive parent container")
            ]

            for container_name, description in required_containers:
                try:
                    container_client = self.blob_service_client.get_container_client(container_name)
                    if not container_client.exists():
                        self.blob_service_client.create_container(container_name)
                        self._logger.info(f"✅ Created container: {container_name} ({description})")
                    else:
                        self._logger.debug(f"✓ Container exists: {container_name}")
                except Exception as e:
                    self._logger.debug(f"Container {container_name} check: {e}")

            # Note about grounding subfolder
            self._logger.info("📁 Note: 'archive/grounding' will be created as a virtual folder within 'archive' container")

        except Exception as e:
            self._logger.error(f"Error ensuring containers exist: {e}")

    def _get_blob_names(self, query_id: str = None) -> tuple:
        """
        Generate blob names based on query_id.

        Args:
            query_id: Query identifier (e.g., 'israel_prime_minister')
                     If None or 'default', uses 'research' prefix for backward compatibility

        Returns:
            Tuple of (chunks_blob_name, qa_blob_name)
        """
        # Use 'research' prefix for default/null query_id (backward compatibility)
        prefix = query_id if query_id and query_id != "default" else "research"

        chunks_blob = f"{prefix}_chunks_with_embeddings_combined.jsonl"
        qa_blob = f"{prefix}_qa_pairs_with_embeddings_combined.jsonl"

        return chunks_blob, qa_blob

    def run(self):
        """Start consuming messages."""
        try:
            from confluent_kafka import TopicPartition

            self._logger.info("🚀 Starting index aggregation consumer...")

            # Manual partition assignment to bypass consumer group coordinator
            # Subscribe to BOTH qa-indexed and session-complete topics
            topics_to_subscribe = [self.input_topic, self.session_complete_topic]
            all_partitions = []

            for topic in topics_to_subscribe:
                cluster_metadata = self.consumer.list_topics(topic, timeout=10)
                topic_metadata = cluster_metadata.topics.get(topic)

                if topic_metadata:
                    partitions = [TopicPartition(topic, p) for p in topic_metadata.partitions.keys()]
                    all_partitions.extend(partitions)
                    self._logger.info(f"✅ Found {len(partitions)} partitions for topic: {topic}")
                else:
                    self._logger.error(f"❌ Topic {topic} not found")
                    raise RuntimeError(f"Topic {topic} not found")

            # Assign all partitions (already includes both topics from topics_to_subscribe)
            self.consumer.assign(all_partitions)
            self._logger.info(f"✅ Subscribed to {len(all_partitions)} total partitions across topics: {topics_to_subscribe}")

            # Start telemetry services if available
            if self.telemetry_enabled:
                try:
                    self._logger.info("📊 Starting telemetry services...")
                    self.system_metrics.start()
                    self.metrics_server.start()
                    self.metrics.set_state(1)  # Set state to running
                    # instrumentation - startup_timestamp_seconds
                    self.metrics.record_startup()
                    # instrumentation - consumer_info
                    self.metrics.set_info({
                        'consumer_name': 'index_aggregator',
                        'consumer_group': self.group_id,
                        'input_topic': self.input_topic,
                        'output_topic': self.session_complete_topic,
                    })
                    self._logger.info("✅ Telemetry services started")
                except Exception as e:
                    self._logger.error(f"⚠️ Failed to start telemetry services: {e}")

            while True:
                # Track poll start time
                poll_start = time.time() if self.telemetry_enabled else None

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
                    # Check for timed-out sessions
                    self._check_session_timeouts()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        # Track connection errors
                        if self.telemetry_enabled:
                            self.metrics.increment_connection_errors(str(msg.error().code()))
                        raise KafkaException(msg.error())

                # Track message age
                if self.telemetry_enabled:
                    timestamp_type, timestamp_ms = msg.timestamp()
                    if timestamp_type != -1 and timestamp_ms > 0:
                        message_timestamp = timestamp_ms / 1000.0
                        self.kafka_metrics.record_message_age(self.input_topic, message_timestamp)

                # Calculate consumer lag
                if self.telemetry_enabled:
                    try:
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

                # Increment messages consumed and in flight
                if self.telemetry_enabled:
                    self.metrics.increment_messages_consumed(self.input_topic)
                    # instrumentation - last_message_processed_timestamp
                    self.metrics.record_message_processed(self.input_topic)
                    self.kafka_metrics.increment_messages_in_flight(self.input_topic)

                # Track processing start time
                processing_start = time.time() if self.telemetry_enabled else None

                # Process message
                self._process_message(msg)

                # Record processing duration and decrement messages in flight
                if self.telemetry_enabled:
                    self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                    if processing_start:
                        duration = time.time() - processing_start
                        self.metrics.record_duration(self.input_topic, duration, 'success')

        except KeyboardInterrupt:
            self._logger.info("⚠️  Received interrupt signal")
        except Exception as e:
            self._logger.error(f"❌ Consumer error: {e}")
            if self.telemetry_enabled:
                # instrumentation - processing_failures_total
                self.metrics.increment_failures(self.input_topic, ErrorCategory.KAFKA_CONSUME_ERROR)
            import traceback
            traceback.print_exc()
        finally:
            # Process any remaining sessions before shutdown
            self._logger.info("Processing remaining sessions before shutdown...")
            for session_id in list(self.session_chunks.keys()):
                self._aggregate_and_upload_session(session_id)

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

            self.consumer.close()
            self._logger.info("✅ Consumer stopped")

    def _process_message(self, msg):
        """Process a single Kafka message."""
        try:
            # Decode message
            message_data = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()

            session_id = message_data.get('session_id', 'unknown')
            request_id = message_data.get('request_id', 'unknown')
            _tokens = bind_correlation_ids(session_id, request_id)
            origin = message_data.get('origin')
            index_type = message_data.get('index_type')  # NEW: 'summaries' or None (for chunks/QA)
            signal = message_data.get('signal')  # Session complete signal from test script

            # Update last activity timestamp
            self.session_last_activity[session_id] = time.time()

            # Store correlation fields — keyed on session_id so any aggregation path can read them
            self.session_origins[session_id] = origin
            if origin == 'frontend':
                self.session_browser_session_ids[session_id] = session_id

            # Ignore session-complete signals from test scripts (these don't have data, just signals)
            if signal == 'session_complete':
                self._logger.debug(f"🏁 Session complete signal received: {session_id}")
                return

            # Handle summary index completion (from Summary Indexer)
            if index_type == 'summaries':
                # Ignore if session already aggregated (duplicate signal)
                if session_id in self.aggregated_sessions:
                    self._logger.debug(f"⏭️  Ignoring summary signal for already-aggregated session: {session_id}")
                    return

                summary_index_file = message_data.get('summary_index_file')
                total_summaries = message_data.get('total_summaries', 0)

                self._logger.info(f"📚 Summary index complete for session: {session_id}")
                self._logger.info(f"   Summary index: {summary_index_file}")
                self._logger.info(f"   Total summaries: {total_summaries}")

                # Store summary index file for later aggregation
                if not hasattr(self, 'session_summary_indices'):
                    self.session_summary_indices = defaultdict(str)
                self.session_summary_indices[session_id] = summary_index_file

                # Mark summaries as complete
                self.session_summaries_complete[session_id] = True
                self._logger.info(f"✅ Summaries complete for session: {session_id}")

                # Check if all pipelines are ready
                self._check_and_aggregate_if_ready(session_id)
                return

            # Track total expected requests if provided (for chunks/QA pipeline)
            if 'total_requests' in message_data:
                self.session_total_requests[session_id] = message_data['total_requests']
                self._logger.info(f"📊 Session {session_id}: Expecting {message_data['total_requests']} total requests")

            # Check if this is a session complete signal (from QA Embedder)
            if message_data.get('session_complete'):
                # Ignore if session already aggregated (duplicate signal)
                if session_id in self.aggregated_sessions:
                    self._logger.debug(f"⏭️  Ignoring QA/Chunks signal for already-aggregated session: {session_id}")
                    return

                self._logger.info(f"🏁 QA/Chunks pipeline complete signal received: {session_id}")

                # Mark both chunks and QA as complete (they finish together)
                self.session_chunks_complete[session_id] = True
                self.session_qa_complete[session_id] = True
                self._logger.info(f"✅ Chunks/QA complete for session: {session_id}")

                # Check if summary file exists (may have completed before signal arrived)
                summary_file = Path(self.cache_dir).parent / "summaries" / session_id / f"{session_id}_summaries_embedded.jsonl"
                if summary_file.exists():
                    self._logger.info(f"📚 Found existing summary file, marking summaries complete: {summary_file}")
                    self.session_summaries_complete[session_id] = True
                    # Store summary index file path for aggregation
                    if not hasattr(self, 'session_summary_indices'):
                        self.session_summary_indices = defaultdict(str)
                    self.session_summary_indices[session_id] = str(summary_file)

                # Check if all pipelines are ready (including summaries)
                self._check_and_aggregate_if_ready(session_id)
                return

            # Regular request processing
            self.session_request_ids[session_id].add(request_id)

            # Get both files from the message
            chunks_file = message_data.get('embedded_chunks_file')
            qa_file = message_data.get('enriched_qa_pairs_file')

            self._logger.info(f"📥 Processing request: {session_id}/{request_id}")

            # Load chunks
            if chunks_file and Path(chunks_file).exists():
                self._logger.info(f"  📦 Loading chunks from: {chunks_file}")
                with open(chunks_file, 'r', encoding='utf-8') as f:
                    line_num = 0
                    for line in f:
                        line_num += 1
                        if line.strip():
                            try:
                                chunk = json.loads(line.strip())
                                self.session_chunks[session_id].append(chunk)

                                # Extract query_id from first chunk's document_metadata if not already set
                                if session_id not in self.session_query_id and line_num == 1:
                                    # Try document_metadata first, then top-level field
                                    doc_meta_qid = chunk.get('document_metadata', {}).get('query_id')
                                    top_level_qid = chunk.get('query_id')
                                    chunk_query_id = doc_meta_qid or top_level_qid
                                    self._logger.debug(f"   🔍 Chunk query_id check: doc_metadata={doc_meta_qid}, top_level={top_level_qid}, final={chunk_query_id}")
                                    if chunk_query_id:
                                        self.session_query_id[session_id] = chunk_query_id
                                        self._logger.info(f"📋 Session {session_id}: query_id = {chunk_query_id} (from chunk metadata)")
                                    else:
                                        self._logger.warning(f"⚠️  No query_id found in first chunk for session {session_id}")

                            except json.JSONDecodeError as e:
                                self._logger.error(f"     ❌ JSON decode error in chunks file {chunks_file} line {line_num}: {e}")
                                if self.telemetry_enabled:
                                    self.metrics.increment_deserialization_errors(self.input_topic, 'json_decode_error')
                chunks_count = len(list(open(chunks_file)))
                self._logger.info(f"     ✅ Loaded {chunks_count} chunks")
            else:
                self._logger.warning(f"     ⚠️  Chunks file not found: {chunks_file}")

            # Load Q&A pairs
            if qa_file and Path(qa_file).exists():
                self._logger.info(f"  ❓ Loading Q&A pairs from: {qa_file}")
                with open(qa_file, 'r', encoding='utf-8') as f:
                    line_num = 0
                    for line in f:
                        line_num += 1
                        if line.strip():
                            try:
                                qa_pair = json.loads(line.strip())
                                self.session_qa_pairs[session_id].append(qa_pair)

                                # Extract query_id from first QA pair's document_metadata if not already set
                                if session_id not in self.session_query_id and line_num == 1:
                                    # Try document_metadata first, then top-level field (QA pairs have it at top level)
                                    doc_meta_qid = qa_pair.get('document_metadata', {}).get('query_id')
                                    top_level_qid = qa_pair.get('query_id')
                                    qa_query_id = doc_meta_qid or top_level_qid
                                    self._logger.debug(f"   🔍 QA query_id check: doc_metadata={doc_meta_qid}, top_level={top_level_qid}, final={qa_query_id}")
                                    if qa_query_id:
                                        self.session_query_id[session_id] = qa_query_id
                                        self._logger.info(f"📋 Session {session_id}: query_id = {qa_query_id} (from QA metadata)")
                                    else:
                                        self._logger.warning(f"⚠️  No query_id found in first QA pair for session {session_id}")

                            except json.JSONDecodeError as e:
                                self._logger.error(f"     ❌ JSON decode error in Q&A file {qa_file} line {line_num}: {e}")
                                if self.telemetry_enabled:
                                    self.metrics.increment_deserialization_errors(self.input_topic, 'json_decode_error')
                qa_count = len(list(open(qa_file)))
                self._logger.info(f"     ✅ Loaded {qa_count} Q&A pairs")
            else:
                self._logger.warning(f"     ⚠️  Q&A pairs file not found: {qa_file}")

            # Check if all expected requests have been received
            if session_id in self.session_total_requests:
                expected = self.session_total_requests[session_id]
                received = len(self.session_request_ids[session_id])
                self._logger.info(f"📊 Session {session_id}: {received}/{expected} requests received")
                if received >= expected:
                    self._logger.info(f"✅ All requests received ({received}/{expected}) for session: {session_id}")

                    # Mark chunks and QA as complete (they're processed together)
                    self.session_chunks_complete[session_id] = True
                    self.session_qa_complete[session_id] = True

                    # Check if summary file exists (may have completed before signal arrived)
                    summary_file = Path(self.cache_dir).parent / "summaries" / session_id / f"{session_id}_summaries_embedded.jsonl"
                    if summary_file.exists():
                        self._logger.info(f"📚 Found existing summary file, marking summaries complete: {summary_file}")
                        self.session_summaries_complete[session_id] = True
                        # Store summary index file path for aggregation
                        if not hasattr(self, 'session_summary_indices'):
                            self.session_summary_indices = defaultdict(str)
                        self.session_summary_indices[session_id] = str(summary_file)

                    # Check if all three pipelines are ready before aggregating
                    self._check_and_aggregate_if_ready(session_id)

        except json.JSONDecodeError as e:
            self._logger.error(f"❌ Invalid JSON in message: {e}")
            if self.telemetry_enabled:
                # instrumentation - deserialization_errors_total
                self.metrics.increment_deserialization_errors(self.input_topic, ErrorCategory.JSON_DECODE)
        except Exception as e:
            self._logger.error(f"Error processing message: {e}")
            if self.telemetry_enabled:
                # instrumentation - processing_failures_total
                self.metrics.increment_failures(self.input_topic, ErrorCategory.UNKNOWN)
            import traceback
            traceback.print_exc()
        finally:
            # Reset only if bind succeeded (i.e. JSON decode didn't raise before the bind call)
            if '_tokens' in dir():
                reset_correlation_ids(_tokens)

    def _check_and_aggregate_if_ready(self, session_id: str):
        """Check if all three pipelines are complete, then aggregate and upload.

        All three pipelines must complete before aggregation:
        1. Chunks pipeline (embedded chunks)
        2. QA pipeline (enriched QA pairs with embeddings)
        3. Summaries pipeline (summary index with embeddings)

        Args:
            session_id: The session identifier to check
        """
        chunks_ready = self.session_chunks_complete[session_id]
        qa_ready = self.session_qa_complete[session_id]
        summaries_ready = self.session_summaries_complete[session_id]

        self._logger.info(f"📊 Pipeline status for {session_id}:")
        self._logger.info(f"   Chunks: {'✅' if chunks_ready else '⏳'}")
        self._logger.info(f"   QA pairs: {'✅' if qa_ready else '⏳'}")
        self._logger.info(f"   Summaries: {'✅' if summaries_ready else '⏳'}")

        if chunks_ready and qa_ready and summaries_ready:
            self._logger.info(f"🎉 All pipelines complete! Aggregating session: {session_id}")
            self._aggregate_and_upload_session(session_id)
        else:
            self._logger.info(f"⏳ Waiting for remaining pipelines to complete...")

    def _check_session_timeouts(self):
        """Check for sessions that have timed out and process them."""
        current_time = time.time()

        for session_id, last_activity in list(self.session_last_activity.items()):
            if current_time - last_activity > self.session_timeout:
                self._logger.info(f"⏰ Session timeout: {session_id}")

                # Check if session has any data before attempting aggregation
                total_requests = self.session_total_requests.get(session_id, 0)
                received_count = len(self.session_request_ids.get(session_id, set()))

                if total_requests == 0 and received_count == 0:
                    self._logger.warning(f"⚠️  Skipping timeout aggregation for empty session: {session_id} (0 requests)")
                    # Clean up tracking for this empty session
                    if session_id in self.session_last_activity:
                        del self.session_last_activity[session_id]
                    if session_id in self.session_chunks_complete:
                        del self.session_chunks_complete[session_id]
                    if session_id in self.session_qa_complete:
                        del self.session_qa_complete[session_id]
                    if session_id in self.session_summaries_complete:
                        del self.session_summaries_complete[session_id]
                    continue

                self._aggregate_and_upload_session(session_id)

    def _verify_all_files_ready(self, session_id: str) -> bool:
        """Verify all expected pipeline output files exist in cache before aggregating.

        Expected files for each request:
        - extracted_linked_text.jsonl (or failure documents)
        - chunked_document_enriched_embedded.jsonl
        - req_*_qa_pairs_enriched_embedded.jsonl

        Returns:
            True if all files exist, False otherwise
        """
        cache_dir = Path("./cache/web_scrapes")

        if not cache_dir.exists():
            self._logger.warning(f"⚠️  Cache directory does not exist: {cache_dir}")
            return False

        request_ids = self.session_request_ids.get(session_id, set())
        if not request_ids:
            self._logger.warning(f"⚠️  No request IDs tracked for session: {session_id}")
            return True  # No requests to verify

        missing_files = []

        for request_id in request_ids:
            request_dir = cache_dir / request_id

            if not request_dir.exists():
                missing_files.append(f"{request_id}/[directory not found]")
                continue

            # Check for extraction output (either success, failure, or summary)
            extraction_file = request_dir / "extracted_linked_text.jsonl"
            failure_file = request_dir / "failure.jsonl"
            summary_file = request_dir / "summary.jsonl"

            if not extraction_file.exists() and not failure_file.exists() and not summary_file.exists():
                missing_files.append(f"{request_id}/extracted_linked_text.jsonl or failure.jsonl or summary.jsonl")

            # Check for enriched embedded chunks
            chunks_file = request_dir / "chunked_document_enriched_embedded.jsonl"
            if not chunks_file.exists():
                missing_files.append(f"{request_id}/chunked_document_enriched_embedded.jsonl")

            # Check for Q&A pairs with embeddings
            qa_files = list(request_dir.glob(f"{request_id}_qa_pairs_enriched_embedded.jsonl"))
            if not qa_files:
                missing_files.append(f"{request_id}/{request_id}_qa_pairs_enriched_embedded.jsonl")

        if missing_files:
            self._logger.warning(f"⚠️  Session {session_id} not ready - missing {len(missing_files)} files:")
            for missing in missing_files[:5]:  # Show first 5
                self._logger.warning(f"     - {missing}")
            if len(missing_files) > 5:
                self._logger.warning(f"     ... and {len(missing_files) - 5} more")
            return False

        self._logger.info(f"✅ All {len(request_ids)} requests have complete pipeline outputs")
        return True

    def _save_session_backup(self, session_id: str, chunks: List[Dict], qa_pairs: List[Dict]):
        """Save session aggregated data as backup for verification.

        This creates a backup of the aggregated chunks and Q&A pairs before upload,
        allowing verification that all data is present even if Azure upload fails.

        Args:
            session_id: The session identifier
            chunks: List of chunk documents
            qa_pairs: List of Q&A pair documents
        """
        try:
            backup_dir = self.session_backup_dir / session_id
            backup_dir.mkdir(parents=True, exist_ok=True)

            self._logger.info(f"💾 Saving session backup: {session_id}")

            # Save chunks backup
            if chunks:
                chunks_backup = backup_dir / "chunks_aggregated.jsonl"
                with open(chunks_backup, 'w', encoding='utf-8') as f:
                    for chunk in chunks:
                        f.write(json.dumps(chunk, ensure_ascii=False) + '\n')
                self._logger.info(f"   ✅ Saved {len(chunks)} chunks to {chunks_backup}")

            # Save Q&A pairs backup
            if qa_pairs:
                qa_backup = backup_dir / "qa_pairs_aggregated.jsonl"
                with open(qa_backup, 'w', encoding='utf-8') as f:
                    for qa in qa_pairs:
                        f.write(json.dumps(qa, ensure_ascii=False) + '\n')
                self._logger.info(f"   ✅ Saved {len(qa_pairs)} Q&A pairs to {qa_backup}")

            # Save session metadata
            metadata = {
                "session_id": session_id,
                "timestamp": datetime.now().isoformat(),
                "total_chunks": len(chunks),
                "total_qa_pairs": len(qa_pairs),
                "request_ids": list(self.session_request_ids.get(session_id, set())),
                "azure_upload_enabled": self.upload_to_azure
            }
            metadata_file = backup_dir / "session_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            self._logger.info(f"   ✅ Saved session metadata to {metadata_file}")

        except Exception as e:
            self._logger.error(f"❌ Failed to save session backup: {e}")
            import traceback
            traceback.print_exc()

    def _aggregate_and_upload_session(self, session_id: str):
        """Aggregate session documents and upload to Azure."""
        try:
            # Track session aggregation start time
            session_start = time.time() if self.telemetry_enabled else None

            self._logger.info("=" * 80)
            self._logger.info(f"📊 Aggregating session: {session_id}")
            self._logger.info(f"   Requests processed: {len(self.session_request_ids[session_id])}")
            self._logger.info(f"   Total chunks: {len(self.session_chunks[session_id])}")
            self._logger.info(f"   Total Q&A pairs: {len(self.session_qa_pairs[session_id])}")

            # Check for summary index
            summary_count = 0
            if hasattr(self, 'session_summary_indices') and session_id in self.session_summary_indices:
                summary_index_file = self.session_summary_indices[session_id]
                if Path(summary_index_file).exists():
                    with open(summary_index_file, 'r', encoding='utf-8') as f:
                        summary_count = sum(1 for _ in f)
                    self._logger.info(f"   Total summaries: {summary_count}")

            # Always save local backup copies for verification
            if self.session_chunks[session_id] or self.session_qa_pairs[session_id]:
                self._save_session_backup(session_id, self.session_chunks[session_id], self.session_qa_pairs[session_id])

            # Process summary index (if available)
            if hasattr(self, 'session_summary_indices') and session_id in self.session_summary_indices:
                summary_index_file = self.session_summary_indices[session_id]
                if Path(summary_index_file).exists():
                    self._logger.info(f"📚 Processing summary index...")
                    self._upload_summary_index(session_id, summary_index_file)

            # Process chunks
            if self.session_chunks[session_id]:
                self._upload_chunks(session_id, self.session_chunks[session_id])

            # Process Q&A pairs
            if self.session_qa_pairs[session_id]:
                self._upload_qa_pairs(session_id, self.session_qa_pairs[session_id])

            # Archive and cleanup
            if self.archive_before_cleanup:
                self._archive_session(session_id)

            if self.cleanup_after_upload:
                self._cleanup_session(session_id)

            # Clear session data
            del self.session_chunks[session_id]
            del self.session_qa_pairs[session_id]
            del self.session_request_ids[session_id]
            del self.session_last_activity[session_id]

            # Clear from total_requests tracking if present
            if session_id in self.session_total_requests:
                del self.session_total_requests[session_id]

            # Clear pipeline completion tracking
            if session_id in self.session_chunks_complete:
                del self.session_chunks_complete[session_id]
            if session_id in self.session_qa_complete:
                del self.session_qa_complete[session_id]
            if session_id in self.session_summaries_complete:
                del self.session_summaries_complete[session_id]

            # Mark session as aggregated to ignore future duplicate signals
            self.aggregated_sessions.add(session_id)

            # Publish session-complete message so other consumers can clean up
            try:
                import json
                complete_message = {
                    'session_id': session_id,
                    'session_complete': True,
                    'completed_at': datetime.now().isoformat()
                }
                _pub_start = time.time()
                self.producer.produce(
                    topic=self.session_complete_topic,
                    value=json.dumps(complete_message).encode('utf-8')
                )
                self.producer.flush()
                _pub_duration = time.time() - _pub_start

                # instrumentation - messages_published_total
                if self.telemetry_enabled:
                    self.kafka_metrics.record_publish(topic=self.session_complete_topic, result='success', duration=_pub_duration)

                self._logger.info(f"📢 Published session-complete signal to {self.session_complete_topic}")
            except Exception as e:
                # instrumentation - messages_published_total
                if self.telemetry_enabled:
                    self.kafka_metrics.record_publish(topic=self.session_complete_topic, result='error', duration=0)
                self._logger.warning(f"⚠️  Failed to publish session-complete: {e}")

            self._logger.info(f"✅ Session aggregation complete: {session_id}")
            self._logger.info("=" * 80)

            # Track session aggregation metrics
            if self.telemetry_enabled and session_start:
                consumer_id_label = f"index_aggregator_{os.getpid()}"
                self.sessions_aggregated.labels(consumer_id=consumer_id_label).inc()

        except Exception as e:
            self._logger.error(f"Error aggregating session {session_id}: {e}")
            import traceback
            traceback.print_exc()

    def _save_pre_append_index(self, session_id: str, index_data: List[Dict], index_type: str):
        """
        Save a snapshot of the index before appending new data.

        This allows tracking what was added through diffs without additional Azure egress costs.
        Saves to archive/{session_id}/indices_before_append/{index_type}.jsonl

        Args:
            session_id: Session identifier
            index_data: Current index data (before append)
            index_type: 'chunks' or 'qa_pairs'
        """
        try:
            # Create local archive directory
            archive_dir = Path(self.cache_dir).parent / "archive" / session_id / "indices_before_append"
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Save locally first
            local_file = archive_dir / f"{index_type}.jsonl"
            with open(local_file, 'w', encoding='utf-8') as f:
                for item in index_data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')

            self._logger.info(f"   💾 Saved pre-append {index_type} index: {len(index_data)} items")

            # Upload to Azure archive if configured
            if self.upload_to_azure and self.blob_service_client:
                blob_name = f"{session_id}/indices_before_append/{index_type}.jsonl"
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        blob_client = self.blob_service_client.get_blob_client(
                            container="archive",
                            blob=blob_name
                        )
                        with open(local_file, 'rb') as f:
                            blob_client.upload_blob(f, overwrite=True, timeout=300)
                        self._logger.info(f"   ☁️  Uploaded pre-append {index_type} to archive/{blob_name}")
                        break
                    except Exception as upload_error:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            self._logger.warning(f"   ⚠️  Upload failed (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s")
                            time.sleep(wait_time)
                        else:
                            self._logger.error(f"   ❌ Failed to upload pre-append {index_type} after {max_retries} attempts: {upload_error}")

        except Exception as e:
            self._logger.error(f"Error saving pre-append index for {index_type}: {e}")
            import traceback
            traceback.print_exc()

    def _cleanup_local_indices_cache(self):
        """Clean up local indices cache to prevent stale data confusion.

        After uploading updated indices to Azure, remove local cached copies
        since the source of truth is Azure Blob Storage. This prevents confusion
        about which version is current.
        """
        try:
            indices_dir = Path(self.cache_dir).parent / "indices"
            if indices_dir.exists():
                for index_file in indices_dir.glob("*.jsonl"):
                    try:
                        index_file.unlink()
                        self._logger.debug(f"   🗑️  Removed stale local index: {index_file.name}")
                    except Exception as e:
                        self._logger.warning(f"   ⚠️  Failed to remove {index_file.name}: {e}")
                self._logger.info(f"   🧹 Cleaned up local indices cache (source of truth: Azure)")
        except Exception as e:
            self._logger.warning(f"⚠️  Error cleaning up local indices cache: {e}")

    def _upload_chunks(self, session_id: str, new_chunks: List[Dict]):
        """Upload chunks to Azure research index using append-only pattern."""
        if not self.upload_to_azure:
            self._logger.warning("⚠️  No Azure credentials - saving locally instead")
            self._save_chunks_locally(new_chunks)
            return

        try:
            self._logger.info(f"📤 Appending chunks to Azure index...")

            # Check for duplicates against local cache (no download needed)
            existing_ids = self._get_existing_chunk_ids_from_cache(session_id)

            # Filter out duplicates from new chunks
            unique_chunks = []
            new_count = 0
            duplicate_count = 0

            for chunk in new_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        unique_chunks.append(chunk)
                        new_count += 1
                    else:
                        duplicate_count += 1

            self._logger.info(f"   ➕ New chunks: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {duplicate_count}")

            # Track chunks aggregation metrics
            if self.telemetry_enabled:
                consumer_id_label = f"index_aggregator_{os.getpid()}"
                self.chunks_aggregated.labels(consumer_id=consumer_id_label).inc(new_count)
                self.chunks_duplicates_filtered.labels(consumer_id=consumer_id_label).inc(duplicate_count)

            # Append to Azure using append-only operation (no download/upload cycle)
            if unique_chunks:
                upload_start = time.time() if self.telemetry_enabled else None
                self._append_to_blob(self.chunks_blob_name, unique_chunks)

                # Update local cache with new IDs
                self._update_chunk_id_cache(session_id, existing_ids)

                # Track Azure append metrics
                if self.telemetry_enabled and upload_start:
                    upload_duration = time.time() - upload_start
                    self.azure_upload_duration.labels(
                        consumer_id=consumer_id_label,
                        blob_type='chunks',
                        status='success'
                    ).observe(upload_duration)

                    # Track appended data size (not full blob)
                    appended_size = len(json.dumps(unique_chunks, ensure_ascii=False).encode('utf-8'))
                    self.azure_storage_bytes.labels(
                        consumer_id=consumer_id_label,
                        container=self.container_name,
                        blob_name=self.chunks_blob_name
                    ).inc(appended_size)

                self._logger.info(f"   ✅ Chunks appended successfully (no full download/upload)")
            else:
                self._logger.info(f"   ℹ️  No new chunks to append (all duplicates)")

        except Exception as e:
            self._logger.error(f"❌ Error uploading chunks to Azure: {e}")
            self._logger.warning(f"⚠️  Falling back to local storage...")
            import traceback
            traceback.print_exc()

            # Track failed upload metrics
            if self.telemetry_enabled:
                consumer_id_label = f"index_aggregator_{os.getpid()}"
                self.azure_upload_duration.labels(
                    consumer_id=consumer_id_label,
                    blob_type='chunks',
                    status='failure'
                ).observe(0)
                if hasattr(self, 'azure_upload_failures'):
                    self.azure_upload_failures.labels(
                        consumer_id=consumer_id_label, index_type='chunks'
                    ).inc()

            # Fallback to local storage
            try:
                self._save_chunks_locally(new_chunks)
            except Exception as fallback_error:
                self._logger.error(f"❌ CRITICAL: Local storage fallback also failed: {fallback_error}")
                traceback.print_exc()

    def _upload_qa_pairs(self, session_id: str, new_qa_pairs: List[Dict]):
        """Normalize and upload Q&A pairs to Azure research index using append-only pattern."""
        if not self.upload_to_azure:
            self._logger.warning("⚠️  No Azure credentials - saving locally instead")
            self._save_qa_pairs_locally(new_qa_pairs)
            return

        try:
            self._logger.info(f"📤 Appending Q&A pairs to Azure index...")

            # Normalize Q&A pairs using index adapter
            self._logger.info(f"   🔄 Normalizing Q&A pairs to enriched chunks schema...")
            normalized_qa = []

            for qa_pair in new_qa_pairs:
                # Transform flattened Q&A to enriched chunks format
                # The Q&A pairs already have the flattened schema, we need to normalize
                normalized = self._normalize_qa_pair(qa_pair)
                if normalized:
                    normalized_qa.append(normalized)

            self._logger.info(f"   ✅ Normalized {len(normalized_qa)} Q&A pairs")

            # Check for duplicates against local cache (no download needed)
            existing_ids = self._get_existing_qa_ids_from_cache(session_id)

            # Filter out duplicates from new Q&A pairs
            unique_qa = []
            new_count = 0
            duplicate_count = 0

            for qa in normalized_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id:
                    if chunk_id not in existing_ids:
                        existing_ids.add(chunk_id)
                        unique_qa.append(qa)
                        new_count += 1
                    else:
                        duplicate_count += 1

            self._logger.info(f"   ➕ New Q&A pairs: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {duplicate_count}")

            # Track QA pairs aggregation metrics
            if self.telemetry_enabled:
                consumer_id_label = f"index_aggregator_{os.getpid()}"
                self.qa_pairs_aggregated.labels(consumer_id=consumer_id_label).inc(new_count)
                self.qa_duplicates_filtered.labels(consumer_id=consumer_id_label).inc(duplicate_count)

            # Append to Azure using append-only operation (no download/upload cycle)
            if unique_qa:
                upload_start = time.time() if self.telemetry_enabled else None
                self._append_to_blob(self.qa_blob_name, unique_qa)

                # Update local cache with new IDs
                self._update_qa_id_cache(session_id, existing_ids)

                # Track Azure append metrics
                if self.telemetry_enabled and upload_start:
                    upload_duration = time.time() - upload_start
                    self.azure_upload_duration.labels(
                        consumer_id=consumer_id_label,
                        blob_type='qa_pairs',
                        status='success'
                    ).observe(upload_duration)

                    # Track appended data size (not full blob)
                    appended_size = len(json.dumps(unique_qa, ensure_ascii=False).encode('utf-8'))
                    self.azure_storage_bytes.labels(
                        consumer_id=consumer_id_label,
                        container=self.container_name,
                        blob_name=self.qa_blob_name
                    ).inc(appended_size)

                self._logger.info(f"   ✅ Q&A pairs appended successfully (no full download/upload)")
            else:
                self._logger.info(f"   ℹ️  No new Q&A pairs to append (all duplicates)")

        except Exception as e:
            self._logger.error(f"❌ Error appending Q&A pairs to Azure: {e}")
            self._logger.warning(f"⚠️  Falling back to local storage...")
            import traceback
            traceback.print_exc()

            # Track failed upload metrics
            if self.telemetry_enabled:
                consumer_id_label = f"index_aggregator_{os.getpid()}"
                self.azure_upload_duration.labels(
                    consumer_id=consumer_id_label,
                    blob_type='qa_pairs',
                    status='failure'
                ).observe(0)

            # Fallback to local storage
            try:
                self._save_qa_pairs_locally(new_qa_pairs)
            except Exception as fallback_error:
                self._logger.error(f"❌ CRITICAL: Local storage fallback also failed: {fallback_error}")
                traceback.print_exc()

    def _upload_summary_index(self, session_id: str, summary_index_file: str):
        """Upload summary index to Azure as a separate blob."""
        if not self.upload_to_azure:
            self._logger.warning("⚠️  No Azure credentials - summary index saved locally only")
            return

        try:
            self._logger.info(f"📤 Uploading summary index to Azure...")

            # Load summaries from file
            summaries = []
            with open(summary_index_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        summaries.append(json.loads(line.strip()))

            self._logger.info(f"   📚 Loaded {len(summaries)} summaries from index")

            # Check for duplicates against existing summary index
            existing_doc_ids = self._get_existing_summary_ids()

            # Filter duplicates
            unique_summaries = []
            new_count = 0
            duplicate_count = 0

            for summary in summaries:
                doc_id = summary.get('document_id')
                if doc_id and doc_id not in existing_doc_ids:
                    existing_doc_ids.add(doc_id)
                    unique_summaries.append(summary)
                    new_count += 1
                else:
                    duplicate_count += 1

            self._logger.info(f"   ➕ New summaries: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {duplicate_count}")

            # Append to Azure blob
            if unique_summaries:
                summary_blob_name = "research_summaries_with_embeddings_combined.jsonl"
                upload_start = time.time() if self.telemetry_enabled else None

                self._append_to_blob(summary_blob_name, unique_summaries)

                # Track metrics
                if self.telemetry_enabled and upload_start:
                    consumer_id_label = f"index_aggregator_{os.getpid()}"
                    upload_duration = time.time() - upload_start

                    self.azure_upload_duration.labels(
                        consumer_id=consumer_id_label,
                        blob_type='summaries',
                        status='success'
                    ).observe(upload_duration)

                    appended_size = len(json.dumps(unique_summaries, ensure_ascii=False).encode('utf-8'))
                    self.azure_storage_bytes.labels(
                        consumer_id=consumer_id_label,
                        container=self.container_name,
                        blob_name=summary_blob_name
                    ).inc(appended_size)

                self._logger.info(f"   ✅ Summary index appended successfully")
            else:
                self._logger.info(f"   ℹ️  No new summaries to append (all duplicates)")

            # Clean up summary index from session tracking
            if hasattr(self, 'session_summary_indices') and session_id in self.session_summary_indices:
                del self.session_summary_indices[session_id]

        except Exception as e:
            self._logger.error(f"❌ Error uploading summary index: {e}")
            import traceback
            traceback.print_exc()

            # Track failed upload
            if self.telemetry_enabled:
                consumer_id_label = f"index_aggregator_{os.getpid()}"
                self.azure_upload_duration.labels(
                    consumer_id=consumer_id_label,
                    blob_type='summaries',
                    status='failure'
                ).observe(0)

    def _get_existing_summary_ids(self) -> set:
        """Get set of existing document IDs from summary index (memory-efficient check)."""
        existing_ids = set()

        if not self.upload_to_azure:
            return existing_ids

        try:
            summary_blob_name = "research_summaries_with_embeddings_combined.jsonl"
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=summary_blob_name
            )

            # Stream and parse only document_id field
            if blob_client.exists():
                stream = blob_client.download_blob()
                for line in stream.readall().decode('utf-8').split('\n'):
                    if line.strip():
                        try:
                            entry = json.loads(line.strip())
                            doc_id = entry.get('document_id')
                            if doc_id:
                                existing_ids.add(doc_id)
                        except json.JSONDecodeError:
                            continue

                self._logger.info(f"   📋 Found {len(existing_ids)} existing summaries in Azure")

        except Exception as e:
            self._logger.warning(f"   ⚠️  Could not check existing summaries: {e}")

        return existing_ids

    def _normalize_qa_pair(self, qa_pair: Dict) -> Dict:
        """
        Normalize a single Q&A pair from flattened to enriched chunks schema.

        The Q&A pairs come in flattened format with fields like:
        - question, answer, context, chunk_id, document_id, title, author, etc.

        Need to transform to nested format with:
        - content, chunk_metadata, document_metadata, embedding_enrichment
        """
        try:
            # Build content from Q&A
            question = qa_pair.get('question', '')
            answer = qa_pair.get('answer', '')
            context = qa_pair.get('context', '')

            enhanced_content = (
                f"QUESTION: {question}\n\n"
                f"ANSWER: {answer}\n\n"
                f"SOURCE CONTEXT:\n{context}"
            )

            # Calculate metadata
            word_count = len(enhanced_content.split())
            char_count = len(enhanced_content)
            line_count = enhanced_content.count('\n') + 1

            # Get section hierarchy for enriched text
            section_hierarchy = qa_pair.get('section_hierarchy', [])
            hierarchy_text = " > ".join(section_hierarchy) if section_hierarchy else ""
            enriched_text = f"{hierarchy_text}: {enhanced_content}" if hierarchy_text else enhanced_content

            # Generate chunk_id
            chunk_id = qa_pair.get('chunk_id', 'unknown')
            qa_index = qa_pair.get('qa_index', 1)
            normalized_chunk_id = f"qa_{chunk_id}_{qa_index}"

            # Build normalized structure
            normalized = {
                "content": enhanced_content,
                "chunk_metadata": {
                    "chunk_id": normalized_chunk_id,
                    "chunk_index": qa_pair.get('qa_index'),
                    "chunk_type": qa_pair.get('chunk_type', 'paragraph'),
                    "word_count": word_count,
                    "char_count": char_count,
                    "line_count": line_count,
                    "section_hierarchy": section_hierarchy,
                    "parent_section": qa_pair.get('parent_section'),
                    "is_first_in_section": False,
                    "is_last_in_section": False,
                    "contains_urls": False,
                    "contains_references": False,
                },
                "document_metadata": {
                    "document_id": qa_pair.get('document_id'),
                    "source_url": qa_pair.get('source_url'),
                    "extraction_method": qa_pair.get('extraction_method', 'unknown'),
                    "language": qa_pair.get('language', 'en'),
                    "title": qa_pair.get('title'),
                    "author": qa_pair.get('author'),
                    "creation_date": qa_pair.get('creation_date'),
                    "content_type": qa_pair.get('content_type'),
                    "query_id": qa_pair.get('query_id'),
                },
                "embedding_enrichment": {
                    "embedding_id": f"emb_{normalized_chunk_id}",
                    "enriched_text": enriched_text,
                    "enriched_text_preview": enriched_text[:200] if len(enriched_text) > 200 else enriched_text,
                    "hierarchy_separator": " > ",
                    "concatenation_method": "hierarchy_first",
                    "is_sub_chunked": False,
                    "num_sub_chunks": 1,
                    "embedding_vector": qa_pair.get('combined_embedding', []),
                    "embedding_dim": qa_pair.get('embedding_dimension', 384),
                    "aggregation_method": "none",
                    "embedding_timestamp": qa_pair.get('enrichment_timestamp'),
                },
                "qa_metadata": {
                    "question": question,
                    "answer": answer,
                    "word_count_question": qa_pair.get('word_count_question'),
                    "word_count_answer": qa_pair.get('word_count_answer'),
                    "qa_index": qa_index,
                    "qa_total_for_source": qa_pair.get('qa_total_for_source'),
                    "original_chunk_content": context,
                },
                "processing_timestamp": qa_pair.get('enrichment_timestamp'),
            }

            return normalized

        except Exception as e:
            self._logger.error(f"Error normalizing Q&A pair: {e}")
            return None

    def _save_chunks_locally(self, new_chunks: List[Dict]):
        """Save chunks to local file when Azure is unavailable."""
        try:
            local_dir = self.indices_dir
            local_dir.mkdir(parents=True, exist_ok=True)
            local_file = local_dir / self.chunks_blob_name

            self._logger.info(f"💾 Saving chunks locally to {local_file}...")

            # Load existing chunks if file exists
            existing_chunks = []
            if local_file.exists():
                with open(local_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            existing_chunks.append(json.loads(line.strip()))

            # Deduplicate by document_id and chunk_id
            existing_ids = set()
            all_chunks = []

            for chunk in existing_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        all_chunks.append(chunk)

            # Add new chunks (skip duplicates)
            new_count = 0
            for chunk in new_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        all_chunks.append(chunk)
                        new_count += 1

            self._logger.info(f"   📊 Existing chunks: {len(existing_chunks)}")
            self._logger.info(f"   ➕ New chunks: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {len(new_chunks) - new_count}")
            self._logger.info(f"   📈 Total chunks: {len(all_chunks)}")

            # Write to file
            with open(local_file, 'w', encoding='utf-8') as f:
                for chunk in all_chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + '\n')

            self._logger.info(f"   ✅ Saved {len(all_chunks)} chunks to {local_file}")

        except Exception as e:
            self._logger.error(f"Error saving chunks locally: {e}")
            import traceback
            traceback.print_exc()

    def _save_qa_pairs_locally(self, new_qa_pairs: List[Dict]):
        """Save Q&A pairs to local file when Azure is unavailable."""
        try:
            local_dir = self.indices_dir
            local_dir.mkdir(parents=True, exist_ok=True)
            local_file = local_dir / self.qa_blob_name

            self._logger.info(f"💾 Normalizing and saving Q&A pairs locally to {local_file}...")

            # Normalize Q&A pairs
            normalized_qa = []
            for qa in new_qa_pairs:
                normalized = self._normalize_qa_pair(qa)
                if normalized:
                    normalized_qa.append(normalized)

            self._logger.info(f"   ✅ Normalized {len(normalized_qa)} Q&A pairs")

            # Load existing Q&A pairs if file exists
            existing_qa = []
            if local_file.exists():
                with open(local_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            existing_qa.append(json.loads(line.strip()))

            # Deduplicate by chunk_id
            existing_ids = set()
            all_qa = []

            for qa in existing_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id and chunk_id not in existing_ids:
                    existing_ids.add(chunk_id)
                    all_qa.append(qa)

            # Add new Q&A pairs (skip duplicates)
            new_count = 0
            for qa in normalized_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id and chunk_id not in existing_ids:
                    existing_ids.add(chunk_id)
                    all_qa.append(qa)
                    new_count += 1

            self._logger.info(f"   📊 Existing Q&A pairs: {len(existing_qa)}")
            self._logger.info(f"   ➕ New Q&A pairs: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {len(normalized_qa) - new_count}")
            self._logger.info(f"   📈 Total Q&A pairs: {len(all_qa)}")

            # Write to file
            with open(local_file, 'w', encoding='utf-8') as f:
                for qa in all_qa:
                    f.write(json.dumps(qa, ensure_ascii=False) + '\n')

            self._logger.info(f"   ✅ Saved {len(all_qa)} Q&A pairs to {local_file}")

        except Exception as e:
            self._logger.error(f"Error saving Q&A pairs locally: {e}")
            import traceback
            traceback.print_exc()

    def _download_blob(self, blob_name: str) -> List[Dict]:
        """Download and parse JSONL blob from Azure."""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            if not blob_client.exists():
                self._logger.info(f"   📥 Blob not found (creating new): {blob_name}")
                return []

            self._logger.info(f"   📥 Downloading existing blob: {blob_name}")
            blob_data = blob_client.download_blob().readall()

            # Parse JSONL
            documents = []
            for line in blob_data.decode('utf-8').splitlines():
                if line.strip():
                    documents.append(json.loads(line.strip()))

            self._logger.info(f"   ✅ Downloaded {len(documents)} documents")
            return documents

        except Exception as e:
            self._logger.warning(f"   ⚠️  Error downloading blob {blob_name}: {e}")
            return []

    def _append_to_blob(self, blob_name: str, documents: List[Dict]):
        """Append JSONL documents to Azure Append Blob (no download required)."""
        try:
            # Convert to JSONL
            jsonl_content = '\n'.join(json.dumps(doc, ensure_ascii=False) for doc in documents) + '\n'

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            # Check if blob exists, if not create as append blob
            try:
                properties = blob_client.get_blob_properties()
                # Blob exists, append to it
                blob_client.append_block(jsonl_content.encode('utf-8'))
                self._logger.info(f"   📤 Appended {len(documents)} documents to {blob_name}")
            except Exception:
                # Blob doesn't exist, create it as append blob
                self._logger.info(f"   📤 Creating new append blob: {blob_name}")
                blob_client.create_append_blob(
                    content_settings=ContentSettings(content_type='application/jsonl')
                )
                blob_client.append_block(jsonl_content.encode('utf-8'))
                self._logger.info(f"   📤 Appended {len(documents)} documents to new blob {blob_name}")

        except Exception as e:
            self._logger.error(f"Error appending to blob {blob_name}: {e}")
            raise

    def _upload_blob(self, blob_name: str, documents: List[Dict]):
        """Upload JSONL documents to Azure blob (legacy method for full replacement)."""
        try:
            # Convert to JSONL
            jsonl_content = '\n'.join(json.dumps(doc, ensure_ascii=False) for doc in documents)

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            # Upload with overwrite
            blob_client.upload_blob(
                jsonl_content.encode('utf-8'),
                overwrite=True,
                content_settings=ContentSettings(content_type='application/jsonl')
            )

            self._logger.info(f"   📤 Uploaded {len(documents)} documents to {blob_name}")

        except Exception as e:
            self._logger.error(f"Error uploading blob {blob_name}: {e}")
            raise

    def _get_existing_chunk_ids_from_cache(self, session_id: str) -> Set[str]:
        """Get existing chunk IDs from local cache to avoid downloading full index."""
        try:
            cache_file = Path(self.cache_dir) / "chunk_ids_cache.txt"
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return set(line.strip() for line in f if line.strip())
            return set()
        except Exception as e:
            self._logger.warning(f"⚠️  Error loading chunk ID cache: {e}")
            return set()

    def _update_chunk_id_cache(self, session_id: str, chunk_ids: Set[str]):
        """Update local cache with new chunk IDs."""
        try:
            cache_file = Path(self.cache_dir) / "chunk_ids_cache.txt"
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w') as f:
                for chunk_id in sorted(chunk_ids):
                    f.write(f"{chunk_id}\n")
        except Exception as e:
            self._logger.warning(f"⚠️  Error updating chunk ID cache: {e}")

    def _get_existing_qa_ids_from_cache(self, session_id: str) -> Set[str]:
        """Get existing Q&A IDs from local cache to avoid downloading full index."""
        try:
            cache_file = Path(self.cache_dir) / "qa_ids_cache.txt"
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return set(line.strip() for line in f if line.strip())
            return set()
        except Exception as e:
            self._logger.warning(f"⚠️  Error loading Q&A ID cache: {e}")
            return set()

    def _update_qa_id_cache(self, session_id: str, qa_ids: Set[str]):
        """Update local cache with new Q&A IDs."""
        try:
            cache_file = Path(self.cache_dir) / "qa_ids_cache.txt"
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w') as f:
                for qa_id in sorted(qa_ids):
                    f.write(f"{qa_id}\n")
        except Exception as e:
            self._logger.warning(f"⚠️  Error updating Q&A ID cache: {e}")

    def _archive_session(self, session_id: str):
        """Archive session files to Azure or local archive directory."""
        try:
            self._logger.info(f"📦 Archiving session files: {session_id}")

            if not self.upload_to_azure:
                # Local archive - copy to archive directory
                archive_dir = self.archive_dir / session_id
                archive_dir.mkdir(parents=True, exist_ok=True)

                archive_count = 0
                for request_id in self.session_request_ids[session_id]:
                    request_dir = self.cache_dir / request_id
                    if not request_dir.exists():
                        continue

                    # Copy all JSONL files to archive
                    for file_path in request_dir.glob('*.jsonl'):
                        import shutil
                        archive_file = archive_dir / request_id / file_path.name
                        archive_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(file_path, archive_file)
                        archive_count += 1

                self._logger.info(f"   ✅ Archived {archive_count} files locally to {archive_dir}")
                return

            # Azure upload - mirror entire local cache structure to Azure
            self._logger.info(f"☁️  Uploading local cache to Azure containers...")

            # 1. Upload session backups to session-backups container
            session_backup_dir = Path(f"cache/session_backups/{session_id}")
            backup_count = 0
            if session_backup_dir.exists():
                self._logger.info(f"   📦 Uploading session backups from {session_backup_dir}...")
                for file_path in session_backup_dir.rglob('*'):
                    if file_path.is_file():
                        # Preserve directory structure under session_id
                        relative_path = file_path.relative_to(session_backup_dir)
                        blob_name = f"{session_id}/{relative_path}"

                        # Retry logic for large files
                        max_retries = 2
                        for attempt in range(max_retries):
                            try:
                                blob_client = self.blob_service_client.get_blob_client(
                                    container="session-backups",
                                    blob=blob_name
                                )
                                with open(file_path, 'rb') as f:
                                    # Set timeout to 10 minutes for slow regional connections
                                    blob_client.upload_blob(f, overwrite=True, timeout=600)
                                backup_count += 1
                                break
                            except Exception as upload_error:
                                if attempt < max_retries - 1:
                                    wait_time = 2 ** attempt
                                    self._logger.warning(f"   ⚠️  Upload failed (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s: {file_path.name}")
                                    time.sleep(wait_time)
                                else:
                                    self._logger.error(f"   ❌ Failed to upload {file_path.name} after {max_retries} attempts: {upload_error}")

                self._logger.info(f"   ✅ Uploaded {backup_count} files to session-backups container")
            else:
                self._logger.warning(f"   ⚠️  Session backup directory not found: {session_backup_dir}")

            # 2. Archive to archive container (mirrors local cache/web_scrapes structure)
            self._logger.info(f"   📦 Archiving to {session_id}/...")
            archive_count = 0
            for request_id in self.session_request_ids[session_id]:
                request_dir = self.cache_dir / request_id
                if not request_dir.exists():
                    continue

                # Upload all relevant files: .jsonl only (includes summaries, stats, chunks, etc.)
                for file_path in request_dir.glob('*.jsonl'):
                    # Mirror local structure: cache/web_scrapes/{request_id}/{filename}
                    # becomes: {session_id}/{request_id}/{filename}
                    blob_name = f"{session_id}/{request_id}/{file_path.name}"

                    # Retry logic
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            blob_client = self.blob_service_client.get_blob_client(
                                container="archive",
                                blob=blob_name
                            )
                            with open(file_path, 'rb') as f:
                                blob_client.upload_blob(f, overwrite=True, timeout=300)
                            archive_count += 1
                            break
                        except Exception as upload_error:
                            if attempt < max_retries - 1:
                                wait_time = 2 ** attempt
                                self._logger.warning(f"   ⚠️  Upload failed (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s: {file_path.name}")
                                time.sleep(wait_time)
                            else:
                                self._logger.error(f"   ❌ Failed to upload {file_path.name} after {max_retries} attempts: {upload_error}")

            self._logger.info(f"   ✅ Archived {archive_count} files to archive/{session_id}")
            self._logger.info(f"☁️  Upload complete - {backup_count} backups, {archive_count} archived files")

        except Exception as e:
            self._logger.error(f"Error archiving session {session_id}: {e}", exc_info=True)

    def _cleanup_session(self, session_id: str):
        """Clean up local cache directories for session."""
        try:
            self._logger.info(f"🧹 Cleaning up local cache: {session_id}")

            cleanup_count = 0
            for request_id in self.session_request_ids[session_id]:
                request_dir = self.cache_dir / request_id
                if request_dir.exists():
                    self._logger.debug(f"   🗑️  Removing {request_dir}")
                    shutil.rmtree(request_dir)
                    cleanup_count += 1

            self._logger.info(f"   ✅ Cleaned up {cleanup_count} request directories")

            # Clean up session backup directory
            session_backup_dir = Path(f"cache/session_backups/{session_id}")
            if session_backup_dir.exists():
                self._logger.debug(f"   🗑️  Removing session backup: {session_backup_dir}")
                shutil.rmtree(session_backup_dir)
                self._logger.info(f"   ✅ Cleaned up session backup directory")

            # Clean up entire web_scrapes cache folder if it exists and has session dirs
            if self.cache_dir.exists():
                # Check if we should clear the entire cache (all session dirs processed)
                remaining_dirs = [d for d in self.cache_dir.iterdir() if d.is_dir() and d.name.startswith('req_')]
                if not remaining_dirs:
                    self._logger.info(f"   🗑️  All sessions processed - clearing web_scrapes cache folder")
                    # Keep the directory but remove all contents
                    for item in self.cache_dir.iterdir():
                        if item.is_dir():
                            shutil.rmtree(item)
                        else:
                            item.unlink()
                    self._logger.info(f"   ✅ Cleared web_scrapes cache folder")
                else:
                    self._logger.debug(f"   📂 {len(remaining_dirs)} directories remaining in cache")

        except Exception as e:
            self._logger.error(f"Error cleaning up session {session_id}: {e}", exc_info=True)


def main():
    """Run the index aggregator consumer."""
    consumer = KafkaIndexAggregatorConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
