#!/usr/bin/env python3
"""
Kafka Document Chunker Consumer

Consumes extracted web content from 'summarise-document' topic,
chunks the content using DocumentChunkingUtility,
and saves chunked documents to cache.

TODO: TELEMETRY & OBSERVABILITY
- [ ] Add Prometheus metrics (port 8002)
- [ ] Track documents_chunked_total counter
- [ ] Track chunks_created_total counter
- [ ] Track chunking_duration_seconds histogram
- [ ] Track cache_files_total gauge
- [ ] Track cache_size_bytes gauge
- [ ] Track messages_consumed_total counter
- [ ] Track message_processing_duration_seconds histogram
- [ ] Add structured JSON logging with trace_id
- [ ] Expose /metrics endpoint on port 8002
"""

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Telemetry imports
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.logging_config import configure_logging
    TELEMETRY_AVAILABLE = True
except ImportError as e:
    # Try relative import for direct execution
    try:
        import os
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
        from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                    PrometheusMetrics, SystemMetricsCollector)
        from src.monitoring.logging_config import configure_logging
        TELEMETRY_AVAILABLE = True
    except ImportError:
        TELEMETRY_AVAILABLE = False

# Add the src directory to the path
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from components.chunkers.document_chunking_utility import \
    DocumentChunkingUtility
from components.chunkers.markdown_chunk_processor import (
    ChunkMergerConfig, MarkdownChunkProcessor)
from config_handler import Base

# Try to import LLM client
try:
    from clients.llm_gcp_client import GeminiLLMClient
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


class KafkaDocumentChunkerConsumer(Base):
    """Consumer for chunking web-extracted documents via Kafka."""

    def __init__(self):
        """Initialize the Kafka document chunker consumer."""
        super().__init__()

        # Setup logging
        self._setup_logging()
        self._logger.info("Initializing KafkaDocumentChunkerConsumer")

        # Get configuration
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS') or self._app_config.get('kafka.bootstrap_servers') or 'localhost:9092'
        consumer_group_id = self._app_config.get('chunker.consumer_group_id') or 'document-chunker-consumer'
        input_topic = self._app_config.get('chunker.input_topic') or 'content-extracted'
        self.output_topic = self._app_config.get('chunker.output_topic') or 'citation-extraction'
        cache_dir = self._app_config.get('chunker.cache_directory') or 'cache/web_scrapes'
        max_chunk_words = self._app_config.get('chunker.max_chunk_words') or 350

        # Log configuration
        self._logger.info("✅ Configuration loaded:")
        self._logger.info(f"   Kafka servers: {bootstrap_servers}")
        self._logger.info(f"   Consumer group: {consumer_group_id}")
        self._logger.info(f"   Input topic: {input_topic}")
        self._logger.info(f"   Output topic: {self.output_topic}")
        self._logger.info(f"   Cache directory: {cache_dir}")
        self._logger.info(f"   Max chunk words: {max_chunk_words}")

        # Initialize Kafka consumer
        # Initialize Kafka consumer with manual partition assignment
        from confluent_kafka import TopicPartition

        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': consumer_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })

        # Initialize Kafka producer
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

        # Use manual partition assignment instead of consumer group subscription
        cluster_metadata = self.consumer.list_topics(input_topic, timeout=10)
        topic_metadata = cluster_metadata.topics.get(input_topic)

        if topic_metadata:
            partitions = [TopicPartition(input_topic, p) for p in topic_metadata.partitions.keys()]
            self.consumer.assign(partitions)
            self._logger.info(f"📡 Manually assigned {len(partitions)} partitions for topic: {input_topic}")
        else:
            raise Exception(f"Topic {input_topic} not found")

        # Initialize document chunker
        self.cache_dir = Path(cache_dir)
        self.chunker = DocumentChunkingUtility(
            output_dir=str(self.cache_dir),
            cache_dir=str(self.cache_dir),
            max_chunk_words=max_chunk_words
        )

        # Initialize markdown chunk processor (optional post-processing)
        self.process_markdown = self._app_config.get('chunker.process_markdown_chunks') or True
        if self.process_markdown:
            chunk_processor_config = ChunkMergerConfig(
                min_words=self._app_config.get('chunker.min_chunk_words') or 3,
                max_chunk_words=max_chunk_words,
                filter_standalone_links=True,
                filter_html_comments=True,
                filter_date_only=True,
                filter_youtube_headers=True
            )

            # Initialize LLM client for validation
            llm_client = None

            # Check environment variable first, then config, then default to True
            env_llm_validation = os.getenv('ENABLE_LLM_VALIDATION', '').lower()
            if env_llm_validation in ('true', '1', 'yes'):
                enable_llm_validation = True
            elif env_llm_validation in ('false', '0', 'no'):
                enable_llm_validation = False
            else:
                enable_llm_validation = self._app_config.get('chunker.enable_llm_validation')
                if enable_llm_validation is None:
                    enable_llm_validation = True

            self._logger.info(f"   🔧 LLM validation: {'✅ ENABLED' if enable_llm_validation else '⚠️  DISABLED'}")

            if enable_llm_validation and GEMINI_AVAILABLE:
                try:
                    llm_client = GeminiLLMClient()
                    self._logger.info(f"   ✅ Gemini LLM client initialized for content validation")
                except Exception as e:
                    self._logger.warning(f"   ⚠️  Failed to initialize LLM client: {e}")
            elif not GEMINI_AVAILABLE:
                self._logger.warning(f"   ⚠️  Gemini client not available, skipping LLM validation")

            self.chunk_processor = MarkdownChunkProcessor(
                config=chunk_processor_config,
                llm_client=llm_client,
                enable_llm_validation=enable_llm_validation and llm_client is not None
            )
            self._logger.info(f"   ✅ MarkdownChunkProcessor enabled (min_words={chunk_processor_config.min_words}, max={max_chunk_words})")
        else:
            self.chunk_processor = None
            self._logger.info("   ⚠️  MarkdownChunkProcessor disabled")

        # Initialize telemetry
        self.input_topic = input_topic
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"document_chunker_{os.getpid()}"

            # Configure structured logging
            configure_logging(
                format='json',
                level=os.getenv('LOG_LEVEL', 'INFO'),
                consumer_id=consumer_id
            )

            # Initialize Prometheus metrics
            self.metrics = PrometheusMetrics(consumer_id=consumer_id)
            self.system_metrics = SystemMetricsCollector(consumer_id=consumer_id)
            self.kafka_metrics = KafkaMetricsWrapper(consumer_id=consumer_id)
            self.metrics_server = MetricsServer(
                port=8002,
                consumer_name='document_chunker',
                health_check=lambda: self.running,
                ready_check=lambda: self.consumer is not None and self.running
            )

            # Consumer-specific metrics
            from prometheus_client import Counter, Gauge, Histogram

            self.documents_chunked = Counter(
                'kafka_consumer_documents_chunked_total',
                'Total documents successfully chunked',
                ['consumer_id']
            )

            self.chunks_created = Counter(
                'kafka_consumer_chunks_created_total',
                'Total chunks created from documents',
                ['consumer_id']
            )

            self.chunking_duration = Histogram(
                'kafka_consumer_chunking_duration_seconds',
                'Time spent chunking documents',
                ['consumer_id', 'status'],
                buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
            )

            self.cache_files = Gauge(
                'kafka_consumer_cache_files_total',
                'Number of files in cache',
                ['consumer_id']
            )

            self.cache_size_bytes = Gauge(
                'kafka_consumer_cache_size_bytes',
                'Total size of cache in bytes',
                ['consumer_id']
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8002")
        else:
            self._logger.warning("⚠️ Telemetry not available - monitoring module not found")
            self.metrics = None
            self.system_metrics = None
            self.kafka_metrics = None
            self.metrics_server = None

        self.running = True

    def _setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='| %(asctime)s | %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self._logger = logging.getLogger(self.__class__.__name__)

    async def process_message(self, message_data: Dict[str, Any]):
        """Process a single message and chunk the document.

        Args:
            message_data: Dictionary containing:
                - request_id: Unique identifier for this request
                - extracted_content: Nested dict with url, text, metadata
                - summary_metadata (optional): Rich metadata from summary (title, authors, date, etc.)
        """
        request_id = message_data.get('request_id', 'unknown')

        # Extract from nested structure (web scraper publishes this way)
        extracted_content = message_data.get('extracted_content', {})
        url = extracted_content.get('url', '')
        text = extracted_content.get('text', '')
        metadata = extracted_content.get('metadata', {})

        # Check for optional summary_metadata (from summarizer fallback)
        summary_metadata = message_data.get('summary_metadata', {})

        # Check extraction method to determine content type
        extraction_method = metadata.get('extraction_method', 'web_extraction')

        # Use summary metadata if available, otherwise fall back to extraction metadata
        if summary_metadata:
            title = summary_metadata.get('title', metadata.get('title', ''))
            authors = summary_metadata.get('authors', [])
            author = ', '.join(authors) if authors else metadata.get('author', '')
            published_date = summary_metadata.get('publication_date', metadata.get('published_date', ''))
            publisher = summary_metadata.get('publisher', metadata.get('publisher', ''))
            document_type = summary_metadata.get('type', metadata.get('document_type'))
            publication_venue = summary_metadata.get('publication_venue', metadata.get('publication_venue'))
            data_source = 'summary_metadata'
        else:
            # For YouTube videos, map channel -> author and upload_date -> creation_date
            if extraction_method == 'youtube_transcript_api':
                title = metadata.get('title', '')
                author = metadata.get('channel', metadata.get('author', ''))  # channel -> author
                published_date = metadata.get('upload_date', metadata.get('published_date', ''))  # upload_date -> published_date
                publisher = metadata.get('channel', '')  # Also use channel as publisher
                document_type = 'video'  # Set content type to 'video' for YouTube
                publication_venue = 'YouTube'
                data_source = 'youtube_extraction'
            else:
                title = metadata.get('title', '')
                author = metadata.get('author', '')
                published_date = metadata.get('published_date', '')
                publisher = metadata.get('publisher', '')
                document_type = metadata.get('document_type')
                publication_venue = metadata.get('publication_venue')
                data_source = 'extraction_metadata'

        self._logger.info("=" * 80)
        self._logger.info("📦 New chunking request received")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   URL: {url[:100]}...")
        self._logger.info(f"   Content length: {len(text)} characters")
        if summary_metadata:
            self._logger.info(f"   📊 Using summary metadata (title, authors, date from Gemini)")

        try:
            # Get data lineage info
            data_lineage = metadata.get('data_lineage', {})
            chunked_from = data_lineage.get('chunked_from', 'extraction')  # 'summary' or 'extraction'
            extraction_method = metadata.get('extraction_method', 'web_extraction')

            # Use the enriched metadata values we extracted above
            doc_title = title
            doc_author = author
            doc_language = metadata.get('language', 'en')

            # Remove the old title extraction logic since we now have it from summary_metadata or extraction

            self._logger.info(f"   📄 Title: {doc_title[:80]}...")
            self._logger.info(f"   ✍️  Author: {doc_author}")
            self._logger.info(f"   📅 Published: {published_date}")
            self._logger.info(f"   🏢 Publisher: {publisher}")
            self._logger.info(f"   🌐 Language: {doc_language}")
            self._logger.info(f"   🔗 Data source: {data_source}")
            self._logger.info(f"   📊 Chunked from: {chunked_from}")
            self._logger.info(f"   🔧 Extraction method: {extraction_method}")

            # Prepare cached data structure for chunker with enriched metadata
            # Include data lineage to track whether we chunked summary or extraction
            cached_data = {
                'url': url,
                'title': doc_title,
                'text': text,
                'metadata': {
                    'title': doc_title,
                    'author': doc_author,
                    'publisher': publisher,
                    'publication_venue': publication_venue,
                    'creation_date': published_date,
                    'document_type': document_type,  # Propagate document type (article, report, etc.)
                    'language': doc_language,
                    'description': metadata.get('description'),
                    'keywords': metadata.get('keywords'),
                    'content_source': metadata.get('content_source', 'extraction'),
                    'data_lineage': data_lineage,  # Preserve data lineage
                    'query_id': metadata.get('query_id', 'default')  # Propagate query_id for index naming
                },
                'method': extraction_method,  # Use the actual method (gemini_web_grounding or web_extraction)
                'success': True,
                'timestamp': extracted_content.get('extracted_at')
            }

            self._logger.info("🔪 Chunking document...")

            # Track chunking duration
            chunking_start = time.time()

            # Chunk the document
            result = self.chunker.chunk_single_document(url, cached_data)

            chunking_duration = time.time() - chunking_start

            if result['status'] == 'success':
                chunks = result['chunks']
                self._logger.info(f"   ✅ Generated {len(chunks)} chunks")

                # Track chunking metrics
                if self.telemetry_enabled:
                    self.documents_chunked.labels(consumer_id=f"document_chunker_{os.getpid()}").inc()
                    self.chunks_created.labels(consumer_id=f"document_chunker_{os.getpid()}").inc(len(chunks))
                    self.chunking_duration.labels(
                        consumer_id=f"document_chunker_{os.getpid()}",
                        status='success'
                    ).observe(chunking_duration)

                # Apply markdown chunk processor (filter noise and merge small chunks)
                if self.process_markdown and self.chunk_processor:
                    self._logger.info("   🔄 Applying MarkdownChunkProcessor...")
                    original_count = len(chunks)
                    original_words = sum(c.chunk_metadata.word_count for c in chunks)

                    # Convert chunks to dicts, process with async, then back
                    chunk_dicts = [c.to_dict() for c in chunks]
                    processed_dicts = await self.chunk_processor.process_chunks(chunk_dicts, source_url=url)

                    # For now, we'll save the processed dicts directly
                    # (avoiding complex Chunk object reconstruction)
                    chunks = processed_dicts

                    processed_count = len(chunks)
                    processed_words = sum(c.get('chunk_metadata', {}).get('word_count', 0) for c in chunks)

                    self._logger.info(f"   ✅ Processed: {original_count} → {processed_count} chunks")
                    self._logger.info(f"   📊 Words: {processed_words}/{original_words} ({processed_words/original_words*100:.1f}% preserved)")

                # Save chunks to request_id folder
                output_dir = self.cache_dir / request_id
                output_dir.mkdir(parents=True, exist_ok=True)

                chunks_file = output_dir / 'chunked_document.jsonl'

                with open(chunks_file, 'w', encoding='utf-8') as f:
                    for chunk in chunks:
                        # chunk is either Chunk object or dict depending on processing
                        chunk_record = chunk.to_dict() if hasattr(chunk, 'to_dict') else chunk
                        f.write(json.dumps(chunk_record, ensure_ascii=False) + '\n')

                self._logger.info(f"   💾 Chunks saved to: {chunks_file}")

                # Save chunk statistics as JSONL
                stats_file = output_dir / 'chunk_stats.jsonl'

                # Calculate stats handling both Chunk objects and dicts
                if chunks and hasattr(chunks[0], 'chunk_metadata'):
                    # Original Chunk objects
                    avg_char_count = sum(c.chunk_metadata.char_count for c in chunks) / len(chunks)
                    avg_word_count = sum(c.chunk_metadata.word_count for c in chunks) / len(chunks)
                    chunk_types = {}
                    for chunk in chunks:
                        chunk_type = chunk.chunk_metadata.chunk_type.value
                        chunk_types[chunk_type] = chunk_types.get(chunk_type, 0) + 1
                else:
                    # Processed dicts
                    avg_char_count = sum(c.get('chunk_metadata', {}).get('char_count', 0) for c in chunks) / len(chunks) if chunks else 0
                    avg_word_count = sum(c.get('chunk_metadata', {}).get('word_count', 0) for c in chunks) / len(chunks) if chunks else 0
                    chunk_types = {}
                    for chunk in chunks:
                        chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type', 'unknown')
                        chunk_types[chunk_type] = chunk_types.get(chunk_type, 0) + 1

                stats = {
                    'total_chunks': len(chunks),
                    'chunk_types': chunk_types,
                    'avg_chunk_size': avg_char_count,
                    'avg_word_count': avg_word_count,
                    'markdown_processed': self.process_markdown
                }

                with open(stats_file, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(stats, ensure_ascii=False) + '\n')

                self._logger.info(f"   📊 Statistics saved to: {stats_file}")
                self._logger.debug(f"   📈 Chunk type distribution: {stats['chunk_types']}")

                # Publish to citation-extraction topic
                try:
                    message = {
                        'request_id': request_id,
                        'session_id': message_data.get('session_id', 'unknown'),
                        'chunks_file': str(chunks_file),
                        'document_id': f"doc-{request_id}",
                        'source_url': url,
                        'chunk_count': len(chunks)
                    }

                    # Pass through total_requests if present
                    if 'total_requests' in message_data:
                        message['total_requests'] = message_data['total_requests']

                    message_json = json.dumps(message)
                    self.producer.produce(
                        topic=self.output_topic,
                        value=message_json.encode('utf-8')
                    )
                    self.producer.flush()

                    self._logger.info(f"   📤 Published chunked document message to {self.output_topic}")
                except Exception as e:
                    self._logger.error(f"   ⚠️ Failed to publish to Kafka: {e}")
                    # Track DLQ metric
                    if self.telemetry_enabled:
                        self.metrics.increment_dead_letter(self.output_topic, 'publish_failed')
                    # Publish to dead letter queue
                    self._publish_to_dlq(message_data, 'chunker', str(e))

            else:
                error = result.get('error', 'Unknown error')
                self._logger.error(f"   ❌ Chunking failed: {error}")

                # Track failure metrics
                if self.telemetry_enabled:
                    self.chunking_duration.labels(
                        consumer_id=f"document_chunker_{os.getpid()}",
                        status='failure'
                    ).observe(chunking_duration)

                # Publish to dead letter queue
                self._publish_to_dlq(message_data, 'chunker', error)

        except Exception as e:
            self._logger.error(f"   ❌ Error processing message: {str(e)}", exc_info=True)
            # Publish to dead letter queue
            self._publish_to_dlq(message_data, 'chunker', str(e))

        self._logger.info("=" * 80)
        self._logger.info("✅ Successfully processed chunking request")

    def _publish_to_dlq(self, original_message: Dict[str, Any], stage: str, error: str):
        """Publish failed message to dead letter queue.

        Args:
            original_message: The original message that failed
            stage: The pipeline stage where failure occurred
            error: The error message
        """
        try:
            from datetime import datetime
            dlq_message = {
                'original_message': original_message,
                'failed_stage': stage,
                'error': error,
                'timestamp': datetime.now().isoformat(),
                'request_id': original_message.get('request_id', 'unknown'),
                'session_id': original_message.get('session_id', 'unknown')
            }

            self.producer.produce(
                topic='dead-letter-queue',
                value=json.dumps(dlq_message).encode('utf-8')
            )
            self.producer.flush()
            self._logger.info(f"   📮 Published failure to dead-letter-queue")
        except Exception as e:
            self._logger.error(f"   ⚠️ Failed to publish to DLQ: {e}")

    async def consume_messages(self):
        """Main consumer loop."""
        self._logger.info("🚀 Starting to consume messages...")

        # Start telemetry services
        if self.telemetry_enabled:
            self._logger.info("📊 Starting telemetry services...")
            self.system_metrics.start()
            self.metrics_server.start()
            self.metrics.set_state(1)  # Running
            self._logger.info("✅ Telemetry services started")

        try:
            while self.running:
                # Poll for messages with timing
                poll_start = time.time()
                msg = self.consumer.poll(timeout=1.0)
                poll_duration = time.time() - poll_start

                # Track poll metrics
                if self.telemetry_enabled and msg is not None:
                    self.kafka_metrics.record_poll(
                        topic=self.input_topic,
                        duration=poll_duration,
                        message_count=1
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
                        if self.telemetry_enabled:
                            self.metrics.increment_connection_errors(
                                error_type=msg.error().name()
                            )
                        continue

                try:
                    # Track message age
                    if self.telemetry_enabled:
                        timestamp_type, timestamp_ms = msg.timestamp()
                        if timestamp_type != -1 and timestamp_ms > 0:
                            message_timestamp = timestamp_ms / 1000.0
                            self.kafka_metrics.record_message_age(
                                topic=self.input_topic,
                                message_timestamp=message_timestamp
                            )

                    # Track consumer lag
                    if self.telemetry_enabled:
                        try:
                            from confluent_kafka import TopicPartition
                            partition = msg.partition()
                            topic = msg.topic()
                            tp = TopicPartition(topic, partition)
                            high_watermark = self.consumer.get_watermark_offsets(tp)[1]
                            current_offset = msg.offset()
                            lag = high_watermark - current_offset - 1
                            if lag >= 0:
                                self.kafka_metrics.record_lag(
                                    topic=topic,
                                    partition=partition,
                                    lag_messages=lag
                                )
                        except Exception as e:
                            self._logger.debug(f"Could not calculate lag: {e}")

                    # Decode message
                    message_str = msg.value().decode('utf-8')
                    try:
                        message_data = json.loads(message_str)
                    except json.JSONDecodeError as e:
                        self._logger.error(f"❌ Invalid JSON in message: {e}")
                        if self.telemetry_enabled:
                            self.metrics.increment_deserialization_errors(
                                topic=self.input_topic,
                                error_type='json_decode_error'
                            )
                        continue

                    # Track messages consumed
                    if self.telemetry_enabled:
                        self.metrics.increment_messages_consumed(self.input_topic)

                    # Track messages in flight
                    if self.telemetry_enabled:
                        self.kafka_metrics.increment_messages_in_flight(self.input_topic)

                    try:
                        # Process the message with timing
                        start_time = time.time()
                        await self.process_message(message_data)

                        # Record processing duration
                        if self.telemetry_enabled:
                            duration = time.time() - start_time
                            self.metrics.record_duration(self.input_topic, duration, 'success')

                    finally:
                        # Decrement messages in flight
                        if self.telemetry_enabled:
                            self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)
                    if self.telemetry_enabled:
                        self.metrics.increment_failures(self.input_topic, type(e).__name__)
                        try:
                            self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                        except:
                            pass

        except KeyboardInterrupt:
            self._logger.info("\n⚠️  Shutting down consumer...")
        finally:
            # Shutdown telemetry
            if self.telemetry_enabled:
                self._logger.info("🛑 Shutting down telemetry...")
                self.system_metrics.stop()
                self.metrics_server.stop()
                self.metrics.set_state(0)  # Stopped

            self.consumer.close()
            self._logger.info("👋 Consumer closed")

    def run(self):
        """Run the consumer."""
        asyncio.run(self.consume_messages())


def main():
    """Main entry point."""
    print("=" * 80)
    print("🤖 Kafka Document Chunker Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaDocumentChunkerConsumer()

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
