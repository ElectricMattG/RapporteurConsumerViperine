#!/usr/bin/env python3
"""
Kafka Summarizer Consumer

Consumes extracted content from Kafka and generates summaries using LLM.
Works in tandem with the web scraper consumer to create a processing pipeline:
  1. Web Scraper Consumer: Extracts content from URLs → saves to cache
  2. Summarizer Consumer: Reads extracted content → generates summaries → saves to cache

Message Format:
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "extracted_content": {
            "url": "https://example.com",
            "text": "Full extracted text...",
            "metadata": {...},
            "extracted_at": "timestamp"
        }
    }

Usage:
    from kafka_summarizer import KafkaSummarizerConsumer

    consumer = KafkaSummarizerConsumer()
    consumer.start()
"""

import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

# Telemetry imports with fallback
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.logging_config import configure_logging
    TELEMETRY_AVAILABLE = True
except ImportError:
    # Fallback for environments where telemetry is not available
    try:
        import sys
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
    from ..components.summarise.llm_citation_researcher import \
        CitationResearchClient
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from components.summarise.llm_citation_researcher import \
        CitationResearchClient
    from config_handler import Base


class KafkaSummarizerConsumer(Base):
    """Kafka consumer for generating summaries from extracted content"""

    def __init__(self):
        """Initialize the Kafka summarizer consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaSummarizerConsumer")

        # Load configuration
        self._load_config()

        # Initialize LLM client for summarization
        llm_provider = self._app_config.get('summarizer.llm_provider') or 'openai'
        self.llm_client = CitationResearchClient(
            llm_provider=llm_provider,
            web_searcher=None  # Summarizer doesn't need web search
        )

        # Initialize telemetry components if available
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"summarizer_{os.getpid()}"
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
                consumer_name='summarizer',
                health_check=lambda: self.running,
                ready_check=lambda: self.consumer is not None and self.running
            )

            # Summarizer-specific metrics
            from prometheus_client import Counter, Gauge, Histogram

            self.documents_summarized = Counter(
                'kafka_consumer_documents_summarized_total',
                'Total documents successfully summarized',
                ['consumer_id']
            )

            self.summarization_duration = Histogram(
                'kafka_consumer_summarization_duration_seconds',
                'Time spent generating summaries',
                ['consumer_id', 'status'],
                buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
            )

            self.llm_api_calls = Counter(
                'kafka_consumer_llm_api_calls_total',
                'Total LLM API calls made',
                ['consumer_id', 'provider']
            )

            self.llm_api_errors = Counter(
                'kafka_consumer_llm_api_errors_total',
                'Total LLM API errors',
                ['consumer_id', 'error_type']
            )

            self.llm_tokens_used = Counter(
                'kafka_consumer_llm_tokens_used_total',
                'Total LLM tokens consumed',
                ['consumer_id', 'provider']
            )

            self.summary_length = Histogram(
                'kafka_consumer_summary_length_chars',
                'Summary length in characters',
                ['consumer_id'],
                buckets=[100, 500, 1000, 2000, 5000, 10000, 20000, 50000]
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8006")
        else:
            self.metrics = None
            self.system_metrics = None
            self.kafka_metrics = None
            self.metrics_server = None

        # Initialize Kafka consumer
        self.consumer = None
        self.producer = None
        self.running = False

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self):
        """Load configuration from config"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers')
            if not bootstrap_servers:
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            self.kafka_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': self._app_config.get('summarizer.consumer_group_id') or 'summarizer-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 30000,
                'max.poll.interval.ms': 300000,  # 5 minutes for processing
            }

            # Add security config if needed
            security_protocol = self._app_config.get('kafka.security_protocol')
            if security_protocol and security_protocol != 'PLAINTEXT':
                self.kafka_config['security.protocol'] = security_protocol

                sasl_mechanism = self._app_config.get('kafka.sasl_mechanism')
                if sasl_mechanism:
                    self.kafka_config['sasl.mechanism'] = sasl_mechanism
                    self.kafka_config['sasl.username'] = os.getenv('KAFKA_SASL_USERNAME', '')
                    self.kafka_config['sasl.password'] = os.getenv('KAFKA_SASL_PASSWORD', '')

            # Topic configuration
            self.input_topic = self._app_config.get('kafka.output_topic') or 'summarise-document'
            self.output_topic = 'content-extracted'  # Publish summary back for chunking when extraction failed
            self.summary_completed_topic = self._app_config.get('summarizer.summary_completed_topic') or 'summary-completed'

            # Session state tracking for summary indexer
            self.session_counts = {}  # {session_id: {count: int, total: int}}

            # Cache configuration
            self.cache_dir = Path(self._app_config.get('web_scraper.cache_dir') or './cache/web_scrapes')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            self._logger.info(f"✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {self.kafka_config['bootstrap.servers']}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _create_consumer(self):
        """Create and configure Kafka consumer and producer"""
        try:
            from confluent_kafka import TopicPartition

            self.consumer = Consumer(self.kafka_config)

            # Manual partition assignment to bypass consumer group coordinator
            cluster_metadata = self.consumer.list_topics(self.input_topic, timeout=10)
            topic_metadata = cluster_metadata.topics.get(self.input_topic)

            if topic_metadata:
                partitions = [TopicPartition(self.input_topic, p) for p in topic_metadata.partitions.keys()]
                self.consumer.assign(partitions)
                self._logger.info(f"✅ Manually assigned {len(partitions)} partitions for topic: {self.input_topic}")
            else:
                self._logger.error(f"❌ Topic {self.input_topic} not found")
                raise RuntimeError(f"Topic {self.input_topic} not found")

            # Create producer for publishing summary content back to content-extracted
            producer_config = {
                'bootstrap.servers': self.kafka_config['bootstrap.servers']
            }
            self.producer = Producer(producer_config)
            self._logger.info(f"✅ Producer ready for topic: {self.output_topic}")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer/producer: {e}")
            raise

    async def _process_message(self, message_data: Dict) -> Dict:
        """
        Process a single message from Kafka by generating a summary.

        Args:
            message_data: The message data containing extracted content

        Returns:
            Processing result dictionary
        """
        session_id = message_data.get('session_id', 'unknown')
        request_id = message_data.get('request_id', f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        query_id = message_data.get('query_id', 'default')  # Read query_id for propagation
        extracted_content = message_data.get('extracted_content', {})

        url = extracted_content.get('url', 'unknown')
        text = extracted_content.get('text', '')
        metadata = extracted_content.get('metadata', {})
        extraction_failed = message_data.get('extraction_failed', False)  # Check if extraction failed

        # Ensure query_id is in metadata for downstream propagation
        if query_id and query_id != 'default':
            metadata['query_id'] = query_id

        self._logger.info("=" * 80)
        self._logger.info(f"📝 Processing summary request")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   URL: {url}")
        self._logger.info(f"   Content length: {len(text)} characters")
        if extraction_failed:
            self._logger.warning(f"   ⚠️ EXTRACTION FAILED - Using URL-only mode with Gemini web grounding")

        result = {
            'session_id': session_id,
            'request_id': request_id,
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'error': None
        }

        # Create request-specific directory
        request_dir = self.cache_dir / request_id
        request_dir.mkdir(parents=True, exist_ok=True)

        # Track summarization start time
        summarization_start = time.time() if self.telemetry_enabled else None

        try:
            # Generate summary using LLM
            if extraction_failed:
                self._logger.info("🌐 Generating summary from URL using Gemini web grounding...")
            else:
                self._logger.info("🤖 Generating summary from extracted content...")

            # Track LLM API call
            if self.telemetry_enabled:
                llm_provider = self._app_config.get('summarizer.llm_provider') or 'openai'
                self.llm_api_calls.labels(
                    consumer_id=f"summarizer_{os.getpid()}",
                    provider=llm_provider
                ).inc()

            # Create a citation-like prompt from the content
            title = metadata.get('title', 'Untitled')
            author = metadata.get('author', 'Unknown')
            citation = f"{author}. {title}. {url}"

            # Detect language from text or metadata (default to English)
            detected_language = metadata.get('language') or 'en'
            if not detected_language and text:
                # Simple language detection: if mostly ASCII, assume English
                detected_language = 'en'

            # For failed extractions with Gemini, rely on web grounding
            context_text = text[:2000] if text and not extraction_failed else None

            # Generate structured summary
            summary_result = await self.llm_client.generate_summary(
                citation=citation,
                urls=[url],
                context=context_text,
                use_web_grounding=extraction_failed  # Tell Gemini to fetch the URL directly
            )

            self._logger.info(f"   ✅ Summary generated successfully")
            self._logger.debug(f"   📊 Summary length: {len(summary_result.get('markdown_summary', ''))} characters")

            # Add query_id to summary result for traceability
            summary_result['query_id'] = query_id

            # Track successful summarization metrics
            if self.telemetry_enabled and summarization_start:
                summarization_duration = time.time() - summarization_start
                consumer_id_label = f"summarizer_{os.getpid()}"
                llm_provider = self._app_config.get('summarizer.llm_provider') or 'openai'

                # Record summarization duration
                self.summarization_duration.labels(
                    consumer_id=consumer_id_label,
                    status='success'
                ).observe(summarization_duration)

                # Increment documents summarized
                self.documents_summarized.labels(
                    consumer_id=consumer_id_label
                ).inc()

                # Track summary length
                summary_text = summary_result.get('markdown_summary', '')
                self.summary_length.labels(
                    consumer_id=consumer_id_label
                ).observe(len(summary_text))

                # Track token usage if available in summary_result
                tokens_used = summary_result.get('tokens_used', 0)
                if tokens_used > 0:
                    self.llm_tokens_used.labels(
                        consumer_id=consumer_id_label,
                        provider=llm_provider
                    ).inc(tokens_used)

            # Save summary to cache as JSONL
            summary_file = request_dir / "summary.jsonl"
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(json.dumps(summary_result, ensure_ascii=False) + '\n')

            self._logger.info(f"   💾 Summary saved to: {summary_file}")

            result['success'] = True
            result['summary_file'] = str(summary_file)

            # Publish summary-completed message for Summary Indexer
            await self._publish_summary_completed(
                request_id=request_id,
                session_id=session_id,
                document_id=metadata.get('document_id', request_id),
                source_url=url,
                summary_file=str(summary_file),
                message_data=message_data
            )

            # CONTINGENCY: If extraction failed (403/404) but summary succeeded,
            # publish the summary content to content-extracted topic for chunking
            # This ensures 100% coverage even when direct scraping fails
            if extraction_failed:
                self._logger.info("   🔄 Extraction failed but summary succeeded - publishing summary for chunking")
                await self._publish_summary_for_chunking(
                    request_id=request_id,
                    session_id=session_id,
                    url=url,
                    summary_text=summary_result.get('markdown_summary', ''),
                    metadata=metadata,
                    title=title,
                    summary_metadata=summary_result.get('metadata', {}),
                    language=detected_language
                )

        except Exception as e:
            result['error'] = str(e)
            self._logger.error(f"❌ Error generating summary: {e}")
            import traceback
            self._logger.error(traceback.format_exc())

            # Track failed summarization metrics
            if self.telemetry_enabled and summarization_start:
                summarization_duration = time.time() - summarization_start
                consumer_id_label = f"summarizer_{os.getpid()}"

                # Record failed summarization duration
                self.summarization_duration.labels(
                    consumer_id=consumer_id_label,
                    status='failure'
                ).observe(summarization_duration)

                # Track API error type
                error_type = type(e).__name__
                self.llm_api_errors.labels(
                    consumer_id=consumer_id_label,
                    error_type=error_type
                ).inc()

        self._logger.info("=" * 80)
        return result

    async def _publish_summary_for_chunking(
        self,
        request_id: str,
        session_id: str,
        url: str,
        summary_text: str,
        metadata: Dict,
        title: str,
        summary_metadata: Dict,
        language: str = 'en'
    ):
        """
        Publish summary content to content-extracted topic for chunking.

        This is used when extraction fails (403/404) but summary generation succeeds
        via Gemini web grounding. By publishing the summary, we ensure it gets chunked
        and embedded, providing 100% coverage even when direct scraping fails.

        Args:
            request_id: Request identifier
            session_id: Session identifier
            url: Source URL
            summary_text: The generated summary (markdown format)
            metadata: Original metadata from extraction attempt
            title: Document title
            summary_metadata: Rich metadata extracted by Gemini (title, authors, date, etc.)
            language: Detected language code (default: 'en')
        """
        try:
            # Enhanced metadata for data lineage tracking
            # Merge summary metadata (from Gemini) with original metadata
            enhanced_metadata = {
                **metadata,
                'title': summary_metadata.get('title', title),
                'authors': summary_metadata.get('authors', []),
                'publication_date': summary_metadata.get('publication_date'),
                'publisher': summary_metadata.get('publisher'),
                'publication_venue': summary_metadata.get('publication_venue'),
                'document_type': summary_metadata.get('type'),  # Propagate type (article, report, etc.)
                'pages': summary_metadata.get('pages'),
                'volume': summary_metadata.get('volume'),
                'issue': summary_metadata.get('issue'),
                'doi': summary_metadata.get('doi'),
                'isbn': summary_metadata.get('isbn'),
                'language': language,  # Add language from detection
                'url': url,
                'content_source': 'gemini_summary',  # Track that this came from summary, not extraction
                'extraction_status': 'failed_used_summary_fallback',
                'extraction_method': 'gemini_web_grounding',  # Clear method indicator
                'summary_generated_at': datetime.now().isoformat(),
                'data_lineage': {
                    'original_extraction_failed': True,
                    'fallback_strategy': 'gemini_web_grounding_summary',
                    'content_type': 'ai_generated_summary',
                    'chunked_from': 'summary',  # Critical: we're chunking the summary, not scraped content
                    'source_method': 'llm_summarization_with_web_grounding'
                }
            }

            # Prepare message for chunking consumer with optional summary_metadata
            message = {
                'request_id': request_id,
                'session_id': session_id,
                'extracted_content': {
                    'url': url,
                    'text': summary_text,  # Use summary text for chunking
                    'metadata': enhanced_metadata,
                    'extracted_at': datetime.now().isoformat()
                },
                # Optional: include full summary metadata for chunker to use
                'summary_metadata': summary_metadata
            }

            # Pass through query_id if present in original message
            if 'query_id' in metadata:
                message['query_id'] = metadata['query_id']
                enhanced_metadata['query_id'] = metadata['query_id']

            # Serialize message
            message_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')

            # Publish to content-extracted topic (same topic web scraper uses)
            self.producer.produce(
                topic=self.output_topic,
                key=request_id.encode('utf-8'),
                value=message_bytes,
                callback=self._delivery_callback
            )

            # Flush to ensure delivery
            self.producer.poll(0)

            self._logger.info(f"   📤 Published summary to '{self.output_topic}' for chunking")
            self._logger.info(f"   📊 Summary length: {len(summary_text)} characters")
            self._logger.info(f"   🏷️  Data lineage: gemini_web_grounding_summary")

        except Exception as e:
            self._logger.error(f"   ❌ Error publishing summary for chunking: {e}")
            import traceback
            self._logger.error(traceback.format_exc())

            # Track DLQ metric for failed publish
            if self.telemetry_enabled:
                self.metrics.increment_dead_letter(self.output_topic, 'publish_failed')

    async def _publish_summary_completed(
        self,
        request_id: str,
        session_id: str,
        document_id: str,
        source_url: str,
        summary_file: str,
        message_data: Dict
    ):
        """
        Publish summary-completed message for Summary Indexer.

        Args:
            request_id: Request identifier
            session_id: Session identifier
            document_id: Document identifier
            source_url: Source URL
            summary_file: Path to saved summary file
            message_data: Original message data (to extract total_requests)
        """
        try:
            # Get or initialize session count
            if session_id not in self.session_counts:
                total_requests = message_data.get('total_requests', 1)
                self.session_counts[session_id] = {'count': 0, 'total': total_requests}

            # Increment count
            self.session_counts[session_id]['count'] += 1
            current_count = self.session_counts[session_id]['count']
            total_requests = self.session_counts[session_id]['total']

            # Create message
            message = {
                'request_id': request_id,
                'session_id': session_id,
                'document_id': document_id,
                'source_url': source_url,
                'summary_file': summary_file,
                'current_count': current_count,
                'total_requests': total_requests,
                'completed_at': datetime.now().isoformat()
            }

            # Publish to summary-completed topic
            self.producer.produce(
                topic=self.summary_completed_topic,
                key=session_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.poll(0)

            self._logger.info(f"   📤 Published to '{self.summary_completed_topic}' ({current_count}/{total_requests})")

        except Exception as e:
            self._logger.error(f"   ❌ Error publishing summary-completed: {e}")
            import traceback
            self._logger.error(traceback.format_exc())

    def _delivery_callback(self, err, msg):
        """Callback for Kafka producer delivery reports"""
        if err:
            self._logger.error(f"❌ Message delivery failed: {err}")
            # Track delivery failure
            if self.telemetry_enabled:
                self.metrics.increment_dead_letter(self.output_topic if hasattr(self, 'output_topic') else 'unknown', 'delivery_failed')
        else:
            self._logger.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    def start(self):
        """Start the Kafka consumer"""
        self._logger.info("🚀 Starting Kafka Summarizer Consumer")

        try:
            self._create_consumer()
            self.running = True

            # Run the async message processing loop
            asyncio.run(self._consume_loop())

        except Exception as e:
            self._logger.error(f"❌ Fatal error: {e}")
            raise
        finally:
            self._shutdown()

    async def _consume_loop(self):
        """Main message consumption loop with batch concurrency"""
        self._logger.info("🔄 Starting message consumption loop with CONCURRENT processing...")

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

        batch_size = 10  # Process up to 10 summaries concurrently
        pending_tasks = []

        while self.running:
            try:
                # Track poll start time
                poll_start = time.time() if self.telemetry_enabled else None

                # Collect batch of messages
                messages = []
                for _ in range(batch_size):
                    msg = self.consumer.poll(timeout=0.1)
                    if msg and not msg.error():
                        messages.append(msg)
                    elif msg and msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            # Track connection errors
                            if self.telemetry_enabled:
                                self.metrics.increment_connection_errors(str(msg.error().code()))
                            raise KafkaException(msg.error())

                # Record poll metrics
                if self.telemetry_enabled and poll_start:
                    poll_duration = time.time() - poll_start
                    self.kafka_metrics.record_poll(
                        self.input_topic,
                        poll_duration,
                        len(messages)
                    )

                if not messages:
                    # Wait a bit before next poll
                    await asyncio.sleep(0.1)
                    continue

                self._logger.info(f"🚀 Processing batch of {len(messages)} summaries CONCURRENTLY")

                # Process all messages concurrently
                tasks = []
                for msg in messages:
                    try:
                        # Track message age
                        if self.telemetry_enabled:
                            timestamp_type, timestamp_ms = msg.timestamp()
                            # timestamp_type: 0=CreateTime, 1=LogAppendTime, -1=NotAvailable
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

                                # Get high water mark (latest offset)
                                tp = TopicPartition(topic, partition)
                                _, high_water_mark = self.consumer.get_watermark_offsets(tp)

                                lag_messages = high_water_mark - current_offset - 1
                                if lag_messages > 0:
                                    # Estimate lag time (very rough, assumes 10 msg/sec processing rate)
                                    lag_seconds = lag_messages / 10.0
                                    self.kafka_metrics.record_lag(topic, partition, lag_messages, lag_seconds)
                            except Exception as e:
                                self._logger.debug(f"Could not calculate lag: {e}")

                        message_data = json.loads(msg.value().decode('utf-8'))

                        # Increment messages consumed
                        if self.telemetry_enabled:
                            self.metrics.increment_messages_consumed(self.input_topic)
                            self.kafka_metrics.increment_messages_in_flight(self.input_topic)

                        tasks.append(self._process_message(message_data))

                    except json.JSONDecodeError as e:
                        self._logger.error(f"❌ Invalid JSON in message: {e}")
                        if self.telemetry_enabled:
                            self.metrics.increment_deserialization_errors(self.input_topic, 'json_decode_error')

                # Execute all summaries concurrently
                if tasks:
                    # Track processing start time
                    processing_start = time.time() if self.telemetry_enabled else None

                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Log results and record metrics
                    success_count = 0
                    for result in results:
                        # Decrement messages in flight
                        if self.telemetry_enabled:
                            self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

                        if isinstance(result, Exception):
                            self._logger.error(f"❌ Task exception: {result}")
                            if self.telemetry_enabled:
                                self.metrics.increment_failures(self.input_topic, 'task_exception')
                        elif result.get('success'):
                            success_count += 1
                            self._logger.info(f"✅ Successfully processed summary for request {result['request_id']}")

                            # Record processing duration for success
                            if self.telemetry_enabled and processing_start:
                                duration = time.time() - processing_start
                                self.metrics.record_duration(self.input_topic, duration, 'success')
                        else:
                            self._logger.warning(f"⚠️ Summary processing completed with errors: {result.get('error')}")
                            if self.telemetry_enabled:
                                self.metrics.increment_failures(self.input_topic, 'processing_error')
                                if processing_start:
                                    duration = time.time() - processing_start
                                    self.metrics.record_duration(self.input_topic, duration, 'failure')

                    self._logger.info(f"📊 Batch complete: {success_count}/{len(tasks)} successful")

            except Exception as e:
                self._logger.error(f"❌ Error in consume loop: {e}")
                if self.telemetry_enabled:
                    self.metrics.increment_connection_errors('consume_loop_error')
                if not self.running:
                    break

        # Shutdown telemetry on exit
        if self.telemetry_enabled:
            try:
                self._logger.info("🛑 Shutting down telemetry services...")
                self.system_metrics.stop()
                self.metrics_server.stop()
                self.metrics.set_state(0)  # Set state to stopped
                self._logger.info("✅ Telemetry services stopped")
            except Exception as e:
                self._logger.error(f"⚠️ Error shutting down telemetry: {e}")

    def _shutdown(self):
        """Cleanup and shutdown"""
        self._logger.info("🛑 Shutting down consumer...")

        if self.producer:
            try:
                self.producer.flush()
                self._logger.info("✅ Producer flushed successfully")
            except Exception as e:
                self._logger.error(f"Error flushing producer: {e}")

        if self.consumer:
            try:
                self.consumer.close()
                self._logger.info("✅ Consumer closed successfully")
            except Exception as e:
                self._logger.error(f"Error closing consumer: {e}")


def main():
    """Main entry point"""
    consumer = KafkaSummarizerConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
