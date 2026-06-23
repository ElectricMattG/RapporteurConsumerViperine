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
import hashlib
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from utils.correlation import (bind_correlation_ids,
                               install_correlation_record_factory,
                               reset_correlation_ids)

# Telemetry imports with fallback
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.cost import estimate_cost
    from src.monitoring.error_categories import (TAXONOMY_TO_ERROR_CATEGORY,
                                                 ErrorCategory)
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
    from ._doc_id import make_document_id
    from ._failure_taxonomy import classify_error, validate_summary_content
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from _doc_id import make_document_id
    from _failure_taxonomy import classify_error, validate_summary_content

    from components.summarise.llm_citation_researcher import \
        CitationResearchClient
    from config_handler import Base


class KafkaSummarizerConsumer(Base):
    """Kafka consumer for generating summaries from extracted content"""

    def __init__(self):
        """Initialize the Kafka summarizer consumer"""
        install_correlation_record_factory()
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
            self.system_metrics = SystemMetricsCollector(consumer_id, cache_mount_path=str(self.cache_dir))
            self.kafka_metrics = KafkaMetricsWrapper(consumer_id)

            # Create metrics server with health checks
            self.metrics_server = MetricsServer(
                port=8010,
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

            self.llm_input_tokens = Counter(
                'kafka_consumer_llm_input_tokens_total',
                'Total LLM input (prompt) tokens consumed',
                ['consumer_id', 'model']
            )

            self.llm_output_tokens = Counter(
                'kafka_consumer_llm_output_tokens_total',
                'Total LLM output (completion) tokens produced',
                ['consumer_id', 'model']
            )

            self.llm_estimated_cost_usd = Counter(
                'kafka_consumer_llm_estimated_cost_usd_total',
                'Total estimated LLM cost in USD',
                ['consumer_id', 'model']
            )

            self.llm_latency = Histogram(
                'kafka_consumer_llm_latency_seconds',
                'LLM call end-to-end latency',
                ['consumer_id', 'model'],
                buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8010")
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
            # Support environment variable override for slim pipeline
            # In slim pipeline: SUMMARIZER_INPUT_TOPIC=viperine.content-extracted (web scraper output)
            # In full pipeline: use 'viperine.summarise-document' (default)
            self.input_topic = os.getenv('SUMMARIZER_INPUT_TOPIC') or \
                              self._app_config.get('kafka.output_topic') or \
                              'viperine.summarise-document'
            self.output_topic = self._app_config.get('kafka.output_topic') or 'viperine.content-extracted'  # Publish summary back for chunking when extraction failed
            self.summary_completed_topic = self._app_config.get('summarizer.summary_completed_topic') or 'viperine.summary-completed'
            self.dead_letter_topic = self._app_config.get('summarizer.dead_letter_topic') or 'viperine.dead-letter-queue'

            # Per-request aggregation counter — keyed on request_id (the per-query UUID),
            # NOT session_id (the chat-tab UUID shared across many requests).
            self.request_counts = {}  # {request_id: {count: int, total: int}}

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
        _tokens = bind_correlation_ids(session_id, request_id)
        origin = message_data.get('origin')       # opaque pass-through; diagnostic only
        url_index = message_data.get('url_index', 0)
        total_urls = message_data.get('total_urls', message_data.get('total_requests', 1))
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

        # ── Retry loop ────────────────────────────────────────────────────────
        # classify_error() drives the retry/degrade policy for each category.
        # On the final attempt we always publish a terminal summary-completed
        # event (success or failure) so the indexer session never gets stuck.
        _last_exc: Exception | None = None
        _attempt = 0
        _classification = None
        summary_result = None
        summary_file_path: str | None = None

        while True:
            _attempt += 1
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
                    detected_language = 'en'

                # For failed extractions with Gemini, rely on web grounding
                context_text = text[:2000] if text and not extraction_failed else None

                # Small delay to avoid rate limiting (stagger async calls)
                await asyncio.sleep(0.2)

                # Snapshot cumulative token usage so we can compute the per-call
                # delta after the LLM call (the result dict carries no usage).
                _inner_client = getattr(self.llm_client, 'client', None)
                _usage_before = {}
                if self.telemetry_enabled and _inner_client is not None and hasattr(_inner_client, 'get_usage'):
                    try:
                        _usage_before = dict(_inner_client.get_usage())
                    except Exception:
                        _usage_before = {}

                # Generate structured summary
                summary_result = await self.llm_client.generate_summary(
                    citation=citation,
                    urls=[url],
                    context=context_text,
                    use_web_grounding=extraction_failed
                )

                # Validate that the LLM didn't return an error-as-content
                summary_text_raw = summary_result.get('markdown_summary', '')
                if not validate_summary_content(summary_text_raw):
                    # instrumentation - model_error_outputs_total
                    if self.telemetry_enabled:
                        consumer_id_label = f"summarizer_{os.getpid()}"
                        llm_provider = self._app_config.get('summarizer.llm_provider') or 'openai'
                        self.metrics.model_error_outputs.labels(
                            consumer_id=consumer_id_label, model=llm_provider
                        ).inc()
                    raise ValueError(f"LLM returned error-as-content: {summary_text_raw[:100]}")

                self._logger.info(f"   ✅ Summary generated successfully (attempt {_attempt})")
                self._logger.debug(f"   📊 Summary length: {len(summary_result.get('markdown_summary', ''))} characters")

                # Add query_id to summary result for traceability
                summary_result['query_id'] = query_id

                # Track successful summarization metrics
                if self.telemetry_enabled and summarization_start:
                    summarization_duration = time.time() - summarization_start
                    consumer_id_label = f"summarizer_{os.getpid()}"
                    llm_provider = self._app_config.get('summarizer.llm_provider') or 'openai'
                    self.summarization_duration.labels(
                        consumer_id=consumer_id_label,
                        status='success'
                    ).observe(summarization_duration)
                    self.documents_summarized.labels(consumer_id=consumer_id_label).inc()
                    summary_text_len = len(summary_result.get('markdown_summary', ''))
                    self.summary_length.labels(consumer_id=consumer_id_label).observe(summary_text_len)

                    # LLM latency (per-call, not total summarization time)
                    self.llm_latency.labels(
                        consumer_id=consumer_id_label,
                        model=llm_provider
                    ).observe(summarization_duration)

                    # Token tracking — prefer usage in the result, else read the
                    # per-call delta from the underlying LLM client.
                    usage = summary_result.get('usage') or {}
                    input_tokens = usage.get('input_tokens') or usage.get('prompt_tokens', 0)
                    output_tokens = usage.get('output_tokens') or usage.get('completion_tokens', 0)
                    if not input_tokens and not output_tokens and _inner_client is not None and hasattr(_inner_client, 'get_usage'):
                        try:
                            _usage_after = _inner_client.get_usage()
                            input_tokens = max(0, _usage_after.get('input_tokens', 0) - _usage_before.get('input_tokens', 0))
                            output_tokens = max(0, _usage_after.get('output_tokens', 0) - _usage_before.get('output_tokens', 0))
                        except Exception:
                            pass
                    total_tokens = summary_result.get('tokens_used', 0) or (input_tokens + output_tokens)

                    # Concrete model name (provider != model; pricing keys on model)
                    llm_model = getattr(self.llm_client, 'model_name', None) or llm_provider

                    if input_tokens > 0:
                        self.llm_input_tokens.labels(consumer_id=consumer_id_label, model=llm_model).inc(input_tokens)
                    if output_tokens > 0:
                        self.llm_output_tokens.labels(consumer_id=consumer_id_label, model=llm_model).inc(output_tokens)
                    if total_tokens > 0:
                        self.llm_tokens_used.labels(consumer_id=consumer_id_label, provider=llm_provider).inc(total_tokens)

                    # Estimated cost via pricing table (uses concrete model name)
                    estimated_cost = estimate_cost(llm_model, input_tokens, output_tokens)
                    if estimated_cost > 0:
                        self.llm_estimated_cost_usd.labels(consumer_id=consumer_id_label, model=llm_model).inc(estimated_cost)

                # Save summary to cache
                summary_file = request_dir / "summary.jsonl"
                summary_file.parent.mkdir(parents=True, exist_ok=True)
                with open(summary_file, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(summary_result, ensure_ascii=False) + '\n')
                self._logger.info(f"   💾 Summary saved to: {summary_file}")
                summary_file_path = str(summary_file)
                result['success'] = True
                result['summary_file'] = summary_file_path
                _last_exc = None
                break  # success — exit retry loop

            except Exception as e:
                _last_exc = e
                _classification = classify_error(e)
                self._logger.error(
                    f"❌ Error generating summary (attempt {_attempt}/{_classification.max_attempts},"
                    f" category={_classification.category}): {e}"
                )
                import traceback
                self._logger.error(traceback.format_exc())

                if self.telemetry_enabled and summarization_start:
                    summarization_duration = time.time() - summarization_start
                    consumer_id_label = f"summarizer_{os.getpid()}"
                    self.summarization_duration.labels(
                        consumer_id=consumer_id_label,
                        status='failure'
                    ).observe(summarization_duration)
                    # instrumentation - llm_api_errors_total
                    error_type = TAXONOMY_TO_ERROR_CATEGORY.get(
                        _classification.category, ErrorCategory.UNKNOWN
                    )
                    self.llm_api_errors.labels(
                        consumer_id=consumer_id_label,
                        error_type=error_type
                    ).inc()

                if _classification.retryable and _attempt < _classification.max_attempts:
                    backoff = min(2 ** (_attempt - 1), 8)
                    self._logger.info(f"   ↩️  Retrying in {backoff}s (attempt {_attempt+1}/{_classification.max_attempts})")
                    await asyncio.sleep(backoff)
                    continue

                # Final failure — build degraded stub if applicable
                if _classification.degrade:
                    self._logger.warning(f"   ⚠️  Degrading: emitting stub summary for {url}")
                    summary_result = {
                        'markdown_summary': f'[Summary unavailable — model returned empty response for: {url}]',
                        'metadata': metadata,
                        'query_id': query_id,
                        'degraded': True,
                    }
                    summary_file = request_dir / "summary.jsonl"
                    summary_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(summary_file, 'w', encoding='utf-8') as f:
                        f.write(json.dumps(summary_result, ensure_ascii=False) + '\n')
                    summary_file_path = str(summary_file)
                    result['success'] = True
                    result['summary_file'] = summary_file_path
                    result['degraded'] = True
                    _last_exc = None

                break  # exit retry loop — either degraded or hard failure

        # ── Always emit a terminal event ─────────────────────────────────────
        # Whether we succeeded, degraded, or hard-failed, we MUST publish
        # summary-completed so the indexer session count closes properly.
        if _last_exc is None:
            # success (or degraded success)
            await self._publish_summary_completed(
                request_id=request_id,
                session_id=session_id,
                origin=origin,
                url_index=url_index,
                total_urls=total_urls,
                document_id=None,
                source_url=url,
                summary_file=summary_file_path,
                message_data=message_data,
                status='degraded' if result.get('degraded') else 'success',
            )
            # Publish for chunking if extraction originally failed
            if extraction_failed and summary_result:
                self._logger.info("   🔄 Extraction failed but summary succeeded - publishing summary for chunking")
                await self._publish_summary_for_chunking(
                    request_id=request_id,
                    session_id=session_id,
                    origin=origin,
                    url_index=url_index,
                    total_urls=total_urls,
                    url=url,
                    summary_text=summary_result.get('markdown_summary', ''),
                    metadata=metadata,
                    title=metadata.get('title', 'Untitled'),
                    summary_metadata=summary_result.get('metadata', {}),
                    language=metadata.get('language') or 'en',
                )
        else:
            # Hard failure after all retries — publish failed terminal event
            result['error'] = str(_last_exc)
            await self._publish_summary_completed(
                request_id=request_id,
                session_id=session_id,
                origin=origin,
                url_index=url_index,
                total_urls=total_urls,
                document_id=None,
                source_url=url,
                summary_file=None,
                message_data=message_data,
                status='failed',
            )
            await self._publish_to_dlq(
                stage='summarizer',
                session_id=session_id,
                request_id=request_id,
                origin=origin,
                query_id=query_id,
                url=url,
                exc=_last_exc,
                classification=_classification,
                attempts=_attempt,
            )

        reset_correlation_ids(_tokens)
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
        language: str = 'en',
        origin: str | None = None,
        url_index: int = 0,
        total_urls: int = 1,
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
                'origin': origin,
                'url_index': url_index,
                'total_urls': total_urls,
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
            _pub_start = time.time()
            self.producer.produce(
                topic=self.output_topic,
                key=request_id.encode('utf-8'),
                value=message_bytes,
                callback=self._delivery_callback
            )

            # Flush to ensure delivery
            self.producer.poll(0)
            _pub_duration = time.time() - _pub_start

            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.output_topic, result='success', duration=_pub_duration)

            self._logger.info(f"   📤 Published summary to '{self.output_topic}' for chunking")
            self._logger.info(f"   📊 Summary length: {len(summary_text)} characters")
            self._logger.info(f"   🏷️  Data lineage: gemini_web_grounding_summary")

        except Exception as e:
            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.output_topic, result='error', duration=0)
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
        summary_file: str | None,
        message_data: Dict,
        status: str = 'success',
        origin: str | None = None,
        url_index: int = 0,
        total_urls: int = 1,
    ):
        """
        Publish summary-completed message for Summary Indexer.

        Always called — on success, degraded success, AND failure — so the
        indexer session count can always reach its total and aggregate.

        Args:
            request_id:   Request identifier (per-query UUID — aggregation key)
            session_id:   Session identifier (chat-tab UUID — opaque pass-through)
            document_id:  Ignored (generated here from source_url + timestamp)
            source_url:   Source URL
            summary_file: Path to saved summary file, or None on hard failure
            message_data: Original message data (to extract total_urls fallback)
            status:       'success' | 'degraded' | 'failed'
            origin:       Opaque origin field from inbound message (pass-through)
            url_index:    Per-URL ordinal within the request (0-based)
            total_urls:   Total URLs in the request
        """
        try:
            # Per-request aggregation — keyed on request_id, not session_id.
            # A chat session (session_id) may contain many requests; conflating
            # them would mix counts from different queries into one indexer bucket.
            if request_id not in self.request_counts:
                total = total_urls or message_data.get('total_urls') or message_data.get('total_requests', 1)
                self.request_counts[request_id] = {'count': 0, 'total': total}

            # Increment count
            self.request_counts[request_id]['count'] += 1
            current_count = self.request_counts[request_id]['count']
            total_requests = self.request_counts[request_id]['total']

            # Build timestamp-based document_id unique per research event.
            # dedup_key (md5 of normalized URL) is the deduplication handle;
            # the indexer uses it to avoid writing the same URL twice even
            # when the same URL is re-processed in a later session.
            _url_for_id = source_url or request_id
            new_document_id, dedup_key = make_document_id(
                _url_for_id, batch_index=current_count
            )

            # Create message
            message = {
                'request_id': request_id,
                'session_id': session_id,
                'origin': origin,
                'url_index': url_index,
                'total_urls': total_urls,
                'document_id': new_document_id,
                'dedup_key': dedup_key,
                'source_url': source_url,
                'summary_file': summary_file,
                'status': status,
                'current_count': current_count,
                'total_requests': total_requests,
                'completed_at': datetime.now(timezone.utc).isoformat(),
            }

            # Publish to summary-completed topic.
            # Key on request_id (per-query) for partition uniqueness — a single
            # chat session could otherwise hotspot all its URLs to one partition.
            _pub_start = time.time()
            self.producer.produce(
                topic=self.summary_completed_topic,
                key=request_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.poll(0)
            _pub_duration = time.time() - _pub_start

            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.summary_completed_topic, result='success', duration=_pub_duration)

            self._logger.info(
                f"   📤 Published to '{self.summary_completed_topic}' "
                f"({current_count}/{total_requests}) status={status}"
            )

        except Exception as e:
            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.summary_completed_topic, result='error', duration=0)
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

    async def _publish_to_dlq(
        self,
        stage: str,
        session_id: str,
        request_id: str,
        query_id: str,
        url: str,
        exc: Exception,
        classification,  # FailureClassification
        attempts: int,
        first_attempt_at: str | None = None,
        origin: str | None = None,
    ):
        """Publish a failure record to the dead-letter-queue topic."""
        try:
            now = datetime.now(timezone.utc).isoformat()
            record = {
                'stage': stage,
                'category': classification.category if classification else 'unknown',
                'session_id': session_id,
                'request_id': request_id,
                'origin': origin,
                'query_id': query_id,
                'url': url,
                'error_class': type(exc).__name__,
                'error_message': str(exc),
                'occurred_at': now,
                'attempts': attempts,
                'retryable': classification.retryable if classification else False,
                'degraded': classification.degrade if classification else False,
                'first_attempt_at': first_attempt_at or now,
                'last_attempt_at': now,
            }
            _pub_start = time.time()
            self.producer.produce(
                topic=self.dead_letter_topic,
                key=request_id.encode('utf-8') if request_id else session_id.encode('utf-8'),
                value=json.dumps(record).encode('utf-8'),
                callback=self._delivery_callback,
            )
            self.producer.poll(0)
            _pub_duration = time.time() - _pub_start

            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.dead_letter_topic, result='success', duration=_pub_duration)

            self._logger.info(
                f"   📨 Published DLQ record: stage={stage} category={record['category']}"
            )
            if self.telemetry_enabled:
                self.metrics.increment_dead_letter(self.dead_letter_topic, record['category'])
        except Exception as e:
            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.dead_letter_topic, result='error', duration=0)
            self._logger.error(f"   ❌ Error publishing to DLQ: {e}")

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
                # instrumentation - startup_timestamp_seconds
                self.metrics.record_startup()
                # instrumentation - consumer_info
                self.metrics.set_info({
                    'consumer_name': 'summarizer',
                    'consumer_group': self.kafka_config.get('group.id', 'summarizer-consumer'),
                    'input_topic': self.input_topic,
                    'output_topic': self.summary_completed_topic,
                })
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
                            # instrumentation - last_message_processed_timestamp
                            self.metrics.record_message_processed(self.input_topic)
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
                                # instrumentation - processing_failures_total
                                self.metrics.increment_failures(self.input_topic, ErrorCategory.UNKNOWN)
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
                                # instrumentation - processing_failures_total
                                self.metrics.increment_failures(self.input_topic, ErrorCategory.SUMMARIZATION_TRANSIENT)
                                if processing_start:
                                    duration = time.time() - processing_start
                                    self.metrics.record_duration(self.input_topic, duration, 'failure')

                    self._logger.info(f"📊 Batch complete: {success_count}/{len(tasks)} successful")

                    # Add small delay between batches for rate limiting
                    if tasks:
                        await asyncio.sleep(0.5)

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
