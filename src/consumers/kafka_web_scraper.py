#!/usr/bin/env python3
"""
Kafka Web Scraper Consumer

Consumes messages from Kafka containing URLs to scrape, resolves redirects,
extracts content, and stores results in cache.

Message Format:
    {
        "uri": "https://example.com/redirect",
        "title": "Example Source",
        "context": ["Context line 1", "Context line 2"],
        "session_id": "unique-session-id"
    }


Usage:
    from kafka_web_scraper import KafkaWebScraperConsumer

    consumer = KafkaWebScraperConsumer()
    consumer.start()

TODO: TELEMETRY & OBSERVABILITY
- [ ] Add Prometheus metrics (port 8001)
- [ ] Track urls_scraped_total counter
- [ ] Track scraping_duration_seconds histogram
- [ ] Track scraping_failures_total counter
- [ ] Track cache_files_total gauge
- [ ] Track cache_size_bytes gauge
- [ ] Track messages_consumed_total counter
- [ ] Track message_processing_duration_seconds histogram
- [ ] Add structured JSON logging with trace_id
- [ ] Expose /metrics endpoint on port 8001
"""

import asyncio
import io
import json
import os
import random
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from PyPDF2 import PdfReader

# Telemetry imports
try:
    from src.monitoring import (CacheMetricsCollector, KafkaMetricsWrapper,
                                MetricsServer, PrometheusMetrics,
                                SystemMetricsCollector)
    from src.monitoring.logging_config import configure_logging
    TELEMETRY_AVAILABLE = True
except ImportError as e:
    # Try relative import for direct execution
    try:
        import os
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
        from src.monitoring import (CacheMetricsCollector, KafkaMetricsWrapper,
                                    MetricsServer, PrometheusMetrics,
                                    SystemMetricsCollector)
        from src.monitoring.logging_config import configure_logging
        TELEMETRY_AVAILABLE = True
    except ImportError:
        TELEMETRY_AVAILABLE = False

try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    CLOUDSCRAPER_AVAILABLE = False

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from docling.document_converter import DocumentConverter
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    DocumentConverter = None

try:
    from playwright_stealth import stealth_async
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False

# Import components
try:
    from youtube_transcript_api import YouTubeTranscriptApi

    from ..components.document_websearcher import DocumentWebSearcher
    from ..components.url_resolver import URLResolver
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from youtube_transcript_api import YouTubeTranscriptApi

    from components.document_websearcher import DocumentWebSearcher
    from components.url_resolver import URLResolver
    from config_handler import Base


class KafkaWebScraperConsumer(Base):
    """Kafka consumer for web scraping requests"""

    def __init__(self):
        """Initialize the Kafka consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaWebScraperConsumer")

        # Load configuration
        self._load_config()

        # Initialize components
        self.url_resolver = URLResolver()

        # Initialize web searcher for content extraction
        self.web_searcher = DocumentWebSearcher()

        # Initialize YouTube transcript API
        self.youtube_api = YouTubeTranscriptApi()

        # Initialize Kafka consumer and producer
        self.consumer = None
        self.producer = None
        self.running = False

        # Initialize telemetry (Phase 1)
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"web_scraper_{os.getpid()}"

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
                port=8001,
                consumer_name='web_scraper',
                health_check=lambda: self.running,
                ready_check=lambda: self.consumer is not None and self.running
            )

            # Consumer-specific metrics
            from prometheus_client import Counter, Histogram

            self.urls_scraped = Counter(
                'kafka_consumer_urls_scraped_total',
                'Total URLs successfully scraped',
                ['consumer_id']
            )

            self.scraping_duration = Histogram(
                'kafka_consumer_scraping_duration_seconds',
                'Time spent scraping URLs',
                ['consumer_id', 'status'],
                buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
            )

            # Cache metrics — managed by CacheMetricsCollector (replaces standalone gauges)
            self.cache_metrics = CacheMetricsCollector(
                consumer_id=consumer_id,
                directories={'scrapes': str(self.cache_dir) if hasattr(self, 'cache_dir') else './cache/web_scrapes'},
                scan_interval=60,
            )

            self.scrape_strategy_total = Counter(
                'kafka_consumer_scrape_strategy_total',
                'Total URL fetches by extraction strategy',
                ['consumer_id', 'strategy', 'outcome']
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8001")
        else:
            self._logger.warning("⚠️ Telemetry not available - monitoring module not found")
            self.metrics = None
            self.system_metrics = None
            self.metrics_server = None

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
                'group.id': self._app_config.get('kafka.group_id') or 'web-scraper-consumer',
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
            self.input_topic = self._app_config.get('kafka.input_topic') or 'viperine.web-scrape-requests'
            self.output_topic = self._app_config.get('kafka.output_topic') or 'viperine.content-extracted'
            self.summarise_document_topic = self._app_config.get('kafka.summarise_document_topic') or 'viperine.summarise-document'

            # Web scraper configuration
            self.cache_dir = Path(self._app_config.get('web_scraper.cache_dir') or './cache/web_scrapes')
            self.max_urls_per_message = self._app_config.get('web_scraper.max_urls_per_message') or 50
            self.extract_markdown = self._app_config.get('web_scraper.extract_markdown') or False

            # Create cache directory
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            self._logger.info(f"✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {self.kafka_config['bootstrap.servers']}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Extract markdown: {self.extract_markdown}")

            # Initialize docling converter if markdown extraction is enabled
            if self.extract_markdown:
                if DOCLING_AVAILABLE:
                    self.markdown_converter = DocumentConverter()
                    self._logger.info("   ✅ Docling HTML-to-markdown converter initialized")
                else:
                    self._logger.warning("   ⚠️ Markdown extraction enabled but docling not available - falling back to text extraction")
                    self.extract_markdown = False

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _resolve_redirects(self, start_url: str) -> Optional[str]:
        """
        Resolve URL redirects to get the final destination URL.

        Args:
            start_url: The initial URL (may be a redirect)

        Returns:
            The final URL after all redirects, or None if resolution fails
        """
        try:
            # Include a User-Agent to prevent basic blocking
            headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

            # First, check if it's a Google redirect URL
            if 'google.com/url?' in start_url and 'q=' in start_url:
                # Extract the target URL from the query parameter
                import urllib.parse
                parsed = urllib.parse.urlparse(start_url)
                query_params = urllib.parse.parse_qs(parsed.query)

                if 'q' in query_params and query_params['q']:
                    final_url = query_params['q'][0]
                    self._logger.debug(f"   🔀 Extracted from Google redirect: {final_url}")
                    return final_url

            # For non-Google redirects, make a GET request and allow redirects
            response = requests.get(start_url, headers=headers, allow_redirects=True, timeout=10)

            # The final URL after all redirects is in response.url
            final_url = response.url

            if final_url != start_url:
                self._logger.debug(f"   🔀 Redirected: {start_url} → {final_url}")

            return final_url

        except requests.RequestException as e:
            self._logger.warning(f"   ⚠️ Error resolving redirects for {start_url}: {e}")
            return None

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _create_consumer(self):
        """Create and configure Kafka consumer and producer"""
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.input_topic])
            self._logger.info(f"✅ Consumer subscribed to topic: {self.input_topic}")

            # Create producer for publishing to output topic
            producer_config = {
                'bootstrap.servers': self.kafka_config['bootstrap.servers'],
                'message.max.bytes': 5242880,  # 5MB - must match Kafka broker message.max.bytes
                'compression.type': 'snappy'  # Compress large messages
            }
            self.producer = Producer(producer_config)
            self._logger.info(f"✅ Producer ready for topic: {self.output_topic}")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer/producer: {e}")
            raise

    def _track_url_scraped(self, url: str, duration: float, extraction_method: str):
        """
        Track metrics for successfully scraped URL.

        Args:
            url: The scraped URL
            duration: Time taken to scrape in seconds
            extraction_method: Method used (cloudscraper, playwright, curl_cffi, etc.)
        """
        if self.telemetry_enabled:
            consumer_id = f"web_scraper_{os.getpid()}"
            self.urls_scraped.labels(consumer_id=consumer_id).inc()
            self.scraping_duration.labels(consumer_id=consumer_id, status='success').observe(duration)
            if hasattr(self, 'scrape_strategy_total'):
                self.scrape_strategy_total.labels(
                    consumer_id=consumer_id, strategy=extraction_method, outcome='success'
                ).inc()
            self._logger.debug(f"📊 Metrics tracked: URL scraped in {duration:.2f}s using {extraction_method}")

    def _update_cache_metrics(self):
        """Trigger an immediate cache scan via CacheMetricsCollector."""
        if self.telemetry_enabled and hasattr(self, 'cache_metrics'):
            try:
                self.cache_metrics.collect_metrics()
            except Exception as e:
                self._logger.warning(f"⚠️ Failed to update cache metrics: {e}")

    async def _process_message(self, message_data: Dict) -> Dict:
        """
        Process a single message from Kafka.

        Args:
            message_data: The message data containing uri, title, context, session_id

        Returns:
            Processing result dictionary
        """
        session_id = message_data.get('session_id', 'unknown')
        request_id = message_data.get('request_id', f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        query_id = message_data.get('query_id', 'default')
        uri = message_data.get('uri')
        title = message_data.get('title', 'Untitled')
        context = message_data.get('context', [])

        self._logger.info("=" * 80)
        self._logger.info(f"🔄 Processing message")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   URI: {uri}")
        self._logger.info(f"   Title: {title}")
        self._logger.info(f"   Context lines: {len(context)}")

        result = {
            'session_id': session_id,
            'request_id': request_id,
            'query_id': query_id,
            'original_uri': uri,
            'title': title,
            'context': context,
            'timestamp': datetime.now().isoformat(),
            'resolved_urls': [],
            'scraped_content': [],
            'success': False,
            'error': None
        }

        # Create request-specific directory
        request_dir = self.cache_dir / request_id
        request_dir.mkdir(parents=True, exist_ok=True)
        output_file = request_dir / "extracted_linked_text.jsonl"

        try:
            # Step 1: Resolve redirects to get final URL
            self._logger.info("📍 Step 1: Resolving redirects to final URL...")
            self._logger.info(f"   🔗 Original URI: {uri}")

            final_url = self._resolve_redirects(uri)

            if final_url:
                self._logger.info(f"   ✅ Resolved to: {final_url}")
                result['resolved_urls'] = [final_url]
            else:
                # If resolution fails, use original URI
                self._logger.warning(f"   ⚠️ Could not resolve redirects, using original URI")
                final_url = uri
                result['resolved_urls'] = [uri]

            # Step 2: Check if this is a YouTube URL and extract transcript if available
            # Support multiple YouTube URL patterns: /watch, /live, /shorts, youtu.be
            is_youtube = ('youtube.com/watch' in final_url or
                         'youtube.com/live' in final_url or
                         'youtube.com/shorts' in final_url or
                         'youtu.be/' in final_url)

            if is_youtube:
                self._logger.info("📹 Detected YouTube URL - attempting transcript extraction...")
                transcript_result = self._extract_youtube_transcript(final_url, request_dir, session_id, request_id, uri, title, message_data)
                print(f"TRANSCRIPT: {transcript_result}")

                if transcript_result['success']:
                    self._logger.info(f"   ✅ Successfully extracted YouTube transcript")
                    self._publish_to_kafka(transcript_result['extraction_result'])

                    result['scraped_content'] = [transcript_result['extraction_result']]
                    result['content_count'] = 1
                    result['success'] = True
                    result['extraction_method'] = 'youtube_transcript'

                    return result
                else:
                    self._logger.warning(f"   ⚠️ YouTube transcript extraction failed: {transcript_result['error']}")
                    self._logger.info("   📄 Falling back to standard web scraping...")

            # Step 3: Extract content directly from the resolved URL (non-YouTube or fallback)
            self._logger.info("📍 Step 3: Extracting content from resolved URL...")

            # Determine extraction strategy based on URL
            # Priority: 1. Cloudscraper (best for Cloudflare), 2. Playwright (JS-heavy sites), 3. curl_cffi (fallback)
            needs_cloudflare_bypass = any(domain in final_url for domain in ['alarabiya.net', 'gov.il', 'jns.org', 'inquirer.net'])
            use_js_rendering = any(domain in final_url for domain in ['youtube.com', 'javascript'])

            extraction_attempts = []

            # Strategy 1: Try cloudscraper for sites with Cloudflare protection
            if needs_cloudflare_bypass and CLOUDSCRAPER_AVAILABLE:
                self._logger.info("   🛡️ Attempting Cloudflare bypass with cloudscraper...")
                cloudscraper_start_time = time.time()
                try:
                    text_content, metadata = self._extract_with_cloudscraper(final_url)
                    # Add query_id to metadata for downstream propagation
                    metadata['query_id'] = result.get('query_id', 'default')

                    if text_content and len(text_content) > 500:  # Minimum content threshold
                        self._logger.debug(f"   📊 Extracted {len(text_content)} characters with cloudscraper")

                        # Check if this is actually a Cloudflare bot page
                        page_title = metadata.get('title', '')
                        if self._is_cloudflare_bot_page(text_content, page_title):
                            self._logger.warning(f"   🤖 Detected Cloudflare bot page - extraction blocked")
                            extraction_attempts.append("cloudscraper: Cloudflare bot page detected")
                            raise Exception("Cloudscraper extracted Cloudflare challenge page, not actual content")

                        # Validate extracted content
                        if not self._is_extraction_valid(text_content):
                            self._logger.warning(f"   ⚠️ Cloudscraper extracted insufficient content (< 10 words)")
                            extraction_attempts.append("cloudscraper: Insufficient content")
                            raise Exception("Cloudscraper extracted insufficient content")

                        extraction_result = {
                            'url': final_url,
                            'original_url': uri,
                            'text': text_content,
                            'metadata': metadata,
                            'extracted_at': datetime.now().isoformat(),
                            'session_id': session_id,
                            'request_id': request_id,
                            'extraction_method': 'cloudscraper'
                        }
                        if 'total_requests' in message_data:
                            extraction_result['total_requests'] = message_data['total_requests']

                        self._save_extraction(extraction_result, output_file)
                        self._logger.info(f"   ✅ Successfully extracted with cloudscraper")
                        self._publish_to_kafka(extraction_result)

                        # Track metrics
                        scrape_duration = time.time() - cloudscraper_start_time
                        self._track_url_scraped(final_url, scrape_duration, 'cloudscraper')
                        self._update_cache_metrics()

                        result['scraped_content'] = [extraction_result]
                        result['content_count'] = 1
                        result['success'] = True

                except Exception as e:
                    self._logger.warning(f"   ⚠️ Cloudscraper extraction failed: {e}")
                    extraction_attempts.append(f"cloudscraper: {str(e)}")

            # Strategy 2: Try JavaScript rendering with enhanced Playwright stealth
            if not result['success'] and (use_js_rendering or needs_cloudflare_bypass) and PLAYWRIGHT_AVAILABLE:
                self._logger.info("   🌐 Attempting JavaScript rendering with enhanced stealth...")
                playwright_start_time = time.time()
                try:
                    text_content, metadata = await self._extract_with_playwright(final_url)
                    # Add query_id to metadata for downstream propagation
                    metadata['query_id'] = result.get('query_id', 'default')

                    if text_content and text_content != "No readable content found" and self._is_extraction_valid(text_content):
                        self._logger.debug(f"   📊 Extracted {len(text_content)} characters with Playwright")

                        # Check if this is actually a Cloudflare bot page
                        page_title = metadata.get('title', '')
                        if self._is_cloudflare_bot_page(text_content, page_title):
                            self._logger.warning(f"   🤖 Detected Cloudflare bot page - extraction blocked")
                            extraction_attempts.append("playwright: Cloudflare bot page detected")
                            raise Exception("Playwright extracted Cloudflare challenge page, not actual content")

                        # Validate extracted content
                        if not self._is_extraction_valid(text_content):
                            self._logger.warning(f"   ⚠️ Playwright extracted insufficient content (< 10 words)")
                            extraction_attempts.append("playwright: Insufficient content")
                            raise Exception("Playwright extracted insufficient content")

                        extraction_result = {
                            'url': final_url,
                            'original_url': uri,
                            'text': text_content,
                            'metadata': metadata,
                            'extracted_at': datetime.now().isoformat(),
                            'session_id': session_id,
                            'request_id': request_id,
                            'extraction_method': 'playwright'
                        }
                        if 'total_requests' in message_data:
                            extraction_result['total_requests'] = message_data['total_requests']

                        self._save_extraction(extraction_result, output_file)
                        self._logger.info(f"   ✅ Successfully extracted with Playwright")
                        self._publish_to_kafka(extraction_result)

                        # Track metrics
                        scrape_duration = time.time() - playwright_start_time
                        self._track_url_scraped(final_url, scrape_duration, 'playwright')
                        self._update_cache_metrics()

                        result['scraped_content'] = [extraction_result]
                        result['content_count'] = 1
                        result['success'] = True

                except Exception as e:
                    self._logger.warning(f"   ⚠️ Playwright extraction failed: {e}")
                    extraction_attempts.append(f"playwright: {str(e)}")

            # Strategy 3: Fallback to standard curl_cffi scraping
            if not result['success']:
                self._logger.info("   📡 Using standard HTTP request (curl_cffi)...")
                # Fetch the page using curl_cffi session
                scrape_start_time = time.time()
                try:
                    response = self.web_searcher.session.get(final_url, timeout=15)

                    if response.status_code == 200:
                        # Check if this is a PDF based on content-type or URL
                        content_type = response.headers.get('content-type', '').lower()
                        is_pdf = 'application/pdf' in content_type or final_url.lower().endswith('.pdf')

                        if is_pdf:
                            # Extract text from PDF
                            self._logger.info("   📄 Detected PDF - extracting text...")
                            text_content, metadata = self._extract_pdf_text(response.content, final_url)
                            # Add query_id to PDF metadata for downstream propagation
                            metadata['query_id'] = result.get('query_id', 'default')
                        else:
                            # Get HTML content
                            html_content = response.content.decode('utf-8', errors='ignore')
                            soup = BeautifulSoup(html_content, 'html.parser')

                            # Extract metadata
                            metadata = self.web_searcher._extract_page_metadata(soup)

                            # Add query_id to metadata for downstream propagation
                            metadata['query_id'] = result.get('query_id', 'default')

                            # Try markdown conversion first if enabled
                            text_content = None
                            if self.extract_markdown:
                                text_content = await self._convert_html_to_markdown(html_content, final_url)
                                if text_content:
                                    metadata['extraction_format'] = 'markdown'

                            # Fall back to plain text extraction if markdown failed or disabled
                            if not text_content:
                                text_content = self._extract_full_text_content(soup)
                                metadata['extraction_format'] = 'plain_text'

                        self._logger.debug(f"   📊 Extracted {len(text_content)} characters of text content")

                        # Validate extracted content
                        if not self._is_extraction_valid(text_content):
                            self._logger.warning(f"   ⚠️ Extracted content is invalid or insufficient (< 10 words)")
                            error_msg = f"Insufficient content - {len(text_content.split())} words (minimum 10 required)"
                            extraction_attempts.append(f"curl_cffi: {error_msg}")
                            raise Exception(error_msg)

                        # Check for low-quality content (sidebar/navigation fragments)
                        quality_check = self._check_content_quality(text_content, metadata.get('title', ''))
                        if not quality_check['is_quality']:
                            self._logger.warning(f"   ⚠️ Low-quality content detected: {quality_check['reason']}")
                            self._logger.info(f"   🔄 Triggering summary fallback for better content extraction")
                            # Treat as failed extraction to trigger summary-based recovery
                            error_msg = f"Low-quality content: {quality_check['reason']}"
                            extraction_attempts.append(f"curl_cffi: {error_msg}")
                            raise Exception(error_msg)

                        # Create extraction result
                        extraction_result = {
                            'url': final_url,
                            'original_url': uri,
                            'text': text_content,
                            'metadata': metadata,
                            'extracted_at': datetime.now().isoformat(),
                            'session_id': session_id,
                            'request_id': request_id,
                            'query_id': result.get('query_id', 'default'),
                            'extraction_method': 'pdf_extraction' if is_pdf else 'curl_cffi'
                        }
                        if 'total_requests' in message_data:
                            extraction_result['total_requests'] = message_data['total_requests']

                        # Save to output file (append mode)
                        self._save_extraction(extraction_result, output_file)
                        self._logger.info(f"   ✅ Successfully extracted with {'PDF extraction' if is_pdf else 'curl_cffi'}")

                        # Publish to Kafka for downstream processing (summarization)
                        self._publish_to_kafka(extraction_result)

                        # Track metrics
                        scrape_duration = time.time() - scrape_start_time
                        self._track_url_scraped(final_url, scrape_duration, 'pdf_extraction' if is_pdf else 'curl_cffi')
                        self._update_cache_metrics()

                        result['scraped_content'] = [extraction_result]
                        result['content_count'] = 1
                        result['success'] = True
                    else:
                        # Create failure document with all attempted methods
                        error_msg = f"HTTP {response.status_code} - All methods failed"
                        if extraction_attempts:
                            error_msg += f" (Attempted: {', '.join(extraction_attempts)})"
                        result['error'] = error_msg
                        self._logger.warning(f"   ❌ {error_msg}")
                        if self.telemetry_enabled and hasattr(self, 'scrape_strategy_total'):
                            self.scrape_strategy_total.labels(
                                consumer_id=f"web_scraper_{os.getpid()}",
                                strategy='all',
                                outcome='failure'
                            ).inc()
                        self._create_failure_document(request_dir, final_url, error_msg, response.status_code, session_id, request_id)

                        # Still attempt to generate summary from URL and title
                        self._publish_failed_url_for_summary(final_url, uri, title, context, error_msg, session_id, request_id, result.get('query_id', 'default'))

                except Exception as e:
                    error_msg = f"Error fetching URL: {str(e)}"
                    if extraction_attempts:
                        error_msg += f" (Previously attempted: {', '.join(extraction_attempts)})"
                    result['error'] = error_msg
                    self._logger.error(f"   ❌ {error_msg}")
                    self._create_failure_document(request_dir, final_url, error_msg, None, session_id, request_id)

                    # Still attempt to generate summary from URL and title
                    self._publish_failed_url_for_summary(final_url, uri, title, context, error_msg, session_id, request_id, result.get('query_id', 'default'))

        except Exception as e:
            result['error'] = str(e)
            self._logger.error(f"❌ Error processing message: {e}")

        self._logger.info("=" * 80)
        return result

    def _extract_youtube_transcript(
        self,
        youtube_url: str,
        request_dir: Path,
        session_id: str,
        request_id: str,
        original_uri: str,
        title: str,
        message_data: Dict
    ) -> Dict[str, Any]:
        """
        Extract YouTube video transcript using youtube-transcript-api.

        Args:
            youtube_url: YouTube video URL
            request_dir: Request-specific directory for storing files
            session_id: Session ID
            request_id: Request ID
            original_uri: Original URI from the message
            title: Video title

        Returns:
            Dict with 'success', 'error', and optionally 'extraction_result'
        """
        try:
            # Extract video ID from URL
            import re
            video_id = None

            # Match multiple YouTube URL patterns: /watch?v=, /live/, /shorts/, youtu.be/
            match = re.search(r'(?:youtube\.com/(?:watch\?v=|live/|shorts/)|youtu\.be/)([a-zA-Z0-9_-]{11})', youtube_url)
            if match:
                video_id = match.group(1)

            if not video_id:
                return {
                    'success': False,
                    'error': 'Could not extract video ID from YouTube URL'
                }

            self._logger.info(f"   📹 Video ID: {video_id}")

            # Get rich metadata from yt-dlp
            self._logger.info("   📊 Fetching video metadata via yt-dlp...")
            import json
            import subprocess

            try:
                result = subprocess.run(
                    ['yt-dlp', '--dump-json', '--no-download', youtube_url],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    yt_metadata = json.loads(result.stdout)
                    # Extract key fields
                    title = yt_metadata.get('title', title or 'YouTube Video')
                    channel = yt_metadata.get('channel', 'Unknown')
                    upload_date = yt_metadata.get('upload_date', '')
                    duration = yt_metadata.get('duration', 0)
                    view_count = yt_metadata.get('view_count', 0)
                    description = yt_metadata.get('description', '')

                    # Format upload date if available (YYYYMMDD -> YYYY-MM-DD)
                    if upload_date and len(upload_date) == 8:
                        upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"

                    self._logger.info(f"   ✅ Retrieved metadata: {title} by {channel}")
                else:
                    self._logger.warning(f"   ⚠️ yt-dlp metadata fetch failed, using basic metadata")
                    channel = 'Unknown'
                    upload_date = ''
                    duration = 0
                    view_count = 0
                    description = ''
            except Exception as e:
                self._logger.warning(f"   ⚠️ Could not fetch yt-dlp metadata: {e}")
                title = title or 'YouTube Video'
                channel = 'Unknown'
                upload_date = ''
                duration = 0
                view_count = 0
                description = ''

            # Fetch transcript using youtube-transcript-api
            self._logger.info("   ⬇️ Fetching transcript via YouTube Transcript API...")

            try:
                transcript = self.youtube_api.fetch(video_id, languages=['en', 'en-US', 'en-GB'])
                snippets = transcript.snippets

                if not snippets:
                    return {
                        'success': False,
                        'error': 'No transcript snippets found'
                    }

                self._logger.info(f"   ✅ Retrieved {len(snippets)} transcript segments")

            except Exception as e:
                self._logger.warning(f"   ⚠️ Could not fetch transcript: {e}")
                return {
                    'success': False,
                    'error': f'Transcript not available: {str(e)}'
                }

            # Convert transcript to markdown
            self._logger.info("   📝 Converting transcript to markdown...")
            transcript_text = self._convert_transcript_to_markdown(
                snippets, video_id, title, channel, upload_date, duration
            )

            if not transcript_text:
                return {
                    'success': False,
                    'error': 'Failed to convert transcript to markdown'
                }

            self._logger.info(f"   📄 Generated {len(transcript_text)} characters of transcript")

            # Create metadata with rich YouTube information
            metadata = {
                'title': title,
                'url': youtube_url,
                'video_id': video_id,
                'channel': channel,
                'upload_date': upload_date,
                'duration': duration,
                'view_count': view_count,
                'description': description[:500] if description else '',  # Limit description length
                'source': 'YouTube',
                'language': transcript.language_code,
                'is_generated': transcript.is_generated,
                'segment_count': len(snippets),
                'extraction_method': 'youtube_transcript_api',
                'query_id': message_data.get('query_id', 'default')
            }

            # Create extraction result
            extraction_result = {
                'url': youtube_url,
                'original_url': original_uri,
                'text': transcript_text,
                'metadata': metadata,
                'extracted_at': datetime.now().isoformat(),
                'session_id': session_id,
                'request_id': request_id,
                'query_id': message_data.get('query_id', 'default'),
                'extraction_method': 'youtube_transcript_api'
            }
            if 'total_requests' in message_data:
                extraction_result['total_requests'] = message_data['total_requests']

            # Save to output file
            output_file = request_dir / "extracted_linked_text.jsonl"
            self._save_extraction(extraction_result, output_file)

            return {
                'success': True,
                'extraction_result': extraction_result
            }

        except Exception as e:
            self._logger.error(f"   ❌ YouTube transcript extraction error: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _convert_transcript_to_markdown(self, snippets, video_id: str, title: str,
                                       channel: str = '', upload_date: str = '',
                                       duration: int = 0) -> str:
        """
        Convert YouTube transcript snippets to clean, readable text.

        Args:
            snippets: List of transcript snippets from youtube-transcript-api
            video_id: YouTube video ID
            title: Video title
            channel: Channel name
            upload_date: Upload date
            duration: Video duration in seconds

        Returns:
            Clean, formatted transcript text suitable for indexing and search
        """
        self._logger.error(f"🚨🚨🚨 _convert_transcript_to_markdown CALLED with {len(snippets)} snippets 🚨🚨🚨")
        import re

        # Collect all text segments
        text_segments = []

        # Log first few raw snippets for debugging
        self._logger.info(f"   📝 Raw transcript snippets (first 5):")
        for i, snippet in enumerate(snippets[:5]):
            self._logger.info(f"      [{i}] {repr(snippet.text)}")

        for snippet in snippets:
            # Get the raw text
            text = snippet.text

            # Remove music notation symbols (♪)
            text = text.replace('♪', '')

            # Remove VTT-style tags and formatting
            text = re.sub(r'<[^>]+>', '', text)

            # Remove extra whitespace and newlines within segments
            text = re.sub(r'\s+', ' ', text)

            # Strip leading/trailing whitespace
            text = text.strip()

            if text:
                text_segments.append(text)

        # Log first few processed segments
        self._logger.info(f"   📝 Processed segments (first 5):")
        for i, seg in enumerate(text_segments[:5]):
            self._logger.info(f"      [{i}] {repr(seg)}")

        # Join all segments with spaces
        full_text = ' '.join(text_segments)
        self._logger.info(f"   📝 Joined text (first 500 chars): {repr(full_text[:500])}")

        # # Clean up the joined text
        # # Fix spacing around punctuation
        # full_text = re.sub(r'\s+([.,!?;:])', r'\1', full_text)

        # self._logger.info(f"   📝 After punctuation fix (first 500 chars): {repr(full_text[:500])}")

        # # Ensure proper sentence spacing
        # full_text = re.sub(r'([.!?])\s*', r'\1 ', full_text)

        # self._logger.info(f"   📝 After sentence spacing (first 500 chars): {repr(full_text[:500])}")

        # # Remove any remaining multiple spaces
        # full_text = re.sub(r'\s+', ' ', full_text)

        # # Remove special brackets like [♪♪♪]
        # full_text = re.sub(r'\[[^\]]*\]', '', full_text)

        # # Clean up any double spaces created by removals
        # full_text = re.sub(r'\s+', ' ', full_text).strip()

        self._logger.info(f"   📝 Final cleaned text (first 500 chars): {repr(full_text[:500])}")

        # Add header with rich metadata
        header = f"# YouTube Video Transcript\n\n"
        header += f"**Title:** {title}\n\n"
        if channel:
            header += f"**Channel:** {channel}\n\n"
        if upload_date:
            header += f"**Upload Date:** {upload_date}\n\n"
        if duration:
            # Format duration as MM:SS or HH:MM:SS
            hours = duration // 3600
            minutes = (duration % 3600) // 60
            seconds = duration % 60
            if hours > 0:
                duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                duration_str = f"{minutes}:{seconds:02d}"
            header += f"**Duration:** {duration_str}\n\n"
        header += f"**Video ID:** {video_id}\n\n"
        header += f"**Source:** YouTube Transcript API\n\n"
        header += "---\n\n"

        final_markdown = header + full_text
        self._logger.info(f"   📄 Final markdown (first 800 chars): {repr(final_markdown[:800])}")

        return final_markdown

    def _is_cloudflare_bot_page(self, text_content: str, title: str) -> bool:
        """
        Detect if extracted content is actually a Cloudflare challenge/bot page.

        Args:
            text_content: The extracted text content
            title: The page title

        Returns:
            True if this appears to be a Cloudflare bot page, False otherwise
        """
        cloudflare_indicators = [
            'just a moment',
            'checking your browser',
            'please wait while we check your browser',
            'cloudflare',
            'enable javascript and cookies',
            'security check',
            'completing the captcha',
            'ray id:',
            'cf-ray',
            'cloudflare ray id'
        ]

        text_lower = text_content.lower()
        title_lower = title.lower()

        # Check title
        if any(indicator in title_lower for indicator in ['just a moment', 'attention required']):
            return True

        # Check if content is suspiciously short and contains Cloudflare keywords
        if len(text_content) < 1000:  # Less than 1KB is suspicious
            indicator_count = sum(1 for indicator in cloudflare_indicators if indicator in text_lower)
            if indicator_count >= 2:  # Multiple indicators = likely bot page
                return True

        return False

    def _extract_pdf_text(self, pdf_content: bytes, url: str) -> tuple[str, Dict]:
        """
        Extract text from PDF content using PyPDF2.

        NOTE: This is a temporary patch. Will be replaced with a call to a
        dedicated PDF extraction service for better quality extraction.

        Args:
            pdf_content: Raw PDF bytes
            url: Source URL for metadata

        Returns:
            Tuple of (extracted_text, metadata)
        """
        try:
            # Create PDF reader from bytes
            pdf_file = io.BytesIO(pdf_content)
            pdf_reader = PdfReader(pdf_file)

            # Extract metadata
            pdf_info = pdf_reader.metadata
            metadata = {
                'title': pdf_info.get('/Title', 'PDF Document') if pdf_info else 'PDF Document',
                'author': pdf_info.get('/Author', '') if pdf_info else '',
                'subject': pdf_info.get('/Subject', '') if pdf_info else '',
                'creator': pdf_info.get('/Creator', '') if pdf_info else '',
                'num_pages': len(pdf_reader.pages),
                'content_type': 'application/pdf',
                'source_url': url
            }

            # Extract text from all pages
            text_parts = []
            for page_num, page in enumerate(pdf_reader.pages, 1):
                try:
                    page_text = page.extract_text()
                    if page_text.strip():
                        text_parts.append(f"--- Page {page_num} ---\n{page_text}")
                except Exception as e:
                    self._logger.warning(f"   ⚠️ Could not extract text from page {page_num}: {e}")
                    continue

            extracted_text = "\n\n".join(text_parts)

            if not extracted_text.strip():
                extracted_text = "No text could be extracted from PDF"
                self._logger.warning("   ⚠️ PDF appears to be image-based or encrypted")

            self._logger.info(f"   ✅ Extracted {len(extracted_text)} characters from {len(pdf_reader.pages)} pages")

            return extracted_text, metadata

        except Exception as e:
            self._logger.error(f"   ❌ PDF extraction failed: {e}")
            return f"PDF extraction error: {str(e)}", {'error': str(e), 'source_url': url}

    def _filter_html_to_main_content(self, html_content: str) -> str:
        """
        Lightweight filtering of HTML before markdown conversion.
        Removes non-content elements (headers, footers, nav, etc.) while preserving main content.

        Args:
            html_content: Raw HTML content

        Returns:
            Filtered HTML focused on main content
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Step 1: Remove structural elements that rarely contain content
            for element in soup.find_all(['script', 'style', 'iframe', 'noscript']):
                element.decompose()

            # Step 2: Remove navigation, headers, and footers
            for element in soup.find_all(['nav', 'header', 'footer']):
                element.decompose()

            # Step 3: Remove elements with specific classes/ids (case-insensitive patterns)
            unwanted_patterns = [
                'advertisement', 'ad-', 'ads-', 'banner', 'promo',
                'sidebar', 'widget', 'related-posts',
                'social-share', 'share-buttons', 'social-media',
                'newsletter', 'subscribe', 'subscription',
                'comment', 'feedback', 'rating',
                'cookie', 'gdpr', 'consent',
                'breadcrumb', 'pagination'
            ]

            for element in soup.find_all(True):  # Find all elements
                # Check class attribute
                element_classes = element.get('class', [])
                if any(any(pattern.lower() in str(cls).lower() for pattern in unwanted_patterns)
                       for cls in element_classes):
                    element.decompose()
                    continue

                # Check id attribute
                element_id = element.get('id', '')
                if element_id and any(pattern.lower() in element_id.lower() for pattern in unwanted_patterns):
                    element.decompose()

            # Step 4: Try to focus on main content area
            # Support both modern HTML5 and legacy HTML patterns
            main_content = None
            content_selectors = [
                # Modern HTML5 semantic elements
                'main',
                'article',
                '[role="main"]',
                # Common class-based patterns (legacy and modern)
                '.main-content',
                '.main',
                '.content',
                '.post-content',
                '.entry-content',
                '.article-content',
                '.article-body',
                '.post-body',
                '.page-content',
                # Common id-based patterns (legacy)
                '#main-content',
                '#main',
                '#content',
                '#article',
                '#post',
                '#primary',
                # Table-based layouts (very old sites)
                'table.content',
                'td.content',
                # Div-based layouts (common in 2000s-2010s)
                'div.main',
                'div.wrapper',
                'div.container > div.content',
                # Fallback to body if nothing found
                'body'
            ]

            for selector in content_selectors:
                try:
                    main_content = soup.select_one(selector)
                    if main_content:
                        self._logger.debug(f"   🎯 Found main content with selector: {selector}")
                        break
                except Exception:
                    continue

            # If we found a main content area, use it; otherwise use the whole soup
            if main_content and main_content.name != 'body':
                filtered_html = str(main_content)
            else:
                filtered_html = str(soup)

            self._logger.debug(f"   ✂️ Filtered HTML: {len(html_content)} → {len(filtered_html)} chars")
            return filtered_html

        except Exception as e:
            self._logger.warning(f"   ⚠️ HTML filtering failed: {e}, using original HTML")
            return html_content

    async def _convert_html_to_markdown(self, html_content: str, url: str) -> Optional[str]:
        """
        Convert HTML content to markdown using docling (async to avoid blocking).

        This preserves document structure (headings, tables, lists, paragraphs)
        which is useful for chunking and filtering.

        Args:
            html_content: HTML content to convert
            url: Source URL (for logging)

        Returns:
            Markdown content or None if conversion fails
        """
        if not self.extract_markdown or not DOCLING_AVAILABLE:
            return None

        try:
            self._logger.info(f"   📝 Filtering HTML and converting to markdown (async)...")

            # Filter HTML before markdown conversion to remove non-content elements
            filtered_html = self._filter_html_to_main_content(html_content)

            # Use asyncio to run blocking docling conversion in thread pool
            # This prevents blocking the event loop
            loop = asyncio.get_event_loop()
            markdown_content = await loop.run_in_executor(
                None,  # Use default executor
                self._do_markdown_conversion,
                filtered_html
            )

            if markdown_content:
                self._logger.info(f"   ✅ Converted to {len(markdown_content)} chars of markdown")
            return markdown_content

        except Exception as e:
            self._logger.warning(f"   ⚠️ Markdown conversion failed: {e}")
            return None

    def _do_markdown_conversion(self, html_content: str) -> Optional[str]:
        """
        Blocking markdown conversion operation (runs in thread pool).

        Args:
            html_content: HTML content to convert

        Returns:
            Markdown content or None
        """
        import os
        import tempfile

        # Save HTML to temporary file (docling requires file input)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(html_content)
            tmp_path = tmp_file.name

        try:
            # Convert HTML using docling
            result = self.markdown_converter.convert(tmp_path)
            markdown_content = result.document.export_to_markdown()
            return markdown_content
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _extract_full_text_content(self, soup: BeautifulSoup) -> str:
        """
        Extract full text content from webpage (not truncated).

        Args:
            soup: BeautifulSoup parsed HTML

        Returns:
            Full text content from the page
        """
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()

        # Try to find main content areas first
        content_selectors = [
            'main', 'article', '.content', '#content',
            '.post-content', '.entry-content', '.article-body'
        ]

        text_content = ""
        for selector in content_selectors:
            element = soup.select_one(selector)
            if element:
                text_content = element.get_text()
                break

        # Fallback to body text if no main content found
        if not text_content:
            body = soup.find('body')
            text_content = body.get_text() if body else soup.get_text()

        # Clean the text - remove extra whitespace and normalize
        lines = (line.strip() for line in text_content.splitlines())
        text_content = '\n'.join(line for line in lines if line)

        return text_content if text_content else "No readable content found"

    def _extract_with_cloudscraper(self, url: str) -> tuple[str, Dict]:
        """
        Extract content from Cloudflare-protected pages using cloudscraper.

        This method automatically solves Cloudflare challenges (JavaScript challenges,
        IUAM, etc.) and returns the final page content.

        Args:
            url: The URL to extract from

        Returns:
            Tuple of (text_content, metadata)
        """
        # Create cloudscraper session with browser impersonation
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'linux',
                'desktop': True
            },
            delay=random.uniform(3, 7)  # Random delay to mimic human behavior
        )

        # Set realistic headers
        scraper.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })

        try:
            # Make request (cloudscraper automatically handles Cloudflare challenges)
            response = scraper.get(url, timeout=30)

            if response.status_code == 200:
                # Parse HTML
                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract metadata
                metadata = {
                    'title': soup.find('title').get_text(strip=True) if soup.find('title') else 'No title found',
                    'url': url
                }

                # Try to get meta tags
                for meta_name in ['author', 'description', 'keywords']:
                    meta_tag = soup.find('meta', attrs={'name': meta_name})
                    if meta_tag and meta_tag.get('content'):
                        metadata[meta_name] = meta_tag.get('content')
                    else:
                        metadata[meta_name] = None

                # Open Graph metadata
                og_title = soup.find('meta', property='og:title')
                if og_title:
                    metadata['og_title'] = og_title.get('content')

                og_description = soup.find('meta', property='og:description')
                if og_description:
                    metadata['description'] = og_description.get('content')

                # Article metadata
                published_time = soup.find('meta', property='article:published_time')
                if published_time:
                    metadata['published_date'] = published_time.get('content')
                else:
                    metadata['published_date'] = None

                modified_time = soup.find('meta', property='article:modified_time')
                if modified_time:
                    metadata['modified_date'] = modified_time.get('content')
                else:
                    metadata['modified_date'] = None

                # Language
                html_lang = soup.find('html')
                metadata['language'] = html_lang.get('lang', 'en') if html_lang else 'en'

                # Extract text content
                text_content = self._extract_full_text_content(soup)

                return text_content, metadata
            else:
                raise Exception(f"HTTP {response.status_code}")

        except Exception as e:
            raise Exception(f"Cloudscraper failed: {str(e)}")

    async def _extract_with_playwright(self, url: str) -> tuple[str, Dict]:
        """
        Extract content from JavaScript-rendered pages using Playwright with enhanced stealth.

        Implements advanced anti-detection techniques:
        - Removes automation indicators
        - Mimics human-like behavior (random delays, mouse movements)
        - Handles cookie consents and Cloudflare challenges
        - Uses realistic browser fingerprinting

        Args:
            url: The URL to extract from

        Returns:
            Tuple of (text_content, metadata)
        """
        async with async_playwright() as p:
            # Launch browser with maximum stealth configuration
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-infobars',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--window-size=1920,1080'
                ]
            )

            # Create context with realistic settings and permissions
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-US',
                timezone_id='America/New_York',
                geolocation={'longitude': -74.0060, 'latitude': 40.7128},  # New York coordinates
                permissions=['geolocation'],
                accept_downloads=False,
                has_touch=False,
                is_mobile=False,
                java_script_enabled=True,
                bypass_csp=True,
                ignore_https_errors=True,
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1'
                }
            )

            page = await context.new_page()

            # Apply stealth if available
            if STEALTH_AVAILABLE:
                await stealth_async(page)

            # Additional stealth: Remove webdriver property and other automation indicators
            await page.add_init_script("""
                // Overwrite the `navigator.webdriver` property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // Overwrite the `plugins` property to fake real plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Overwrite the `languages` property
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });

                // Mock chrome object
                window.chrome = {
                    runtime: {}
                };

                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)

            try:
                # Set cookies to accept consent (common cookie names)
                domain = url.split('/')[2].replace('www.', '')
                cookies = [
                    {'name': 'cookie_consent', 'value': 'accepted', 'domain': domain, 'path': '/'},
                    {'name': 'cookies_accepted', 'value': 'true', 'domain': domain, 'path': '/'},
                    {'name': 'gdpr_consent', 'value': 'true', 'domain': domain, 'path': '/'},
                    {'name': 'cookieconsent_status', 'value': 'allow', 'domain': domain, 'path': '/'},
                    {'name': 'euconsent-v2', 'value': 'accepted', 'domain': domain, 'path': '/'},
                ]
                await context.add_cookies(cookies)

                # Human-like delay before navigation (2-5 seconds)
                await page.wait_for_timeout(random.randint(2000, 5000))

                # Navigate with realistic timeout
                await page.goto(url, wait_until='domcontentloaded', timeout=45000)

                # Wait a bit for page to stabilize and Turnstile iframe to load
                await page.wait_for_timeout(random.randint(3000, 5000))

                # Check for Cloudflare Turnstile checkbox challenge
                self._logger.debug("   🔍 Checking for Cloudflare Turnstile challenge...")

                # Check page title and content for Cloudflare indicators
                page_title = await page.title()
                page_content = await page.content()

                is_cloudflare_challenge = (
                    "just a moment" in page_title.lower() or
                    "cloudflare" in page_content.lower() or
                    "captcha" in page_content.lower() or
                    "security check" in page_content.lower()
                )

                if is_cloudflare_challenge:
                    self._logger.info(f"   🛡️ Detected Cloudflare challenge page (title: {page_title})")

                    # Wait longer for iframe to potentially load
                    await page.wait_for_timeout(5000)

                    # Get all iframes and log them
                    all_iframes = await page.query_selector_all('iframe')
                    self._logger.debug(f"   Found {len(all_iframes)} iframe(s) on page")

                    for idx, iframe_element in enumerate(all_iframes):
                        try:
                            iframe_src = await iframe_element.get_attribute('src')
                            iframe_id = await iframe_element.get_attribute('id')
                            iframe_title = await iframe_element.get_attribute('title')
                            self._logger.debug(f"   iframe[{idx}]: src={iframe_src}, id={iframe_id}, title={iframe_title}")
                        except:
                            pass

                    # Try to find Turnstile iframe with broader selectors
                    challenge_frame = None
                    frames = page.frames

                    self._logger.debug(f"   Checking {len(frames)} frame(s)")
                    for frame in frames:
                        frame_url = frame.url
                        frame_name = frame.name
                        self._logger.debug(f"   Frame: url={frame_url[:100] if frame_url else 'about:blank'}, name={frame_name}")

                        # Check if this frame is related to Cloudflare/Turnstile
                        if frame_url and any(keyword in frame_url.lower() for keyword in [
                            'cloudflare', 'turnstile', 'challenges', 'captcha', 'cf-', 'chl'
                        ]):
                            challenge_frame = frame
                            self._logger.info(f"   ✅ Found Cloudflare challenge frame: {frame_url[:80]}...")
                            break

                    if challenge_frame:
                        try:
                            # Human-like delay
                            await page.wait_for_timeout(random.randint(3000, 5000))

                            # Try comprehensive list of checkbox/button selectors
                            click_selectors = [
                                'input[type="checkbox"]',
                                'input[type="checkbox"][id*="challenge"]',
                                'input[type="checkbox"][id*="cf"]',
                                'label',
                                'button',
                                'div[role="checkbox"]',
                                'span[role="checkbox"]',
                                'div[role="button"]',
                                'div.ctp-checkbox',
                                'label.ctp-checkbox-label',
                                '#challenge-stage',
                                'body',  # Last resort - click anywhere in the frame
                            ]

                            clicked = False
                            for selector in click_selectors:
                                try:
                                    element = await challenge_frame.query_selector(selector)
                                    if element:
                                        is_visible = await element.is_visible()
                                        self._logger.debug(f"   Found {selector}, visible={is_visible}")

                                        if is_visible or selector == 'body':
                                            # Get position and move mouse
                                            box = await element.bounding_box()
                                            if box:
                                                target_x = box['x'] + box['width']/2
                                                target_y = box['y'] + box['height']/2
                                                await page.mouse.move(target_x, target_y, steps=random.randint(15, 30))
                                                await page.wait_for_timeout(random.randint(500, 1200))

                                            await element.click()
                                            self._logger.info(f"   ✅ Clicked element in challenge frame: {selector}")
                                            clicked = True

                                            # Wait for verification
                                            await page.wait_for_timeout(random.randint(5000, 8000))
                                            break
                                except Exception as e:
                                    self._logger.debug(f"   Selector {selector} failed: {str(e)[:100]}")
                                    continue

                            if clicked:
                                # Check if we've passed the challenge
                                new_title = await page.title()
                                if "moment" not in new_title.lower():
                                    self._logger.info(f"   🎉 Successfully bypassed Cloudflare! New title: {new_title}")
                                else:
                                    self._logger.warning(f"   ⚠️ Click completed but still on challenge page: {new_title}")

                                # Wait for potential redirect
                                try:
                                    await page.wait_for_load_state('networkidle', timeout=15000)
                                except:
                                    await page.wait_for_timeout(5000)
                            else:
                                self._logger.warning("   ⚠️ No clickable element found in challenge frame")

                        except Exception as e:
                            self._logger.warning(f"   ⚠️ Error interacting with challenge frame: {e}")
                    else:
                        self._logger.warning("   ⚠️ Cloudflare challenge detected but no challenge frame found")
                        # The challenge might be directly in the main page
                        # Try clicking in the center of the page as last resort
                        try:
                            await page.mouse.click(960, 540)  # Center of 1920x1080
                            self._logger.info("   🖱️ Clicked center of page as fallback")
                            await page.wait_for_timeout(8000)
                        except Exception as e:
                            self._logger.debug(f"   Center click failed: {e}")

                # Additional wait for dynamic content
                await page.wait_for_timeout(random.randint(2000, 4000))

                # Human-like behavior: Random mouse movements
                for _ in range(random.randint(2, 5)):
                    await page.mouse.move(
                        random.randint(100, 1800),
                        random.randint(100, 1000)
                    )
                    await page.wait_for_timeout(random.randint(100, 500))

                # Try to click common cookie accept buttons
                cookie_selectors = [
                    'button:has-text("Accept")',
                    'button:has-text("Accept all")',
                    'button:has-text("Accept All")',
                    'button:has-text("Allow")',
                    'button:has-text("Allow all")',
                    'button:has-text("I agree")',
                    'button:has-text("Agree")',
                    'button:has-text("I Accept")',
                    'a:has-text("Accept")',
                    'a:has-text("I agree")',
                    '[id*="accept" i]',
                    '[class*="accept" i]',
                    '[id*="consent" i]',
                    '[class*="consent" i]',
                    '.cookie-accept',
                    '#cookieAccept',
                    '#acceptCookies',
                    'button[aria-label*="Accept" i]',
                    'button[aria-label*="consent" i]'
                ]

                for selector in cookie_selectors:
                    try:
                        button = await page.query_selector(selector)
                        if button and await button.is_visible():
                            # Human-like delay before clicking
                            await page.wait_for_timeout(random.randint(500, 1500))
                            await button.click()
                            self._logger.debug(f"   🍪 Clicked cookie consent button: {selector}")
                            await page.wait_for_timeout(random.randint(1000, 2000))
                            break
                    except:
                        continue

                # Wait for dynamic content to load (with random delay)
                await page.wait_for_timeout(random.randint(3000, 7000))

                # Scroll to simulate reading (helps with lazy-loading content)
                for _ in range(random.randint(2, 4)):
                    await page.evaluate(f'window.scrollBy(0, {random.randint(300, 800)})')
                    await page.wait_for_timeout(random.randint(800, 1500))

                # Scroll back to top
                await page.evaluate('window.scrollTo(0, 0)')
                await page.wait_for_timeout(random.randint(500, 1000))

                # Get the rendered HTML
                html_content = await page.content()

                # Parse with BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')

                # Extract metadata
                metadata = {
                    'title': await page.title() or 'No title found',
                    'url': page.url,
                    'viewport': str(page.viewport_size)
                }

                # Try to get Open Graph metadata
                og_title = await page.query_selector('meta[property="og:title"]')
                if og_title:
                    metadata['og_title'] = await og_title.get_attribute('content')

                og_description = await page.query_selector('meta[property="og:description"]')
                if og_description:
                    metadata['description'] = await og_description.get_attribute('content')

                # Extract author if available
                author_meta = await page.query_selector('meta[name="author"]')
                if author_meta:
                    metadata['author'] = await author_meta.get_attribute('content')
                else:
                    metadata['author'] = ''

                # Extract dates
                published_meta = await page.query_selector('meta[property="article:published_time"]')
                if published_meta:
                    metadata['published_date'] = await published_meta.get_attribute('content')
                else:
                    metadata['published_date'] = None

                modified_meta = await page.query_selector('meta[property="article:modified_time"]')
                if modified_meta:
                    metadata['modified_date'] = await modified_meta.get_attribute('content')
                else:
                    metadata['modified_date'] = None

                # Publisher
                publisher_meta = await page.query_selector('meta[property="og:site_name"]')
                if publisher_meta:
                    metadata['publisher'] = await publisher_meta.get_attribute('content')
                else:
                    metadata['publisher'] = None

                # Language
                metadata['language'] = await page.evaluate('document.documentElement.lang') or 'en'

                # Extract text content
                text_content = self._extract_full_text_content(soup)

                return text_content, metadata

            finally:
                await context.close()
                await browser.close()

    def _is_extraction_valid(self, text_content: str) -> bool:
        """
        Check if extracted text content is valid and contains meaningful content.

        Args:
            text_content: The extracted text content

        Returns:
            True if content is valid, False if it should be treated as failed extraction
        """
        if not text_content:
            return False

        # Remove extra whitespace and count words
        cleaned_text = ' '.join(text_content.split())
        word_count = len(cleaned_text.split())

        # Check for common failure messages or insufficient content
        failure_indicators = [
            "no readable content found",
            "content not found",
            "page not found",
            "access denied"
        ]

        text_lower = cleaned_text.lower()
        for indicator in failure_indicators:
            if text_lower == indicator or text_lower.startswith(indicator):
                return False

        # Get minimum words from config (default 10)
        # CustConf.get() only takes one argument - access nested config with dotted path
        min_words = self._app_config.get('web_scraper.min_words_for_valid_extraction')
        if not min_words or not isinstance(min_words, int):
            min_words = 10
        return word_count >= min_words

    def _check_content_quality(self, text_content: str, title: str) -> dict:
        """
        Check if extracted content appears to be low-quality sidebar/navigation text.

        Uses heuristics to detect content that passes basic validation but is actually
        navigation links, sidebars, date listings, or other non-article content.

        Args:
            text_content: The extracted text to check
            title: The page title (if available)

        Returns:
            dict with 'is_quality' (bool) and 'reason' (str) explaining the determination
        """
        if not text_content:
            return {'is_quality': False, 'reason': 'Empty content'}

        words = text_content.split()
        word_count = len(words)
        lines = [line.strip() for line in text_content.strip().split('\n') if line.strip()]
        line_count = len(lines)

        # Has title suggests metadata extraction succeeded
        has_metadata = bool(title and title.strip())

        # Heuristic 1: Very short content with full metadata suggests sidebar/navigation
        # Real articles are typically much longer even with good metadata
        if word_count < 50 and has_metadata:
            return {
                'is_quality': False,
                'reason': f'Short content ({word_count} words) with metadata suggests sidebar/navigation'
            }

        # Heuristic 2: Single-line or very few lines with metadata likely a navigation link
        if line_count <= 2 and word_count < 100 and has_metadata:
            return {
                'is_quality': False,
                'reason': f'Few lines ({line_count}) with low word count suggests navigation link'
            }

        # Heuristic 3: Date patterns with low word count suggests date listing sidebar
        import re
        date_patterns = [
            r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',
            r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',
            r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\b'
        ]
        text_lower = text_content.lower()
        has_dates = any(re.search(pattern, text_lower) for pattern in date_patterns)
        if has_dates and word_count < 100:
            return {
                'is_quality': False,
                'reason': 'Date patterns with low word count suggests sidebar date listing'
            }

        # Heuristic 4: High ratio of newlines to words suggests list/navigation structure
        newline_ratio = line_count / word_count if word_count > 0 else 0
        if newline_ratio > 0.5 and word_count < 200:
            return {
                'is_quality': False,
                'reason': f'High newline-to-word ratio ({newline_ratio:.2f}) suggests navigation list'
            }

        # Content appears to be quality article text
        return {'is_quality': True, 'reason': 'Content passes quality checks'}

    def _save_extraction(self, extraction_result: Dict, output_file: Path):
        """
        Save extraction result to the request-specific output file in append mode.

        Args:
            extraction_result: The extraction result dictionary
            output_file: Path to the output file for this request
        """
        try:
            # Save to output file (append mode - progressive writing)
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(extraction_result, ensure_ascii=False) + '\n')

            self._logger.debug(f"Appended extraction to {output_file}")

        except Exception as e:
            self._logger.error(f"Error saving extraction: {e}")

    def _create_failure_document(self, request_dir: Path, url: str, error_message: str,
                                 status_code: Optional[int], session_id: str, request_id: str):
        """
        Create a failure document when URL extraction fails.

        Args:
            request_dir: Directory for this request
            url: The URL that failed
            error_message: Description of the error
            status_code: HTTP status code if applicable
            session_id: Session identifier
            request_id: Request identifier
        """
        try:
            failure_data = {
                'url': url,
                'request_id': request_id,
                'session_id': session_id,
                'status': 'failed',
                'error': error_message,
                'http_status_code': status_code,
                'failed_at': datetime.now().isoformat(),
                'retry_possible': status_code in [429, 500, 502, 503, 504] if status_code else False
            }

            # Save as JSON
            failure_file = request_dir / "extraction_failure.json"
            with open(failure_file, 'w', encoding='utf-8') as f:
                json.dump(failure_data, f, indent=2, ensure_ascii=False)

            # Also create a markdown summary
            failure_md = request_dir / "extraction_failure.md"
            with open(failure_md, 'w', encoding='utf-8') as f:
                f.write(f"# Extraction Failure Report\n\n")
                f.write(f"**URL:** {url}\n\n")
                f.write(f"**Request ID:** {request_id}\n\n")
                f.write(f"**Session ID:** {session_id}\n\n")
                f.write(f"**Error:** {error_message}\n\n")
                if status_code:
                    f.write(f"**HTTP Status Code:** {status_code}\n\n")
                f.write(f"**Failed At:** {failure_data['failed_at']}\n\n")
                if failure_data['retry_possible']:
                    f.write(f"**Retry Recommended:** Yes (transient error)\n\n")
                else:
                    f.write(f"**Retry Recommended:** No (permanent error)\n\n")

            self._logger.info(f"   📄 Created failure documents in {request_dir}")

        except Exception as e:
            self._logger.error(f"Error creating failure document: {e}")

    def _publish_to_kafka(self, extraction_result: Dict):
        """
        Publish extraction result to Kafka topics for downstream processing.

        Publishes to BOTH:
        1. summarise-document - for AI summary generation (always high quality)
        2. content-extracted - for direct chunking (legacy/fallback)

        Args:
            extraction_result: The extraction result dictionary
        """
        try:
            request_id = extraction_result.get('request_id', 'unknown')
            session_id = extraction_result.get('session_id', 'unknown')

            # Prepare message
            message = {
                'request_id': request_id,
                'session_id': session_id,
                'extracted_content': {
                    'url': extraction_result.get('url'),
                    'text': extraction_result.get('text'),
                    'metadata': extraction_result.get('metadata', {}),
                    'extracted_at': extraction_result.get('extracted_at')
                }
            }

            # Pass through query_id for index naming
            query_id = extraction_result.get('query_id') or extraction_result.get('metadata', {}).get('query_id', 'default')
            message['query_id'] = query_id

            # Pass through total_requests for aggregator
            if 'total_requests' in extraction_result:
                message['total_requests'] = extraction_result['total_requests']

            # Serialize message
            message_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')

            # Publish to BOTH topics to ensure always-on summarization
            # 1. Summarization topic - AI will generate high-quality summary
            self.producer.produce(
                topic=self.summarise_document_topic,
                key=request_id.encode('utf-8'),
                value=message_bytes,
                callback=self._delivery_callback
            )

            # 2. Direct chunking topic (kept for backwards compatibility)
            self.producer.produce(
                topic=self.output_topic,
                key=request_id.encode('utf-8'),
                value=message_bytes,
                callback=self._delivery_callback
            )

            # Flush to ensure delivery
            self.producer.poll(0)

            self._logger.info(f"📤 Published to '{self.summarise_document_topic}' and '{self.output_topic}' topics: request_id={request_id}")

        except Exception as e:
            self._logger.error(f"❌ Error publishing to Kafka: {e}")
            import traceback
            traceback.print_exc()

    def _publish_failed_url_for_summary(self, url: str, original_url: str, title: str,
                                       context: List[str], error_msg: str,
                                       session_id: str, request_id: str, query_id: str = 'default'):
        """
        Publish failed URL to Kafka for LLM summary generation attempt.

        When extraction fails, send the URL to Gemini which can access it directly
        using web grounding to retrieve and summarize the content.

        Args:
            url: The final resolved URL
            original_url: The original URI from the request
            title: The article title
            context: Context lines provided in the request
            error_msg: The error message from failed extraction
            session_id: Session identifier
            request_id: Request identifier
            query_id: The query identifier for multi-index support
        """
        try:
            # Don't pass any extracted text - just the URL and metadata
            # Gemini will access the URL directly using web grounding
            message = {
                'request_id': request_id,
                'session_id': session_id,
                'extraction_failed': True,
                'query_id': query_id,
                'extracted_content': {
                    'url': url,
                    'text': '',  # Empty - Gemini will fetch from URL
                    'metadata': {
                        'title': title,
                        'url': url,
                        'original_url': original_url,
                        'extraction_status': 'failed',
                        'error': error_msg,
                        'context': context,
                        'query_id': query_id,
                    },
                    'extracted_at': datetime.now().isoformat()
                }
            }

            # Serialize message
            message_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')

            # Publish to output topic
            self.producer.produce(
                topic=self.output_topic,
                key=request_id.encode('utf-8'),
                value=message_bytes,
                callback=self._delivery_callback
            )

            # Flush to ensure delivery
            self.producer.poll(0)

            self._logger.info(f"📤 Published failed URL for summary attempt: request_id={request_id}")

        except Exception as e:
            self._logger.error(f"❌ Error publishing failed URL: {e}")

    def _delivery_callback(self, err, msg):
        """Callback for Kafka producer delivery reports"""
        if err:
            self._logger.error(f"❌ Message delivery failed: {err}")
        else:
            self._logger.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    def start(self):
        """Start the Kafka consumer"""
        self._logger.info("🚀 Starting Kafka Web Scraper Consumer")

        try:
            # Start telemetry services
            if self.telemetry_enabled:
                self._logger.info("📊 Starting telemetry services...")
                self.system_metrics.start()
                self.metrics_server.start()
                if hasattr(self, 'cache_metrics'):
                    self.cache_metrics.start()
                self.metrics.set_state(1)  # running
                self._logger.info("✅ Telemetry services started")

            self._create_consumer()
            self.running = True

            # Run the async message processing loop
            asyncio.run(self._consume_loop())

        except Exception as e:
            self._logger.error(f"❌ Fatal error: {e}")
            if self.telemetry_enabled:
                self.metrics.set_state(2)  # error
            raise
        finally:
            self._shutdown()

    async def _consume_loop(self):
        """Main message consumption loop with async processing"""
        self._logger.info("🔄 Starting message consumption loop...")
        poll_count = 0

        # Get event loop for running sync operations
        loop = asyncio.get_event_loop()

        while self.running:
            try:
                poll_count += 1
                if poll_count % 10 == 0:
                    self._logger.debug(f"📊 Poll count: {poll_count}, still running...")

                # Poll in executor to not block the event loop
                # confluent_kafka poll() is synchronous, so run it in thread pool
                poll_start = time.time()
                msg = await loop.run_in_executor(None, self.consumer.poll, 1.0)
                poll_duration = time.time() - poll_start

                # Track poll metrics
                if self.telemetry_enabled and msg is not None:
                    self.kafka_metrics.record_poll(
                        topic=self.input_topic,
                        duration=poll_duration,
                        message_count=1
                    )

                if msg is None:
                    if poll_count <= 5:
                        self._logger.debug(f"Poll {poll_count}: No message (expected during startup)")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self._logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        # Track connection errors
                        if self.telemetry_enabled:
                            self.metrics.increment_connection_errors(
                                error_type=msg.error().name()
                            )
                        raise KafkaException(msg.error())
                    continue

                # Process the message
                try:
                    # Track message age
                    if self.telemetry_enabled:
                        timestamp_type, timestamp_ms = msg.timestamp()
                        # timestamp_type: 0=CreateTime, 1=LogAppendTime, -1=NotAvailable
                        if timestamp_type != -1 and timestamp_ms > 0:
                            message_timestamp = timestamp_ms / 1000.0  # Convert ms to seconds
                            self.kafka_metrics.record_message_age(
                                topic=self.input_topic,
                                message_timestamp=message_timestamp
                            )

                    # Track consumer lag
                    if self.telemetry_enabled:
                        try:
                            # Get partition metadata
                            partition = msg.partition()
                            topic = msg.topic()

                            # Get high watermark (latest offset) using TopicPartition
                            from confluent_kafka import TopicPartition
                            tp = TopicPartition(topic, partition)
                            high_watermark = self.consumer.get_watermark_offsets(tp)[1]

                            # Get current offset
                            current_offset = msg.offset()

                            # Calculate lag
                            lag = high_watermark - current_offset - 1
                            if lag >= 0:
                                self.kafka_metrics.record_lag(
                                    topic=topic,
                                    partition=partition,
                                    lag_messages=lag
                                )
                        except Exception as e:
                            self._logger.debug(f"Could not calculate lag: {e}")

                    # Parse message
                    try:
                        message_data = json.loads(msg.value().decode('utf-8'))
                    except json.JSONDecodeError as e:
                        self._logger.error(f"❌ Invalid JSON in message: {e}")
                        if self.telemetry_enabled:
                            self.metrics.increment_deserialization_errors(
                                topic=self.input_topic,
                                error_type='json_decode_error'
                            )
                        continue

                    # Track message consumption
                    if self.telemetry_enabled:
                        self.metrics.increment_messages_consumed(self.input_topic)

                    # Track messages in flight (increment before processing)
                    if self.telemetry_enabled:
                        self.kafka_metrics.increment_messages_in_flight(self.input_topic)

                    try:
                        # Time the message processing
                        start_time = time.time()

                        # Process message asynchronously (truly async now)
                        result = await self._process_message(message_data)

                        # Record processing duration
                        if self.telemetry_enabled:
                            duration = time.time() - start_time
                            status = 'success' if result['success'] else 'failure'
                            self.metrics.record_duration(self.input_topic, duration, status)

                            if not result['success']:
                                error_type = result.get('error', 'unknown')
                                self.metrics.increment_failures(self.input_topic, error_type)

                        if result['success']:
                            self._logger.info(f"✅ Successfully processed message for session {result['session_id']}")
                        else:
                            self._logger.warning(f"⚠️ Message processing completed with errors: {result['error']}")

                    finally:
                        # Decrement messages in flight after processing completes
                        if self.telemetry_enabled:
                            self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}")
                    # Make sure to decrement if we're tracking
                    if self.telemetry_enabled:
                        try:
                            self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                        except:
                            pass

            except Exception as e:
                self._logger.error(f"❌ Error in consume loop: {e}")
                if not self.running:
                    break

    def _shutdown(self):
        """Cleanup and shutdown"""
        self._logger.info("🛑 Shutting down consumer...")

        # Stop telemetry services
        if self.telemetry_enabled:
            self._logger.info("📊 Stopping telemetry services...")
            self.metrics.set_state(0)  # stopped
            self.system_metrics.stop()
            self.metrics_server.stop()
            self._logger.info("✅ Telemetry services stopped")

        if self.producer:
            try:
                self._logger.info("Flushing producer...")
                self.producer.flush(timeout=10)
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
    consumer = KafkaWebScraperConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
