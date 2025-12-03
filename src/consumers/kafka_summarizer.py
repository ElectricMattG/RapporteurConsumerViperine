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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

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

        # Initialize Kafka consumer
        self.consumer = None
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
        """Create and configure Kafka consumer"""
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.input_topic])
            self._logger.info(f"✅ Subscribed to topic: {self.input_topic}")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer: {e}")
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
        extracted_content = message_data.get('extracted_content', {})

        url = extracted_content.get('url', 'unknown')
        text = extracted_content.get('text', '')
        metadata = extracted_content.get('metadata', {})
        extraction_failed = message_data.get('extraction_failed', False)  # Check if extraction failed

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

        try:
            # Generate summary using LLM
            if extraction_failed:
                self._logger.info("🌐 Generating summary from URL using Gemini web grounding...")
            else:
                self._logger.info("🤖 Generating summary from extracted content...")

            # Create a citation-like prompt from the content
            title = metadata.get('title', 'Untitled')
            author = metadata.get('author', 'Unknown')
            citation = f"{author}. {title}. {url}"

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

            # Save summary to cache
            summary_file = request_dir / "summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_result, f, ensure_ascii=False, indent=2)

            self._logger.info(f"   💾 Summary saved to: {summary_file}")

            # Also save just the markdown summary for easy access
            markdown_file = request_dir / "summary.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(summary_result.get('markdown_summary', ''))

            self._logger.info(f"   📄 Markdown saved to: {markdown_file}")

            result['success'] = True
            result['summary_file'] = str(summary_file)
            result['markdown_file'] = str(markdown_file)

        except Exception as e:
            result['error'] = str(e)
            self._logger.error(f"❌ Error generating summary: {e}")
            import traceback
            self._logger.error(traceback.format_exc())

        self._logger.info("=" * 80)
        return result

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
        """Main message consumption loop"""
        self._logger.info("🔄 Starting message consumption loop...")

        while self.running:
            try:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self._logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                    continue

                # Process the message
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))

                    # Process message asynchronously
                    result = await self._process_message(message_data)

                    if result['success']:
                        self._logger.info(f"✅ Successfully processed summary for request {result['request_id']}")
                    else:
                        self._logger.warning(f"⚠️ Summary processing completed with errors: {result['error']}")

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}")

            except Exception as e:
                self._logger.error(f"❌ Error in consume loop: {e}")
                if not self.running:
                    break

    def _shutdown(self):
        """Cleanup and shutdown"""
        self._logger.info("🛑 Shutting down consumer...")

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
