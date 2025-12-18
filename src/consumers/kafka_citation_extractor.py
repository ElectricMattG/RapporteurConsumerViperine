#!/usr/bin/env python3
"""
Kafka Citation Extractor Consumer

Enriches document chunks by extracting inline citations/URLs from the text content.
This is an enrichment step that adds citation metadata to existing chunks.

Pipeline Flow:
  1. Web Scraper → extracts content from URLs
  2. Summarizer → generates summaries
  3. Document Chunker → chunks the content → publishes to citation-extraction topic
  4. Citation Extractor (this) → extracts URLs from chunk text → enriches chunks with citation metadata
  5. Enriched chunks saved to disk

Message Format (Input - from chunker):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "chunks_file": "path/to/chunks.jsonl",
        "document_id": "doc-id",
        "source_url": "original-document-url",
        "chunk_count": 100
    }

Enrichment:
    - Reads chunks from JSONL file
    - Extracts inline citations [text](url) from chunk text
    - Adds 'citations' field to chunk metadata
    - Saves enriched chunks to new JSONL file

Schema Addition:
    chunk_metadata: {
        ...existing fields...,
        "citations": [
            {
                "url": "https://example.com",
                "text": "citation text",
                "position": 123
            }
        ],
        "citation_count": 3
    }

Usage:
    python run_kafka_citation_extractor.py
"""

import asyncio
import json
import os
import re
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from confluent_kafka import Consumer, KafkaError, Producer

# Import components
try:
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class KafkaCitationExtractorConsumer(Base):
    """Kafka consumer for enriching chunks with citation metadata"""

    def __init__(self):
        """Initialize the Kafka citation extractor consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaCitationExtractorConsumer")

        # Load configuration
        self._load_config()

        # Initialize Kafka consumer and producer
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
                'group.id': self._app_config.get('citation_extractor.consumer_group_id') or 'citation-extractor-consumer',
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
            self.input_topic = self._app_config.get('citation_extractor.input_topic') or 'citation-extraction'
            self.output_topic = self._app_config.get('citation_extractor.output_topic') or 'chunk-embedding'

            # Producer config (same bootstrap servers)
            self.producer_config = {
                'bootstrap.servers': bootstrap_servers
            }

            # Add security config to producer if needed
            if security_protocol and security_protocol != 'PLAINTEXT':
                self.producer_config['security.protocol'] = security_protocol
                if sasl_mechanism:
                    self.producer_config['sasl.mechanism'] = sasl_mechanism
                    self.producer_config['sasl.username'] = os.getenv('KAFKA_SASL_USERNAME', '')
                    self.producer_config['sasl.password'] = os.getenv('KAFKA_SASL_PASSWORD', '')

            # Citation extraction configuration
            self.cache_dir = Path(self._app_config.get('citation_extractor.cache_directory') or 'cache/citations')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            self.min_citation_length = self._app_config.get('citation_extractor.min_citation_length') or 10
            self.max_citations_per_chunk = self._app_config.get('citation_extractor.max_citations_per_chunk') or 50

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Min citation length: {self.min_citation_length} chars")
            self._logger.info(f"   Max citations per chunk: {self.max_citations_per_chunk}")

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _create_consumer(self):
        """Create Kafka consumer instance"""
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
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer: {e}")
            raise

    def _create_producer(self):
        """Create Kafka producer instance"""
        try:
            self.producer = Producer(self.producer_config)
            self._logger.info(f"✅ Kafka producer created")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka producer: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._logger.info(f"⚠️ Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _shutdown(self):
        """Shutdown consumer gracefully"""
        self._logger.info("🛑 Shutting down consumer...")
        if self.producer:
            self.producer.flush()
        if self.consumer:
            self.consumer.close()
        self._logger.info("✅ Consumer shut down successfully")

    def _extract_inline_citations(self, text: str, base_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract inline citations/URLs from markdown text.

        Looks for markdown link patterns: [text](url)

        Args:
            text: Markdown text to extract citations from
            base_url: Base URL for resolving relative URLs

        Returns:
            List of citation dictionaries with url, text, and position
        """
        citations = []

        # Pattern for markdown links: [text](url)
        link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'

        for match in re.finditer(link_pattern, text):
            citation_text = match.group(1).strip()
            url = match.group(2).strip()

            # Skip if citation text is too short
            if len(citation_text) < self.min_citation_length:
                continue

            # Resolve relative URLs
            if base_url and not url.startswith(('http://', 'https://', 'mailto:', 'tel:', '#')):
                url = urljoin(base_url, url)

            # Only keep HTTP(S) URLs (skip mailto:, tel:, anchors)
            if not url.startswith(('http://', 'https://')):
                continue

            citations.append({
                'url': url,
                'anchor_text': citation_text,  # Match schema: 'anchor_text' not 'text'
                'position': match.start()
            })

        # Limit citations per chunk
        if len(citations) > self.max_citations_per_chunk:
            citations = citations[:self.max_citations_per_chunk]

        return citations

    def _enrich_chunk_with_citations(self, chunk: Dict[str, Any], base_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Enrich a chunk by extracting citations from its content text.

        Args:
            chunk: Chunk dictionary with 'content' and metadata (matching citation_extractor.py schema)
            base_url: Base URL for resolving relative URLs

        Returns:
            Enriched chunk with citations in metadata
        """
        # Extract citations from chunk content (use 'content' key to match schema)
        content = chunk.get('content', '')
        citations = self._extract_inline_citations(content, base_url)

        # Ensure chunk_metadata exists
        if 'chunk_metadata' not in chunk:
            chunk['chunk_metadata'] = {}

        # Add citation metadata (matching citation_extractor.py schema)
        if citations:
            chunk['chunk_metadata']['citations'] = citations
            chunk['chunk_metadata']['citation_count'] = len(citations)
            chunk['chunk_metadata']['has_citations'] = True
        else:
            chunk['chunk_metadata']['has_citations'] = False
            chunk['chunk_metadata']['citation_count'] = 0

        return chunk

    async def _process_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a citation extraction message.

        Args:
            message_data: Message containing chunks file path and metadata

        Returns:
            Result dictionary with extraction status
        """
        session_id = message_data.get('session_id', 'unknown')
        request_id = message_data.get('request_id', f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        chunks_file = message_data.get('chunks_file')
        document_id = message_data.get('document_id', 'unknown')
        source_url = message_data.get('source_url', 'unknown')
        chunk_count = message_data.get('chunk_count', 0)

        self._logger.info("=" * 80)
        self._logger.info(f"📚 Processing citation extraction request")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   Document ID: {document_id}")
        self._logger.info(f"   Chunks file: {chunks_file}")
        self._logger.info(f"   Source URL: {source_url}")
        self._logger.info(f"   Chunk count: {chunk_count}")

        result = {
            'session_id': session_id,
            'request_id': request_id,
            'document_id': document_id,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'chunks_processed': 0,
            'total_citations_extracted': 0,
            'enriched_file': None,
            'error': None
        }

        # Validate chunks file exists
        if not chunks_file or not Path(chunks_file).exists():
            error_msg = f"Chunks file not found: {chunks_file}"
            self._logger.error(f"   ❌ {error_msg}")
            result['error'] = error_msg
            return result

        try:
            # Read chunks from JSONL file
            chunks = []
            with open(chunks_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        chunks.append(json.loads(line))

            self._logger.info(f"   📄 Loaded {len(chunks)} chunks from file")

            # Enrich each chunk with citations
            base_url = source_url if source_url != 'unknown' else None
            enriched_chunks = []
            total_citations = 0

            for chunk in chunks:
                enriched_chunk = self._enrich_chunk_with_citations(chunk, base_url)
                enriched_chunks.append(enriched_chunk)

                citation_count = enriched_chunk.get('chunk_metadata', {}).get('citation_count', 0)
                total_citations += citation_count

            self._logger.info(f"   🔗 Extracted {total_citations} total citations from {len(chunks)} chunks")

            # Save enriched chunks to new file
            original_path = Path(chunks_file)
            enriched_filename = f"{original_path.stem}_enriched{original_path.suffix}"
            enriched_file = original_path.parent / enriched_filename

            with open(enriched_file, 'w', encoding='utf-8') as f:
                for chunk in enriched_chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + '\n')

            self._logger.info(f"   💾 Saved enriched chunks to: {enriched_file}")

            # Publish enriched chunks to embedding topic
            try:
                message = {
                    'request_id': request_id,
                    'session_id': session_id,
                    'enriched_chunks_file': str(enriched_file),
                    'document_id': document_id,
                    'source_url': source_url,
                    'chunk_count': len(chunks),
                    'total_citations': total_citations
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

                self._logger.info(f"   📤 Published enriched chunks message to {self.output_topic}")
            except Exception as e:
                self._logger.error(f"   ⚠️ Failed to publish to Kafka: {e}")

            # Create citation summary
            summary = {
                'request_id': request_id,
                'session_id': session_id,
                'document_id': document_id,
                'source_url': source_url,
                'original_chunks_file': str(chunks_file),
                'enriched_chunks_file': str(enriched_file),
                'chunk_count': len(chunks),
                'total_citations': total_citations,
                'processed_at': datetime.now().isoformat()
            }

            # Save summary
            summary_file = self.cache_dir / f"{request_id}_citation_summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            self._logger.info(f"   📊 Saved citation summary to: {summary_file}")

            result['chunks_processed'] = len(chunks)
            result['total_citations_extracted'] = total_citations
            result['enriched_file'] = str(enriched_file)
            result['success'] = True

            self._logger.info(f"   ✅ Successfully enriched {len(chunks)} chunks with {total_citations} citations")
            self._logger.info("=" * 80)

        except Exception as e:
            error_msg = f"Error processing citations: {e}"
            self._logger.error(f"   ❌ {error_msg}", exc_info=True)
            result['error'] = error_msg
            # Publish to dead letter queue
            self._publish_to_dlq(message_data, 'citation_extractor', error_msg)
            self._logger.info("=" * 80)

        return result

    def _publish_to_dlq(self, original_message: Dict[str, Any], stage: str, error: str):
        """Publish failed message to dead letter queue.

        Args:
            original_message: The original message that failed
            stage: The pipeline stage where failure occurred
            error: The error message
        """
        try:
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
        """Start consuming messages from Kafka"""
        self._create_consumer()
        self._create_producer()

        self.running = True
        self._logger.info("🚀 Starting to consume messages...")

        try:
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
                    result = await self._process_message(message_data)

                    if result['success']:
                        self._logger.info(f"✅ Successfully processed request {result['request_id']}")
                    else:
                        self._logger.warning(f"⚠️ Failed to process request {result['request_id']}: {result.get('error')}")

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            self._logger.info("\n⚠️ Shutting down consumer...")
        finally:
            self._shutdown()

    def run(self):
        """Run the consumer."""
        asyncio.run(self.consume_messages())


def main():
    """Main entry point."""
    print("=" * 80)
    print("📚 Kafka Citation Extractor Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaCitationExtractorConsumer()

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
