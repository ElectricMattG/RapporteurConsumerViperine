#!/usr/bin/env python3
"""
Kafka Document Chunker Consumer

Consumes extracted web content from 'summarise-document' topic,
chunks the content using DocumentChunkingUtility,
and saves chunked documents to cache.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Add the src directory to the path
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from components.chunkers.document_chunking_utility import \
    DocumentChunkingUtility
from config_handler import Base


class KafkaDocumentChunkerConsumer(Base):
    """Consumer for chunking web-extracted documents via Kafka."""

    def __init__(self):
        """Initialize the Kafka document chunker consumer."""
        super().__init__()

        # Setup logging
        self._setup_logging()
        self._logger.info("Initializing KafkaDocumentChunkerConsumer")

        # Get configuration
        bootstrap_servers = self._app_config.get('kafka.bootstrap_servers') or 'localhost:9092'
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

        # Subscribe to topic
        self.consumer.subscribe([input_topic])
        self._logger.info(f"📡 Subscribed to topic: {input_topic}")

        # Initialize document chunker
        self.cache_dir = Path(cache_dir)
        self.chunker = DocumentChunkingUtility(
            output_dir=str(self.cache_dir),
            cache_dir=str(self.cache_dir),
            max_chunk_words=max_chunk_words
        )

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
                    'data_lineage': data_lineage  # Preserve data lineage
                },
                'method': extraction_method,  # Use the actual method (gemini_web_grounding or web_extraction)
                'success': True,
                'timestamp': extracted_content.get('extracted_at')
            }

            self._logger.info("🔪 Chunking document...")

            # Chunk the document
            result = self.chunker.chunk_single_document(url, cached_data)

            if result['status'] == 'success':
                chunks = result['chunks']
                self._logger.info(f"   ✅ Generated {len(chunks)} chunks")

                # Save chunks to request_id folder
                output_dir = self.cache_dir / request_id
                output_dir.mkdir(parents=True, exist_ok=True)

                chunks_file = output_dir / 'chunked_document.jsonl'

                with open(chunks_file, 'w', encoding='utf-8') as f:
                    for chunk in chunks:
                        chunk_record = chunk.to_dict()
                        f.write(json.dumps(chunk_record, ensure_ascii=False) + '\n')

                self._logger.info(f"   💾 Chunks saved to: {chunks_file}")

                # Save chunk statistics
                stats_file = output_dir / 'chunk_stats.json'
                stats = {
                    'total_chunks': len(chunks),
                    'chunk_types': {},
                    'avg_chunk_size': sum(c.chunk_metadata.char_count for c in chunks) / len(chunks) if chunks else 0,
                    'avg_word_count': sum(c.chunk_metadata.word_count for c in chunks) / len(chunks) if chunks else 0
                }

                # Count chunk types
                for chunk in chunks:
                    chunk_type = chunk.chunk_metadata.chunk_type.value
                    stats['chunk_types'][chunk_type] = stats['chunk_types'].get(chunk_type, 0) + 1

                with open(stats_file, 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, ensure_ascii=False)

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

                    message_json = json.dumps(message)
                    self.producer.produce(
                        topic=self.output_topic,
                        value=message_json.encode('utf-8')
                    )
                    self.producer.flush()

                    self._logger.info(f"   📤 Published chunked document message to {self.output_topic}")
                except Exception as e:
                    self._logger.error(f"   ⚠️ Failed to publish to Kafka: {e}")
                    # Publish to dead letter queue
                    self._publish_to_dlq(message_data, 'chunker', str(e))

            else:
                error = result.get('error', 'Unknown error')
                self._logger.error(f"   ❌ Chunking failed: {error}")
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
                    await self.process_message(message_data)

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            self._logger.info("\n⚠️  Shutting down consumer...")
        finally:
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
