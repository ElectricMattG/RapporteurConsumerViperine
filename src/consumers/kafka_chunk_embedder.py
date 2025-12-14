#!/usr/bin/env python3
"""
Kafka Chunk Embedder Consumer

Generates embeddings for document chunks after citation extraction enrichment.
Works as part of the processing pipeline:
  1. Web Scraper → extracts content
  2. Summarizer → generates summaries
  3. Document Chunker → chunks the content
  4. Citation Extractor → enriches chunks with citation metadata
  5. Chunk Embedder (this) → generates embeddings for enriched chunks
  6. Ready for vector search / retrieval

Message Format (Input - from citation extractor or manual trigger):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "enriched_chunks_file": "path/to/chunked_document_enriched.jsonl",
        "document_id": "doc-id",
        "source_url": "original-document-url"
    }

Embedding Strategy:
    - Concatenates section_hierarchy + content for context-aware embeddings
    - Handles long chunks via sub-chunking (max 7500 chars per embedding)
    - Aggregates sub-chunk embeddings using mean pooling
    - Preserves original chunk schema with embedding_enrichment field

Output Schema:
    Original chunk fields + embedding_enrichment:
    {
        "content": "chunk text",
        "chunk_metadata": {...},
        "document_metadata": {...},
        "embedding_enrichment": {
            "embedding_id": "emb_chunk-123",
            "enriched_text": "[Section > Subsection]: content",
            "embedding_vector": [0.123, ...],  # 384-dim
            "embedding_dim": 384,
            "is_sub_chunked": false,
            "num_sub_chunks": 1,
            "aggregation_method": "none",
            "embedding_timestamp": "2025-12-02T..."
        }
    }

Usage:
    python run_kafka_chunk_embedder.py
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Import components
try:
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base

# Import embedding client
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../clients'))
from embedding_client import EmbeddingServiceClient


class KafkaChunkEmbedderConsumer(Base):
    """Kafka consumer for generating embeddings for enriched chunks"""

    def __init__(self):
        """Initialize the Kafka chunk embedder consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaChunkEmbedderConsumer")

        # Load configuration
        self._load_config()

        # Initialize Kafka consumer
        self.consumer = None
        self.producer = None
        self.running = False

        # Initialize embedding client
        self.embedding_client = None

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
                'group.id': self._app_config.get('chunk_embedder.consumer_group_id') or 'chunk-embedder-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 45000,  # 45 seconds
                'heartbeat.interval.ms': 3000,  # 3 seconds - should be 1/3 of session timeout
                'max.poll.interval.ms': 600000,  # 10 minutes for embedding generation
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
            self.input_topic = self._app_config.get('chunk_embedder.input_topic') or 'chunk-embedding'
            self.output_topic = self._app_config.get('chunk_embedder.output_topic') or 'qa-generation'

            # Embedding configuration
            self.cache_dir = Path(self._app_config.get('chunk_embedder.cache_directory') or 'cache/embeddings')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            self.batch_size = self._app_config.get('chunk_embedder.batch_size') or 50
            self.hierarchy_separator = self._app_config.get('chunk_embedder.hierarchy_separator') or ' > '
            self.min_chunk_length = self._app_config.get('chunk_embedder.min_chunk_length') or 20
            self.max_chunk_size = self._app_config.get('chunk_embedder.max_chunk_size') or 7500

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Batch size: {self.batch_size}")
            self._logger.info(f"   Hierarchy separator: '{self.hierarchy_separator}'")

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _create_consumer(self):
        """Create Kafka consumer instance"""
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.input_topic])
            self._logger.info(f"✅ Subscribed to topic: {self.input_topic}")

            # Create producer
            self.producer = Producer({
                'bootstrap.servers': self.kafka_config['bootstrap.servers']
            })
            self._logger.info(f"✅ Kafka producer initialized")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer/producer: {e}")
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
        self._logger.info("✅ Consumer shut down successfully")

    def _concatenate_hierarchy_content(self, section_hierarchy: List[str], content: str) -> str:
        """
        Concatenate section hierarchy with content for context-aware embedding.

        Format: [Section > Subsection > ...]: Content text

        Args:
            section_hierarchy: List of section names from root to current
            content: Chunk content

        Returns:
            Enriched text with hierarchy context
        """
        if not section_hierarchy:
            return content

        hierarchy_str = self.hierarchy_separator.join(section_hierarchy)
        return f"{hierarchy_str}: {content}"

    def _split_long_text(self, text: str, max_chunk_size: int = 7500, overlap: int = 200) -> List[str]:
        """
        Split long text into overlapping sub-chunks.

        Args:
            text: Text to split
            max_chunk_size: Maximum size per sub-chunk
            overlap: Overlap between consecutive chunks

        Returns:
            List of text sub-chunks
        """
        if len(text) <= max_chunk_size:
            return [text]

        sub_chunks = []
        start = 0

        while start < len(text):
            end = start + max_chunk_size

            # Try to break at sentence boundary
            if end < len(text):
                last_segment = text[max(end-200, start):end]
                sentence_breaks = [i for i, c in enumerate(last_segment) if c in '.!?\n']

                if sentence_breaks:
                    break_point = sentence_breaks[-1] + 1
                    end = max(end-200, start) + break_point
                else:
                    # Fall back to word boundary
                    last_segment = text[max(end-100, start):end]
                    space_breaks = [i for i, c in enumerate(last_segment) if c == ' ']
                    if space_breaks:
                        break_point = space_breaks[-1]
                        end = max(end-100, start) + break_point

            chunk = text[start:end].strip()
            if chunk:
                sub_chunks.append(chunk)

            start = end - overlap if end < len(text) else end

        return sub_chunks if sub_chunks else [text]

    async def _generate_embeddings_for_chunks(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate embeddings for all chunks.

        Args:
            chunks: List of chunk dictionaries

        Returns:
            List of chunks enriched with embeddings
        """
        if not chunks:
            return []

        self._logger.info(f"   🧠 Preparing {len(chunks)} chunks for embedding...")

        # Prepare enriched texts and track sub-chunking
        texts_to_embed = []
        chunk_mapping = []  # Maps embedding index to (chunk_idx, is_sub_chunk, sub_idx)

        for chunk_idx, chunk in enumerate(chunks):
            content = chunk.get('content', '').strip()

            # Process all chunks, even short ones
            if len(content) < self.min_chunk_length:
                self._logger.debug(f"   ⚠️  Processing short chunk {chunk_idx}: {len(content)} chars")

            # Get section hierarchy
            chunk_metadata = chunk.get('chunk_metadata', {})
            section_hierarchy = chunk_metadata.get('section_hierarchy', [])

            # Create enriched text
            enriched_text = self._concatenate_hierarchy_content(section_hierarchy, content)

            # Check if sub-chunking needed
            if len(enriched_text) > self.max_chunk_size:
                sub_chunks = self._split_long_text(enriched_text, self.max_chunk_size)
                self._logger.info(f"   ✂️  Chunk {chunk_idx}: Split into {len(sub_chunks)} sub-chunks")

                # Add each sub-chunk
                for sub_idx, sub_text in enumerate(sub_chunks):
                    if sub_text.strip():
                        texts_to_embed.append(sub_text)
                        chunk_mapping.append((chunk_idx, True, sub_idx, sub_chunks))
            else:
                # Single chunk
                texts_to_embed.append(enriched_text)
                chunk_mapping.append((chunk_idx, False, 0, [enriched_text]))

        if not texts_to_embed:
            self._logger.warning("   ⚠️  No valid texts to embed after filtering")
            return chunks

        self._logger.info(f"   📊 Generating {len(texts_to_embed)} embeddings...")

        # Generate embeddings
        embeddings_array = await self.embedding_client.embed(
            texts_to_embed,
            batch_size=self.batch_size,
            show_progress=True
        )

        all_embeddings = embeddings_array.tolist()
        self._logger.info(f"   ✅ Generated {len(all_embeddings)} embeddings")

        # Aggregate embeddings back to chunks
        enriched_chunks = []
        embedding_idx = 0

        for chunk_idx, chunk in enumerate(chunks):
            # Find mapping entries for this chunk
            chunk_entries = [
                (idx, is_sub, sub_idx, sub_texts)
                for idx, (c_idx, is_sub, sub_idx, sub_texts) in enumerate(chunk_mapping)
                if c_idx == chunk_idx
            ]

            if not chunk_entries:
                # Chunk was skipped (too short)
                enriched_chunks.append(chunk)
                continue

            # Get the mapping info
            _, is_sub_chunked, _, sub_texts = chunk_entries[0]

            if is_sub_chunked and len(sub_texts) > 1:
                # Aggregate sub-chunk embeddings via mean pooling
                import numpy as np
                num_subs = len(sub_texts)
                sub_embeddings = all_embeddings[embedding_idx:embedding_idx + num_subs]
                aggregated_embedding = np.mean(sub_embeddings, axis=0).tolist()

                embedding_data = {
                    'aggregated_embedding': aggregated_embedding,
                    'sub_embeddings': sub_embeddings,
                    'is_sub_chunked': True,
                    'num_sub_chunks': num_subs,
                    'aggregation_method': 'mean'
                }

                embedding_idx += num_subs
            else:
                # Single embedding
                embedding_data = {
                    'aggregated_embedding': all_embeddings[embedding_idx],
                    'sub_embeddings': None,
                    'is_sub_chunked': False,
                    'num_sub_chunks': 1,
                    'aggregation_method': 'none'
                }

                embedding_idx += 1

            # Enrich chunk with embedding
            enriched_chunk = chunk.copy()

            # Get enriched text
            content = chunk.get('content', '')
            chunk_metadata = chunk.get('chunk_metadata', {})
            section_hierarchy = chunk_metadata.get('section_hierarchy', [])
            enriched_text = self._concatenate_hierarchy_content(section_hierarchy, content)

            # Generate embedding ID
            chunk_id = chunk_metadata.get('chunk_id', '')
            embedding_id = f"emb_{chunk_id}" if chunk_id else hashlib.md5(
                f"{chunk_idx}_{datetime.now().timestamp()}".encode()
            ).hexdigest()[:16]

            # Add embedding enrichment
            enriched_chunk['embedding_enrichment'] = {
                'embedding_id': embedding_id,
                'enriched_text': enriched_text,
                'enriched_text_preview': enriched_text[:200],
                'hierarchy_separator': self.hierarchy_separator,
                'concatenation_method': 'hierarchy_first',
                'is_sub_chunked': embedding_data['is_sub_chunked'],
                'num_sub_chunks': embedding_data['num_sub_chunks'],
                'embedding_vector': embedding_data['aggregated_embedding'],
                'embedding_dim': len(embedding_data['aggregated_embedding']),
                'aggregation_method': embedding_data['aggregation_method'],
                'sub_chunk_embeddings': embedding_data['sub_embeddings'],
                'embedding_timestamp': datetime.now().isoformat()
            }

            enriched_chunks.append(enriched_chunk)

        return enriched_chunks

    async def _process_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a chunk embedding message.

        Args:
            message_data: Message containing enriched chunks file path

        Returns:
            Result dictionary with embedding status
        """
        session_id = message_data.get('session_id', 'unknown')
        request_id = message_data.get('request_id', f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        enriched_chunks_file = message_data.get('enriched_chunks_file')
        document_id = message_data.get('document_id', 'unknown')
        source_url = message_data.get('source_url', 'unknown')

        self._logger.info("=" * 80)
        self._logger.info(f"🧠 Processing chunk embedding request")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   Document ID: {document_id}")
        self._logger.info(f"   Enriched chunks file: {enriched_chunks_file}")
        self._logger.info(f"   Source URL: {source_url}")

        result = {
            'session_id': session_id,
            'request_id': request_id,
            'document_id': document_id,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'chunks_processed': 0,
            'embeddings_generated': 0,
            'output_file': None,
            'error': None
        }

        # Validate enriched chunks file exists
        if not enriched_chunks_file or not Path(enriched_chunks_file).exists():
            error_msg = f"Enriched chunks file not found: {enriched_chunks_file}"
            self._logger.error(f"   ❌ {error_msg}")
            result['error'] = error_msg
            return result

        try:
            # Initialize embedding client if not already done
            if not self.embedding_client:
                self._logger.info("   🔧 Initializing embedding service client...")
                self.embedding_client = EmbeddingServiceClient()

            # Read enriched chunks from JSONL file
            chunks = []
            with open(enriched_chunks_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        chunks.append(json.loads(line))

            self._logger.info(f"   📄 Loaded {len(chunks)} chunks from file")

            # Generate embeddings for all chunks
            embedded_chunks = await self._generate_embeddings_for_chunks(chunks)

            # Save embedded chunks
            original_path = Path(enriched_chunks_file)
            embedded_filename = f"{original_path.stem}_embedded{original_path.suffix}"
            embedded_file = original_path.parent / embedded_filename

            with open(embedded_file, 'w', encoding='utf-8') as f:
                for chunk in embedded_chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + '\n')

            self._logger.info(f"   💾 Saved embedded chunks to: {embedded_file}")

            # Count embeddings
            embeddings_count = sum(
                1 for chunk in embedded_chunks
                if 'embedding_enrichment' in chunk and chunk['embedding_enrichment'].get('embedding_vector')
            )

            # Create summary
            summary = {
                'request_id': request_id,
                'session_id': session_id,
                'document_id': document_id,
                'source_url': source_url,
                'input_file': str(enriched_chunks_file),
                'output_file': str(embedded_file),
                'chunks_processed': len(chunks),
                'embeddings_generated': embeddings_count,
                'embedding_dimension': embedded_chunks[0]['embedding_enrichment']['embedding_dim'] if embedded_chunks and 'embedding_enrichment' in embedded_chunks[0] else 0,
                'processed_at': datetime.now().isoformat()
            }

            # Save summary
            summary_file = self.cache_dir / f"{request_id}_embedding_summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            self._logger.info(f"   📊 Saved embedding summary to: {summary_file}")

            result['chunks_processed'] = len(chunks)
            result['embeddings_generated'] = embeddings_count
            result['output_file'] = str(embedded_file)
            result['success'] = True

            self._logger.info(f"   ✅ Successfully generated {embeddings_count} embeddings for {len(chunks)} chunks")

            # Publish to qa-generation topic
            try:
                qa_message = {
                    'request_id': request_id,
                    'session_id': session_id,
                    'embedded_chunks_file': str(embedded_file),
                    'document_id': document_id,
                    'source_url': source_url,
                    'chunk_count': len(chunks),
                    'embeddings_generated': embeddings_count
                }

                # Pass through total_requests if present
                if 'total_requests' in message_data:
                    qa_message['total_requests'] = message_data['total_requests']

                message_json = json.dumps(qa_message)
                self.producer.produce(
                    topic=self.output_topic,
                    value=message_json.encode('utf-8')
                )
                self.producer.flush()

                self._logger.info(f"   📤 Published embedded chunks message to {self.output_topic}")
            except Exception as e:
                self._logger.error(f"   ⚠️ Failed to publish to Kafka: {e}")
                # Publish to dead letter queue
                self._publish_to_dlq(message_data, 'chunk_embedder', str(e))

            self._logger.info("=" * 80)

        except Exception as e:
            error_msg = f"Error generating embeddings: {e}"
            self._logger.error(f"   ❌ {error_msg}", exc_info=True)
            result['error'] = error_msg
            # Publish to dead letter queue
            self._publish_to_dlq(message_data, 'chunk_embedder', error_msg)
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
    print("🧠 Kafka Chunk Embedder Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaChunkEmbedderConsumer()

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
