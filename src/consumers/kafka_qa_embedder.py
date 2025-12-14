#!/usr/bin/env python3
"""
Kafka Q&A Pair Embedder Consumer

Generates embeddings for Q&A pairs with three modes:
1. question-only: Embed questions separately for question-based search
2. answer-only: Embed answers separately (useful for answer similarity)
3. combined: Embed question+answer together (captures full pair meaning)

Pipeline Flow:
  6. Q&A Generator → creates Q&A pairs
  7. Q&A Embedder (this) → generates embeddings for Q&A pairs

Message Format (Input - from Q&A generator):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "qa_pairs_file": "path/to/request_id_qa_pairs_incremental.jsonl",
        "document_id": "doc-id",
        "source_url": "original-url",
        "qa_count": 14
    }

Output Schema (enriched Q&A pairs):
    {
        # Original Q&A data
        "question": "What information does this text provide?",
        "answer": "According to the text, ...",
        "context": "original chunk text",
        "chunk_id": "chunk-123",
        "source_url": "https://...",
        "document_id": "doc-123",
        "section_hierarchy": ["Section 1", "Subsection 1.1"],

        # Embedding data
        "question_embedding": [0.123, ...],  # 384-dim
        "answer_embedding": [0.456, ...],    # 384-dim
        "combined_embedding": [0.789, ...],  # 384-dim

        # Metadata
        "embedding_model": "all-MiniLM-L6-v2",
        "embedding_dimension": 384,
        "enrichment_timestamp": "2025-12-03T...",

        # Original metadata
        "chunk_type": "paragraph",
        "processing_type": "paragraph_processing",
        "word_count_question": 10,
        "word_count_answer": 25,
        "qa_index": 0,
        "qa_total_for_source": 14
    }

Usage:
    python run_kafka_qa_embedder.py
"""

import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from confluent_kafka import Consumer, KafkaError, Producer
from sentence_transformers import SentenceTransformer

# Import components
try:
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class KafkaQAEmbedderConsumer(Base):
    """Kafka consumer for generating embeddings for Q&A pairs"""

    def __init__(self):
        """Initialize the Kafka Q&A embedder consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaQAEmbedderConsumer")

        # Load configuration
        self._load_config()

        # Initialize Kafka consumer and producer
        self.consumer = None
        self.producer = None
        self.running = False

        # Initialize embedding model
        self.embedding_model = None
        self.embedding_dimension = None

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._logger.info("✅ KafkaQAEmbedderConsumer initialized successfully")

    def _load_config(self):
        """Load configuration for Q&A embedder"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers')
            if not bootstrap_servers:
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            self.kafka_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': self._app_config.get('qa_embedder.consumer_group_id') or 'qa-embedder-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 45000,  # 45 seconds
                'heartbeat.interval.ms': 3000,  # 3 seconds - should be 1/3 of session timeout
                'max.poll.interval.ms': 600000,  # 10 minutes for embedding generation
            }

            # Topic configuration
            self.input_topic = self._app_config.get('qa_embedder.input_topic') or 'qa-embedding'
            self.output_topic = self._app_config.get('qa_embedder.output_topic')  # Optional

            # Dead letter queue topic (hardcoded since not in config)
            self.dlq_topic = 'dead-letter-queue'

            # Q&A embedding configuration
            self.cache_dir = Path(self._app_config.get('qa_embedder.cache_directory') or 'cache/qa_embeddings')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            # Embedding model configuration
            self.model_name = self._app_config.get('qa_embedder.model_name') or 'all-MiniLM-L6-v2'
            self.batch_size = self._app_config.get('qa_embedder.batch_size') or 32
            self.separator = self._app_config.get('qa_embedder.separator') or ' [SEP] '
            self.embedding_mode = self._app_config.get('qa_embedder.mode') or 'all'

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Model: {self.model_name}")
            self._logger.info(f"   Batch size: {self.batch_size}")
            self._logger.info(f"   Embedding mode: {self.embedding_mode}")

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

    def _publish_to_dlq(self, original_message: Dict[str, Any], error: str, failed_stage: str = "qa_embedder"):
        """
        Publish failed message to dead letter queue.

        Args:
            original_message: Original message that failed
            error: Error message
            failed_stage: Stage where failure occurred
        """
        try:
            if not self.producer:
                self._create_producer()

            dlq_message = {
                "original_message": original_message,
                "failed_stage": failed_stage,
                "error": str(error),
                "timestamp": datetime.now().isoformat(),
                "request_id": original_message.get('request_id'),
                "session_id": original_message.get('session_id')
            }

            self.producer.produce(
                self.dlq_topic,
                key=original_message.get('request_id', '').encode('utf-8'),
                value=json.dumps(dlq_message).encode('utf-8')
            )
            self.producer.flush()

            self._logger.warning(f"📮 Published to DLQ: {failed_stage} - {error}")

        except Exception as e:
            self._logger.error(f"❌ Error publishing to DLQ: {e}")

    def _init_embedding_model(self):
        """Initialize sentence transformer embedding model"""
        try:
            self._logger.info(f"🤖 Loading embedding model: {self.model_name}")
            self.embedding_model = SentenceTransformer(self.model_name)
            self.embedding_dimension = self.embedding_model.get_sentence_embedding_dimension()
            self._logger.info(f"✅ Model loaded (dimension: {self.embedding_dimension})")
        except Exception as e:
            self._logger.error(f"❌ Error loading embedding model: {e}")
            raise

    def _load_qa_pairs(self, qa_file: str) -> List[Dict[str, Any]]:
        """
        Load Q&A pairs from JSONL file.

        Args:
            qa_file: Path to Q&A pairs JSONL

        Returns:
            List of Q&A pair dictionaries
        """
        qa_pairs = []
        qa_path = Path(qa_file)

        if not qa_path.exists():
            raise FileNotFoundError(f"Q&A file not found: {qa_file}")

        self._logger.info(f"📂 Loading Q&A pairs from: {qa_file}")

        with open(qa_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    qa_pair = json.loads(line.strip())
                    qa_pairs.append(qa_pair)
                except json.JSONDecodeError as e:
                    self._logger.warning(f"⚠️  Line {line_num}: JSON decode error: {e}")
                    continue

        self._logger.info(f"✅ Loaded {len(qa_pairs)} Q&A pairs")
        return qa_pairs

    def _create_combined_text(self, question: str, answer: str) -> str:
        """
        Create combined text from question and answer.

        Args:
            question: Question text
            answer: Answer text

        Returns:
            Combined text with separator
        """
        return f"{question.strip()}{self.separator}{answer.strip()}"

    def _generate_embeddings(
        self,
        qa_pairs: List[Dict[str, Any]],
        mode: str = 'all'
    ) -> List[Dict[str, Any]]:
        """
        Generate embeddings for Q&A pairs.

        Args:
            qa_pairs: List of Q&A pair dictionaries
            mode: Embedding mode - 'question', 'answer', 'combined', or 'all'

        Returns:
            List of enriched Q&A pairs with embeddings
        """
        self._logger.info(f"🔢 Generating embeddings (mode: {mode})")
        self._logger.info(f"   Processing {len(qa_pairs)} Q&A pairs...")

        # Prepare texts for each mode
        questions = []
        answers = []
        combined_texts = []

        for qa in qa_pairs:
            question = qa.get('question', '')
            answer = qa.get('answer', '')

            questions.append(question)
            answers.append(answer)
            combined_texts.append(self._create_combined_text(question, answer))

        # Generate embeddings based on mode
        question_embeddings = None
        answer_embeddings = None
        combined_embeddings = None

        if mode in ['question', 'all']:
            self._logger.info(f"   → Encoding {len(questions)} questions...")
            question_embeddings = self.embedding_model.encode(
                questions,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                show_progress_bar=False
            )
            self._logger.info(f"   ✓ Question embeddings: {question_embeddings.shape}")

        if mode in ['answer', 'all']:
            self._logger.info(f"   → Encoding {len(answers)} answers...")
            answer_embeddings = self.embedding_model.encode(
                answers,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                show_progress_bar=False
            )
            self._logger.info(f"   ✓ Answer embeddings: {answer_embeddings.shape}")

        if mode in ['combined', 'all']:
            self._logger.info(f"   → Encoding {len(combined_texts)} combined texts...")
            combined_embeddings = self.embedding_model.encode(
                combined_texts,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                show_progress_bar=False
            )
            self._logger.info(f"   ✓ Combined embeddings: {combined_embeddings.shape}")

        # Create enriched Q&A pairs
        enriched_pairs = []
        timestamp = datetime.now().isoformat()

        for i, qa in enumerate(qa_pairs):
            enriched = {
                # Core Q&A data
                'question': qa.get('question', ''),
                'answer': qa.get('answer', ''),
                'context': qa.get('context', ''),
                'chunk_id': qa.get('chunk_id', ''),
                'source_url': qa.get('source_url', ''),
                'document_id': qa.get('document_id', ''),
                'section_hierarchy': qa.get('section_hierarchy', []),

                # Embeddings
                'question_embedding': question_embeddings[i].tolist() if question_embeddings is not None else None,
                'answer_embedding': answer_embeddings[i].tolist() if answer_embeddings is not None else None,
                'combined_embedding': combined_embeddings[i].tolist() if combined_embeddings is not None else None,

                # Embedding metadata
                'embedding_model': self.model_name,
                'embedding_dimension': self.embedding_dimension,
                'enrichment_timestamp': timestamp,

                # Original metadata (copy all other fields)
                'chunk_type': qa.get('chunk_type'),
                'processing_type': qa.get('processing_type'),
                'parent_section': qa.get('parent_section'),
                'word_count_question': len(qa.get('question', '').split()),
                'word_count_answer': len(qa.get('answer', '').split()),
                'qa_index': qa.get('qa_index'),
                'qa_total_for_source': qa.get('qa_total_for_source'),
                'generation_timestamp': qa.get('generation_timestamp'),

                # Important metadata from Q&A generation
                'title': qa.get('title'),
                'author': qa.get('author'),
                'creation_date': qa.get('creation_date'),
                'related_chunk_ids': qa.get('related_chunk_ids'),
                'used_enriched_text': qa.get('used_enriched_text'),
                'word_count_source': qa.get('word_count_source'),
                'word_count_context': qa.get('word_count_context')
            }

            # Remove None values
            enriched = {k: v for k, v in enriched.items() if v is not None}
            enriched_pairs.append(enriched)

        self._logger.info(f"✅ Created {len(enriched_pairs)} enriched Q&A pairs")

        return enriched_pairs

    def _save_enriched_pairs(
        self,
        enriched_pairs: List[Dict[str, Any]],
        output_file: str
    ):
        """
        Save enriched Q&A pairs to JSONL file.

        Args:
            enriched_pairs: List of enriched Q&A pairs
            output_file: Output JSONL file path
        """
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self._logger.info(f"💾 Saving enriched Q&A pairs to: {output_file}")

        with open(output_path, 'w', encoding='utf-8') as f:
            for enriched in enriched_pairs:
                json.dump(enriched, f, ensure_ascii=False)
                f.write('\n')

        file_size = output_path.stat().st_size / (1024 * 1024)  # MB
        self._logger.info(f"✅ Saved {len(enriched_pairs)} pairs ({file_size:.2f} MB)")

    def _generate_summary(
        self,
        enriched_pairs: List[Dict[str, Any]],
        request_id: str,
        output_dir: Path
    ) -> Dict[str, Any]:
        """
        Generate processing summary.

        Args:
            enriched_pairs: List of enriched Q&A pairs
            request_id: Request ID
            output_dir: Output directory

        Returns:
            Summary dictionary
        """
        # Count embeddings
        has_question = sum(1 for p in enriched_pairs if p.get('question_embedding') is not None)
        has_answer = sum(1 for p in enriched_pairs if p.get('answer_embedding') is not None)
        has_combined = sum(1 for p in enriched_pairs if p.get('combined_embedding') is not None)

        # Word count statistics
        word_counts_q = [p.get('word_count_question', 0) for p in enriched_pairs if p.get('word_count_question')]
        word_counts_a = [p.get('word_count_answer', 0) for p in enriched_pairs if p.get('word_count_answer')]

        summary = {
            'processing_metadata': {
                'request_id': request_id,
                'total_qa_pairs': len(enriched_pairs),
                'embedding_model': self.model_name,
                'embedding_dimension': self.embedding_dimension,
                'enrichment_timestamp': datetime.now().isoformat()
            },
            'embedding_statistics': {
                'pairs_with_question_embedding': has_question,
                'pairs_with_answer_embedding': has_answer,
                'pairs_with_combined_embedding': has_combined
            },
            'content_statistics': {
                'question_word_count': {
                    'min': min(word_counts_q) if word_counts_q else 0,
                    'max': max(word_counts_q) if word_counts_q else 0,
                    'mean': sum(word_counts_q) / len(word_counts_q) if word_counts_q else 0
                },
                'answer_word_count': {
                    'min': min(word_counts_a) if word_counts_a else 0,
                    'max': max(word_counts_a) if word_counts_a else 0,
                    'mean': sum(word_counts_a) / len(word_counts_a) if word_counts_a else 0
                }
            },
            'document_coverage': {
                'unique_documents': len(set(p.get('document_id', '') for p in enriched_pairs)),
                'unique_chunks': len(set(p.get('chunk_id', '') for p in enriched_pairs)),
                'unique_sources': len(set(p.get('source_url', '') for p in enriched_pairs))
            }
        }

        # Save summary
        summary_file = output_dir / f"{request_id}_qa_embedding_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        self._logger.info(f"📊 Processing Summary:")
        self._logger.info(f"   Total Q&A pairs: {summary['processing_metadata']['total_qa_pairs']}")
        self._logger.info(f"   Question embeddings: {has_question}")
        self._logger.info(f"   Answer embeddings: {has_answer}")
        self._logger.info(f"   Combined embeddings: {has_combined}")
        self._logger.info(f"   Questions: {summary['content_statistics']['question_word_count']['mean']:.1f} words (avg)")
        self._logger.info(f"   Answers: {summary['content_statistics']['answer_word_count']['mean']:.1f} words (avg)")
        self._logger.info(f"💾 Summary saved to: {summary_file}")

        return summary

    def _process_message(self, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single message to generate Q&A pair embeddings.

        Args:
            message_data: Message data from Kafka

        Returns:
            Processing result or None if failed
        """
        try:
            # Extract message fields
            request_id = message_data.get('request_id')
            session_id = message_data.get('session_id')
            qa_pairs_file = message_data.get('qa_pairs_file')
            embedded_chunks_file = message_data.get('embedded_chunks_file')  # From chunk embedder via QA generator
            document_id = message_data.get('document_id')
            source_url = message_data.get('source_url')

            self._logger.info("="*80)
            self._logger.info(f"🔄 Processing Q&A Embedding Request")
            self._logger.info(f"   Request ID: {request_id}")
            self._logger.info(f"   Session ID: {session_id}")
            self._logger.info(f"   Q&A pairs file: {qa_pairs_file}")
            self._logger.info(f"   Embedded chunks file: {embedded_chunks_file}")
            self._logger.info("="*80)

            # Validate Q&A pairs file exists
            if not qa_pairs_file or not Path(qa_pairs_file).exists():
                raise FileNotFoundError(f"Q&A pairs file not found: {qa_pairs_file}")

            # Load Q&A pairs
            qa_pairs = self._load_qa_pairs(qa_pairs_file)

            if not qa_pairs:
                raise ValueError("No Q&A pairs found in file")

            # Generate embeddings
            enriched_pairs = self._generate_embeddings(qa_pairs, mode=self.embedding_mode)

            # Determine output directory (same as Q&A pairs file)
            qa_path = Path(qa_pairs_file)
            output_dir = qa_path.parent

            # Save enriched Q&A pairs
            enriched_file = output_dir / f"{request_id}_qa_pairs_enriched_embedded.jsonl"
            self._save_enriched_pairs(enriched_pairs, str(enriched_file))

            # Generate and save summary
            summary = self._generate_summary(enriched_pairs, request_id, output_dir)

            # Prepare result message
            result = {
                'request_id': request_id,
                'session_id': session_id,
                'embedded_chunks_file': embedded_chunks_file,  # Pass through from chunk embedder
                'enriched_qa_pairs_file': str(enriched_file),
                'document_id': document_id,
                'source_url': source_url,
                'qa_count': len(enriched_pairs),
                'embedding_statistics': summary['embedding_statistics'],
                'processing_timestamp': datetime.now().isoformat()
            }

            # Pass through total_requests if present for aggregator
            if 'total_requests' in message_data:
                result['total_requests'] = message_data['total_requests']

            self._logger.info("="*80)
            self._logger.info(f"✅ Successfully processed Q&A embedding for session {session_id}")
            self._logger.info(f"   Enriched Q&A pairs: {enriched_file}")
            self._logger.info(f"   Total pairs: {len(enriched_pairs)}")
            self._logger.info("="*80)

            return result

        except Exception as e:
            self._logger.error(f"❌ Error processing message: {e}", exc_info=True)
            self._publish_to_dlq(message_data, str(e))
            return None

    async def consume_messages(self):
        """Main consumer loop"""
        try:
            # Initialize embedding model
            if not self.embedding_model:
                self._init_embedding_model()

            # Create Kafka consumer
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.input_topic])

            self._logger.info(f"📡 Subscribed to topic: {self.input_topic}")
            self._logger.info("🚀 Starting to consume Q&A embedding messages...")

            self.running = True

            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self._logger.error(f"❌ Consumer error: {msg.error()}")
                        continue

                try:
                    # Parse message
                    message_data = json.loads(msg.value().decode('utf-8'))

                    # Process message
                    result = self._process_message(message_data)

                    # Publish result to output topic if configured
                    if result and self.output_topic:
                        if not self.producer:
                            self._create_producer()

                        self.producer.produce(
                            self.output_topic,
                            key=result.get('request_id', '').encode('utf-8'),
                            value=json.dumps(result).encode('utf-8')
                        )
                        self.producer.flush()
                        self._logger.info(f"📤 Published result to topic: {self.output_topic}")

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Error decoding message: {e}")
                    continue
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)
                    continue

        except Exception as e:
            self._logger.error(f"❌ Consumer error: {e}", exc_info=True)
            raise
        finally:
            if self.consumer:
                self.consumer.close()
                self._logger.info("👋 Kafka consumer closed")
            if self.producer:
                self.producer.flush()
                self._logger.info("👋 Kafka producer closed")

    def run(self):
        """Run the consumer"""
        try:
            import asyncio
            asyncio.run(self.consume_messages())
        except KeyboardInterrupt:
            self._logger.info("🛑 Interrupted by user")
        except Exception as e:
            self._logger.error(f"❌ Fatal error: {e}", exc_info=True)
            sys.exit(1)


def main():
    """Main entry point"""
    consumer = KafkaQAEmbedderConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
