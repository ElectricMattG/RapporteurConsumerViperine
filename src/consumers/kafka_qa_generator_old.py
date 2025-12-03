#!/usr/bin/env python3
"""
Kafka Q&A Pair Generator Consumer

Generates question-answer pairs from enriched embedded chunks using an LLM.
This consumer wraps the PDF Q&A Generator component which handles all the
sophisticated prompt engineering and content processing.

Pipeline Flow:
  1. Web Scraper → extracts content
  2. Summarizer → generates summaries
  3. Chunker → creates chunks
  4. Citation Extractor → enriches with citations
  5. Chunk Embedder → generates embeddings
  6. Q&A Generator (this) → creates Q&A pairs from chunks

Message Format (Input - from chunk embedder):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "embedded_chunks_file": "path/to/chunked_document_enriched_embedded.jsonl",
        "document_id": "doc-id",
        "source_url": "original-url",
        "chunk_count": 4
    }

Output:
    - Q&A pairs JSONL file (via PDFQAGenerator)
    - Q&A statistics JSON
    - Published to next topic (if configured)

Usage:
    python run_kafka_qa_generator.py
"""

import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Import components
try:
    from ..components.q_and_a.pdf_qa_generator import PDFQAGenerator
    from ..config_handler import Base
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from components.q_and_a.pdf_qa_generator import PDFQAGenerator
    from config_handler import Base


class KafkaQAGeneratorConsumer(Base):
    """Kafka consumer for generating Q&A pairs from enriched chunks"""

    def __init__(self):
        """Initialize the Kafka Q&A generator consumer"""
        super().__init__()
        self._logger.info("Initializing KafkaQAGeneratorConsumer")

        # Load configuration
        self._load_config()

        # Initialize LLM client
        self._init_llm_client()

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
                'group.id': self._app_config.get('qa_generator.consumer_group_id') or 'qa-generator-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 30000,
                'max.poll.interval.ms': 600000,  # 10 minutes for Q&A generation
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
            self.input_topic = self._app_config.get('qa_generator.input_topic') or 'chunk-embedding'

            # Q&A generation configuration
            self.cache_dir = Path(self._app_config.get('qa_generator.cache_directory') or 'cache/qa_pairs')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            self.qa_pairs_per_chunk = self._app_config.get('qa_generator.pairs_per_chunk') or 2
            self.min_chunk_length = self._app_config.get('qa_generator.min_chunk_length') or 100
            self.llm_provider = self._app_config.get('qa_generator.llm_provider') or 'gemini'

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Q&A pairs per chunk: {self.qa_pairs_per_chunk}")
            self._logger.info(f"   Min chunk length: {self.min_chunk_length}")
            self._logger.info(f"   LLM provider: {self.llm_provider}")

        except Exception as e:
            self._logger.error(f"❌ Error loading configuration: {e}")
            raise

    def _init_llm_client(self):
        """Initialize the LLM client based on configuration"""
        try:
            if self.llm_provider == 'gemini':
                if GeminiLLMClient is None:
                    raise ImportError("GeminiLLMClient not available")
                self.llm_client = GeminiLLMClient()
                self._logger.info("✅ Initialized Gemini LLM client")
            elif self.llm_provider == 'openai':
                if OpenAILLMClient is None:
                    raise ImportError("OpenAILLMClient not available")
                self.llm_client = OpenAILLMClient()
                self._logger.info("✅ Initialized OpenAI LLM client")
            else:
                raise ValueError(f"Unknown LLM provider: {self.llm_provider}")
        except Exception as e:
            self._logger.error(f"❌ Error initializing LLM client: {e}")
            raise

    def _create_consumer(self):
        """Create Kafka consumer instance"""
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.input_topic])
            self._logger.info(f"✅ Subscribed to topic: {self.input_topic}")
        except Exception as e:
            self._logger.error(f"❌ Error creating Kafka consumer: {e}")
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

    def _publish_to_dlq(self, original_message: Dict[str, Any], stage: str, error: str):
        """Publish failed message to dead letter queue.

        Args:
            original_message: The original message that failed
            stage: The pipeline stage where failure occurred
            error: The error message
        """
        try:
            import json
            from datetime import datetime

            # Initialize producer if not exists
            if not hasattr(self, 'dlq_producer'):
                from confluent_kafka import Producer
                self.dlq_producer = Producer({
                    'bootstrap.servers': self._app_config.get('kafka.bootstrap_servers') or 'localhost:9092'
                })

            dlq_message = {
                'original_message': original_message,
                'failed_stage': stage,
                'error': error,
                'timestamp': datetime.now().isoformat(),
                'request_id': original_message.get('request_id', 'unknown'),
                'session_id': original_message.get('session_id', 'unknown')
            }

            self.dlq_producer.produce(
                topic='dead-letter-queue',
                value=json.dumps(dlq_message).encode('utf-8')
            )
            self.dlq_producer.flush()
            self._logger.info(f"   📮 Published failure to dead-letter-queue")
        except Exception as e:
            self._logger.error(f"   ⚠️ Failed to publish to DLQ: {e}")

    async def _generate_qa_pairs(self, chunk_content: str, chunk_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate Q&A pairs from a chunk using LLM.

        Args:
            chunk_content: The chunk text content
            chunk_metadata: Metadata about the chunk

        Returns:
            List of Q&A pair dictionaries
        """
        # Check if chunk is too short for meaningful Q&A
        word_count = len(chunk_content.split())
        if word_count < 15:
            self._logger.debug(f"   ⚠️  Chunk too short ({word_count} words) for Q&A generation, skipping")
            return []

        # Build improved prompt for Q&A generation (factual, objective, avoids safety triggers)
        prompt = f"""Generate {self.qa_pairs_per_chunk} factual question-answer pairs from this text excerpt.

IMPORTANT INSTRUCTIONS:
- Focus ONLY on factual information explicitly stated in the text
- Questions should be specific and directly answerable from the content
- Answers should be concise and based solely on the provided text
- Use objective, neutral language (avoid sensationalism or judgment)
- Do NOT add external context, opinions, or interpretations
- Do NOT make inferences beyond what is explicitly stated

Text excerpt:
{chunk_content}

Output ONLY valid JSON in this exact format:
```json
[{{"question": "What information does this text provide about X?", "answer": "According to the text, ..."}}]
```

Generate exactly {self.qa_pairs_per_chunk} factual Q&A pairs now:
"""

        try:
            # Call LLM to generate Q&A pairs
            response = await self.llm_client.generate_content(prompt)

            # Check if response is None or empty
            if not response:
                self._logger.error(f"   ❌ Gemini returned empty response")
                return []

            # Parse JSON response
            # Try to extract JSON from markdown code blocks if present
            response_text = response.strip()
            self._logger.debug(f"   📝 Gemini response ({len(response_text)} chars): {response_text[:200]}...")

            if '```json' in response_text:
                json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(1)
            elif '```' in response_text:
                json_match = re.search(r'```\s*(.*?)\s*```', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(1)

            qa_pairs = json.loads(response_text)

            # Validate structure
            if not isinstance(qa_pairs, list):
                raise ValueError("Response is not a list")

            valid_pairs = []
            for pair in qa_pairs:
                if isinstance(pair, dict) and 'question' in pair and 'answer' in pair:
                    valid_pairs.append(pair)

            return valid_pairs

        except Exception as e:
            self._logger.error(f"   ❌ Error generating Q&A pairs: {e}")
            return []

    async def _process_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a Q&A generation message.

        Args:
            message_data: Message containing embedded chunks file path

        Returns:
            Result dictionary with generation status
        """
        session_id = message_data.get('session_id', 'unknown')
        request_id = message_data.get('request_id', f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        embedded_chunks_file = message_data.get('embedded_chunks_file')
        document_id = message_data.get('document_id', 'unknown')
        source_url = message_data.get('source_url', 'unknown')
        chunk_count = message_data.get('chunk_count', 0)

        self._logger.info("=" * 80)
        self._logger.info(f"❓ Processing Q&A generation request")
        self._logger.info(f"   Session ID: {session_id}")
        self._logger.info(f"   Request ID: {request_id}")
        self._logger.info(f"   Document ID: {document_id}")
        self._logger.info(f"   Embedded chunks file: {embedded_chunks_file}")
        self._logger.info(f"   Source URL: {source_url}")
        self._logger.info(f"   Chunk count: {chunk_count}")

        result = {
            'session_id': session_id,
            'request_id': request_id,
            'document_id': document_id,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'chunks_processed': 0,
            'qa_pairs_generated': 0,
            'qa_file': None,
            'error': None
        }

        # Validate embedded chunks file exists
        if not embedded_chunks_file or not Path(embedded_chunks_file).exists():
            error_msg = f"Embedded chunks file not found: {embedded_chunks_file}"
            self._logger.error(f"   ❌ {error_msg}")
            result['error'] = error_msg
            return result

        try:
            # Read embedded chunks from JSONL file
            chunks = []
            with open(embedded_chunks_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        chunks.append(json.loads(line))

            self._logger.info(f"   📄 Loaded {len(chunks)} chunks from file")

            # Generate Q&A pairs for each chunk
            all_qa_pairs = []
            chunks_processed = 0

            for i, chunk in enumerate(chunks):
                content = chunk.get('content', '')
                chunk_metadata = chunk.get('chunk_metadata', {})
                chunk_id = chunk_metadata.get('chunk_id', f'chunk-{i}')

                # Skip chunks that are too short
                if len(content) < self.min_chunk_length:
                    self._logger.debug(f"   ⏭️  Skipping chunk {chunk_id} (too short: {len(content)} chars)")
                    continue

                self._logger.info(f"   🤖 Generating Q&A pairs for chunk {chunk_id}...")

                # Generate Q&A pairs
                qa_pairs = await self._generate_qa_pairs(content, chunk_metadata)

                if qa_pairs:
                    # Add metadata to each Q&A pair
                    for j, pair in enumerate(qa_pairs):
                        pair_id = f"qa-{request_id}-{chunk_id}-{j}"
                        qa_record = {
                            'pair_id': pair_id,
                            'question': pair['question'],
                            'answer': pair['answer'],
                            'chunk_id': chunk_id,
                            'source_url': source_url,
                            'context': content[:500],  # First 500 chars as context
                            'metadata': {
                                'chunk_type': chunk_metadata.get('chunk_type'),
                                'has_citations': chunk_metadata.get('has_citations', False),
                                'citation_count': chunk_metadata.get('citation_count', 0),
                                'document_id': document_id,
                                'generated_at': datetime.now().isoformat()
                            }
                        }
                        all_qa_pairs.append(qa_record)

                    self._logger.info(f"   ✅ Generated {len(qa_pairs)} Q&A pairs from chunk {chunk_id}")
                    chunks_processed += 1
                else:
                    self._logger.warning(f"   ⚠️ No Q&A pairs generated for chunk {chunk_id}")

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)

            self._logger.info(f"   📊 Total: {len(all_qa_pairs)} Q&A pairs from {chunks_processed} chunks")

            # Save Q&A pairs to file
            original_path = Path(embedded_chunks_file)
            qa_filename = f"{original_path.stem.replace('_enriched_embedded', '')}_qa_pairs.jsonl"
            qa_file = original_path.parent / qa_filename

            with open(qa_file, 'w', encoding='utf-8') as f:
                for pair in all_qa_pairs:
                    f.write(json.dumps(pair, ensure_ascii=False) + '\n')

            self._logger.info(f"   💾 Saved Q&A pairs to: {qa_file}")

            # Create Q&A summary
            summary = {
                'request_id': request_id,
                'session_id': session_id,
                'document_id': document_id,
                'source_url': source_url,
                'embedded_chunks_file': str(embedded_chunks_file),
                'qa_pairs_file': str(qa_file),
                'total_chunks': len(chunks),
                'chunks_processed': chunks_processed,
                'qa_pairs_generated': len(all_qa_pairs),
                'avg_pairs_per_chunk': len(all_qa_pairs) / chunks_processed if chunks_processed > 0 else 0,
                'processed_at': datetime.now().isoformat()
            }

            # Save summary
            summary_file = self.cache_dir / f"{request_id}_qa_summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            self._logger.info(f"   📊 Saved Q&A summary to: {summary_file}")

            result['chunks_processed'] = chunks_processed
            result['qa_pairs_generated'] = len(all_qa_pairs)
            result['qa_file'] = str(qa_file)
            result['success'] = True

            self._logger.info(f"   ✅ Successfully generated {len(all_qa_pairs)} Q&A pairs")
            self._logger.info("=" * 80)

        except Exception as e:
            error_msg = f"Error generating Q&A pairs: {e}"
            self._logger.error(f"   ❌ {error_msg}", exc_info=True)
            result['error'] = error_msg
            # Publish to dead letter queue
            self._publish_to_dlq(message_data, 'qa_generator', error_msg)
            self._logger.info("=" * 80)

        return result

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
    print("❓ Kafka Q&A Pair Generator Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaQAGeneratorConsumer()

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
