#!/usr/bin/env python3
"""
Kafka Index Aggregator Consumer

Aggregates embedded chunks and Q&A pairs from research sessions into
unified research indices stored in Azure Blob Storage.

Features:
- Consumes from qa-generation and qa-embedding topics
- Accumulates documents by session_id
- Downloads existing research indices from Azure
- Normalizes Q&A pairs to enriched chunks schema
- Deduplicates by document_id and chunk_id
- Uploads updated indices to Azure
- Archives processed files with 7-day retention
- Cleans up local cache directories

Output:
- research_chunks_with_embeddings_combined.jsonl
- research_qa_pairs_with_embeddings_combined.jsonl
"""

import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from pathlib import Path
from typing import Dict, List, Set

from azure.storage.blob import BlobServiceClient, ContentSettings
from confluent_kafka import Consumer, KafkaError, KafkaException

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from config_handler import Base
from utils.index_adapter import transform_qa_index_to_chunks


class KafkaIndexAggregatorConsumer(Base):
    """
    Kafka consumer that aggregates embedded documents into research indices.

    Consumes from:
    - qa-generation: Enriched document chunks with embeddings (from chunk embedder)
    - qa-embedding: Q&A pairs with embeddings (from qa embedder, normalized before aggregation)

    Uploads to Azure:
    - research_chunks_with_embeddings_combined.jsonl
    - research_qa_pairs_with_embeddings_combined.jsonl
    """

    def __init__(self):
        """Initialize the index aggregator consumer."""
        super().__init__()
        self._logger.info("Initializing KafkaIndexAggregatorConsumer")

        # Kafka configuration
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.group_id = 'index-aggregator-consumer'
        self.input_topic = 'qa-indexed'  # Subscribe to QA embedder output (includes both chunks and QA pairs)

        # Azure Blob Storage configuration
        self.connection_string = (
            os.getenv('AZURE_STORAGE_CONNECTION_STRING') or
            self._app_config.get('azure_blob.connection_string')
        )
        if not self.connection_string:
            self._logger.warning("⚠️  AZURE_STORAGE_CONNECTION_STRING not set - index aggregator will not upload to Azure")
            self._logger.warning("⚠️  Files will be saved locally only")
            self.upload_to_azure = False
        else:
            self.upload_to_azure = True

        self.container_name = 'indices'
        self.archive_container = 'research-archive'
        self.chunks_blob_name = 'research_chunks_with_embeddings_combined.jsonl'
        self.qa_blob_name = 'research_qa_pairs_with_embeddings_combined.jsonl'

        # Session accumulation
        self.session_chunks: Dict[str, List[Dict]] = defaultdict(list)
        self.session_qa_pairs: Dict[str, List[Dict]] = defaultdict(list)
        self.session_request_ids: Dict[str, Set[str]] = defaultdict(set)
        self.session_last_activity: Dict[str, float] = {}
        self.session_total_requests: Dict[str, int] = {}  # Expected total requests per session

        # Configuration
        self.cache_dir = Path(self._app_config.get('web_scraper.cache_dir') or './cache/web_scrapes')
        self.session_timeout = 300  # 5 minutes of inactivity triggers aggregation (fallback)
        self.cleanup_after_upload = True
        self.archive_before_cleanup = True

        # Initialize Azure Blob Service Client (if credentials available)
        if self.upload_to_azure:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            # Ensure containers exist
            self._ensure_containers_exist()
        else:
            self.blob_service_client = None

        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,  # 45 seconds
            'heartbeat.interval.ms': 3000,  # 3 seconds
            'max.poll.interval.ms': 600000,  # 10 minutes
        })

        self._logger.info("✅ Configuration loaded:")
        self._logger.info(f"   Kafka servers: {self.bootstrap_servers}")
        self._logger.info(f"   Consumer group: {self.group_id}")
        self._logger.info(f"   Input topic: {self.input_topic}")
        self._logger.info(f"   Azure container: {self.container_name}")
        self._logger.info(f"   Archive container: {self.archive_container}")
        self._logger.info(f"   Chunks blob: {self.chunks_blob_name}")
        self._logger.info(f"   Q&A blob: {self.qa_blob_name}")
        self._logger.info(f"   Session timeout: {self.session_timeout}s")
        self._logger.info("✅ KafkaIndexAggregatorConsumer initialized successfully")

    def _ensure_containers_exist(self):
        """Ensure Azure containers exist."""
        try:
            # Create indices container if not exists
            try:
                container_client = self.blob_service_client.get_container_client(self.container_name)
                if not container_client.exists():
                    self.blob_service_client.create_container(self.container_name)
                    self._logger.info(f"✅ Created container: {self.container_name}")
            except Exception as e:
                self._logger.debug(f"Container {self.container_name} check: {e}")

            # Create archive container with lifecycle policy
            try:
                archive_client = self.blob_service_client.get_container_client(self.archive_container)
                if not archive_client.exists():
                    self.blob_service_client.create_container(self.archive_container)
                    self._logger.info(f"✅ Created archive container: {self.archive_container}")
                    self._logger.warning("⚠️  Configure 7-day lifecycle policy on archive container via Azure Portal")
            except Exception as e:
                self._logger.debug(f"Archive container {self.archive_container} check: {e}")

        except Exception as e:
            self._logger.error(f"Error ensuring containers exist: {e}")

    def run(self):
        """Start consuming messages."""
        try:
            self._logger.info("🚀 Starting index aggregation consumer...")

            # Subscribe to qa-indexed and session-complete topics
            topics = [self.input_topic, 'session-complete']
            self.consumer.subscribe(topics)
            self._logger.info(f"✅ Subscribed to topics: {', '.join(topics)}")

            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # Check for timed-out sessions
                    self._check_session_timeouts()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                # Process message
                self._process_message(msg)

        except KeyboardInterrupt:
            self._logger.info("⚠️  Received interrupt signal")
        except Exception as e:
            self._logger.error(f"❌ Consumer error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Process any remaining sessions before shutdown
            self._logger.info("Processing remaining sessions before shutdown...")
            for session_id in list(self.session_chunks.keys()):
                self._aggregate_and_upload_session(session_id)

            self.consumer.close()
            self._logger.info("✅ Consumer stopped")

    def _process_message(self, msg):
        """Process a single Kafka message."""
        try:
            # Decode message
            message_data = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()

            session_id = message_data.get('session_id', 'unknown')
            request_id = message_data.get('request_id', 'unknown')

            # Update last activity timestamp
            self.session_last_activity[session_id] = time.time()

            # Track total expected requests if provided
            if 'total_requests' in message_data:
                self.session_total_requests[session_id] = message_data['total_requests']

            # Check if this is a session complete signal
            if message_data.get('session_complete'):
                self._logger.info(f"🏁 Session complete signal received: {session_id}")
                self._aggregate_and_upload_session(session_id)
                return

            # Regular request processing
            self.session_request_ids[session_id].add(request_id)

            # Get both files from the message
            chunks_file = message_data.get('embedded_chunks_file')
            qa_file = message_data.get('enriched_qa_pairs_file')

            self._logger.info(f"📥 Processing request: {session_id}/{request_id}")

            # Load chunks
            if chunks_file and Path(chunks_file).exists():
                self._logger.info(f"  📦 Loading chunks from: {chunks_file}")
                with open(chunks_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            chunk = json.loads(line.strip())
                            self.session_chunks[session_id].append(chunk)
                chunks_count = len(list(open(chunks_file)))
                self._logger.info(f"     ✅ Loaded {chunks_count} chunks")
            else:
                self._logger.warning(f"     ⚠️  Chunks file not found: {chunks_file}")

            # Load Q&A pairs
            if qa_file and Path(qa_file).exists():
                self._logger.info(f"  ❓ Loading Q&A pairs from: {qa_file}")
                with open(qa_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            qa_pair = json.loads(line.strip())
                            self.session_qa_pairs[session_id].append(qa_pair)
                qa_count = len(list(open(qa_file)))
                self._logger.info(f"     ✅ Loaded {qa_count} Q&A pairs")
            else:
                self._logger.warning(f"     ⚠️  Q&A pairs file not found: {qa_file}")

            # Check if all expected requests have been received
            if session_id in self.session_total_requests:
                expected = self.session_total_requests[session_id]
                received = len(self.session_request_ids[session_id])
                if received >= expected:
                    self._logger.info(f"✅ All requests received ({received}/{expected}) for session: {session_id}")
                    self._aggregate_and_upload_session(session_id)

        except Exception as e:
            self._logger.error(f"Error processing message: {e}")
            import traceback
            traceback.print_exc()

    def _check_session_timeouts(self):
        """Check for sessions that have timed out and process them."""
        current_time = time.time()

        for session_id, last_activity in list(self.session_last_activity.items()):
            if current_time - last_activity > self.session_timeout:
                self._logger.info(f"⏰ Session timeout: {session_id}")
                self._aggregate_and_upload_session(session_id)

    def _aggregate_and_upload_session(self, session_id: str):
        """Aggregate session documents and upload to Azure."""
        try:
            self._logger.info("=" * 80)
            self._logger.info(f"📊 Aggregating session: {session_id}")
            self._logger.info(f"   Requests processed: {len(self.session_request_ids[session_id])}")
            self._logger.info(f"   Total chunks: {len(self.session_chunks[session_id])}")
            self._logger.info(f"   Total Q&A pairs: {len(self.session_qa_pairs[session_id])}")

            # Process chunks
            if self.session_chunks[session_id]:
                self._upload_chunks(session_id, self.session_chunks[session_id])

            # Process Q&A pairs
            if self.session_qa_pairs[session_id]:
                self._upload_qa_pairs(session_id, self.session_qa_pairs[session_id])

            # Archive and cleanup
            if self.archive_before_cleanup:
                self._archive_session(session_id)

            if self.cleanup_after_upload:
                self._cleanup_session(session_id)

            # Clear session data
            del self.session_chunks[session_id]
            del self.session_qa_pairs[session_id]
            del self.session_request_ids[session_id]
            del self.session_last_activity[session_id]

            self._logger.info(f"✅ Session aggregation complete: {session_id}")
            self._logger.info("=" * 80)

        except Exception as e:
            self._logger.error(f"Error aggregating session {session_id}: {e}")
            import traceback
            traceback.print_exc()

    def _upload_chunks(self, session_id: str, new_chunks: List[Dict]):
        """Upload chunks to Azure research index."""
        if not self.upload_to_azure:
            self._logger.warning("⚠️  No Azure credentials - saving locally instead")
            self._save_chunks_locally(new_chunks)
            return

        try:
            self._logger.info(f"📤 Uploading chunks to Azure...")

            # Download existing index
            existing_chunks = self._download_blob(self.chunks_blob_name)

            # Deduplicate by document_id and chunk_id
            existing_ids = set()
            all_chunks = []

            for chunk in existing_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        all_chunks.append(chunk)

            # Add new chunks (skip duplicates)
            new_count = 0
            for chunk in new_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        all_chunks.append(chunk)
                        new_count += 1

            self._logger.info(f"   📊 Existing chunks: {len(existing_chunks)}")
            self._logger.info(f"   ➕ New chunks: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {len(new_chunks) - new_count}")
            self._logger.info(f"   📈 Total chunks: {len(all_chunks)}")

            # Upload to Azure
            self._upload_blob(self.chunks_blob_name, all_chunks)
            self._logger.info(f"   ✅ Chunks uploaded successfully")

        except Exception as e:
            self._logger.error(f"Error uploading chunks: {e}")
            import traceback
            traceback.print_exc()

    def _upload_qa_pairs(self, session_id: str, new_qa_pairs: List[Dict]):
        """Normalize and upload Q&A pairs to Azure research index."""
        if not self.upload_to_azure:
            self._logger.warning("⚠️  No Azure credentials - saving locally instead")
            self._save_qa_pairs_locally(new_qa_pairs)
            return

        try:
            self._logger.info(f"❓ Uploading Q&A pairs to Azure...")

            # Normalize Q&A pairs using index adapter
            self._logger.info(f"   🔄 Normalizing Q&A pairs to enriched chunks schema...")
            normalized_qa = []

            for qa_pair in new_qa_pairs:
                # Transform flattened Q&A to enriched chunks format
                # The Q&A pairs already have the flattened schema, we need to normalize
                normalized = self._normalize_qa_pair(qa_pair)
                if normalized:
                    normalized_qa.append(normalized)

            self._logger.info(f"   ✅ Normalized {len(normalized_qa)} Q&A pairs")

            # Download existing index
            existing_qa = self._download_blob(self.qa_blob_name)

            # Deduplicate by chunk_id (Q&A pairs use qa_{chunk_id}_{qa_index} as chunk_id)
            existing_ids = set()
            all_qa = []

            for qa in existing_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id:
                    if chunk_id not in existing_ids:
                        existing_ids.add(chunk_id)
                        all_qa.append(qa)

            # Add new Q&A pairs (skip duplicates)
            new_count = 0
            for qa in normalized_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id:
                    if chunk_id not in existing_ids:
                        existing_ids.add(chunk_id)
                        all_qa.append(qa)
                        new_count += 1

            self._logger.info(f"   📊 Existing Q&A pairs: {len(existing_qa)}")
            self._logger.info(f"   ➕ New Q&A pairs: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {len(normalized_qa) - new_count}")
            self._logger.info(f"   📈 Total Q&A pairs: {len(all_qa)}")

            # Upload to Azure
            self._upload_blob(self.qa_blob_name, all_qa)
            self._logger.info(f"   ✅ Q&A pairs uploaded successfully")

        except Exception as e:
            self._logger.error(f"Error uploading Q&A pairs: {e}")
            import traceback
            traceback.print_exc()

    def _normalize_qa_pair(self, qa_pair: Dict) -> Dict:
        """
        Normalize a single Q&A pair from flattened to enriched chunks schema.

        The Q&A pairs come in flattened format with fields like:
        - question, answer, context, chunk_id, document_id, title, author, etc.

        Need to transform to nested format with:
        - content, chunk_metadata, document_metadata, embedding_enrichment
        """
        try:
            # Build content from Q&A
            question = qa_pair.get('question', '')
            answer = qa_pair.get('answer', '')
            context = qa_pair.get('context', '')

            enhanced_content = (
                f"QUESTION: {question}\n\n"
                f"ANSWER: {answer}\n\n"
                f"SOURCE CONTEXT:\n{context}"
            )

            # Calculate metadata
            word_count = len(enhanced_content.split())
            char_count = len(enhanced_content)
            line_count = enhanced_content.count('\n') + 1

            # Get section hierarchy for enriched text
            section_hierarchy = qa_pair.get('section_hierarchy', [])
            hierarchy_text = " > ".join(section_hierarchy) if section_hierarchy else ""
            enriched_text = f"{hierarchy_text}: {enhanced_content}" if hierarchy_text else enhanced_content

            # Generate chunk_id
            chunk_id = qa_pair.get('chunk_id', 'unknown')
            qa_index = qa_pair.get('qa_index', 1)
            normalized_chunk_id = f"qa_{chunk_id}_{qa_index}"

            # Build normalized structure
            normalized = {
                "content": enhanced_content,
                "chunk_metadata": {
                    "chunk_id": normalized_chunk_id,
                    "chunk_index": qa_pair.get('qa_index'),
                    "chunk_type": qa_pair.get('chunk_type', 'paragraph'),
                    "word_count": word_count,
                    "char_count": char_count,
                    "line_count": line_count,
                    "section_hierarchy": section_hierarchy,
                    "parent_section": qa_pair.get('parent_section'),
                    "is_first_in_section": False,
                    "is_last_in_section": False,
                    "contains_urls": False,
                    "contains_references": False,
                },
                "document_metadata": {
                    "document_id": qa_pair.get('document_id'),
                    "source_url": qa_pair.get('source_url'),
                    "extraction_method": qa_pair.get('extraction_method', 'unknown'),
                    "language": qa_pair.get('language', 'en'),
                    "title": qa_pair.get('title'),
                    "author": qa_pair.get('author'),
                    "creation_date": qa_pair.get('creation_date'),
                    "content_type": qa_pair.get('content_type'),
                },
                "embedding_enrichment": {
                    "embedding_id": f"emb_{normalized_chunk_id}",
                    "enriched_text": enriched_text,
                    "enriched_text_preview": enriched_text[:200] if len(enriched_text) > 200 else enriched_text,
                    "hierarchy_separator": " > ",
                    "concatenation_method": "hierarchy_first",
                    "is_sub_chunked": False,
                    "num_sub_chunks": 1,
                    "embedding_vector": qa_pair.get('combined_embedding', []),
                    "embedding_dim": qa_pair.get('embedding_dimension', 384),
                    "aggregation_method": "none",
                    "embedding_timestamp": qa_pair.get('enrichment_timestamp'),
                },
                "qa_metadata": {
                    "question": question,
                    "answer": answer,
                    "word_count_question": qa_pair.get('word_count_question'),
                    "word_count_answer": qa_pair.get('word_count_answer'),
                    "qa_index": qa_index,
                    "qa_total_for_source": qa_pair.get('qa_total_for_source'),
                    "original_chunk_content": context,
                },
                "processing_timestamp": qa_pair.get('enrichment_timestamp'),
            }

            return normalized

        except Exception as e:
            self._logger.error(f"Error normalizing Q&A pair: {e}")
            return None

    def _save_chunks_locally(self, new_chunks: List[Dict]):
        """Save chunks to local file when Azure is unavailable."""
        try:
            local_dir = Path("./cache/indices")
            local_dir.mkdir(parents=True, exist_ok=True)
            local_file = local_dir / self.chunks_blob_name

            self._logger.info(f"💾 Saving chunks locally to {local_file}...")

            # Load existing chunks if file exists
            existing_chunks = []
            if local_file.exists():
                with open(local_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            existing_chunks.append(json.loads(line.strip()))

            # Deduplicate by document_id and chunk_id
            existing_ids = set()
            all_chunks = []

            for chunk in existing_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        all_chunks.append(chunk)

            # Add new chunks (skip duplicates)
            new_count = 0
            for chunk in new_chunks:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
                if doc_id and chunk_id:
                    key = f"{doc_id}:{chunk_id}"
                    if key not in existing_ids:
                        existing_ids.add(key)
                        all_chunks.append(chunk)
                        new_count += 1

            self._logger.info(f"   📊 Existing chunks: {len(existing_chunks)}")
            self._logger.info(f"   ➕ New chunks: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {len(new_chunks) - new_count}")
            self._logger.info(f"   📈 Total chunks: {len(all_chunks)}")

            # Write to file
            with open(local_file, 'w', encoding='utf-8') as f:
                for chunk in all_chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + '\n')

            self._logger.info(f"   ✅ Saved {len(all_chunks)} chunks to {local_file}")

        except Exception as e:
            self._logger.error(f"Error saving chunks locally: {e}")
            import traceback
            traceback.print_exc()

    def _save_qa_pairs_locally(self, new_qa_pairs: List[Dict]):
        """Save Q&A pairs to local file when Azure is unavailable."""
        try:
            local_dir = Path("./cache/indices")
            local_dir.mkdir(parents=True, exist_ok=True)
            local_file = local_dir / self.qa_blob_name

            self._logger.info(f"💾 Normalizing and saving Q&A pairs locally to {local_file}...")

            # Normalize Q&A pairs
            normalized_qa = []
            for qa in new_qa_pairs:
                normalized = self._normalize_qa_pair(qa)
                if normalized:
                    normalized_qa.append(normalized)

            self._logger.info(f"   ✅ Normalized {len(normalized_qa)} Q&A pairs")

            # Load existing Q&A pairs if file exists
            existing_qa = []
            if local_file.exists():
                with open(local_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            existing_qa.append(json.loads(line.strip()))

            # Deduplicate by chunk_id
            existing_ids = set()
            all_qa = []

            for qa in existing_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id and chunk_id not in existing_ids:
                    existing_ids.add(chunk_id)
                    all_qa.append(qa)

            # Add new Q&A pairs (skip duplicates)
            new_count = 0
            for qa in normalized_qa:
                chunk_id = qa.get('chunk_metadata', {}).get('chunk_id')
                if chunk_id and chunk_id not in existing_ids:
                    existing_ids.add(chunk_id)
                    all_qa.append(qa)
                    new_count += 1

            self._logger.info(f"   📊 Existing Q&A pairs: {len(existing_qa)}")
            self._logger.info(f"   ➕ New Q&A pairs: {new_count}")
            self._logger.info(f"   🔄 Duplicates skipped: {len(normalized_qa) - new_count}")
            self._logger.info(f"   📈 Total Q&A pairs: {len(all_qa)}")

            # Write to file
            with open(local_file, 'w', encoding='utf-8') as f:
                for qa in all_qa:
                    f.write(json.dumps(qa, ensure_ascii=False) + '\n')

            self._logger.info(f"   ✅ Saved {len(all_qa)} Q&A pairs to {local_file}")

        except Exception as e:
            self._logger.error(f"Error saving Q&A pairs locally: {e}")
            import traceback
            traceback.print_exc()

    def _download_blob(self, blob_name: str) -> List[Dict]:
        """Download and parse JSONL blob from Azure."""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            if not blob_client.exists():
                self._logger.info(f"   📥 Blob not found (creating new): {blob_name}")
                return []

            self._logger.info(f"   📥 Downloading existing blob: {blob_name}")
            blob_data = blob_client.download_blob().readall()

            # Parse JSONL
            documents = []
            for line in blob_data.decode('utf-8').splitlines():
                if line.strip():
                    documents.append(json.loads(line.strip()))

            self._logger.info(f"   ✅ Downloaded {len(documents)} documents")
            return documents

        except Exception as e:
            self._logger.warning(f"   ⚠️  Error downloading blob {blob_name}: {e}")
            return []

    def _upload_blob(self, blob_name: str, documents: List[Dict]):
        """Upload JSONL documents to Azure blob."""
        try:
            # Convert to JSONL
            jsonl_content = '\n'.join(json.dumps(doc, ensure_ascii=False) for doc in documents)

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            # Upload with overwrite
            blob_client.upload_blob(
                jsonl_content.encode('utf-8'),
                overwrite=True,
                content_settings=ContentSettings(content_type='application/jsonl')
            )

            self._logger.info(f"   📤 Uploaded {len(documents)} documents to {blob_name}")

        except Exception as e:
            self._logger.error(f"Error uploading blob {blob_name}: {e}")
            raise

    def _archive_session(self, session_id: str):
        """Archive session files to Azure or local archive directory."""
        try:
            self._logger.info(f"📦 Archiving session files: {session_id}")

            if not self.upload_to_azure:
                # Local archive - copy to archive directory
                archive_dir = Path("./cache/archive") / session_id
                archive_dir.mkdir(parents=True, exist_ok=True)

                archive_count = 0
                for request_id in self.session_request_ids[session_id]:
                    request_dir = self.cache_dir / request_id
                    if not request_dir.exists():
                        continue

                    # Copy all JSONL files to archive
                    for file_path in request_dir.glob('*.jsonl'):
                        import shutil
                        archive_file = archive_dir / request_id / file_path.name
                        archive_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(file_path, archive_file)
                        archive_count += 1

                self._logger.info(f"   ✅ Archived {archive_count} files locally to {archive_dir}")
                return

            # Azure archive
            archive_count = 0
            for request_id in self.session_request_ids[session_id]:
                request_dir = self.cache_dir / request_id
                if not request_dir.exists():
                    continue

                # Archive all JSONL files from the request directory
                for file_path in request_dir.glob('*.jsonl'):
                    blob_name = f"{session_id}/{request_id}/{file_path.name}"

                    blob_client = self.blob_service_client.get_blob_client(
                        container=self.archive_container,
                        blob=blob_name
                    )

                    with open(file_path, 'rb') as f:
                        blob_client.upload_blob(f, overwrite=True)

                    archive_count += 1

            self._logger.info(f"   ✅ Archived {archive_count} files to Azure {self.archive_container}")

        except Exception as e:
            self._logger.error(f"Error archiving session {session_id}: {e}")

    def _cleanup_session(self, session_id: str):
        """Clean up local cache directories for session."""
        try:
            self._logger.info(f"🧹 Cleaning up local cache: {session_id}")

            cleanup_count = 0
            for request_id in self.session_request_ids[session_id]:
                request_dir = self.cache_dir / request_id
                if request_dir.exists():
                    shutil.rmtree(request_dir)
                    cleanup_count += 1

            self._logger.info(f"   ✅ Cleaned up {cleanup_count} request directories")

        except Exception as e:
            self._logger.error(f"Error cleaning up session {session_id}: {e}")


def main():
    """Run the index aggregator consumer."""
    consumer = KafkaIndexAggregatorConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
