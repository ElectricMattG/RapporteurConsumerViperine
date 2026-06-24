#!/usr/bin/env python3
"""
Kafka Summary Indexer Consumer

Generates embeddings for document summaries incrementally and creates a searchable summary index.
Works as part of the processing pipeline:
  1. Web Scraper → extracts content
  2. Summarizer → generates summaries → summary-completed
  3. Summary Indexer (this) → embeds summaries → session-complete (summaries)
  4. Index Aggregator → aggregates summary index separately

Message Format (Input - from Summarizer):
    {
        "request_id": "unique-request-id",
        "session_id": "session-id",
        "document_id": "doc-id",
        "source_url": "original-url",
        "summary_file": "path/to/doc_summary.json",
        "current_count": 3,
        "total_requests": 15
    }

Output Message (to Index Aggregator):
    {
        "session_id": "session-id",
        "summary_index_file": "path/to/session_summaries_embedded.jsonl",
        "total_summaries": 15,
        "index_type": "summaries"
    }

Output File Schema (JSONL - one entry per document):
    {
        "document_id": "doc-123",
        "source_url": "https://...",
        "summary_text": "Full summary content...",
        "summary_embedding": [0.123, ...],  # 384-dim
        "doc_metadata": {
            "title": "...",
            "author": "...",
            "creation_date": "...",
            "word_count": 5000
        },
        "embedding_metadata": {
            "embedding_id": "sum_doc-123",
            "embedding_dim": 384,
            "embedding_model": "all-MiniLM-L6-v2",
            "embedded_at": "..."
        },
        "indexed_at": "..."
    }

Usage:
    python run_kafka_summary_indexer.py
"""

import asyncio
import fcntl
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, Producer

# Telemetry imports with fallback
try:
    from src.monitoring import (KafkaMetricsWrapper, MetricsServer,
                                PrometheusMetrics, SystemMetricsCollector)
    from src.monitoring.error_categories import ErrorCategory
    from src.monitoring.logging_config import configure_logging
    TELEMETRY_AVAILABLE = True
except ImportError:
    try:
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
    from ..config_handler import Base
    from ..utils.url_normalize import normalize_url
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base
    from utils.url_normalize import normalize_url

# Import embedding client
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../clients'))
from embedding_client import EmbeddingServiceClient

from utils.correlation import (bind_correlation_ids,
                               install_correlation_record_factory,
                               reset_correlation_ids)

# Sessions idle longer than this (seconds) are force-aggregated by the sweeper.
SESSION_IDLE_TIMEOUT: int = int(os.getenv('SESSION_IDLE_TIMEOUT', '120'))


class KafkaSummaryIndexerConsumer(Base):
    """Kafka consumer for indexing and embedding document summaries"""

    def __init__(self):
        """Initialize the Kafka summary indexer consumer"""
        install_correlation_record_factory()
        super().__init__()
        self._logger.info("Initializing KafkaSummaryIndexerConsumer")

        # Load configuration
        self._load_config()

        # Initialize telemetry components if available
        self.telemetry_enabled = TELEMETRY_AVAILABLE
        if self.telemetry_enabled:
            consumer_id = f"summary_indexer_{os.getpid()}"
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
                port=8006,
                consumer_name='summary_indexer',
                health_check=lambda: self.running,
                ready_check=lambda: self.consumer is not None and self.running
            )

            # Summary indexer-specific metrics
            from prometheus_client import Counter, Gauge, Histogram

            self.summaries_indexed = Counter(
                'kafka_consumer_summaries_indexed_total',
                'Total summaries indexed',
                ['consumer_id']
            )

            self.sessions_completed = Counter(
                'kafka_consumer_summary_sessions_completed_total',
                'Total summary indexing sessions completed',
                ['consumer_id']
            )

            self.summary_embedding_duration = Histogram(
                'kafka_consumer_summary_embedding_duration_seconds',
                'Time spent generating summary embeddings',
                ['consumer_id', 'status'],
                buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
            )

            self.summaries_per_session = Histogram(
                'kafka_consumer_summaries_per_session',
                'Number of summaries per session',
                ['consumer_id'],
                buckets=[0, 5, 10, 20, 50, 100, 200, 500]
            )

            self.duplicates_detected = Counter(
                'kafka_consumer_summary_duplicates_detected_total',
                'Total summaries skipped during aggregation as duplicates',
                ['consumer_id', 'match']
            )

            self.summary_id_scheme = Counter(
                'kafka_consumer_summary_id_scheme_total',
                'Total summaries received by document_id scheme (req_/doc_/uuid_legacy)',
                ['consumer_id', 'scheme']
            )

            self._logger.info(f"✅ Telemetry initialized - consumer_id: {consumer_id}, port: 8006")
        else:
            self.metrics = None
            self.system_metrics = None
            self.kafka_metrics = None
            self.metrics_server = None

        # Initialize Kafka consumer and producer
        self.consumer = None
        self.producer = None
        self.running = False

        # Initialize embedding client
        self.embedding_client = None

        # Per-request aggregation state — keyed on request_id (per-query UUID).
        # Multiple requests may share one session_id (chat-tab UUID); session_id
        # is stored inside each state record for frontend-message replay.
        self.request_states = {}  # {request_id: {count, total, started_at, document_ids}}

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._logger.info("✅ KafkaSummaryIndexerConsumer initialized successfully")

    def _load_config(self):
        """Load configuration for summary indexer"""
        try:
            # Kafka configuration
            bootstrap_servers = self._app_config.get('kafka.bootstrap_servers')
            if not bootstrap_servers:
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            self.kafka_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': self._app_config.get('summary_indexer.consumer_group_id') or 'summary-indexer-consumer',
                'auto.offset.reset': self._app_config.get('kafka.auto_offset_reset') or 'earliest',
                'enable.auto.commit': self._app_config.get('kafka.enable_auto_commit') or True,
                'session.timeout.ms': self._app_config.get('kafka.session_timeout_ms') or 45000,
                'heartbeat.interval.ms': 3000,
                'max.poll.interval.ms': 300000,  # 5 minutes
            }

            # Topic configuration
            self.input_topic = self._app_config.get('summary_indexer.input_topic') or 'viperine.summary-completed'
            self.output_topic = self._app_config.get('summary_indexer.output_topic') or 'viperine.session-complete'
            self.frontend_topic = self._app_config.get('summary_indexer.frontend_topic') or 'viperine.frontend-message'

            # Cache directory
            self.cache_dir = Path(self._app_config.get('summary_indexer.cache_directory') or 'cache/summaries')
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            # Combined index file path - prefer active index folder, fallback to root
            try:
                combined_index_filename = self._app_config.get('summary_indexer.combined_index_file')
            except (KeyError, AttributeError):
                combined_index_filename = None

            if not combined_index_filename:
                combined_index_filename = 'research_summaries_with_embeddings_combined.jsonl'

            project_root = Path(__file__).resolve().parent.parent.parent

            # Use ACTIVE_INDEX_FOLDER env var to write into the correct index folder
            active_folder = os.getenv('ACTIVE_INDEX_FOLDER', '').strip()
            if active_folder and active_folder.startswith('x_indices_'):
                folder_path = project_root / active_folder
                candidates = [
                    folder_path / "research_summaries_filtered.jsonl",
                    folder_path / "research_summaries_with_embeddings_combined.jsonl",
                ]
                active_path = next((p for p in candidates if p.exists()), None)
                if active_path:
                    self.combined_index_file = active_path
                else:
                    # Folder may exist but file not yet created; default to filtered file
                    self.combined_index_file = folder_path / "research_summaries_filtered.jsonl"
            else:
                # No active folder set — fall back to root combined index
                self.combined_index_file = project_root / combined_index_filename

            # For backward compatibility, also reference it as root_index_file
            self.root_index_file = self.combined_index_file

            # Embedding configuration
            self.model_name = self._app_config.get('summary_indexer.model_name') or 'all-MiniLM-L6-v2'

            self._logger.info("✅ Configuration loaded:")
            self._logger.info(f"   Kafka servers: {bootstrap_servers}")
            self._logger.info(f"   Consumer group: {self.kafka_config['group.id']}")
            self._logger.info(f"   Input topic: {self.input_topic}")
            self._logger.info(f"   Output topic: {self.output_topic}")
            self._logger.info(f"   Cache directory: {self.cache_dir}")
            self._logger.info(f"   Combined index: {self.combined_index_file}")
            self._logger.info(f"   Combined index exists: {self.combined_index_file.exists()}")
            self._logger.info(f"   ACTIVE_INDEX_FOLDER: {os.environ.get('ACTIVE_INDEX_FOLDER', 'x_indices_merged (default)')}")
            self._logger.info(f"   Model: {self.model_name}")

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

    def _watch_folder_switch(self):
        """Background thread: listen on index-folder-switch Kafka topic and re-route output."""
        import threading

        from confluent_kafka import Consumer as _FolderWatchConsumer

        cfg = {
            "bootstrap.servers": self.kafka_config["bootstrap.servers"],
            "group.id": f"{self.kafka_config['group.id']}-folder-watch",
            "auto.offset.reset": "latest",
        }
        watcher = None
        try:
            watcher = _FolderWatchConsumer(cfg)
            watcher.subscribe(["index-folder-switch"])
            self._logger.info("✅ Folder-switch watcher subscribed to index-folder-switch")
            while self.running:
                msg = watcher.poll(timeout=2.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        self._logger.warning(f"⚠️ Folder-switch watcher error: {msg.error()}")
                    continue
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    new_folder = data.get("folder", "").strip()
                    if not new_folder or not new_folder.startswith("x_indices_"):
                        continue
                    project_root = Path(__file__).resolve().parent.parent.parent
                    folder_path = project_root / new_folder
                    candidates = [
                        folder_path / "research_summaries_filtered.jsonl",
                        folder_path / "research_summaries_with_embeddings_combined.jsonl",
                    ]
                    new_path = next((p for p in candidates if p.exists()), None)
                    if new_path:
                        old_name = self.combined_index_file.parent.name
                        self.combined_index_file = new_path
                        self.root_index_file = new_path
                        self._logger.info(
                            f"🔀 Folder-switch: consumer output re-routed "
                            f"{old_name} → {new_folder}"
                        )
                    else:
                        self._logger.warning(
                            f"⚠️ Folder-switch: no summaries file in {new_folder}; ignoring"
                        )
                except Exception as parse_err:
                    self._logger.warning(f"⚠️ Folder-switch parse error: {parse_err}")
        except Exception as e:
            self._logger.warning(f"⚠️ Folder-switch watcher startup failed: {e}")
        finally:
            if watcher:
                try:
                    watcher.close()
                except Exception:
                    pass

    def _get_request_dir(self, request_id: str) -> Path:
        """Get directory for per-request files (state, session index JSONL)."""
        if not request_id:
            raise ValueError(
                f"_get_request_dir called with request_id={request_id!r}; "
                "request_id must be a non-empty string."
            )
        request_dir = self.cache_dir / request_id
        request_dir.mkdir(parents=True, exist_ok=True)
        return request_dir

    def _load_request_state(self, request_id: str) -> Dict[str, Any]:
        """Load per-request state from disk."""
        state_file = self._get_request_dir(request_id) / f"{request_id}_state.json"

        if state_file.exists():
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self._logger.warning(f"   ⚠️ Error loading state file: {e}")

        return {
            'request_id': request_id,
            'session_id': None,    # populated on first message; replayed to frontend-message
            'origin': None,        # populated on first message; replayed to frontend-message
            'total_requests': 0,
            'received_count': 0,
            'document_ids_processed': [],
            'dedup_keys_processed': [],
            'failed': [],
            'started_at': datetime.now(timezone.utc).isoformat(),
            'last_event_at': datetime.now(timezone.utc).isoformat(),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'completed': False
        }

    def _save_request_state(self, request_id: str, state: Dict[str, Any]):
        """Save per-request state to disk."""
        state_file = self._get_request_dir(request_id) / f"{request_id}_state.json"
        state['last_updated'] = datetime.now().isoformat()

        try:
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self._logger.error(f"   ⚠️ Error saving state file: {e}")

    def _load_summary_file(self, summary_file: str) -> Dict[str, Any]:
        """Load summary data from file"""
        summary_path = Path(summary_file)

        if not summary_path.exists():
            raise FileNotFoundError(f"Summary file not found: {summary_file}")

        with open(summary_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async def _embed_summary(self, summary_text: str) -> List[float]:
        """Generate embedding for summary text"""
        if not self.embedding_client:
            self.embedding_client = EmbeddingServiceClient()

        embeddings = await self.embedding_client.embed(
            [summary_text],
            batch_size=1,
            show_progress=False
        )

        return embeddings[0].tolist()

    def _append_to_index(self, request_id: str, entry: Dict[str, Any]):
        """Append summary entry to per-request JSONL index file."""
        request_dir = self._get_request_dir(request_id)
        index_file = request_dir / f"{request_id}_summaries_embedded.jsonl"

        with open(index_file, 'a', encoding='utf-8') as f:
            json.dump(entry, f, ensure_ascii=False)
            f.write('\n')

    def _publish_session_complete(
        self,
        session_id: str,
        total_summaries: int,
        failed: List[str] | None = None,
        request_id: str | None = None,
        origin: str | None = None,
    ):
        """Publish session-complete message to Index Aggregator"""
        if not self.producer:
            self._create_producer()

        failed = failed or []
        # Directory is keyed on request_id when available (Phase 3); fall back
        # to session_id for any legacy callers that haven't been updated yet.
        dir_key = request_id or session_id
        dir_path = self._get_request_dir(dir_key)
        index_file = dir_path / f"{dir_key}_summaries_embedded.jsonl"

        message = {
            'session_id': session_id,
            'request_id': request_id,
            'origin': origin,
            'summary_index_file': str(index_file),
            'total_summaries': total_summaries,
            'index_type': 'summaries',
            'partial': len(failed) > 0,
            'failed_request_ids': failed,
            'completed_at': datetime.now(timezone.utc).isoformat(),
        }

        try:
            _pub_start = time.time()
            self.producer.produce(
                self.output_topic,
                key=session_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()
            _pub_duration = time.time() - _pub_start

            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.output_topic, result='success', duration=_pub_duration)

            self._logger.info(f"   📤 Published session-complete to {self.output_topic}")

            # Track session completion
            if self.telemetry_enabled:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                self.sessions_completed.labels(consumer_id=consumer_id_label).inc()
                self.summaries_per_session.labels(consumer_id=consumer_id_label).observe(total_summaries)

        except Exception as e:
            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.output_topic, result='error', duration=0)
            self._logger.error(f"   ❌ Error publishing session-complete: {e}")

    def _publish_reload_index_message(self, new_count: int, total_count: int):
        """Publish reload-index message for RAG service"""
        if not self.producer:
            self._create_producer()

        message = {
            'action': 'reload_index',
            'new_summaries': new_count,
            'total_summaries': total_count,
            'index_file': str(self.root_index_file),
            'timestamp': datetime.now().isoformat()
        }

        try:
            _pub_start = time.time()
            self.producer.produce(
                'reload-index',
                key='rag-service'.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()
            _pub_duration = time.time() - _pub_start

            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic='reload-index', result='success', duration=_pub_duration)

            self._logger.info(f"   📤 Published reload-index message: +{new_count} summaries")
        except Exception as e:
            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic='reload-index', result='error', duration=0)
            self._logger.error(f"   ❌ Error publishing reload-index: {e}")

    def _aggregate_to_combined_index(self, request_id: str):
        """
        Aggregate per-request index into the combined master index.
        Handles deduplication by document_id.
        """
        try:
            request_dir = self._get_request_dir(request_id)
            session_index_file = request_dir / f"{request_id}_summaries_embedded.jsonl"

            if not session_index_file.exists():
                self._logger.warning(f"   ⚠️ Request index file not found: {session_index_file}")
                return

            self._logger.info(f"   📚 Aggregating request index to combined index...")
            self._logger.info(f"   Source: {session_index_file}")
            self._logger.info(f"   Target: {self.combined_index_file}")

            # Ensure parent directory exists for combined index file
            self.combined_index_file.parent.mkdir(parents=True, exist_ok=True)

            # Acquire an exclusive advisory lock on a sibling lockfile so that
            # concurrent writers (e.g. old + new pod overlap during a rolling
            # update, or retries within the same pod) cannot interleave reads
            # and appends — that race produced both duplicated document_ids
            # and torn JSONL lines (records glued together without a newline).
            # CRITICAL: Resolve symlinks first so all processes use the same lock file
            real_index_file = self.combined_index_file.resolve()
            lock_path = real_index_file.with_suffix(
                real_index_file.suffix + '.lock'
            )
            with open(lock_path, 'w') as lock_fh:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
                try:
                    self._aggregate_session_into_combined(
                        request_id, session_index_file
                    )
                finally:
                    fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

        except Exception as e:
            self._logger.error(f"   ❌ Error aggregating to combined index: {e}", exc_info=True)

    def _aggregate_session_into_combined(
        self, request_id: str, session_index_file: Path
    ):
        """Inner aggregation routine; caller must hold the combined-index lock."""
        try:
            existing_doc_ids: set = set()
            existing_dedup_keys: set = set()
            import hashlib as _hashlib

            if self.combined_index_file.exists():
                with open(self.combined_index_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            try:
                                entry = json.loads(line)
                                doc_id = entry.get('document_id')
                                if doc_id:
                                    existing_doc_ids.add(doc_id)
                                # Accept stored dedup_key or derive from url (legacy records)
                                dk = entry.get('dedup_key')
                                if not dk and entry.get('source_url'):
                                    dk = _hashlib.md5(normalize_url(entry['source_url']).encode()).hexdigest()
                                if dk:
                                    existing_dedup_keys.add(dk)
                            except json.JSONDecodeError:
                                continue

            self._logger.info(f"   📊 Found {len(existing_doc_ids)} existing documents in combined index")

            # Read session index and append new entries
            new_count = 0
            duplicate_count = 0
            new_additions = []  # Track new summaries for frontend message

            with open(session_index_file, 'r', encoding='utf-8') as source:
                with open(self.combined_index_file, 'a', encoding='utf-8') as target:
                    for line in source:
                        if line.strip():
                            try:
                                entry = json.loads(line)
                                doc_id = entry.get('document_id')
                                dk = entry.get('dedup_key')
                                if not dk and entry.get('source_url'):
                                    dk = _hashlib.md5(normalize_url(entry['source_url']).encode()).hexdigest()

                                # Dedup: skip if either document_id or dedup_key already present
                                doc_id_match = bool(doc_id and doc_id in existing_doc_ids)
                                dedup_key_match = bool(dk and dk in existing_dedup_keys)
                                if doc_id_match or dedup_key_match:
                                    duplicate_count += 1
                                    if self.telemetry_enabled and hasattr(self, 'duplicates_detected'):
                                        consumer_id_label = f"summary_indexer_{os.getpid()}"
                                        if doc_id_match and dedup_key_match:
                                            match_label = 'both'
                                        elif doc_id_match:
                                            match_label = 'document_id'
                                        else:
                                            match_label = 'dedup_key'
                                        self.duplicates_detected.labels(
                                            consumer_id=consumer_id_label,
                                            match=match_label
                                        ).inc()
                                    continue

                                # New document — append to combined index
                                json.dump(entry, target, ensure_ascii=False)
                                target.write('\n')
                                if doc_id:
                                    existing_doc_ids.add(doc_id)
                                if dk:
                                    existing_dedup_keys.add(dk)
                                new_count += 1

                                # Track new addition for frontend message
                                new_additions.append({
                                    'url': entry.get('source_url', ''),
                                    'title': entry.get('doc_metadata', {}).get('title', 'Untitled')
                                })

                            except json.JSONDecodeError as e:
                                self._logger.warning(f"   ⚠️ Invalid JSON in session index: {e}")
                                continue

            self._logger.info(f"   ✅ Aggregation complete:")
            self._logger.info(f"      New entries: {new_count}")
            self._logger.info(f"      Duplicates skipped: {duplicate_count}")
            self._logger.info(f"      Total documents: {len(existing_doc_ids)}")

            # Publish notifications if new summaries were added
            if new_count > 0:
                try:
                    # Reload correlation fields from on-disk state so we can replay
                    # session_id and origin onto the frontend-message even when this
                    # method is called from the idle sweeper (not _process_message).
                    _state = self._load_request_state(request_id)
                    _session_id = _state.get('session_id')
                    _origin = _state.get('origin')

                    # Publish reload-index message for RAG service
                    self._publish_reload_index_message(new_count, len(existing_doc_ids))

                    # Publish frontend-message for UI notification (accurate count, limited preview)
                    self._publish_frontend_message(
                        request_id=request_id,
                        session_id=_session_id,
                        origin=_origin,
                        new_count=new_count,
                        new_additions_preview=new_additions[:5],
                        total_summaries=len(existing_doc_ids),
                    )

                except Exception as e:
                    self._logger.error(f"   ❌ Error publishing notifications: {e}")

        except Exception as e:
            self._logger.error(f"   ❌ Error aggregating to combined index: {e}", exc_info=True)

    def _publish_frontend_message(
        self,
        request_id: str,
        session_id: str | None,
        origin: str | None,
        new_count: int,
        new_additions_preview: List[Dict[str, str]],
        total_summaries: int,
    ):
        """
        Publish a message to the frontend-message topic with aggregation results.

        This is the terminal message the BFF SSE filter inspects to route a
        research-complete notification to the originating browser tab.  It
        carries all three correlation fields so the BFF can match on session_id.

        Args:
            request_id: Per-query UUID (aggregation key)
            session_id: Chat-tab UUID from inbound message (BFF SSE routing key)
            origin: Opaque origin field from inbound message (diagnostic)
            new_count: Actual count of new summaries added
            new_additions_preview: Up to 5 dicts with 'url' and 'title'
            total_summaries: Total summaries in the combined index
        """
        if not self.producer:
            self._create_producer()

        message = {
            'type': 'summary_aggregation',
            'request_id': request_id,
            'session_id': session_id,
            'origin': origin,
            'new_summaries_count': new_count,
            'total_summaries_count': total_summaries,
            'new_summaries': new_additions_preview,
            'timestamp': datetime.now().isoformat()
        }

        # Key on session_id so the BFF SSE consumer can use partition affinity
        # per chat session.  Fall back to request_id for origin="direct" events
        # where no session bucket is registered.
        kafka_key = (session_id.encode('utf-8') if session_id
                     else request_id.encode('utf-8'))

        try:
            _pub_start = time.time()
            self.producer.produce(
                self.frontend_topic,
                key=kafka_key,
                value=json.dumps(message).encode('utf-8')
            )
            self.producer.flush()
            _pub_duration = time.time() - _pub_start

            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.frontend_topic, result='success', duration=_pub_duration)

            self._logger.info(f"   📢 Published frontend message: {new_count} new summaries, {total_summaries} total")

        except Exception as e:
            # instrumentation - messages_published_total
            if self.telemetry_enabled:
                self.kafka_metrics.record_publish(topic=self.frontend_topic, result='error', duration=0)
            self._logger.error(f"   ❌ Error publishing frontend message: {e}")

    async def _process_message(self, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single summary-completed message"""
        _tokens = bind_correlation_ids(
            message_data.get('session_id'),
            message_data.get('request_id'),
        )
        try:
            # Extract message fields
            request_id = message_data.get('request_id')
            session_id = message_data.get('session_id')
            document_id = message_data.get('document_id')
            dedup_key = message_data.get('dedup_key')  # new field; may be absent in legacy messages
            source_url = message_data.get('source_url')
            summary_file = message_data.get('summary_file')
            current_count = message_data.get('current_count', 1)
            total_urls = message_data.get('total_urls', message_data.get('total_requests', 1))
            origin = message_data.get('origin')
            url_index = message_data.get('url_index', 0)
            status = message_data.get('status', 'success')  # 'success' | 'degraded' | 'failed'

            # Track document_id scheme to surface rogue producers
            if self.telemetry_enabled and hasattr(self, 'summary_id_scheme') and document_id:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                if document_id.startswith('req_'):
                    scheme = 'req'
                elif document_id.startswith('doc_'):
                    scheme = 'doc'
                else:
                    scheme = 'uuid_legacy'
                self.summary_id_scheme.labels(consumer_id=consumer_id_label, scheme=scheme).inc()

            self._logger.info("="*80)
            self._logger.info(f"📝 Processing Summary Indexing")
            self._logger.info(f"   Session ID: {session_id}")
            self._logger.info(f"   Request ID: {request_id}")
            self._logger.info(f"   Origin:     {origin}")
            self._logger.info(f"   Document ID: {document_id}")
            self._logger.info(f"   Status: {status}")
            self._logger.info(f"   Progress: {url_index + 1}/{total_urls}")
            self._logger.info(f"   Summary file: {summary_file}")

            # Guard: request_id is the primary key for all on-disk state.
            # A missing request_id cannot be processed; log a clear, actionable
            # error rather than crashing with a cryptic TypeError inside pathlib.
            if not request_id:
                self._logger.error(
                    f"\u274c Received message without request_id — cannot process "
                    f"(session_id={session_id!r}, document_id={document_id!r}). "
                    f"Message will be dropped. Investigate the upstream producer."
                )
                return None

            # Load per-request state (keyed on request_id, not session_id)
            state = self._load_request_state(request_id)

            # Ensure new state fields exist (backward-compat with persisted states)
            state.setdefault('dedup_keys_processed', [])
            state.setdefault('failed', [])
            state.setdefault('last_event_at', datetime.now(timezone.utc).isoformat())

            # Persist session_id and origin in state so the idle sweeper can
            # replay them onto frontend-message without re-receiving the message.
            state['session_id'] = session_id
            state['origin'] = origin

            # Update total if not set (total_urls takes priority; legacy total_requests as fallback)
            if state['total_requests'] == 0:
                state['total_requests'] = total_urls

            # Stamp last_event_at so the idle sweeper knows this session is alive
            state['last_event_at'] = datetime.now(timezone.utc).isoformat()

            # ── Idempotency check ─────────────────────────────────────────────
            # For new messages: dedup on dedup_key (md5 of normalized URL).
            # For legacy messages without dedup_key: fall back to document_id.
            _eff_dedup = dedup_key or (
                __import__('hashlib').md5(normalize_url(source_url).encode()).hexdigest()
                if source_url else None
            )
            if _eff_dedup and _eff_dedup in state['dedup_keys_processed']:
                self._logger.info(f"   ⏭️  URL already indexed (dedup_key={_eff_dedup}), skipping")
                # Still count toward total so request can complete
                state['received_count'] += 1
                self._save_request_state(request_id, state)
                self._logger.info("="*80)
                return {'session_id': session_id, 'request_id': request_id, 'document_id': document_id, 'indexed': False, 'skipped': True}

            if document_id in state['document_ids_processed']:
                self._logger.info(f"   ⏭️  Document {document_id} already processed, skipping")
                state['received_count'] += 1
                self._save_request_state(request_id, state)
                self._logger.info("="*80)
                return {'session_id': session_id, 'request_id': request_id, 'document_id': document_id, 'indexed': False, 'skipped': True}

            # ── Handle failed terminal events ─────────────────────────────────
            # The summarizer publishes status='failed' when it exhausts retries.
            # We count the doc toward the session total (so it can complete) but
            # do NOT try to embed or write it to the JSONL index.
            if status == 'failed':
                self._logger.warning(
                    f"   ⚠️  Summarizer reported failure for {request_id} ({source_url}); "
                    f"counting as received, not embedding."
                )
                state['received_count'] += 1
                state['failed'].append(request_id)
                if document_id:
                    state['document_ids_processed'].append(document_id)
                if _eff_dedup:
                    state['dedup_keys_processed'].append(_eff_dedup)
                self._save_request_state(request_id, state)

                # Check if request is now complete (failure was the last outstanding doc)
                if state['received_count'] >= state['total_requests'] and not state.get('completed', False):
                    self._logger.info(f"   🎉 Request complete (with failures): {state['received_count']}/{state['total_requests']}")
                    state['completed'] = True
                    self._save_request_state(request_id, state)
                    self._publish_session_complete(
                        session_id, state['received_count'],
                        failed=state['failed'],
                        request_id=request_id, origin=origin,
                    )
                    self._aggregate_to_combined_index(request_id)

                self._logger.info("="*80)
                return {
                    'session_id': session_id,
                    'request_id': request_id,
                    'document_id': document_id,
                    'indexed': False,
                    'failed': True,
                    'session_complete': state['completed']
                }

            # ── Happy path: embed and index ───────────────────────────────────
            # Track embedding start time
            embed_start = time.time() if self.telemetry_enabled else None

            # Load summary data
            try:
                summary_data = self._load_summary_file(summary_file)
            except FileNotFoundError as e:
                self._logger.error(f"   ❌ {e}")
                return None

            # Extract summary text and metadata
            summary_text = summary_data.get('markdown_summary', '')
            doc_metadata = summary_data.get('metadata', {})

            if not summary_text:
                self._logger.warning(f"   ⚠️ No summary text found for {document_id}")
                return None

            self._logger.info(f"   📄 Summary length: {len(summary_text)} chars")

            # Generate embedding
            self._logger.info(f"   🧠 Generating embedding...")
            embedding = await self._embed_summary(summary_text)
            self._logger.info(f"   ✅ Embedding generated: {len(embedding)}-dim")

            # Create index entry — include dedup_key for future cross-session dedup
            index_entry = {
                'document_id': document_id,
                'dedup_key': _eff_dedup,
                'source_url': source_url,
                'summary_text': summary_text,
                'summary_embedding': embedding,
                'doc_metadata': doc_metadata,
                'degraded': summary_data.get('degraded', False),
                'embedding_metadata': {
                    'embedding_id': f"sum_{document_id}",
                    'embedding_dim': len(embedding),
                    'embedding_model': self.model_name,
                    'embedded_at': datetime.now(timezone.utc).isoformat()
                },
                'indexed_at': datetime.now(timezone.utc).isoformat()
            }

            # Append to index file
            self._append_to_index(request_id, index_entry)
            self._logger.info(f"   💾 Appended to index")

            # Update state
            state['received_count'] += 1
            state['document_ids_processed'].append(document_id)
            if _eff_dedup:
                state['dedup_keys_processed'].append(_eff_dedup)
            self._save_request_state(request_id, state)

            # Track metrics
            if self.telemetry_enabled and embed_start:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                embed_duration = time.time() - embed_start

                self.summary_embedding_duration.labels(
                    consumer_id=consumer_id_label,
                    status='success'
                ).observe(embed_duration)

                self.summaries_indexed.labels(
                    consumer_id=consumer_id_label
                ).inc()

            # Check if request is complete
            if state['received_count'] >= state['total_requests'] and not state.get('completed', False):
                self._logger.info(f"   🎉 Request complete: {state['received_count']}/{state['total_requests']}")
                state['completed'] = True
                self._save_request_state(request_id, state)

                # Publish session-complete message
                self._publish_session_complete(
                    session_id, state['received_count'],
                    failed=state.get('failed', []),
                    request_id=request_id, origin=origin,
                )

                # Aggregate request index to combined master index
                self._aggregate_to_combined_index(request_id)

            elif state.get('completed', False):
                self._logger.info(f"   ℹ️  Request already marked complete, skipping signal")
            else:
                self._logger.info(f"   ⏳ Request progress: {state['received_count']}/{state['total_requests']}")

            self._logger.info("="*80)

            return {
                'session_id': session_id,
                'request_id': request_id,
                'document_id': document_id,
                'indexed': True,
                'session_complete': state['completed']
            }

        except Exception as e:
            self._logger.error(f"❌ Error processing message: {e}", exc_info=True)

            # Track failed embedding
            if self.telemetry_enabled and 'embed_start' in locals() and embed_start:
                consumer_id_label = f"summary_indexer_{os.getpid()}"
                embed_duration = time.time() - embed_start

                self.summary_embedding_duration.labels(
                    consumer_id=consumer_id_label,
                    status='failure'
                ).observe(embed_duration)

            return None
        finally:
            reset_correlation_ids(_tokens)

    def _check_idle_sessions(self) -> None:
        """
        Sweep open (non-completed) sessions and aggregate any that have been
        idle longer than SESSION_IDLE_TIMEOUT seconds.

        Called periodically inside the main poll loop.  Uses wall-clock
        difference against `last_event_at` stored in session state on disk.
        """
        now = datetime.now(timezone.utc)
        try:
            if not self.cache_dir.exists():
                return
            for state_file in self.cache_dir.glob("*/*_state.json"):
                try:
                    with open(state_file, 'r', encoding='utf-8') as f:
                        state = json.load(f)
                except Exception:
                    continue

                if state.get('completed', False):
                    continue
                if state.get('total_requests', 0) == 0:
                    continue

                last_event_str = state.get('last_event_at') or state.get('started_at')
                if not last_event_str:
                    continue

                try:
                    # Handle both offset-aware and naive timestamps
                    last_event = datetime.fromisoformat(last_event_str)
                    if last_event.tzinfo is None:
                        last_event = last_event.replace(tzinfo=timezone.utc)
                    idle_seconds = (now - last_event).total_seconds()
                except ValueError:
                    continue

                if idle_seconds < SESSION_IDLE_TIMEOUT:
                    continue

                # Re-bind all three correlation fields from stored state so the
                # sweeper can replay them onto frontend-message / session-complete
                # without needing to re-process the original inbound message.
                request_id = state.get('request_id')
                session_id = state.get('session_id')
                origin = state.get('origin')
                if not request_id:
                    # Legacy state file written before Phase 3 — skip gracefully
                    continue

                self._logger.warning(
                    f"⏰ Idle sweeper: request {request_id} (session={session_id}) has been idle "
                    f"{idle_seconds:.0f}s (>{SESSION_IDLE_TIMEOUT}s); force-aggregating "
                    f"({state.get('received_count', 0)}/{state.get('total_requests', 0)} received)"
                )

                # Mark as completed so it won't be re-triggered
                state['completed'] = True
                state['last_updated'] = now.isoformat()
                try:
                    with open(state_file, 'w', encoding='utf-8') as f:
                        json.dump(state, f, indent=2, ensure_ascii=False)
                except Exception as save_err:
                    self._logger.error(f"   ❌ Sweeper: failed to update state file: {save_err}")
                    continue

                _sweep_tokens = bind_correlation_ids(session_id, request_id)
                try:
                    self._publish_session_complete(
                        session_id,
                        state.get('received_count', 0),
                        failed=state.get('failed', []),
                        request_id=request_id,
                        origin=origin,
                    )
                    self._aggregate_to_combined_index(request_id)
                finally:
                    reset_correlation_ids(_sweep_tokens)

        except Exception as e:
            self._logger.error(f"❌ Error in idle session sweeper: {e}", exc_info=True)

    def _update_sessions_gauge(self):
        """Count non-completed session state files and update the gauge."""
        try:
            if not self.cache_dir.exists():
                self.metrics.set_sessions_in_progress(0)
                return
            count = 0
            for state_file in self.cache_dir.glob("*/*_state.json"):
                try:
                    with open(state_file, 'r', encoding='utf-8') as f:
                        state = json.load(f)
                    if not state.get('completed', False):
                        count += 1
                except Exception:
                    continue
            self.metrics.set_sessions_in_progress(count)
        except Exception:
            pass

    async def consume_messages(self):
        """Main consumer loop"""
        try:
            # Create Kafka consumer
            from confluent_kafka import TopicPartition

            self.consumer = Consumer(self.kafka_config)

            # Manual partition assignment
            cluster_metadata = self.consumer.list_topics(self.input_topic, timeout=10)
            topic_metadata = cluster_metadata.topics.get(self.input_topic)

            if topic_metadata:
                partitions = [TopicPartition(self.input_topic, p) for p in topic_metadata.partitions.keys()]
                self.consumer.assign(partitions)
                self._logger.info(f"✅ Manually assigned {len(partitions)} partitions for topic: {self.input_topic}")
            else:
                self._logger.error(f"❌ Topic {self.input_topic} not found")
                raise RuntimeError(f"Topic {self.input_topic} not found")

            self._logger.info("🚀 Starting to consume summary-completed messages...")

            # Start folder-switch watcher in background thread
            import threading
            _switch_watcher = threading.Thread(
                target=self._watch_folder_switch,
                daemon=True,
                name="folder-switch-watcher",
            )
            _switch_watcher.start()
            self._logger.info("✅ Folder-switch watcher thread started")

            # Start telemetry services if available
            if self.telemetry_enabled:
                try:
                    self.metrics_server.start()
                    self.system_metrics.start()
                    self._logger.info("✅ Telemetry services started")
                except Exception as e:
                    self._logger.warning(f"⚠️ Failed to start telemetry: {e}")

            self.running = True

            # Set consumer state to running
            if self.telemetry_enabled:
                self.metrics.set_state(1)
                # instrumentation - startup_timestamp_seconds
                self.metrics.record_startup()
                # instrumentation - consumer_info
                self.metrics.set_info({
                    'consumer_name': 'summary_indexer',
                    'consumer_group': self.kafka_config.get('group.id', 'summary-indexer-consumer'),
                    'input_topic': self.input_topic,
                    'output_topic': self.output_topic,
                })

            # Sweeper: fire every SWEEPER_INTERVAL polls (≈ seconds given poll timeout=1s)
            _SWEEPER_INTERVAL = 30  # check every ~30 seconds
            _poll_count = 0

            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                # Periodic idle-session sweeper
                _poll_count += 1
                if _poll_count >= _SWEEPER_INTERVAL:
                    _poll_count = 0
                    self._check_idle_sessions()
                    # instrumentation - sessions_in_progress
                    if self.telemetry_enabled:
                        self._update_sessions_gauge()

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self._logger.error(f"❌ Kafka error: {msg.error()}")
                        continue

                try:
                    # Parse message
                    message_data = json.loads(msg.value().decode('utf-8'))

                    # Track Kafka metrics
                    if self.telemetry_enabled:
                        self.kafka_metrics.increment_messages_in_flight(self.input_topic)
                        # instrumentation - last_message_processed_timestamp
                        self.metrics.record_message_processed(self.input_topic)

                    # Process message
                    result = await self._process_message(message_data)

                    # Track processing metrics
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)

                except json.JSONDecodeError as e:
                    self._logger.error(f"❌ Invalid JSON in message: {e}")
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                        # instrumentation - processing_failures_total
                        self.metrics.increment_failures(self.input_topic, ErrorCategory.DESERIALIZATION_ERROR)
                except Exception as e:
                    self._logger.error(f"❌ Error processing message: {e}", exc_info=True)
                    if self.telemetry_enabled:
                        self.kafka_metrics.decrement_messages_in_flight(self.input_topic)
                        # instrumentation - processing_failures_total
                        self.metrics.increment_failures(self.input_topic, ErrorCategory.PROCESSING_ERROR)

        except Exception as e:
            self._logger.error(f"❌ Consumer error: {e}", exc_info=True)
            raise
        finally:
            # Shutdown telemetry on exit
            if self.telemetry_enabled:
                try:
                    self.system_metrics.stop()
                    self.metrics_server.stop()
                    self._logger.info("✅ Telemetry services stopped")
                except Exception as e:
                    self._logger.warning(f"⚠️ Error stopping telemetry: {e}")

            if self.consumer:
                self.consumer.close()
                self._logger.info("✅ Consumer closed")
            if self.producer:
                self.producer.flush()
                self._logger.info("✅ Producer flushed")

    def run(self):
        """Run the consumer"""
        try:
            asyncio.run(self.consume_messages())
        except KeyboardInterrupt:
            self._logger.info("🛑 Interrupted by user")
        except Exception as e:
            self._logger.error(f"❌ Fatal error: {e}", exc_info=True)
            sys.exit(1)


def main():
    """Main entry point"""
    print("=" * 80)
    print("📚 Kafka Summary Indexer Consumer")
    print("=" * 80)
    print()

    try:
        print("⏳ Initializing consumer...")
        consumer = KafkaSummaryIndexerConsumer()

        print("✅ Consumer initialized successfully")
        print()

        consumer.run()

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
