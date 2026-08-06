"""Postgres dual-write client for the summary-indexer.

Mirrors the design of Gaboon's ``PostgresIndexClient`` (see
RapporteurConsumerGaboon/src/clients/postgres_index_client.py) but is
trimmed to Viperine's single table: ``research_summaries``. The two consumer
groups share the same Postgres instance/schema so their redundant copies of
research data live side by side and can be reconciled together.

The summary-indexer's consumer loop is fully synchronous (confluent-kafka's
blocking ``poll()``), so this client owns a dedicated background thread
running its own event loop for the process lifetime, and marshals every call
onto it via ``asyncio.run_coroutine_threadsafe``. This is a REDUNDANCY store,
not the source of truth — the flat-file JSONL index remains authoritative.
Every public method here is designed to never raise.
"""

import asyncio
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from prometheus_client import Counter, Gauge

from config_handler import Base

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"

postgres_rows_written_total = Counter(
    "viperine_postgres_rows_written_total",
    "Total rows successfully upserted into Postgres",
    ["table"],
)
postgres_write_failures_total = Counter(
    "viperine_postgres_write_failures_total",
    "Total failed Postgres upsert attempts (batch-level, not row-level)",
    ["table"],
)
postgres_last_success_timestamp = Gauge(
    "viperine_postgres_last_success_timestamp",
    "Unix timestamp of the last successful Postgres write",
)
postgres_up = Gauge(
    "viperine_postgres_up",
    "1 if the last Postgres operation succeeded, 0 if it failed",
)


def _first(*values: Any) -> Optional[Any]:
    for v in values:
        if v not in (None, "", []):
            return v
    return None


def _extract_summary_fields(entry: Dict) -> Dict[str, Any]:
    """Extract fields from a combined-index entry (see _aggregate_session_into_combined).

    Maps onto corpus_summaries's REAL columns (document_id, content,
    doc_metadata, embedding) rather than the old research_summaries shape.
    """
    doc_metadata_in = entry.get("doc_metadata") or {}
    embedding_metadata = entry.get("embedding_metadata") or {}

    document_id = entry.get("document_id")
    dedup_key = entry.get("dedup_key")
    source_url = entry.get("source_url")
    summary_text = _first(entry.get("summary_text"), entry.get("summary"))
    summary_embedding = _first(entry.get("summary_embedding"), embedding_metadata.get("embedding_vector"))
    query_id = _first(doc_metadata_in.get("query_id"), entry.get("query_id"))
    session_id = entry.get("session_id")

    # doc_metadata mirrors the document-level metadata convention already
    # used in corpus_qa/corpus_summaries (title/source_url/etc.), plus the
    # fields (dedup_key, query_id, session_id) that don't have their own
    # dedicated column on corpus_summaries.
    doc_metadata = dict(doc_metadata_in)
    doc_metadata.setdefault("document_id", document_id)
    doc_metadata.setdefault("source_url", source_url)
    doc_metadata["dedup_key"] = dedup_key
    doc_metadata["query_id"] = query_id
    doc_metadata["session_id"] = session_id

    return {
        "document_id": document_id,
        "source_url": source_url,
        "summary_text": summary_text,
        "summary_embedding": summary_embedding or None,
        "doc_metadata": doc_metadata,
    }


class PostgresSummaryClient(Base):
    """Dual-write client: research_summaries, upserted alongside the flat file."""

    def __init__(self):
        super().__init__()
        self.enabled = bool(self._app_config.get("postgres.enabled"))
        self._pool = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

        if not self.enabled:
            self._logger.info("Postgres dual-write disabled (postgres.enabled=False)")
            return

        self.command_timeout = float(self._app_config.get("postgres.command_timeout") or 30.0)
        self._dsn = self._resolve_dsn()

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True, name="postgres-summary-client")
        self._thread.start()

        try:
            self._run(self._ensure_pool_and_schema(), timeout=self.command_timeout + 20.0)
            postgres_up.set(1)
            self._logger.info("✅ PostgresSummaryClient ready (schema ensured)")
        except Exception as e:
            postgres_up.set(0)
            postgres_write_failures_total.labels(table="schema").inc()
            self._logger.error(f"❌ PostgresSummaryClient failed to initialize — dual-write disabled for this run: {e}")
            self.enabled = False

    def _resolve_dsn(self) -> str:
        dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL") or self._app_config.get("postgres.dsn")
        if dsn:
            return dsn
        host = self._app_config.get("postgres.host") or "localhost"
        port = self._app_config.get("postgres.port") or 5432
        database = self._app_config.get("postgres.database") or "rapporteur"
        user = self._app_config.get("postgres.user") or "rapporteur"
        password = os.getenv("POSTGRES_PASSWORD") or self._app_config.get("postgres.password") or ""
        auth = f"{user}:{password}@" if password else f"{user}@"
        return f"postgresql://{auth}{host}:{port}/{database}"

    def _run(self, coro, timeout: float = 30.0):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    async def _ensure_pool_and_schema(self):
        import asyncpg

        async def _init_connection(conn):
            try:
                from pgvector.asyncpg import register_vector

                await register_vector(conn)
            except Exception:
                pass

        self._pool = await asyncpg.create_pool(
            dsn=self._dsn,
            min_size=int(self._app_config.get("postgres.pool_min_size") or 2),
            max_size=int(self._app_config.get("postgres.pool_max_size") or 10),
            command_timeout=self.command_timeout,
            timeout=float(self._app_config.get("postgres.connect_timeout") or 10.0),
            init=_init_connection,
        )

        async with self._pool.acquire() as conn:
            for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
                await conn.execute(sql_file.read_text())
            try:
                from pgvector.asyncpg import register_vector

                await register_vector(conn)
            except Exception:
                pass

    def close(self):
        if not self._loop:
            return
        try:
            if self._pool is not None:
                self._run(self._pool.close(), timeout=10.0)
        except Exception:
            pass
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5.0)

    def upsert_summaries(self, entries: List[Dict]) -> int:
        """Upsert combined-index entries into corpus_summaries. Never raises."""
        if not self.enabled or not entries:
            return 0
        try:
            written = self._run(self._upsert_summaries_async(entries), timeout=self.command_timeout + 10.0)
            postgres_rows_written_total.labels(table="corpus_summaries").inc(written)
            postgres_last_success_timestamp.set(time.time())
            postgres_up.set(1)
            return written
        except Exception as e:
            postgres_write_failures_total.labels(table="corpus_summaries").inc()
            postgres_up.set(0)
            self._logger.error(f"❌ Postgres upsert_summaries failed: {e}")
            return 0

    async def _upsert_summaries_async(self, entries: List[Dict]) -> int:
        import json

        tenant_id = self._app_config.get("postgres.tenant_id") or "default"
        corpus = self._app_config.get("postgres.corpus") or "x_indices_merged"

        rows = []
        for entry in entries:
            f = _extract_summary_fields(entry)
            if not f["document_id"]:
                continue
            rows.append(
                (
                    tenant_id, corpus, f["document_id"], f["summary_text"], f["summary_embedding"],
                    json.dumps(f["doc_metadata"], ensure_ascii=False, default=str),
                )
            )
        if not rows:
            return 0

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO corpus_summaries
                    (tenant_id, corpus, document_id, content, embedding, doc_metadata, created_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, now())
                ON CONFLICT (tenant_id, corpus, document_id) DO UPDATE SET
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    doc_metadata = EXCLUDED.doc_metadata
                """,
                rows,
            )
        return len(rows)
