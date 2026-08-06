-- Viperine's Postgres dual-write target: research_summaries.
--
-- This table is OWNED/defined canonically by the Gaboon repo's migrations
-- (RapporteurConsumerGaboon/migrations/003_research_summaries.sql) since
-- Gaboon's index-aggregator shares the same Postgres instance. This copy
-- lets Viperine's summary-indexer run its own idempotent
-- CREATE TABLE IF NOT EXISTS independently of Gaboon's deployment order —
-- whichever consumer group starts first against a fresh database creates it.
--
-- Design: the flat-file JSONL index remains authoritative (read by RAG);
-- Postgres is a redundant, queryable copy so the two can be reconciled and
-- alerted on divergence ("Postgres when Viperine and Gaboon work hand-in-hand").

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS research_summaries (
    document_id         TEXT PRIMARY KEY,
    dedup_key            TEXT,
    query_id             TEXT,
    session_id           TEXT,
    source_url           TEXT,
    summary_text         TEXT,
    summary_embedding    vector(384),
    embedding_dim        INTEGER,
    raw                  JSONB NOT NULL,
    indexed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_summaries_query_id   ON research_summaries (query_id);
CREATE INDEX IF NOT EXISTS idx_research_summaries_session_id ON research_summaries (session_id);
CREATE INDEX IF NOT EXISTS idx_research_summaries_dedup_key  ON research_summaries (dedup_key);
CREATE INDEX IF NOT EXISTS idx_research_summaries_embedding
    ON research_summaries USING ivfflat (summary_embedding vector_cosine_ops) WITH (lists = 100);

INSERT INTO schema_migrations (version) VALUES ('001_research_summaries')
ON CONFLICT (version) DO NOTHING;
