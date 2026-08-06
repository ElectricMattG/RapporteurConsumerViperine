-- corpus_summaries — Viperine's summary-indexer dual-write target.
-- Gaboon owns this same migration file too ("Postgres when Viperine and
-- Gaboon work hand-in-hand" per the design brief) — kept identical in both
-- repos since each consumer group runs its own migrations independently.
--
-- Ensures the REAL, existing production table exists with the same shape
-- if bootstrapping a fresh database (idempotent no-op against the current
-- production DB, which already has this exact table/columns). This table
-- already has a clean, verified-duplicate-free unique constraint on
-- (tenant_id, corpus, document_id), so PostgresSummaryClient can use a
-- normal ON CONFLICT DO UPDATE upsert.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS corpus_summaries (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    corpus          TEXT NOT NULL,
    document_id     TEXT,
    content         TEXT,
    doc_metadata    JSONB,
    embedding       vector(384),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, corpus, document_id)
);

CREATE INDEX IF NOT EXISTS ix_corpus_summaries_tenant_corpus ON corpus_summaries (tenant_id, corpus);
CREATE INDEX IF NOT EXISTS ix_corpus_summaries_ann
    ON corpus_summaries USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

INSERT INTO schema_migrations (version) VALUES ('003_corpus_summaries')
ON CONFLICT (version) DO NOTHING;
