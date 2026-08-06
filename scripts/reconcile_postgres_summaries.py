#!/usr/bin/env python3
"""Reconcile Postgres corpus_summaries with the flat-file summaries index.

kafka_summary_indexer dual-writes each batch of new combined-index entries to
Postgres, but those writes are deliberately never-raise - interrupted flushes
(pod restarts, transient DB errors) accumulate silent drift where
research_summaries_combined.jsonl is ahead of corpus_summaries. This script
closes the gap idempotently by re-upserting EVERY flat-file entry through the
same PostgresSummaryClient the indexer uses (same conflict key), so
already-synced rows are no-ops.

Run inside the viperine-summary-indexer pod (has the flat-file mount + DSN):
    kubectl -n consumers exec deploy/viperine-summary-indexer -- \
        python3 scripts/reconcile_postgres_summaries.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/src")

try:
    from src.clients.postgres_summary_client import PostgresSummaryClient
except ImportError:
    from clients.postgres_summary_client import PostgresSummaryClient

IDX = Path("/app/x_indices_merged")
BATCH = 500


def iter_jsonl(path: Path):
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def main() -> int:
    client = PostgresSummaryClient()
    if not client.enabled:
        print("Postgres client disabled - nothing to reconcile.")
        return 1

    total = 0
    path = IDX / "research_summaries_combined.jsonl"
    if not path.exists():
        print(f"flat file not found: {path}")
        return 1

    batch = []
    for rec in iter_jsonl(path):
        batch.append(rec)
        if len(batch) >= BATCH:
            total += client.upsert_summaries(batch)
            batch = []
    if batch:
        total += client.upsert_summaries(batch)

    print(f"summaries reconciled (upserted): {total}")
    client.close()
    print("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
