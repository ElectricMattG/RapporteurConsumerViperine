#!/usr/bin/env python3
"""
R1.2 Diagnostic — verify that the summary indexer and the RAG service
are reading/writing the same physical file.

Usage:
    python scripts/verify_index_path.py [--rag-url http://localhost:8000]

Checks:
  1. What path config.py resolves for combined_index_file
  2. Whether that file exists and how many lines it contains
  3. What the RAG /ready endpoint reports for summaries_loaded
  4. Whether the two counts match (path alignment confirmed)

ACTIVE_INDEX_FOLDER env var is honoured (default: x_indices_trump).
"""
import argparse
import json
import os
import sys
from pathlib import Path

# Make src importable from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import requests
except ImportError:
    requests = None  # type: ignore


def count_lines(path: Path) -> int:
    """Count non-empty lines in a JSONL file (fast, no JSON parsing)."""
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description="Verify index path alignment")
    parser.add_argument("--rag-url", default="http://localhost:8000",
                        help="RAG service base URL (default: http://localhost:8000)")
    args = parser.parse_args()

    active_folder = os.environ.get("ACTIVE_INDEX_FOLDER", "x_indices_merged")
    print(f"\n{'='*60}")
    print(f"Index Path Alignment Diagnostic")
    print(f"{'='*60}")
    print(f"ACTIVE_INDEX_FOLDER : {active_folder}")

    # --- Step 1: resolve combined_index_file from config ---
    from src.config import defaults
    combined_rel = defaults["summary_indexer"]["combined_index_file"]
    combined_abs = PROJECT_ROOT / combined_rel
    print(f"\n[1] Config path (relative) : {combined_rel}")
    print(f"    Config path (absolute)  : {combined_abs}")

    if not combined_abs.exists():
        print(f"    ❌  FILE NOT FOUND — indexer will fail to append")
        sys.exit(1)

    local_count = count_lines(combined_abs)
    print(f"    ✅  File exists — {local_count} summary entries")

    # --- Step 2: query RAG /ready ---
    if requests is None:
        print("\n[2] Skipping RAG check (requests not installed)")
        return

    ready_url = f"{args.rag_url.rstrip('/')}/ready"
    print(f"\n[2] Querying RAG /ready → {ready_url}")
    try:
        resp = requests.get(ready_url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        rag_count = data.get("total_summaries", data.get("summaries_loaded", data.get("summaries_count", "N/A")))
        index_path_in_rag = data.get("index_path", "unknown")
        print(f"    RAG summaries_loaded : {rag_count}")
        print(f"    RAG index_path       : {index_path_in_rag}")
    except Exception as e:
        print(f"    ⚠️  Could not reach RAG service: {e}")
        print("    (Is rag-service running on the expected URL?)")
        return

    # --- Step 3: compare ---
    print(f"\n[3] Comparison")
    print(f"    Local file lines : {local_count}")
    print(f"    RAG loaded count : {rag_count}")

    if isinstance(rag_count, int) and rag_count == local_count:
        print("    ✅  ALIGNED — indexer writes to the same file the RAG service reads")
    elif isinstance(rag_count, int):
        diff = local_count - rag_count
        if diff > 0:
            print(f"    ⚠️  OUT OF SYNC — local file has {diff} more lines than RAG loaded")
            print("       This is expected if new summaries were appended since last restart.")
            print("       Publish a reload-index Kafka message to sync, or restart rag-service.")
        else:
            print(f"    ❌  MISMATCH — RAG loaded {abs(diff)} MORE than local file")
            print("       Check that the Docker volume mount points to this file.")
    else:
        print("    ⚠️  Could not compare (RAG did not return a numeric count)")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
