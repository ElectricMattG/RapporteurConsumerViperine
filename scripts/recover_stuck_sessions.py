#!/usr/bin/env python3
"""
recover_stuck_sessions.py

One-shot script to force-aggregate orphaned summary indexer sessions.

A session is "stuck" if:
  - Its state.json shows received_count < total_requests
  - It has been idle (no last_event_at update) for > IDLE_THRESHOLD seconds
  - It has a non-empty *_summaries_embedded.jsonl file

Usage:
    # Dry-run (preview what would be aggregated)
    python scripts/recover_stuck_sessions.py --dry-run

    # Live run
    python scripts/recover_stuck_sessions.py

    # Override idle threshold (default 300s / 5 min)
    python scripts/recover_stuck_sessions.py --idle-threshold 60

    # Point at a specific cache dir
    python scripts/recover_stuck_sessions.py --cache-dir /app/cache/summaries
"""

import argparse
import fcntl
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

try:
    from utils.url_normalize import normalize_url
except ImportError:
    def normalize_url(url: str) -> str:  # type: ignore[misc]
        return url.strip()


def _iter_stuck_sessions(cache_dir: Path, idle_threshold: float):
    """Yield (session_id, state, session_index_file) for all stuck sessions."""
    now = datetime.now(timezone.utc)
    for state_file in sorted(cache_dir.glob("*/*_state.json")):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception as e:
            print(f"  SKIP {state_file}: {e}", file=sys.stderr)
            continue

        if state.get('completed', False):
            continue
        if state.get('total_requests', 0) == 0:
            continue

        session_id = state.get('session_id')
        if not session_id:
            continue

        # Check if index file has any content
        session_dir = state_file.parent
        index_file = session_dir / f"{session_id}_summaries_embedded.jsonl"
        if not index_file.exists() or index_file.stat().st_size == 0:
            continue

        # Check idleness
        last_event_str = state.get('last_event_at') or state.get('last_updated') or state.get('started_at')
        if last_event_str:
            try:
                last_event = datetime.fromisoformat(last_event_str)
                if last_event.tzinfo is None:
                    last_event = last_event.replace(tzinfo=timezone.utc)
                idle_secs = (now - last_event).total_seconds()
            except ValueError:
                idle_secs = float('inf')
        else:
            idle_secs = float('inf')

        if idle_secs < idle_threshold:
            continue

        yield session_id, state, index_file, idle_secs


def _aggregate_session(
    session_id: str,
    session_index_file: Path,
    combined_index_file: Path,
    dry_run: bool,
) -> tuple[int, int]:
    """
    Aggregate session JSONL into combined index.
    Returns (new_count, duplicate_count).
    """
    existing_doc_ids: set = set()
    existing_dedup_keys: set = set()

    if combined_index_file.exists():
        with open(combined_index_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get('document_id'):
                        existing_doc_ids.add(entry['document_id'])
                    dk = entry.get('dedup_key')
                    if not dk and entry.get('source_url'):
                        dk = hashlib.md5(normalize_url(entry['source_url']).encode()).hexdigest()
                    if dk:
                        existing_dedup_keys.add(dk)
                except json.JSONDecodeError:
                    continue

    new_count = 0
    dup_count = 0
    entries_to_write = []

    with open(session_index_file, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                doc_id = entry.get('document_id')
                dk = entry.get('dedup_key')
                if not dk and entry.get('source_url'):
                    dk = hashlib.md5(normalize_url(entry['source_url']).encode()).hexdigest()

                if (doc_id and doc_id in existing_doc_ids) or (dk and dk in existing_dedup_keys):
                    dup_count += 1
                    continue

                entries_to_write.append((entry, doc_id, dk))
                if doc_id:
                    existing_doc_ids.add(doc_id)
                if dk:
                    existing_dedup_keys.add(dk)
                new_count += 1
            except json.JSONDecodeError:
                continue

    if not dry_run and entries_to_write:
        combined_index_file.parent.mkdir(parents=True, exist_ok=True)
        real_path = combined_index_file.resolve()
        lock_path = real_path.with_suffix(real_path.suffix + '.lock')
        with open(lock_path, 'w') as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            try:
                with open(combined_index_file, 'a', encoding='utf-8') as out:
                    for entry, _, _ in entries_to_write:
                        json.dump(entry, out, ensure_ascii=False)
                        out.write('\n')
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    return new_count, dup_count


def main():
    parser = argparse.ArgumentParser(description="Recover stuck summary indexer sessions")
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--cache-dir', default=None, help='Path to cache/summaries directory')
    parser.add_argument('--combined-index', default=None, help='Path to combined JSONL index file')
    parser.add_argument('--idle-threshold', type=float, default=300,
                        help='Seconds of idle before a session is considered stuck (default: 300)')
    args = parser.parse_args()

    # Resolve paths
    project_root = Path(__file__).resolve().parent.parent
    cache_dir = Path(args.cache_dir) if args.cache_dir else (project_root / 'cache' / 'summaries')
    if args.combined_index:
        combined_index_file = Path(args.combined_index)
    else:
        # Check common locations
        candidates = [
            project_root / 'x_indices_merged' / 'research_summaries_filtered.jsonl',
            project_root / 'x_indices_merged' / 'research_summaries_with_embeddings_combined.jsonl',
            project_root / 'research_summaries_with_embeddings_combined.jsonl',
        ]
        combined_index_file = next((p for p in candidates if p.exists()), candidates[0])

    print(f"Cache dir:      {cache_dir}")
    print(f"Combined index: {combined_index_file}")
    print(f"Idle threshold: {args.idle_threshold}s")
    print(f"Dry run:        {args.dry_run}")
    print()

    if not cache_dir.exists():
        print(f"ERROR: Cache dir not found: {cache_dir}", file=sys.stderr)
        sys.exit(1)

    stuck = list(_iter_stuck_sessions(cache_dir, args.idle_threshold))
    if not stuck:
        print("No stuck sessions found.")
        return

    total_new = 0
    total_dup = 0
    for session_id, state, index_file, idle_secs in stuck:
        received = state.get('received_count', 0)
        total_req = state.get('total_requests', 0)
        failed = state.get('failed', [])

        # Count lines in the index file
        line_count = sum(1 for l in open(index_file) if l.strip())

        print(f"Session: {session_id}")
        print(f"  State:    {received}/{total_req} received, {len(failed)} failed")
        print(f"  Index:    {line_count} lines in {index_file.name}")
        print(f"  Idle:     {idle_secs:.0f}s")

        if args.dry_run:
            print(f"  [DRY RUN] Would aggregate {line_count} entries to {combined_index_file}")
        else:
            new_c, dup_c = _aggregate_session(session_id, index_file, combined_index_file, dry_run=False)
            print(f"  ✅ Wrote {new_c} new, {dup_c} skipped (duplicates)")
            total_new += new_c
            total_dup += dup_c

            # Mark state as completed
            state['completed'] = True
            state['recovered_at'] = datetime.now(timezone.utc).isoformat()
            state_file = index_file.parent / f"{session_id}_state.json"
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        print()

    if not args.dry_run:
        print(f"Done. Total new={total_new}, duplicates_skipped={total_dup}")


if __name__ == '__main__':
    main()
