#!/usr/bin/env python3
"""
Backfill missing creation_date (and optionally author) in QA pairs index
by matching source_url against the summaries index which has publication_date.

Usage:
    python scripts/backfill_qa_dates.py [--dry-run]

Reads:
    research_summaries_with_embeddings_combined.jsonl   (source of dates)
    research_qa_pairs_with_embeddings_combined.jsonl     (target to fix)

Writes:
    research_qa_pairs_with_embeddings_combined.jsonl     (in-place, with backup)
"""

import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SUMMARIES_PATH = PROJECT_ROOT / "research_summaries_with_embeddings_combined.jsonl"
QA_PATH = PROJECT_ROOT / "research_qa_pairs_with_embeddings_combined.jsonl"

DRY_RUN = "--dry-run" in sys.argv


def build_url_lookup(summaries_path: Path) -> dict:
    """Build source_url → {publication_date, authors, publisher} from summaries."""
    lookup = {}
    with open(summaries_path, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            url = rec.get("source_url", "").strip()
            if not url:
                continue
            meta = rec.get("doc_metadata", {})
            pub_date = meta.get("publication_date")
            authors = meta.get("authors", [])
            publisher = meta.get("publisher")

            if pub_date and str(pub_date).lower() not in ("null", "none", ""):
                entry = {"publication_date": pub_date}
                # Include author info for optional backfill
                if authors and isinstance(authors, list):
                    valid = [a for a in authors if a and str(a).lower() not in ("null", "none", "")]
                    if valid:
                        entry["author"] = valid[0]
                if publisher and str(publisher).lower() not in ("null", "none", ""):
                    entry["publisher"] = publisher
                lookup[url] = entry

    return lookup


def backfill_qa(qa_path: Path, url_lookup: dict, dry_run: bool = False) -> dict:
    """Backfill missing dates in QA pairs. Returns stats."""
    stats = {
        "total": 0,
        "date_missing": 0,
        "date_fixed": 0,
        "author_missing": 0,
        "author_fixed": 0,
        "unchanged": 0,
    }

    output_lines = []

    with open(qa_path, "r", encoding="utf-8") as f:
        for line in f:
            stats["total"] += 1
            rec = json.loads(line)
            meta = rec.get("document_metadata", {})
            url = meta.get("source_url", "").strip()
            changed = False

            # Check creation_date
            cd = meta.get("creation_date")
            if not cd or str(cd).lower() in ("null", "none", ""):
                stats["date_missing"] += 1
                if url in url_lookup:
                    meta["creation_date"] = url_lookup[url]["publication_date"]
                    stats["date_fixed"] += 1
                    changed = True

            # Check author
            author = meta.get("author")
            if not author or str(author).lower() in ("null", "none", "", "unknown"):
                stats["author_missing"] += 1
                if url in url_lookup and "author" in url_lookup[url]:
                    meta["author"] = url_lookup[url]["author"]
                    stats["author_fixed"] += 1
                    changed = True

            if not changed:
                stats["unchanged"] += 1

            rec["document_metadata"] = meta
            output_lines.append(json.dumps(rec, ensure_ascii=False))

    if not dry_run:
        # Create backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = qa_path.with_suffix(f".jsonl.bak.{timestamp}")
        shutil.copy2(qa_path, backup_path)
        print(f"📦 Backup: {backup_path}")

        # Write updated file
        with open(qa_path, "w", encoding="utf-8") as f:
            f.write("\n".join(output_lines) + "\n")
        print(f"✅ Written: {qa_path}")

    return stats


def main():
    if DRY_RUN:
        print("🔍 DRY RUN — no files will be modified\n")

    print(f"📄 Summaries: {SUMMARIES_PATH}")
    print(f"📄 QA pairs:  {QA_PATH}\n")

    # Step 1: Build URL → date lookup from summaries
    print("Building URL lookup from summaries...")
    url_lookup = build_url_lookup(SUMMARIES_PATH)
    print(f"   URLs with dates: {len(url_lookup)}\n")

    # Step 2: Backfill QA pairs
    print("Scanning QA pairs...")
    stats = backfill_qa(QA_PATH, url_lookup, dry_run=DRY_RUN)

    # Report
    print(f"\n{'=' * 50}")
    print(f"  Total QA records:     {stats['total']:>8,}")
    print(f"  Dates missing:        {stats['date_missing']:>8,}")
    print(f"  Dates fixed:          {stats['date_fixed']:>8,}")
    print(f"  Authors missing:      {stats['author_missing']:>8,}")
    print(f"  Authors fixed:        {stats['author_fixed']:>8,}")
    print(f"  Unchanged:            {stats['unchanged']:>8,}")
    print(f"{'=' * 50}")

    if DRY_RUN:
        print("\n⚠️  Re-run without --dry-run to apply changes")


if __name__ == "__main__":
    main()
