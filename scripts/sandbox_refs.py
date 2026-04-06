#!/usr/bin/env python3
"""Sandbox reference sections from EO chapters before editorial processing.

Strips everything from ``## Top Sources`` onward (Top Sources, References,
Implementation Record) into ``tables/{stem}_refs.md`` so that the editorial
pipeline processes only the analytical content.

Usage:
    python scripts/sandbox_refs.py /mnt/workspace_2/bboa/documents/culture_and_media

Idempotent: skips chapters whose refs have already been sandboxed.
"""
import json
import re
import sys
from pathlib import Path

CUT_PATTERN = re.compile(r"^## Top Sources", re.MULTILINE)


def sandbox(build_dir: Path) -> None:
    chapters_dir = build_dir / "chapters"
    tables_dir = build_dir / "tables"
    manifest_path = chapters_dir / "manifest.json"

    if not manifest_path.exists():
        print(f"ERROR: {manifest_path} not found", file=sys.stderr)
        sys.exit(1)

    tables_dir.mkdir(exist_ok=True)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    chapters = manifest.get("chapters", [])

    sandboxed = 0
    skipped = 0

    for entry in chapters:
        fn = entry.get("filename", "")
        # Only process EO explanation chapters (have format_status and start
        # with a digit prefix matching 01-10)
        if "format_status" not in entry:
            continue
        if not fn.startswith(("01-", "02-", "03-", "04-", "05-",
                              "06-", "07-", "08-", "09-", "10-")):
            continue

        chapter_path = chapters_dir / fn
        if not chapter_path.exists():
            print(f"  WARN: {fn} not found on disk — skipping")
            continue

        stem = chapter_path.stem
        sandbox_path = tables_dir / f"{stem}_refs.md"

        # Idempotent: skip if already sandboxed
        if sandbox_path.exists():
            print(f"  SKIP: {fn} (refs already sandboxed)")
            skipped += 1
            continue

        text = chapter_path.read_text(encoding="utf-8")
        m = CUT_PATTERN.search(text)
        if not m:
            print(f"  SKIP: {fn} (no '## Top Sources' found)")
            skipped += 1
            continue

        keep = text[: m.start()].rstrip() + "\n"
        refs = text[m.start():]

        # Write sandboxed refs
        sandbox_path.write_text(refs, encoding="utf-8")

        # Overwrite chapter with trimmed content
        chapter_path.write_text(keep, encoding="utf-8")

        old_wc = entry.get("word_count", 0)
        new_wc = len(keep.split())
        entry["word_count"] = new_wc
        entry["refs_sandboxed"] = True

        print(f"  SAND: {fn}  {old_wc} → {new_wc} words  "
              f"(stripped {old_wc - new_wc}, saved to {sandbox_path.name})")
        sandboxed += 1

    # Write updated manifest
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"\nDone: {sandboxed} sandboxed, {skipped} skipped")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <build_dir>", file=sys.stderr)
        sys.exit(1)
    sandbox(Path(sys.argv[1]))
