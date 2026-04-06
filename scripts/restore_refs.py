#!/usr/bin/env python3
"""Restore sandboxed reference sections to EO chapters after editorial processing.

Re-attaches ``tables/{stem}_refs.md`` content to the end of each chapter that
has ``refs_sandboxed: true`` in the manifest.

Usage:
    python scripts/restore_refs.py /mnt/workspace_2/bboa/documents/culture_and_media

Idempotent: skips chapters whose refs have already been restored.
"""
import json
import sys
from pathlib import Path


def restore(build_dir: Path) -> None:
    chapters_dir = build_dir / "chapters"
    tables_dir = build_dir / "tables"
    manifest_path = chapters_dir / "manifest.json"

    if not manifest_path.exists():
        print(f"ERROR: {manifest_path} not found", file=sys.stderr)
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    chapters = manifest.get("chapters", [])

    restored = 0
    skipped = 0

    for entry in chapters:
        if not entry.get("refs_sandboxed"):
            continue

        fn = entry["filename"]
        chapter_path = chapters_dir / fn
        stem = chapter_path.stem
        sandbox_path = tables_dir / f"{stem}_refs.md"

        if not sandbox_path.exists():
            print(f"  WARN: {sandbox_path.name} not found — cannot restore {fn}")
            skipped += 1
            continue

        if not chapter_path.exists():
            print(f"  WARN: {fn} not found on disk — skipping")
            skipped += 1
            continue

        chapter_text = chapter_path.read_text(encoding="utf-8").rstrip()
        refs_text = sandbox_path.read_text(encoding="utf-8")

        # Re-attach
        combined = chapter_text + "\n\n" + refs_text
        chapter_path.write_text(combined, encoding="utf-8")

        new_wc = len(combined.split())
        entry["word_count"] = new_wc
        del entry["refs_sandboxed"]

        print(f"  RESTORED: {fn}  → {new_wc} words")
        restored += 1

    # Write updated manifest
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"\nDone: {restored} restored, {skipped} skipped")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <build_dir>", file=sys.stderr)
        sys.exit(1)
    restore(Path(sys.argv[1]))
