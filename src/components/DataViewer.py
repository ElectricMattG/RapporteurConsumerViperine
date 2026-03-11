#!/usr/bin/env python3
"""
DataViewer - Interactive JSONL Index Viewer

A lightweight Flask server that renders Q&A pair index data from JSONL files,
grouped by document_id. Supports browsing, editing metadata, and bulk-writing
updates back to the index file.

Usage:
    python -m src.components.DataViewer                             # defaults
    python -m src.components.DataViewer --file path/to/index.jsonl  # custom file
    python -m src.components.DataViewer --port 5050                 # custom port
"""

import argparse
import json
import os
import sys
import threading
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.config_handler import Base

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_QA_INDEX = "research_qa_pairs_with_embeddings_combined.jsonl"
DEFAULT_SUMMARY_INDEX = "research_summaries_with_embeddings_combined.jsonl"
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


class DataViewer(Base):
    """Interactive JSONL index viewer with edit + bulk-write capability.

    Inherits logging from ``Base``.
    """

    def __init__(
        self,
        jsonl_path: str = None,
        index_type: str = "qa",
        host: str = "0.0.0.0",
        port: int = 5055,
    ):
        super().__init__()
        self._logger.info("📊 Initialising DataViewer")

        # Resolve path
        workspace = Path(__file__).resolve().parents[2]
        if jsonl_path is None:
            default = DEFAULT_QA_INDEX if index_type == "qa" else DEFAULT_SUMMARY_INDEX
            self.jsonl_path = str(workspace / default)
        else:
            self.jsonl_path = str(Path(jsonl_path).resolve())

        self.index_type = index_type
        self.host = host
        self.port = port

        # In-memory document group index: document_id -> [line_numbers]
        self._doc_groups: OrderedDict[str, List[int]] = OrderedDict()
        # All records keyed by line number (0-based)
        self._records: Dict[int, dict] = {}
        # Ordered list of document_ids for navigation
        self._doc_ids: List[str] = []

        self._lock = threading.Lock()
        self._load_index()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def _load_index(self) -> None:
        """Load the JSONL file and build the grouped index."""
        self._logger.info(f"📂 Loading index from {self.jsonl_path}")
        self._doc_groups.clear()
        self._records.clear()

        if not os.path.isfile(self.jsonl_path):
            self._logger.error(f"File not found: {self.jsonl_path}")
            return

        with open(self.jsonl_path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError as exc:
                    self._logger.warning(f"Skipping malformed line {line_no}: {exc}")
                    continue

                self._records[line_no] = rec
                doc_id = self._extract_doc_id(rec)
                if doc_id not in self._doc_groups:
                    self._doc_groups[doc_id] = []
                self._doc_groups[doc_id].append(line_no)

        self._doc_ids = list(self._doc_groups.keys())
        self._logger.info(
            f"✅ Loaded {len(self._records)} records across "
            f"{len(self._doc_ids)} documents"
        )

    def _extract_doc_id(self, rec: dict) -> str:
        """Extract document_id from a record (handles both QA and summary schemas)."""
        # QA schema: nested under document_metadata
        dm = rec.get("document_metadata", {})
        if dm and "document_id" in dm:
            return dm["document_id"]
        # Summary schema: top-level
        if "document_id" in rec:
            return rec["document_id"]
        return "unknown"

    # ------------------------------------------------------------------
    # Data access helpers
    # ------------------------------------------------------------------
    def _get_doc_at_index(self, idx: int) -> Tuple[str, List[dict]]:
        """Return (doc_id, [records]) for the document at navigation index ``idx``."""
        idx = max(0, min(idx, len(self._doc_ids) - 1))
        doc_id = self._doc_ids[idx]
        records = [self._records[ln] for ln in self._doc_groups[doc_id]]
        return doc_id, records

    def _extract_display_metadata(self, rec: dict) -> dict:
        """Pull the key metadata fields for display (no embeddings)."""
        dm = rec.get("document_metadata", {})
        if not dm:
            # Summary schema
            dm = rec.get("doc_metadata", {})

        qa = rec.get("qa_metadata", {})
        cm = rec.get("chunk_metadata", {})

        source_url = dm.get("source_url") or rec.get("source_url", "")
        # Decode percent-encoded URLs for display (Arabic etc.)
        display_url = unquote(source_url) if source_url else ""

        return {
            "document_id": dm.get("document_id") or rec.get("document_id", ""),
            "title": dm.get("title", ""),
            "author": dm.get("author") or dm.get("authors", ""),
            "creation_date": dm.get("creation_date") or dm.get("publication_date", ""),
            "language": dm.get("language", ""),
            "source_url": source_url,
            "display_url": display_url,
            "content_type": dm.get("content_type", ""),
            # QA-specific
            "question": qa.get("question", ""),
            "answer": qa.get("answer", ""),
            "qa_index": qa.get("qa_index", ""),
            "qa_total": qa.get("qa_total_for_source", ""),
            # Chunk info
            "chunk_id": cm.get("chunk_id", ""),
            "chunk_index": cm.get("chunk_index", ""),
            "word_count": cm.get("word_count") or qa.get("word_count_answer", ""),
        }

    # ------------------------------------------------------------------
    # Write-back
    # ------------------------------------------------------------------
    def _apply_metadata_update(self, doc_id: str, updates: dict) -> int:
        """Apply metadata updates to ALL records with the given ``document_id``.

        Returns the number of records updated.
        """
        if doc_id not in self._doc_groups:
            self._logger.warning(f"document_id {doc_id} not found in index")
            return 0

        count = 0
        with self._lock:
            for ln in self._doc_groups[doc_id]:
                rec = self._records[ln]
                dm = rec.get("document_metadata")
                if dm is None:
                    dm = rec.get("doc_metadata")
                if dm is None:
                    continue

                for key, val in updates.items():
                    if key in dm:
                        dm[key] = val
                count += 1

        self._logger.info(f"✏️  Updated {count} records for {doc_id}: {updates}")
        return count

    def _flush_to_disk(self) -> None:
        """Rewrite the entire JSONL file from the in-memory records."""
        backup = self.jsonl_path + f".bak.{datetime.now():%Y%m%d_%H%M%S}"
        self._logger.info(f"💾 Flushing to {self.jsonl_path} (backup: {backup})")

        with self._lock:
            # Backup first
            if os.path.isfile(self.jsonl_path):
                os.rename(self.jsonl_path, backup)

            with open(self.jsonl_path, "w", encoding="utf-8") as fh:
                for ln in sorted(self._records.keys()):
                    fh.write(json.dumps(self._records[ln], ensure_ascii=False) + "\n")

        self._logger.info(f"✅ Wrote {len(self._records)} records to disk")

    # ------------------------------------------------------------------
    # Flask server
    # ------------------------------------------------------------------
    def _build_app(self):
        """Build and return the Flask application."""
        from flask import Flask, jsonify, render_template_string, request

        app = Flask(__name__)
        viewer = self

        # Load HTML template
        template_path = os.path.join(TEMPLATE_DIR, "data_viewer.html")
        if os.path.isfile(template_path):
            with open(template_path, "r") as fh:
                TEMPLATE = fh.read()
        else:
            viewer._logger.warning(
                f"Template not found at {template_path}, using inline fallback"
            )
            TEMPLATE = "<html><body><h1>Template missing</h1></body></html>"

        @app.route("/")
        def index():
            return render_template_string(
                TEMPLATE,
                total_docs=len(viewer._doc_ids),
                total_records=len(viewer._records),
                index_file=os.path.basename(viewer.jsonl_path),
            )

        @app.route("/api/doc/<int:idx>")
        def get_doc(idx):
            """Return document group at navigation index ``idx``."""
            if not viewer._doc_ids:
                return jsonify({"error": "No data loaded"}), 404
            doc_id, records = viewer._get_doc_at_index(idx)
            display_records = [viewer._extract_display_metadata(r) for r in records]
            return jsonify({
                "doc_id": doc_id,
                "index": max(0, min(idx, len(viewer._doc_ids) - 1)),
                "total_docs": len(viewer._doc_ids),
                "record_count": len(records),
                "records": display_records,
            })

        @app.route("/api/search")
        def search():
            """Search documents by doc_id, author, or title substring."""
            q = request.args.get("q", "").strip().lower()
            if not q:
                return jsonify({"results": []})

            results = []
            for i, doc_id in enumerate(viewer._doc_ids):
                if q in doc_id.lower():
                    results.append({"index": i, "doc_id": doc_id, "match": "doc_id"})
                    continue
                # Check first record metadata
                first_ln = viewer._doc_groups[doc_id][0]
                rec = viewer._records[first_ln]
                dm = rec.get("document_metadata", rec.get("doc_metadata", {}))
                title = (dm.get("title") or "").lower()
                author = (dm.get("author") or dm.get("authors") or "").lower()
                if q in title:
                    results.append({"index": i, "doc_id": doc_id, "match": "title"})
                elif q in author:
                    results.append({"index": i, "doc_id": doc_id, "match": "author"})

                if len(results) >= 50:
                    break

            return jsonify({"results": results, "query": q})

        @app.route("/api/update", methods=["POST"])
        def update():
            """Update metadata for all records sharing a document_id."""
            data = request.get_json(force=True)
            doc_id = data.get("doc_id")
            updates = data.get("updates", {})

            if not doc_id or not updates:
                return jsonify({"error": "doc_id and updates required"}), 400

            count = viewer._apply_metadata_update(doc_id, updates)
            return jsonify({"updated": count, "doc_id": doc_id})

        @app.route("/api/write", methods=["POST"])
        def write():
            """Flush all in-memory changes to disk."""
            try:
                viewer._flush_to_disk()
                return jsonify({"status": "ok", "records": len(viewer._records)})
            except Exception as exc:
                viewer._logger.error(f"Write failed: {exc}")
                return jsonify({"error": str(exc)}), 500

        @app.route("/api/reload", methods=["POST"])
        def reload():
            """Reload the index from disk (discards unsaved changes)."""
            viewer._load_index()
            return jsonify({
                "status": "ok",
                "total_docs": len(viewer._doc_ids),
                "total_records": len(viewer._records),
            })

        return app

    def serve(self) -> None:
        """Start the Flask development server."""
        app = self._build_app()
        self._logger.info(
            f"🚀 DataViewer serving on http://{self.host}:{self.port}"
        )
        app.run(host=self.host, port=self.port, debug=False)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="DataViewer - Interactive JSONL Index Viewer"
    )
    parser.add_argument(
        "--file", "-f",
        default=None,
        help="Path to JSONL index file (defaults to QA index in workspace root)",
    )
    parser.add_argument(
        "--type", "-t",
        choices=["qa", "summary"],
        default="qa",
        help="Index type: 'qa' or 'summary' (default: qa)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=5055,
        help="Port to serve on (default: 5055)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    viewer = DataViewer(
        jsonl_path=args.file,
        index_type=args.type,
        host=args.host,
        port=args.port,
    )
    viewer.serve()


if __name__ == "__main__":
    main()
