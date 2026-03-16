#!/usr/bin/env python3
"""
rapporteur_cli.py — Unified Document Publishing Pipeline

Orchestrates the full chat-to-published-document pipeline:
  Stage 1 (export)   — Pull conversations from the frontend API or static file
  Stage 2 (parse)    — Classify turns, generate footnotes, build citation map
  Stage 3 (chapters) — Render session Markdown with per-chapter references
  Stage 4 (kaliya)   — Detect threat clusters, send to Kaliya, collect results
  Stage 5 (publish)  — Build PDF / HTML / Confluence from chapter markdowns

Usage
-----
    # Export conversations from the live frontend:
    python docs/rapporteur_cli.py docs/AtrumVolentia/document.yaml --export

    # Export + parse + build chapters:
    python docs/rapporteur_cli.py docs/AtrumVolentia/document.yaml --export --parse --chapters

    # Run the full pipeline:
    python docs/rapporteur_cli.py docs/AtrumVolentia/document.yaml --all

    # Dry-run (no writes):
    python docs/rapporteur_cli.py docs/AtrumVolentia/document.yaml --export --dry-run

    # Override build directory:
    python docs/rapporteur_cli.py docs/AtrumVolentia/document.yaml --all --build-dir /tmp/test_build
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from PIL import Image

# Re-use rendering helpers from the original document builder.
from src.components.publisher.rapporteur_docs import (build_enrichment_section,
                                                      build_references,
                                                      build_top_sources,
                                                      extract_abstract,
                                                      postprocess_response,
                                                      resolve_inline_citations)
from src.config_handler import Base
from src.models.publisher import ChapterSpec, PipelineConfig

# ---------------------------------------------------------------------------
# Logging (fallback for CLI entry-point; stages use Base._logger)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rapporteur")


# ---------------------------------------------------------------------------
# Stage 1 — Export
# ---------------------------------------------------------------------------

class ExportStage(Base):
    """Pull conversations from the frontend API (or copy a static file)."""

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> Path:
        """Execute the export stage.

        Returns the path to the saved conversations JSON.
        """
        raw_dir = self.cfg.build_dir / "raw"
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_file = raw_dir / f"conversations_{ts}.json"
        link_path = raw_dir / "conversations.json"

        if self.cfg.export.static_file:
            return self._copy_static(raw_dir, out_file, link_path)
        return self._fetch_api(raw_dir, out_file, link_path)

    # ------------------------------------------------------------------

    def _fetch_api(self, raw_dir: Path, out_file: Path, link_path: Path) -> Path:
        url = f"{self.cfg.export.frontend_url.rstrip('/')}/api/conversations"
        self._logger.info("Exporting conversations from %s", url)

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        conversations: list[dict] = resp.json()

        return self._write(conversations, raw_dir, out_file, link_path)

    def _copy_static(self, raw_dir: Path, out_file: Path, link_path: Path) -> Path:
        src = Path(self.cfg.export.static_file)
        self._logger.info("Using static export: %s", src)

        if not src.exists():
            raise FileNotFoundError(f"Static export not found: {src}")

        with open(src, "r", encoding="utf-8") as fh:
            conversations: list[dict] = json.load(fh)

        return self._write(conversations, raw_dir, out_file, link_path)

    def _write(
        self,
        conversations: list[dict],
        raw_dir: Path,
        out_file: Path,
        link_path: Path,
    ) -> Path:
        n_convs = len(conversations)
        total_docs = sum(
            len(c.get("retrieved_docs") or []) for c in conversations
        )
        total_chars = sum(
            len(c.get("assistant_response") or "") for c in conversations
        )

        self._logger.info(
            "  %d conversations, %d total docs, %s response chars",
            n_convs, total_docs, f"{total_chars:,}",
        )

        if self.dry_run:
            self._logger.info("  [dry-run] Would write %s", out_file)
            return out_file

        raw_dir.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as fh:
            json.dump(conversations, fh, indent=2, ensure_ascii=False)

        # Stable pointer — try symlink, fall back to copy
        if link_path.is_symlink() or link_path.exists():
            link_path.unlink()
        try:
            link_path.symlink_to(out_file.name)
            self._logger.info("  Symlink: conversations.json -> %s", out_file.name)
        except OSError:
            shutil.copy2(out_file, link_path)
            self._logger.info("  Copied:  conversations.json (symlink not supported)")

        self._logger.info("  Written: %s (%s)", out_file.name, _human_size(out_file))

        return out_file


# ---------------------------------------------------------------------------
# Stage stubs (Phases 2-5 — not yet implemented)
# ---------------------------------------------------------------------------

class ParseStage(Base):
    """Phase 2 — Classify turns, generate footnotes, build unified citation map."""

    # Regex that matches citation tokens the LLM embeds in response text.
    # Two flavours:
    #   req_20260304_112859_006   (request-based)
    #   doc_298e93575             (document hash)
    _TOKEN_RE = re.compile(r'(req_\d{8}_\d{6}_\d{2,4}|doc_[a-f0-9]{4,12})')

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def run(self) -> Path:
        """Classify turns, build per-session JSON, footnotes, and unified citations.

        Returns the path to the ``parsed/`` directory.
        """
        parsed_dir = self.cfg.build_dir / "parsed"
        raw_file = self.cfg.build_dir / "raw" / "conversations.json"

        if not raw_file.exists():
            raise FileNotFoundError(
                f"No exported conversations at {raw_file}. Run --export first."
            )

        with open(raw_file, "r", encoding="utf-8") as fh:
            conversations: list[dict] = json.load(fh)

        self._logger.info("Loaded %d conversations from %s", len(conversations), raw_file.name)

        # --- 2.1  classify ---
        chapter_turns: list[dict] = []
        footnote_turns: list[dict] = []
        include = self.cfg.sessions.include  # explicit whitelist or None

        for idx, conv in enumerate(conversations):
            n_docs = len(conv.get("retrieved_docs") or [])
            is_included = include is None or idx in include

            entry = {
                "index": idx,
                "user_query": conv.get("user_query", ""),
                "timestamp": conv.get("timestamp", ""),
                "n_docs": n_docs,
                "n_qa": len(conv.get("retrieved_qa") or []),
                "n_eo": len(conv.get("eo_results") or []),
                "n_events": len(conv.get("event_results") or []),
                "n_enrichment": len(conv.get("enrichment_results") or []),
                "response_chars": len(conv.get("assistant_response") or ""),
                "citation_map_size": len(
                    (conv.get("metadata") or {}).get("citation_map") or {}
                ),
            }

            if n_docs > 0 and is_included:
                entry["classification"] = "chapter"
                chapter_turns.append(entry)
            else:
                entry["classification"] = "footnote"
                footnote_turns.append(entry)

        self._logger.info(
            "  Classified: %d chapter turns, %d footnote turns",
            len(chapter_turns), len(footnote_turns),
        )

        # --- 2.2  footnotes (heuristic — first 2 sentences of response) ---
        footnotes = self._build_footnotes(conversations, footnote_turns)

        # --- 2.3  unified citation map ---
        unified_citations, cited_count = self._build_unified_citations(
            conversations, chapter_turns
        )

        # --- write outputs ---
        if self.dry_run:
            self._logger.info("  [dry-run] Would write %d session files to %s", len(chapter_turns), parsed_dir)
            self._logger.info("  [dry-run] Would write footnotes.json (%d entries)", len(footnotes))
            self._logger.info("  [dry-run] Would write unified_citations.json (%d cited / %d total)", cited_count, len(unified_citations))
            return parsed_dir

        parsed_dir.mkdir(parents=True, exist_ok=True)

        # Per-session JSON
        for entry in chapter_turns:
            idx = entry["index"]
            out = parsed_dir / f"session_{idx:03d}.json"
            with open(out, "w", encoding="utf-8") as fh:
                json.dump(entry, fh, indent=2, ensure_ascii=False)

        # Footnotes
        fn_path = parsed_dir / "footnotes.json"
        with open(fn_path, "w", encoding="utf-8") as fh:
            json.dump(footnotes, fh, indent=2, ensure_ascii=False)

        # Unified citations
        uc_path = parsed_dir / "unified_citations.json"
        with open(uc_path, "w", encoding="utf-8") as fh:
            json.dump(unified_citations, fh, indent=2, ensure_ascii=False)

        self._logger.info("  Written: %d session files to %s/", len(chapter_turns), parsed_dir.name)
        self._logger.info("  Written: footnotes.json (%d entries)", len(footnotes))
        self._logger.info(
            "  Written: unified_citations.json (%d cited / %d total entries)",
            cited_count, len(unified_citations),
        )

        return parsed_dir

    # ------------------------------------------------------------------
    # 2.2 — footnotes
    # ------------------------------------------------------------------

    def _build_footnotes(
        self, conversations: list[dict], footnote_turns: list[dict]
    ) -> list[dict]:
        """Build footnote summaries for zero-doc turns.

        Uses a simple heuristic: first two sentences of the assistant response.
        """
        footnotes: list[dict] = []
        for entry in footnote_turns:
            idx = entry["index"]
            conv = conversations[idx]
            resp = conv.get("assistant_response") or ""
            footnotes.append({
                "index": idx,
                "user_query": entry["user_query"],
                "footnote_text": _first_n_sentences(resp, n=2),
            })
        return footnotes

    # ------------------------------------------------------------------
    # 2.3 — unified citation map
    # ------------------------------------------------------------------

    def _build_unified_citations(
        self,
        conversations: list[dict],
        chapter_turns: list[dict],
    ) -> tuple[dict, int]:
        """Merge citation maps across chapter turns and tag which are actually cited.

        Returns ``(unified_map, cited_count)``.
        The unified map is keyed by normalised URL.  Each value carries the
        original citation metadata plus ``"cited": True/False`` and a list of
        ``"cited_in"`` session indices.
        """
        # Collect all tokens referenced in chapter response text
        referenced_tokens: dict[str, list[int]] = {}  # token → [session indices]
        for entry in chapter_turns:
            idx = entry["index"]
            resp = conversations[idx].get("assistant_response") or ""
            for tok in self._TOKEN_RE.findall(resp):
                referenced_tokens.setdefault(tok, []).append(idx)

        # Merge citation maps
        unified: dict[str, dict] = {}  # normalised_url → entry
        token_to_url: dict[str, str] = {}  # cm_key → normalised_url

        for entry in chapter_turns:
            idx = entry["index"]
            cm = (conversations[idx].get("metadata") or {}).get("citation_map") or {}
            for cm_key, cm_val in cm.items():
                url = (cm_val.get("url") or "").strip()
                if not url:
                    continue
                norm = _normalise_url(url)
                if not norm:
                    continue
                token_to_url[cm_key] = norm

                if norm not in unified:
                    unified[norm] = {
                        "url": url,
                        "author": cm_val.get("author", ""),
                        "date": cm_val.get("date", ""),
                        "card_type": cm_val.get("card_type", ""),
                        "cited": False,
                        "cited_in": [],
                    }

        # Mark which entries are actually cited in response text
        cited_count = 0
        for tok, session_indices in referenced_tokens.items():
            # token might be a substring of a cm_key
            matched_urls: set[str] = set()
            for cm_key, norm_url in token_to_url.items():
                if tok in cm_key:
                    matched_urls.add(norm_url)
            for norm_url in matched_urls:
                if norm_url in unified:
                    if not unified[norm_url]["cited"]:
                        cited_count += 1
                    unified[norm_url]["cited"] = True
                    for si in session_indices:
                        if si not in unified[norm_url]["cited_in"]:
                            unified[norm_url]["cited_in"].append(si)

        return unified, cited_count


class ChapterStage(Base):
    """Phase 3 — Render sessions to chapter Markdown with per-chapter references."""

    # Regex for threat-cluster headings: **Threat Cluster N: Name**
    _TC_HEADING_RE = re.compile(
        r'^\*\*Threat\s+Cluster\s+(\d+)\s*:\s*(.+?)\*\*\s*$',
        re.MULTILINE,
    )
    # Markdown table row (pipe-delimited, at least 4 columns)
    _TABLE_ROW_RE = re.compile(r'^\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|')

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def run(self) -> Path:
        """Generate chapter Markdown files from parsed session data.

        Returns the path to the ``chapters/`` directory.
        """
        chapters_dir = self.cfg.build_dir / "chapters"
        parsed_dir = self.cfg.build_dir / "parsed"
        raw_file = self.cfg.build_dir / "raw" / "conversations.json"

        if not raw_file.exists():
            raise FileNotFoundError(
                f"No exported conversations at {raw_file}. Run --export first."
            )
        if not parsed_dir.exists():
            raise FileNotFoundError(
                f"No parsed data at {parsed_dir}. Run --parse first."
            )

        with open(raw_file, "r", encoding="utf-8") as fh:
            conversations: list[dict] = json.load(fh)

        # Load unified citations for reference cross-checking
        uc_path = parsed_dir / "unified_citations.json"
        unified_citations: dict = {}
        if uc_path.exists():
            with open(uc_path, "r", encoding="utf-8") as fh:
                unified_citations = json.load(fh)

        # Load parsed session metadata to get chapter-eligible indices
        chapter_indices = self._get_chapter_indices(parsed_dir)
        self._logger.info(
            "Loaded %d conversations, %d chapter-eligible sessions",
            len(conversations), len(chapter_indices),
        )

        # Build chapter plan (group sessions into chapters)
        chapter_plan = self._build_chapter_plan(chapter_indices, conversations)
        self._logger.info("  Chapter plan: %d chapters", len(chapter_plan))

        # Detect threat clusters across all chapter responses
        all_clusters = self._detect_threat_clusters(conversations, chapter_indices)
        if all_clusters:
            self._logger.info(
                "  Detected %d threat clusters across %d sessions",
                len(all_clusters),
                len({c["session_index"] for c in all_clusters}),
            )

        # Dry-run summary
        if self.dry_run:
            for i, ch in enumerate(chapter_plan):
                self._logger.info(
                    "  [dry-run] Chapter %d: %s (sessions %s)",
                    i, ch["title"], ch["sessions"],
                )
            self._logger.info(
                "  [dry-run] Would write %d chapter files to %s/",
                len(chapter_plan), chapters_dir.name,
            )
            if all_clusters:
                self._logger.info(
                    "  [dry-run] Would write threat_clusters.json (%d clusters)",
                    len(all_clusters),
                )
            return chapters_dir

        # Render and write chapters
        chapters_dir.mkdir(parents=True, exist_ok=True)
        manifest_entries = []

        for ch_idx, ch in enumerate(chapter_plan):
            slug = self._slugify(ch["title"])
            filename = f"{ch_idx+1:02d}-{slug}.md"
            md = self._render_chapter(
                ch_idx, ch, conversations, unified_citations,
            )
            out_path = chapters_dir / filename
            with open(out_path, "w", encoding="utf-8") as fh:
                fh.write(md)

            word_count = len(md.split())
            manifest_entries.append({
                "index": ch_idx,
                "filename": filename,
                "title": ch["title"],
                "sessions": ch["sessions"],
                "word_count": word_count,
                "has_threat_clusters": any(
                    c["session_index"] in ch["sessions"]
                    for c in all_clusters
                ),
            })
            self._logger.info(
                "  Written: %s (%d words)", filename, word_count,
            )

        # Index / table of contents
        index_md = self._render_index(chapter_plan, manifest_entries)
        with open(chapters_dir / "index.md", "w", encoding="utf-8") as fh:
            fh.write(index_md)

        # Manifest
        manifest = {
            "project": self.cfg.project.title,
            "generated": datetime.now(timezone.utc).isoformat(),
            "chapters": manifest_entries,
        }
        with open(chapters_dir / "manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2, ensure_ascii=False)

        # Threat clusters JSON
        if all_clusters:
            tc_path = parsed_dir / "threat_clusters.json"
            with open(tc_path, "w", encoding="utf-8") as fh:
                json.dump(all_clusters, fh, indent=2, ensure_ascii=False)
            self._logger.info(
                "  Written: threat_clusters.json (%d clusters)", len(all_clusters),
            )

        self._logger.info(
            "  Written: %d chapter files + index.md + manifest.json to %s/",
            len(chapter_plan), chapters_dir.name,
        )
        return chapters_dir

    # ------------------------------------------------------------------
    # chapter planning
    # ------------------------------------------------------------------

    def _get_chapter_indices(self, parsed_dir: Path) -> list[int]:
        """Return sorted list of session indices classified as chapters."""
        indices = []
        for p in sorted(parsed_dir.glob("session_*.json")):
            with open(p, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            if meta.get("classification") == "chapter":
                indices.append(meta["index"])
        return indices

    def _build_chapter_plan(
        self,
        chapter_indices: list[int],
        conversations: list[dict],
    ) -> list[dict]:
        """Build a list of chapter dicts with title and session list.

        Uses ``render.chapters`` from config if defined, otherwise creates
        one chapter per session.
        """
        chapter_specs = self.cfg.render.chapters

        if chapter_specs:
            plan = []
            for spec in chapter_specs:
                # Only include sessions that are actually chapter-eligible
                sessions = [s for s in spec.sessions if s in chapter_indices]
                if sessions:
                    plan.append({"title": spec.title, "sessions": sessions})
            return plan

        # Default: one chapter per session
        plan = []
        for idx in chapter_indices:
            conv = conversations[idx]
            query = conv.get("user_query", "")
            title = self._title_from_query(query, idx)
            plan.append({"title": title, "sessions": [idx]})
        return plan

    @staticmethod
    def _title_from_query(query: str, idx: int) -> str:
        """Derive a short chapter title from the user query."""
        q = query.strip()
        if not q:
            return f"Session {idx}"
        # Use the first sentence, truncated
        first = q.split(".")[0].split("?")[0].split("!")[0].strip()
        if len(first) > 80:
            first = first[:77] + "..."
        return first

    @staticmethod
    def _slugify(title: str) -> str:
        """Convert a title to a filesystem-safe slug."""
        slug = re.sub(r'[^a-z0-9]+', '_', title.lower()).strip('_')
        return slug[:60] if len(slug) > 60 else slug

    # ------------------------------------------------------------------
    # chapter rendering
    # ------------------------------------------------------------------

    def _render_chapter(
        self,
        ch_idx: int,
        chapter: dict,
        conversations: list[dict],
        unified_citations: dict,
    ) -> str:
        """Render a single chapter to Markdown."""
        cfg = self.cfg.render
        parts: list[str] = []

        # Chapter heading
        parts.append(f"# {chapter['title']}\n")

        # Collect all citation maps and retrieved_docs across sections for
        # the unified chapter references at the end.
        all_citation_maps: dict = {}
        all_retrieved_docs: list = []
        all_retrieved_qa: list = []

        # Abstract from first section
        first_conv = conversations[chapter["sessions"][0]]
        abstract = extract_abstract(
            first_conv.get("assistant_response", ""),
            max_words=cfg.abstract_max_words,
        )
        if abstract:
            parts.append(f"> {abstract}\n")

        # Render each section
        for sec_idx, session_id in enumerate(chapter["sessions"]):
            conv = conversations[session_id]
            query = conv.get("user_query", "")
            response = conv.get("assistant_response", "")
            citation_map = (conv.get("metadata") or {}).get("citation_map") or {}
            retrieved_docs = conv.get("retrieved_docs") or []
            retrieved_qa = conv.get("retrieved_qa") or []
            enrichment_results = conv.get("enrichment_results") or []

            # Accumulate for chapter-level references
            all_citation_maps.update(citation_map)
            all_retrieved_docs.extend(retrieved_docs)
            all_retrieved_qa.extend(retrieved_qa)

            # Section heading (only if multi-section chapter)
            if len(chapter["sessions"]) > 1:
                section_title = self._title_from_query(query, session_id)
                parts.append(f"\n## {section_title}\n")

            # Post-process and resolve inline citations
            if cfg.postprocess:
                response = postprocess_response(response)
            response = resolve_inline_citations(response, citation_map)

            parts.append(response)
            parts.append("")

            # Top sources (abbreviated — 5 per section)
            top = build_top_sources(retrieved_docs, n=5)
            if top:
                parts.append(top)
                parts.append("")

            # Enrichment section if present
            if enrichment_results:
                enr = build_enrichment_section(enrichment_results)
                if enr:
                    parts.append(enr)
                    parts.append("")

        # Chapter-level unified References
        refs = build_references(
            all_citation_maps,
            all_retrieved_docs,
            retrieved_qa=all_retrieved_qa if all_retrieved_qa else None,
        )
        if refs:
            parts.append("\n")
            parts.append(refs)

        return "\n".join(parts)

    def _render_index(
        self,
        chapter_plan: list[dict],
        manifest_entries: list[dict],
    ) -> str:
        """Render an index.md table of contents."""
        parts = [
            f"# {self.cfg.project.title}\n",
            f"**Author:** {self.cfg.project.author}  ",
            f"**Date:** {self.cfg.project.date}\n",
            "## Table of Contents\n",
        ]
        for entry in manifest_entries:
            filename = entry["filename"]
            title = entry["title"]
            wc = entry["word_count"]
            tc_flag = " 🎯" if entry.get("has_threat_clusters") else ""
            parts.append(
                f"{entry['index'] + 1}. [{title}]({filename}) "
                f"— {wc:,} words{tc_flag}"
            )
        parts.append("")
        parts.append(f"\n---\n*Generated by Rapporteur CLI — "
                     f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n")
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # 3.3 — threat cluster detection
    # ------------------------------------------------------------------

    def _detect_threat_clusters(
        self,
        conversations: list[dict],
        chapter_indices: list[int],
    ) -> list[dict]:
        """Scan chapter responses for threat cluster tables.

        Returns a list of cluster dicts with heading, description, and table rows.
        """
        clusters: list[dict] = []

        for idx in chapter_indices:
            resp = conversations[idx].get("assistant_response", "")
            citation_map = (
                (conversations[idx].get("metadata") or {}).get("citation_map") or {}
            )
            lines = resp.split("\n")

            i = 0
            while i < len(lines):
                m = self._TC_HEADING_RE.match(lines[i])
                if not m:
                    i += 1
                    continue

                cluster_num = int(m.group(1))
                cluster_name = m.group(2).strip()

                # Collect description (lines between heading and table)
                desc_lines: list[str] = []
                i += 1
                while i < len(lines) and not lines[i].startswith("|"):
                    if lines[i].strip():
                        desc_lines.append(lines[i].strip())
                    i += 1
                description = " ".join(desc_lines)

                # Parse table rows (skip header + separator)
                rows: list[dict] = []
                header_seen = False
                while i < len(lines) and lines[i].startswith("|"):
                    row_m = self._TABLE_ROW_RE.match(lines[i])
                    if row_m:
                        cells = [c.strip() for c in row_m.groups()]
                        # Skip header row and separator
                        if cells[0] in ("Author", ":---"):
                            header_seen = True
                            i += 1
                            continue
                        if not header_seen:
                            # First row might be header
                            i += 1
                            continue

                        # Extract URL from citation token in Title column
                        title_cell = cells[3] if len(cells) > 3 else ""
                        url = self._resolve_url_from_title(
                            title_cell, citation_map,
                        )

                        rows.append({
                            "author": cells[0],
                            "date": cells[1],
                            "publication": cells[2],
                            "title": re.sub(
                                r'\s*\([^)]*\)\s*$', '', title_cell,
                            ).strip(),
                            "url": url,
                        })
                    i += 1

                clusters.append({
                    "session_index": idx,
                    "cluster_number": cluster_num,
                    "cluster_name": cluster_name,
                    "description": description,
                    "rows": rows,
                    "n_rows": len(rows),
                    "n_resolved_urls": sum(1 for r in rows if r["url"]),
                })

        return clusters

    def _resolve_url_from_title(
        self, title_cell: str, citation_map: dict,
    ) -> str:
        """Extract a URL from a citation token embedded in the title cell."""
        # Look for (req_..., summary) or (doc_..., qa-N) at end of cell
        tok_match = re.search(
            r'\((req_\d{8}_\d{6}_\d{2,4}|doc_[a-f0-9]{4,12})[^)]*\)',
            title_cell,
        )
        if not tok_match:
            return ""
        token = tok_match.group(1)
        # Search citation_map keys for this token
        for cm_key, cm_val in citation_map.items():
            if token in cm_key:
                return (cm_val.get("url") or "").strip()
        return ""


class KaliyaStage(Base):
    """Phase 4 — Match threat clusters to cached Kaliya analyses,
    convert/copy images, and inject narrative sections into chapters."""

    # Max image width for PDF output — larger images are downscaled.
    _MAX_WIDTH = 1200
    _JPEG_QUALITY = 85

    # Regex for [Figure N] inline references in Kaliya narratives.
    _FIGURE_REF_RE = re.compile(r'\[Figure\s+(\d+)\]')

    # Regex for (req_YYYYMMDD_HHMMSS_NNN, summary) inline citations.
    _KALIYA_CITE_RE = re.compile(
        r'\((req_\d{8}_\d{6}_\d{3}),\s*summary\)',
    )

    # Cluster table end: next bold heading or horizontal rule or section heading.
    _CLUSTER_TABLE_END_RE = re.compile(
        r'^\*\*(?:Threat Cluster \d|[IVX]+\.)|^---\s*$|^#{1,3}\s',
    )

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the Kaliya stage: match, convert images, inject sections."""
        parsed_dir = self.cfg.build_dir / "parsed"
        chapters_dir = self.cfg.build_dir / "chapters"
        images_dir = self.cfg.build_dir / "kaliya" / "images"
        kaliya_cache = Path(self.cfg.paths.kaliya_cache)

        tc_path = parsed_dir / "threat_clusters.json"
        if not tc_path.exists():
            self._logger.warning(
                "No threat_clusters.json found — nothing for Kaliya to do.",
            )
            return

        with open(tc_path, "r", encoding="utf-8") as fh:
            clusters: list[dict] = json.load(fh)
        self._logger.info("Loaded %d threat clusters", len(clusters))

        if not kaliya_cache.is_dir():
            self._logger.error(
                "Kaliya cache not found at %s — cannot match clusters.",
                kaliya_cache,
            )
            return

        # --- 4.1  Match clusters → cached analyses ---
        cache_index = self._index_cache(kaliya_cache)
        matches = self._match_clusters(clusters, cache_index)
        if not matches:
            self._logger.warning("No cached Kaliya analyses matched any cluster.")
            return

        # Log matches
        for key, req_id in matches.items():
            self._logger.info("  Matched %s → %s", key, req_id)

        # --- 4.4  Image conversion & copy ---
        unique_analyses = set(matches.values())
        figure_map = self._process_images(
            unique_analyses, kaliya_cache, images_dir,
        )

        # --- 4.5  Build and inject Kaliya sections ---
        self._inject_sections(
            matches, clusters, kaliya_cache,
            chapters_dir, images_dir, figure_map,
        )

        # --- Update manifest ---
        self._update_manifest(chapters_dir, matches, figure_map)

    # ------------------------------------------------------------------
    # 4.1 — Cache indexing & cluster matching
    # ------------------------------------------------------------------

    def _index_cache(self, cache_dir: Path) -> dict[str, set[str]]:
        """Build {request_id: set_of_normalised_urls} from cached analyses."""
        index: dict[str, set[str]] = {}
        for entry in sorted(cache_dir.iterdir()):
            report_path = entry / "report.json"
            if not report_path.exists():
                continue
            try:
                with open(report_path, "r", encoding="utf-8") as fh:
                    report = json.load(fh)
                urls = {
                    _normalise_url(u)
                    for u in report.get("source_urls", [])
                    if u
                }
                if urls:
                    index[entry.name] = urls
            except (json.JSONDecodeError, OSError) as exc:
                self._logger.warning("Skipping %s: %s", entry.name, exc)
        self._logger.info("  Indexed %d cached Kaliya analyses", len(index))
        return index

    def _match_clusters(
        self,
        clusters: list[dict],
        cache_index: dict[str, set[str]],
    ) -> dict[str, str]:
        """Match clusters to cached analyses by URL overlap.

        Returns {cluster_key: request_id} where cluster_key is
        "tc{cluster_number}_{cluster_name_slug}".  Deduplicates across
        sessions (s7 and s8 share the same 3 cluster names).
        """
        matches: dict[str, str] = {}

        for cluster in clusters:
            cluster_key = self._cluster_key(cluster)
            if cluster_key in matches:
                continue  # Already matched from another session

            cluster_urls = {
                _normalise_url(r.get("url", ""))
                for r in cluster.get("rows", [])
                if r.get("url")
            }
            if not cluster_urls:
                self._logger.warning(
                    "  Cluster %s has no URLs — skipping.", cluster_key,
                )
                continue

            # Find best overlap
            best_id: str | None = None
            best_overlap = 0
            for req_id, cached_urls in cache_index.items():
                overlap = len(cluster_urls & cached_urls)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_id = req_id

            if best_id and best_overlap > 0:
                matches[cluster_key] = best_id
            else:
                self._logger.warning(
                    "  No cached match for cluster %s (%d URLs)",
                    cluster_key, len(cluster_urls),
                )

        return matches

    @staticmethod
    def _cluster_key(cluster: dict) -> str:
        slug = re.sub(
            r'[^a-z0-9]+', '_',
            cluster.get("cluster_name", "").lower(),
        ).strip('_')
        return f"tc{cluster['cluster_number']}_{slug}"

    # ------------------------------------------------------------------
    # 4.4 — Image conversion & copy
    # ------------------------------------------------------------------

    def _process_images(
        self,
        request_ids: set[str],
        cache_dir: Path,
        images_dir: Path,
    ) -> dict[str, dict]:
        """Convert and copy images for all matched analyses.

        Returns {request_id: {image_id: {path, label, caption}}} mapping
        for use in section rendering.
        """
        figure_map: dict[str, dict] = {}
        total_processed = 0
        total_skipped = 0

        for req_id in sorted(request_ids):
            figs_path = cache_dir / req_id / "figures.json"
            if not figs_path.exists():
                self._logger.warning("No figures.json for %s", req_id)
                figure_map[req_id] = {}
                continue

            with open(figs_path, "r", encoding="utf-8") as fh:
                figures: list[dict] = json.load(fh)

            req_figures: dict[str, dict] = {}
            for fig in figures:
                image_id = fig.get("image_id", "")
                local_path = fig.get("local_path", "")
                label = fig.get("label", "")
                caption = fig.get("caption", "")

                if not local_path or not Path(local_path).exists():
                    self._logger.warning(
                        "  Image missing: %s (%s) — skipping",
                        image_id, local_path,
                    )
                    continue

                out_name = f"{req_id}_{image_id}.jpeg"
                out_path = images_dir / out_name
                rel_path = f"kaliya/images/{out_name}"

                req_figures[image_id] = {
                    "path": rel_path,
                    "label": label,
                    "caption": caption,
                }

                if self.dry_run:
                    total_processed += 1
                    continue

                if out_path.exists():
                    total_skipped += 1
                    continue

                try:
                    self._convert_image(local_path, out_path)
                    total_processed += 1
                except Exception as exc:
                    self._logger.warning(
                        "  Failed to convert %s: %s — skipping",
                        image_id, exc,
                    )

            figure_map[req_id] = req_figures

        if self.dry_run:
            self._logger.info(
                "  [dry-run] Would process %d images to %s/",
                total_processed, images_dir,
            )
        else:
            self._logger.info(
                "  Images: %d processed, %d already present",
                total_processed, total_skipped,
            )
        return figure_map

    def _convert_image(self, src_path: str, dst_path: Path) -> None:
        """Open an image, resize if needed, save as JPEG."""
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        with Image.open(src_path) as img:
            # Ensure RGB for JPEG output
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")

            # Downscale if too wide
            if img.width > self._MAX_WIDTH:
                ratio = self._MAX_WIDTH / img.width
                new_size = (self._MAX_WIDTH, int(img.height * ratio))
                img = img.resize(new_size, Image.LANCZOS)

            img.save(dst_path, format="JPEG", quality=self._JPEG_QUALITY)

    # ------------------------------------------------------------------
    # 4.5 — Section rendering & chapter injection
    # ------------------------------------------------------------------

    def _inject_sections(
        self,
        matches: dict[str, str],
        clusters: list[dict],
        cache_dir: Path,
        chapters_dir: Path,
        images_dir: Path,
        figure_map: dict[str, dict],
    ) -> None:
        """Render Kaliya narrative sections and inject into chapter files."""
        # Group clusters by session_index
        session_clusters: dict[int, list[dict]] = {}
        for cluster in clusters:
            sid = cluster["session_index"]
            session_clusters.setdefault(sid, []).append(cluster)

        # Build rendered sections per cluster_key (render once, reuse)
        rendered_sections: dict[str, str] = {}
        for cluster in clusters:
            cluster_key = self._cluster_key(cluster)
            if cluster_key in rendered_sections:
                continue

            req_id = matches.get(cluster_key)
            if not req_id:
                continue

            section_md = self._render_kaliya_section(
                cluster, req_id, cache_dir, figure_map.get(req_id, {}),
            )
            rendered_sections[cluster_key] = section_md

        if not rendered_sections:
            self._logger.warning("No Kaliya sections rendered.")
            return

        self._logger.info(
            "  Rendered %d unique Kaliya sections", len(rendered_sections),
        )

        # Find chapter files that contain threat clusters and inject
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.error("No manifest.json in chapters/ — cannot inject.")
            return

        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        chapters_modified = 0
        for entry in manifest.get("chapters", []):
            if not entry.get("has_threat_clusters"):
                continue

            chapter_file = chapters_dir / entry["filename"]
            if not chapter_file.exists():
                continue

            # Determine which session IDs are in this chapter
            session_ids = entry.get("sessions", [])
            # Collect cluster_keys for these sessions
            chapter_cluster_keys: list[str] = []
            for cluster in clusters:
                if cluster["session_index"] in session_ids:
                    ck = self._cluster_key(cluster)
                    if ck not in chapter_cluster_keys:
                        chapter_cluster_keys.append(ck)

            if not any(ck in rendered_sections for ck in chapter_cluster_keys):
                continue

            with open(chapter_file, "r", encoding="utf-8") as fh:
                content = fh.read()

            new_content = self._inject_into_chapter(
                content, chapter_cluster_keys, rendered_sections, clusters,
            )

            if new_content != content:
                if self.dry_run:
                    self._logger.info(
                        "  [dry-run] Would inject Kaliya sections into %s",
                        entry["filename"],
                    )
                else:
                    with open(chapter_file, "w", encoding="utf-8") as fh:
                        fh.write(new_content)
                    self._logger.info(
                        "  Injected Kaliya sections into %s",
                        entry["filename"],
                    )
                chapters_modified += 1

        self._logger.info(
            "  %s %d chapter files with Kaliya sections",
            "[dry-run] Would modify" if self.dry_run else "Modified",
            chapters_modified,
        )

    def _render_kaliya_section(
        self,
        cluster: dict,
        req_id: str,
        cache_dir: Path,
        req_figures: dict[str, dict],
    ) -> str:
        """Render a single Kaliya analysis section as Markdown."""
        report_path = cache_dir / req_id / "report.json"
        narrative_path = cache_dir / req_id / "narrative.md"

        # Load report metadata
        report: dict = {}
        if report_path.exists():
            with open(report_path, "r", encoding="utf-8") as fh:
                report = json.load(fh)

        # Load narrative
        narrative = ""
        if narrative_path.exists():
            with open(narrative_path, "r", encoding="utf-8") as fh:
                narrative = fh.read().strip()

        cluster_name = cluster.get("cluster_name", "Unknown")
        confidence = report.get("confidence", "UNKNOWN")
        agenda = report.get("agenda_detected", False)
        surface = report.get("surface_evidence_count", 0)
        deep = report.get("deep_evidence_count", 0)
        publishers = report.get("publishers_analysed", [])

        # Build section
        parts: list[str] = []
        parts.append(f"\n#### Kaliya Agenda Analysis: {cluster_name}\n")

        # Metadata block
        agenda_str = "**Detected**" if agenda else "Not detected"
        parts.append(
            f"> **Confidence:** {confidence} | "
            f"**Agenda:** {agenda_str} | "
            f"**Evidence:** {surface} surface, {deep} deep | "
            f"**Publishers:** {', '.join(publishers)}\n",
        )

        # Resolve inline citations in narrative
        source_urls = report.get("source_urls", [])
        narrative = self._resolve_kaliya_citations(narrative, source_urls)

        # Build figure lookup: figure_number → figure info
        fig_lookup: dict[int, dict] = {}
        for info in req_figures.values():
            label = info.get("label", "")
            m = re.match(r'Figure\s+(\d+)', label)
            if m:
                fig_lookup[int(m.group(1))] = info

        # Replace [Figure N] inline references
        referenced_nums: set[int] = set()

        def _replace_fig(m: re.Match) -> str:
            num = int(m.group(1))
            fig = fig_lookup.get(num)
            if not fig:
                return m.group(0)  # Keep original if not found
            referenced_nums.add(num)
            caption = fig["caption"]
            path = fig["path"]
            return f'\n\n![{caption}]({path})\n*{fig["label"]}*\n'

        narrative = self._FIGURE_REF_RE.sub(_replace_fig, narrative)
        parts.append(narrative)
        parts.append("")

        # Figure plate for unreferenced figures
        unreferenced = [
            info for info in req_figures.values()
            if info.get("label")
            and re.match(r'Figure\s+(\d+)', info["label"])
            and int(re.match(r'Figure\s+(\d+)', info["label"]).group(1))
               not in referenced_nums
        ]
        if unreferenced:
            parts.append(self._render_figure_plate(unreferenced))

        # If no figures were referenced inline and some exist, render all as a plate
        if not referenced_nums and req_figures:
            all_figs = list(req_figures.values())
            parts.append(self._render_figure_plate(all_figs))

        return "\n".join(parts)

    def _resolve_kaliya_citations(
        self, narrative: str, source_urls: list[str],
    ) -> str:
        """Replace (req_YYYYMMDD_HHMMSS_NNN, summary) with markdown links."""
        # Build lookup: req_id prefix → URL
        url_lookup: dict[str, str] = {}
        for url in source_urls:
            # Extract request-like prefix from URL (if it maps)
            # The req IDs in narratives reference the grounding requests
            url_lookup[url] = url

        def _replace_cite(m: re.Match) -> str:
            req_token = m.group(1)
            # Try matching against source URLs indirectly: find any URL
            # that was part of the grounding for this req_token.
            # For now, replace with a generic source reference since
            # the req→URL mapping is in grounding.jsonl
            return f"(source)"

        return self._KALIYA_CITE_RE.sub(_replace_cite, narrative)

    def _render_figure_plate(self, figures: list[dict]) -> str:
        """Render a grid of figures as Markdown."""
        n = len(figures)
        if n == 0:
            return ""

        parts = ["\n**Figures:**\n"]

        # Determine grid columns
        if n <= 2:
            cols = 1  # Full width
        elif n <= 6:
            cols = 2
        else:
            cols = 3

        # Render as a simple image list (Markdown — grid via CSS/LaTeX later)
        for fig in figures:
            caption = fig.get("caption", "")
            # Truncate caption for grid display
            if len(caption) > 80:
                caption = caption[:77] + "..."
            parts.append(
                f'![{caption}]({fig["path"]})\n'
                f'*{fig.get("label", "")}*\n',
            )

        return "\n".join(parts)

    def _inject_into_chapter(
        self,
        content: str,
        cluster_keys: list[str],
        rendered_sections: dict[str, str],
        clusters: list[dict],
    ) -> str:
        """Inject rendered Kaliya sections after each threat cluster table.

        Previously injected sections (marked by ``#### Kaliya Agenda Analysis:``)
        are stripped first so re-runs are idempotent.
        """
        # Strip any previously injected Kaliya sections
        content = self._strip_kaliya_sections(content)

        lines = content.split("\n")
        result_lines: list[str] = []
        i = 0

        while i < len(lines):
            result_lines.append(lines[i])

            # Check if this line starts a threat cluster heading
            m = re.match(
                r'^\*\*Threat Cluster\s+(\d+):\s+(.+?)\*\*',
                lines[i],
            )
            if m:
                cluster_num = int(m.group(1))
                cluster_name = m.group(2).strip()

                # Find the matching cluster_key
                slug = re.sub(
                    r'[^a-z0-9]+', '_', cluster_name.lower(),
                ).strip('_')
                cluster_key = f"tc{cluster_num}_{slug}"

                if cluster_key in rendered_sections:
                    # Advance past the description + table
                    i += 1
                    while i < len(lines):
                        result_lines.append(lines[i])
                        # Check if next line starts a new section
                        if (i + 1 < len(lines)
                            and self._CLUSTER_TABLE_END_RE.match(lines[i + 1])):
                            break
                        # End of table: non-table line after table lines
                        if (i + 1 < len(lines)
                            and not lines[i + 1].startswith("|")
                            and lines[i].startswith("|")):
                            break
                        i += 1

                    # Inject the Kaliya section
                    result_lines.append("")
                    result_lines.append(rendered_sections[cluster_key])
                    result_lines.append("")

            i += 1

        return "\n".join(result_lines)

    @staticmethod
    def _strip_kaliya_sections(content: str) -> str:
        """Remove previously injected Kaliya sections for idempotent re-runs.

        Kaliya sections start with ``#### Kaliya Agenda Analysis:`` and end
        before the next ``**Threat Cluster``, ``---``, or ``##``/``###`` heading.
        """
        lines = content.split("\n")
        result: list[str] = []
        i = 0
        while i < len(lines):
            if lines[i].startswith("#### Kaliya Agenda Analysis:"):
                # Skip until next structural element
                i += 1
                while i < len(lines):
                    line = lines[i]
                    if (line.startswith("**Threat Cluster ")
                            or line.startswith("---")
                            or re.match(r'^#{1,3}\s', line)):
                        break
                    i += 1
                # Remove trailing blank lines left by the removed section
                while result and result[-1].strip() == "":
                    result.pop()
            else:
                result.append(lines[i])
                i += 1
        return "\n".join(result)

    # ------------------------------------------------------------------
    # Manifest update
    # ------------------------------------------------------------------

    def _update_manifest(
        self,
        chapters_dir: Path,
        matches: dict[str, str],
        figure_map: dict[str, dict],
    ) -> None:
        """Update manifest.json with Kaliya metadata."""
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            return

        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        total_figs = sum(len(figs) for figs in figure_map.values())
        manifest["kaliya"] = {
            "matched_analyses": len(matches),
            "matched_clusters": list(matches.keys()),
            "analysis_ids": list(set(matches.values())),
            "total_figures": total_figs,
            "injected": not self.dry_run,
        }

        if not self.dry_run:
            with open(manifest_path, "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, indent=2, ensure_ascii=False)
            self._logger.info("  Updated manifest.json with Kaliya metadata")


class PublishStage(Base):
    """Phase 5 — Build PDF / HTML / Confluence."""

    # Regex to strip the trailing footer line from each chapter
    _FOOTER_RE = re.compile(
        r'\n---\n\*Generated by Rapporteur CLI[^*]*\*\s*$'
    )

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        build_dir = Path(self.cfg.paths.build_dir)
        chapters_dir = build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"

        if not manifest_path.exists():
            self._logger.error(
                "  manifest.json not found — run --chapters first"
            )
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.error("  No chapters in manifest.json")
            return

        formats = self.cfg.publish.formats
        self._logger.info("  Formats: %s", ", ".join(formats))
        self._logger.info("  Chapters: %d", len(chapter_entries))

        for fmt in formats:
            if fmt == "pdf":
                self._build_pdf(build_dir, chapters_dir, chapter_entries)
            else:
                self._logger.warning("  Format %r not yet supported", fmt)

    # ------------------------------------------------------------------
    # PDF builder
    # ------------------------------------------------------------------

    def _build_pdf(
        self,
        build_dir: Path,
        chapters_dir: Path,
        chapter_entries: list[dict],
    ) -> None:
        """Assemble chapter markdowns and invoke pandoc → pdflatex."""
        slug = re.sub(r'[^a-z0-9]+', '_', self.cfg.project.title.lower()).strip('_')
        output_pdf = build_dir / f"{slug}.pdf"
        combined_md = build_dir / f"{slug}_combined.md"

        # -- 1. Build YAML front matter ------------------------------------
        front_matter = self._build_front_matter()

        # -- 2. Concatenate chapters ---------------------------------------
        parts: list[str] = [front_matter]
        loaded = 0
        for entry in chapter_entries:
            chapter_file = chapters_dir / entry["filename"]
            if not chapter_file.exists():
                self._logger.warning(
                    "  Chapter file missing: %s", entry["filename"]
                )
                continue
            text = chapter_file.read_text(encoding="utf-8")
            # Strip the "Generated by Rapporteur CLI" footer to avoid
            # pandoc misinterpreting --- as YAML delimiters on concat.
            text = self._FOOTER_RE.sub('', text).rstrip()
            if loaded > 0:
                parts.append('\n\n\\newpage\n\n')
            parts.append(text)
            loaded += 1

        if loaded == 0:
            self._logger.error("  No chapter files found")
            return

        body = '\n'.join(parts) + '\n'
        word_count = len(body.split())
        self._logger.info(
            "  Combined %d chapters (%d words)", loaded, word_count
        )

        if self.dry_run:
            self._logger.info(
                "  [dry-run] Would write %s and invoke pandoc → %s",
                combined_md.name, output_pdf.name,
            )
            return

        # -- 3. Write combined markdown ------------------------------------
        combined_md.write_text(body, encoding="utf-8")

        # -- 4. Invoke pandoc ----------------------------------------------
        cmd = self._pandoc_cmd(build_dir, combined_md, output_pdf)
        self._logger.info("  Running: %s", ' '.join(str(c) for c in cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(build_dir),
            timeout=300,
        )

        if result.returncode != 0:
            self._logger.error("  pandoc failed (exit %d)", result.returncode)
            stderr = result.stderr.strip()
            if stderr:
                for line in stderr.splitlines()[-10:]:
                    self._logger.error("    %s", line)
            return

        if result.stderr:
            for line in result.stderr.strip().splitlines():
                self._logger.debug("  pandoc: %s", line)

        # -- 5. Report -----------------------------------------------------
        if output_pdf.exists():
            size = _human_size(output_pdf)
            pages = self._pdf_page_count(output_pdf)
            page_info = f", {pages} pages" if pages else ""
            self._logger.info("  PDF written: %s (%s%s)", output_pdf.name, size, page_info)
        else:
            self._logger.error("  pandoc completed but PDF not found")
            return

        # -- 6. Update manifest with publish metadata ----------------------
        manifest_path = chapters_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["publish"] = {
            "pdf": str(output_pdf),
            "pdf_size": output_pdf.stat().st_size,
            "pdf_pages": self._pdf_page_count(output_pdf),
            "word_count": word_count,
            "generated": datetime.now(tz=timezone.utc).isoformat(),
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + '\n',
            encoding="utf-8",
        )
        self._logger.info("  Updated manifest.json with publish metadata")

    def _build_front_matter(self) -> str:
        """YAML metadata block for pandoc with LaTeX styling matching Sphinx documents."""
        # Extract release version if present in title
        release = "1.0"

        lines = [
            '---',
            f'title: "{self.cfg.project.title}"',
            f'author: "{self.cfg.project.author}"',
            f'date: "{self.cfg.project.date}"',
            f'release: "{release}"',
            'documentclass: report',
            'papersize: a4paper',
            'fontsize: 11pt',
            'toc: true',
            'toc-depth: 2',
            'number-sections: true',
            'colorlinks: true',
            'linkcolor: black',
            'urlcolor: black',
            'include-before:',
            '  - \\newcommand{\\release}{' + release + '}',
            'header-includes:',
            '  - \\usepackage[top=2.5cm,bottom=2.5cm,left=2.5cm,right=2.5cm]{geometry}',
            '  - \\usepackage{tgtermes}',
            '  - \\usepackage{tgheros}',
            '  - \\renewcommand{\\ttdefault}{txtt}',
            '  - \\usepackage{graphicx}',
            '  - \\setkeys{Gin}{width=\\linewidth,height=0.85\\textheight,keepaspectratio=true}',
            '  - \\usepackage{float}',
            '  - \\usepackage[breaklinks=true]{hyperref}',
            '  - \\usepackage{longtable,booktabs}',
            '  - \\usepackage{caption}',
            '  - \\captionsetup{font=small,labelfont=bf,skip=4pt}',
            '  - \\usepackage[Bjarne]{fncychap}',
            '---',
            '',
        ]
        return '\n'.join(lines)

    def _pandoc_cmd(
        self, build_dir: Path, input_md: Path, output_pdf: Path
    ) -> list[str]:
        """Build the pandoc command list."""
        return [
            'pandoc',
            str(input_md),
            '-o', str(output_pdf),
            f'--pdf-engine={self.cfg.publish.pdf_engine}',
            f'--resource-path={build_dir}',
            '--standalone',
        ]

    @staticmethod
    def _pdf_page_count(pdf_path: Path) -> int | None:
        """Extract page count via pdfinfo (returns None if unavailable)."""
        try:
            result = subprocess.run(
                ['pdfinfo', str(pdf_path)],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.splitlines():
                if line.startswith('Pages:'):
                    return int(line.split(':', 1)[1].strip())
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            pass
        return None


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

STAGES = [
    ("export",   ExportStage),
    ("parse",    ParseStage),
    ("chapters", ChapterStage),
    ("kaliya",   KaliyaStage),
    ("publish",  PublishStage),
]


def run_pipeline(cfg: PipelineConfig, selected: list[str], dry_run: bool) -> None:
    """Run the selected stages in sequence."""
    log.info("=" * 70)
    log.info("Rapporteur CLI — %s", cfg.project.title)
    log.info("  Build dir: %s", cfg.paths.build_dir)
    log.info("  Dry run:   %s", dry_run)
    log.info("=" * 70)

    for stage_name, stage_cls in STAGES:
        if stage_name in selected:
            log.info("")
            log.info("── Stage: %s ──", stage_name.upper())
            stage = stage_cls(cfg, dry_run=dry_run)
            stage.run()

    log.info("")
    log.info("Pipeline complete.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _human_size(path: Path) -> str:
    try:
        size = path.stat().st_size
    except OSError:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _normalise_url(url: str) -> str:
    """Stable key from a URL — strips scheme, fragment, query, trailing slash."""
    if not url:
        return ""
    normalised = re.sub(r'^https?://', '', url.strip().lower())
    normalised = normalised.split('#')[0]
    normalised = normalised.split('?')[0]
    return normalised.rstrip('/')


def _first_n_sentences(text: str, n: int = 2) -> str:
    """Return the first *n* sentences of *text* (simple heuristic)."""
    # Strip markdown headings and leading whitespace
    cleaned = re.sub(r'^#{1,4}\s+.*$', '', text, flags=re.MULTILINE).strip()
    # Split on sentence boundaries (. ! ? followed by space or end)
    parts = re.split(r'(?<=[.!?])\s+', cleaned)
    sentences = [s.strip() for s in parts if s.strip()][:n]
    return " ".join(sentences)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rapporteur_cli",
        description="Unified Document Publishing Pipeline — "
                    "chat sessions to published documents in one command.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s config.yaml --export              Export conversations\n"
            "  %(prog)s config.yaml --export --parse       Export + parse\n"
            "  %(prog)s config.yaml --all                  Full pipeline\n"
            "  %(prog)s config.yaml --export --dry-run     Preview without writes\n"
        ),
    )
    parser.add_argument(
        "config",
        help="Path to document.yaml configuration file.",
    )

    stages = parser.add_argument_group("stages", "Select which pipeline stages to run")
    stages.add_argument("--export",   action="store_true", help="Stage 1: Export conversations from frontend API")
    stages.add_argument("--parse",    action="store_true", help="Stage 2: Parse and classify turns")
    stages.add_argument("--chapters", action="store_true", help="Stage 3: Generate chapter Markdown")
    stages.add_argument("--kaliya",   action="store_true", help="Stage 4: Run Kaliya threat-cluster analysis")
    stages.add_argument("--publish",  action="store_true", help="Stage 5: Build PDF / HTML / Confluence output")
    stages.add_argument("--all",      action="store_true", help="Run all stages (1-5)")

    opts = parser.add_argument_group("options")
    opts.add_argument("--dry-run",      action="store_true", help="Preview actions without writing files")
    opts.add_argument("--build-dir",    default=None,        help="Override build directory from config")
    opts.add_argument("--frontend-url", default=None,        help="Override frontend URL from config")
    opts.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load config
    cfg = PipelineConfig.from_yaml(args.config)

    # Apply CLI overrides
    if args.build_dir:
        cfg.paths.build_dir = str(Path(args.build_dir).resolve())
    if args.frontend_url:
        cfg.export.frontend_url = args.frontend_url

    # Determine which stages to run
    stage_names = [name for name, _ in STAGES]
    if args.all:
        selected = list(stage_names)
    else:
        selected = [s for s in stage_names if getattr(args, s, False)]

    if not selected:
        parser.error("No stages selected. Use --export, --parse, --chapters, --kaliya, --publish, or --all.")

    run_pipeline(cfg, selected, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
