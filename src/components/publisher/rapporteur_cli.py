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
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

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
            filename = f"{ch_idx:02d}_{slug}.md"
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

            # Section heading
            section_title = self._title_from_query(query, session_id)
            parts.append(f"---\n\n## {section_title}\n")

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
            parts.append("---\n")
            parts.append(refs)

        # Metadata footer
        parts.append(f"\n---\n*Generated by Rapporteur CLI — "
                     f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n")

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
    """Phase 4 — Detect threat clusters, send to Kaliya, collect results."""

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        self._logger.warning("KaliyaStage not yet implemented (Phase 4)")


class PublishStage(Base):
    """Phase 5 — Build PDF / HTML / Confluence."""

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        self._logger.warning("PublishStage not yet implemented (Phase 5)")


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
