#!/usr/bin/env python3
"""
rapporteur_cli.py — Unified Document Publishing Pipeline

Orchestrates the full chat-to-published-document pipeline:
  Stage 1  (export)   — Pull conversations from the frontend API or static file
  Stage 2  (parse)    — Classify turns, generate footnotes, build citation map
  Stage 3  (chapters) — Render session Markdown with per-chapter references
  Stage 4  (kaliya)   — Detect threat clusters, send to Kaliya, collect results
  Stage 5  (format)   — Apply LLM-based style formatting to chapter Markdown
  Stage 5b (conclude) — Generate conclusion chapter via two-call LLM synthesis
  Stage 6  (publish)  — Build PDF / HTML / Confluence from chapter markdowns

Usage
-----
    # Export conversations from the live frontend:
    python /home/electric/workspace/RapporteurApp/src/components/publisher/rapporteur_cli.py docs/CultureTechnologyProcurementMedia/document.yaml --export

    # Export + parse + build chapters:
    python docs/rapporteur_cli.py docs/CultureTechnologyProcurementMedia/document.yaml --export --parse --chapters

    # Run the full pipeline:
    python docs/rapporteur_cli.py docs/CultureTechnologyProcurementMedia/document.yaml --all

    # Dry-run (no writes):
    python docs/rapporteur_cli.py docs/CultureTechnologyProcurementMedia/document.yaml --export --dry-run

    # Override build directory:
    python docs/rapporteur_cli.py docs/CultureTechnologyProcurementMedia/document.yaml --all --build-dir /tmp/test_build
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

# Ensure project root is on sys.path so `src.*` imports work when invoked directly.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import requests
from PIL import Image

from src.components.analysis.eo_linking_stage import EOLinkStage
from src.components.analysis.media_linking_stage import MediaLinkStage
from src.components.publisher.analytics_conclude import AnalyticsConcludeStage
from src.components.publisher.fuse_analysis import FuseAnalysisStage
# Re-use rendering helpers from the original document builder.
from src.components.publisher.rapporteur_docs import (_CITE_GROUP_RE,
                                                      _cite_token_to_cm_key,
                                                      build_enrichment_section,
                                                      build_references,
                                                      build_top_sources,
                                                      extract_abstract,
                                                      postprocess_response,
                                                      resolve_inline_citations)
from src.config_handler import Base
from src.models.publisher import (AnalyticsConcludeConfig, ChapterSpec,
                                  CiteConfig, ConcludeConfig, FormatConfig,
                                  FuseAnalysisConfig, IntroduceConfig,
                                  PipelineConfig, SectionDef)

# ---------------------------------------------------------------------------
# Logging (fallback for CLI entry-point; stages use Base._logger)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rapporteur")


def _gemini_location(model: str) -> str | None:
    """Return the GCP location override for models that require it."""
    if "3.1" in model or "preview" in model:
        return "global"
    return None


# ---------------------------------------------------------------------------
# LLM response cache helpers  (sync, uses centralized Redis config)
# ---------------------------------------------------------------------------

import hashlib as _hashlib


def _llm_cache_key(model: str, prompt: str) -> str:
    """Deterministic cache key: first 32 hex chars of sha256(model|prompt)."""
    return _hashlib.sha256(f"{model}|{prompt}".encode()).hexdigest()[:32]


def _get_redis_sync():
    """Return a **sync** Redis client using centralized config, or None."""
    try:
        from src.config import app_conf
        host = app_conf.get("redis.host") or ""
        if not host:
            return None
        import redis as _redis
        return _redis.Redis(
            host=host,
            port=int(app_conf.get("redis.port") or 6379),
            password=app_conf.get("redis.password") or None,
            decode_responses=False,
        )
    except Exception:
        return None


def _llm_cache_get(model: str, prompt: str) -> str | None:
    """Check Redis for a cached LLM response. Returns text or None."""
    r = _get_redis_sync()
    if r is None:
        return None
    try:
        val = r.get(_llm_cache_key(model, prompt))
        if val is not None:
            log.debug("LLM cache HIT  key=%s…", _llm_cache_key(model, prompt)[:12])
            return val.decode("utf-8") if isinstance(val, bytes) else val
    except Exception:
        log.debug("Redis GET failed; falling through to LLM")
    return None


def _llm_cache_set(model: str, prompt: str, result: str) -> None:
    """Store an LLM response in Redis with the configured TTL."""
    r = _get_redis_sync()
    if r is None:
        return
    try:
        from src.config import app_conf
        ttl = int(app_conf.get("redis.llm_cache_ttl_seconds") or 432000)
        r.setex(_llm_cache_key(model, prompt), ttl, result)
    except Exception:
        log.debug("Redis SETEX failed; result not cached")


# ---------------------------------------------------------------------------
# Phase 5.5 — Full EO index grounding (shared helper)
# ---------------------------------------------------------------------------

def _load_all_eo_urls(project_root: Path | None = None) -> list[str]:
    """Read ALL unique source URLs from the EO summaries index JSONL.

    Phase 5.5: Extract every URL so we can reconstitute ALL EOs via
    ``/query_metadata`` — no filtering by what chapters cite.
    """
    if project_root is None:
        project_root = Path(_PROJECT_ROOT)

    index_path = project_root / "x_indices_eo" / "research_summaries_with_embeddings_combined.jsonl"
    if not index_path.exists():
        log.warning("EO index not found at %s", index_path)
        return []

    urls: set[str] = set()
    try:
        with open(index_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                url = record.get("source_url", "").strip()
                if url:
                    urls.add(url)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Failed to read EO index URLs: %s", exc)
        return []

    return sorted(urls)


def _fetch_full_eo_grounding(rag_url: str, project_root: Path | None = None) -> str:
    """Reconstitute ALL EO articles via ``/query_metadata`` and return grounding text.

    Phase 5.5: Always inject the FULL reconstituted EO index into grounding.
    This avoids false negatives from URL-filtered grounding — every EO's
    summary + QA context is included so the LLM can correctly identify
    relationships, rescissions, and precedence.

    Steps:
      1. Read all source URLs from the EO summaries JSONL.
      2. Switch RAG service to ``x_indices_eo`` if needed.
      3. Call ``/query_metadata`` per URL to get summary + QA pairs.
      4. Format into grounding text.

    Returns formatted grounding text, or empty string on failure.
    """
    all_urls = _load_all_eo_urls(project_root)
    if not all_urls:
        return ""

    if not rag_url:
        log.warning("No RAG service URL — cannot reconstitute EO articles")
        return ""

    import requests as _requests

    # Ensure the EO index is active
    try:
        ready = _requests.get(f"{rag_url}/ready", timeout=5)
        active = ready.json().get("active_folder", "")
        if "eo" not in active.lower():
            log.info("RAG not on EO index (%s), switching to x_indices_eo...", active)
            resp = _requests.post(
                f"{rag_url}/switch-index",
                json={"folder": "x_indices_eo"},
                timeout=30,
            )
            if resp.status_code != 200:
                log.warning("Could not switch to EO index: %s", resp.status_code)
                return ""
            log.info("Switched to EO index")
    except Exception as exc:
        log.warning("Could not check/switch RAG index: %s", exc)
        # continue anyway — might already be on the right index

    grounding_parts: list[str] = []

    for url in all_urls:
        try:
            resp = _requests.post(
                f"{rag_url}/query_metadata",
                json={
                    "source_url": url,
                    "include_summaries": True,
                    "include_qa": True,
                    "max_results": 100,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            retrieved_docs = data.get("retrieved_docs", [])
            retrieved_qa = data.get("retrieved_qa", [])

            if not retrieved_docs and not retrieved_qa:
                continue

            # Build grounding section for this EO
            if retrieved_docs:
                doc = retrieved_docs[0]
                title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                summary_text = doc.get("summary_text", "")

                parts = [f"### {title}\n"]
                parts.append(f"**Source:** {url}\n")

                if summary_text:
                    parts.append(f"\n**Summary:**\n{summary_text}\n")

                # QA context is the actual reconstituted article text
                if retrieved_qa:
                    parts.append("\n**Detailed Content (Federal Register):**\n")
                    for qa in retrieved_qa:
                        context = qa.get("context", "")
                        if context:
                            parts.append(f"\n{context}\n")

                grounding_parts.append("".join(parts))
                log.debug(
                    "Retrieved EO: %s (%d QA pairs)", title[:60], len(retrieved_qa),
                )

        except Exception as exc:
            log.warning("Failed to fetch EO from %s: %s", url[:80], exc)

    if not grounding_parts:
        return ""

    header = (
        "## LEGAL FRAMEWORK GROUNDING\n\n"
        f"Full Executive Order index ({len(grounding_parts)} documents) "
        "reconstituted via /query_metadata:\n\n"
    )
    return header + "\n\n".join(grounding_parts)


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

        # Chapter footer
        parts.append(
            f"\n---\n*Generated by Rapporteur CLI — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
        )

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
        if self.cfg.kaliya.include_images:
            figure_map = self._process_images(
                unique_analyses, kaliya_cache, images_dir,
            )
        else:
            self._logger.info("  Images disabled (include_images: false) — skipping")
            figure_map = {req_id: {} for req_id in unique_analyses}

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


# ---------------------------------------------------------------------------
# Phase 5 — Style Formatter (togglable)
# ---------------------------------------------------------------------------

class FormatStage(Base):
    """Phase 5 — Apply LLM-based style formatting to chapter Markdown.

    Enforces the STYLE CONVENTIONS (FLASH) rules from the project style guide:
    numbered headings with cross-chapter continuity, table labelling, seriation
    rules, and heading rewrites.  Uses Gemini Pro 2.5 by default.
    """

    _HEADING_RE = re.compile(r'^(#{1,3})\s+', re.MULTILINE)

    # Regex to split body from reference/citation sections
    _REF_SPLIT_RE = re.compile(
        r'^(## (?:Top Sources|References)\s*)$',
        re.MULTILINE,
    )

    # Pricing per 1M tokens (Gemini 2.5 models, prompts ≤200k)
    _PRICING = {
        "gemini-2.5-pro":            {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":          {"input": 0.30,  "output":  2.50},
        "gemini-2.5-flash-lite":     {"input": 0.10,  "output":  0.40},
        "gemini-3.1-pro-preview":    {"input": 1.25,  "output": 10.00},
    }

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # -- public -------------------------------------------------------------

    def run(self) -> None:
        fmt = self.cfg.format
        if not fmt.enabled:
            self._logger.info("  Format stage disabled — skipping.")
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to format.")
            return

        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Load style rules
        style_rules = self._load_style_rules(Path(fmt.style_guide))
        if not style_rules:
            self._logger.warning("Could not extract style rules — skipping.")
            return

        self._logger.info(
            "  Formatting %d chapters with %s (%s)",
            len(chapter_entries), fmt.model, fmt.provider,
        )

        # Initialise Gemini client
        import asyncio

        from google.genai import types as genai_types

        from src._clients.llm_gcp_client import GeminiLLMClient
        client = GeminiLLMClient(model_name=fmt.model, location=_gemini_location(fmt.model))

        table_counter = 0     # running table number

        # Cost tracking accumulators
        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0

        # Recover accumulators from any prior partial run
        prior_fmt = manifest.get("format", {})
        prior_usage = prior_fmt.get("usage", {})
        total_input_tokens = prior_usage.get("input_tokens", 0)
        total_output_tokens = prior_usage.get("output_tokens", 0)
        total_thinking_tokens = prior_usage.get("thinking_tokens", 0)

        for idx, entry in enumerate(chapter_entries):
            chapter_path = chapters_dir / entry["filename"]
            if not chapter_path.exists():
                self._logger.warning("  Chapter file missing: %s", entry["filename"])
                continue

            # Skip chapters already formatted in a prior run
            if entry.get("format_status") == "done" and not self.cfg.force:
                self._logger.info(
                    "  [%d/%d] %s — already formatted, skipping.",
                    idx + 1, len(chapter_entries), entry["filename"],
                )
                # Restore table counter from the formatted file
                chapter_md = chapter_path.read_text(encoding="utf-8")
                table_counter = self._extract_last_table_num(chapter_md)
                continue

            chapter_md = chapter_path.read_text(encoding="utf-8")
            self._logger.info(
                "  [%d/%d] %s (%d chars)",
                idx + 1, len(chapter_entries), entry["filename"], len(chapter_md),
            )

            if self.dry_run:
                self._logger.info("    (dry-run) would format chapter")
                continue

            # --- PRE-PROCESSING: strip reference sections (cost saving) ---
            body_md, refs_md = self._split_refs(chapter_md)
            if refs_md:
                self._logger.info(
                    "    Ref sandbox: stripped %d chars of citations",
                    len(refs_md),
                )

            prompt = self._build_prompt(
                body_md, style_rules,
                table_counter,
                idx, len(chapter_entries),
            )

            # Scale max_tokens to input size: ~4 chars/token, add 20% headroom
            estimated_tokens = len(body_md) // 3
            max_tokens = max(16_000, estimated_tokens)

            formatted_md, usage = self._generate_with_usage(
                client, prompt, max_tokens=max_tokens,
            )
            total_input_tokens += usage.get("input", 0)
            total_output_tokens += usage.get("output", 0)
            total_thinking_tokens += usage.get("thinking", 0)
            self._log_usage(entry["filename"], usage, fmt.model)

            if not formatted_md or not formatted_md.strip():
                self._logger.warning("    Empty response — keeping original.")
                continue

            # Strip markdown code fencing if the model wraps output
            formatted_md = self._strip_code_fencing(formatted_md)

            # Continuation: if output is truncated (<80% of body),
            # send follow-up prompts to get the remaining content.
            max_continuations = 5
            for cont in range(max_continuations):
                ratio = len(formatted_md) / len(body_md)
                if ratio >= 0.80:
                    break
                self._logger.info(
                    "    Continuation %d (%.0f%% of original so far)…",
                    cont + 1, ratio * 100,
                )
                cont_prompt = self._build_continuation_prompt(formatted_md)
                continuation, cont_usage = self._generate_with_usage(
                    client, cont_prompt, max_tokens=max_tokens,
                )
                total_input_tokens += cont_usage.get("input", 0)
                total_output_tokens += cont_usage.get("output", 0)
                total_thinking_tokens += cont_usage.get("thinking", 0)
                self._log_usage(f"{entry['filename']} (cont {cont + 1})", cont_usage, fmt.model)
                if not continuation or not continuation.strip():
                    self._logger.warning("    Empty continuation — using what we have.")
                    break
                continuation = self._strip_code_fencing(continuation)
                formatted_md = formatted_md.rstrip() + "\n" + continuation
            else:
                self._logger.warning(
                    "    Reached max continuations — using partial result (%.0f%%).",
                    len(formatted_md) / len(body_md) * 100,
                )

            # --- POST-PROCESSING: strip leading blockquote (duplicate abstract) ---
            lines = formatted_md.split('\n')
            first_h = next((i for i, l in enumerate(lines) if l.startswith('#')), None)
            if first_h is not None and first_h > 0:
                pre = '\n'.join(lines[:first_h]).strip()
                if pre.startswith('>'):
                    formatted_md = '\n'.join(lines[first_h:])

            # --- POST-PROCESSING: strip **Date:** / **Subject:** lines ---
            formatted_md = re.sub(
                r'^\*\*(?:Date|Subject):\*\*.*\n?', '', formatted_md, flags=re.MULTILINE
            )

            # --- POST-PROCESSING: re-attach sandboxed references ---
            if refs_md:
                formatted_md = formatted_md.rstrip() + "\n\n" + refs_md

            # Re-inject footer if the LLM stripped it
            footer_line = (
                f"\n---\n*Generated by Rapporteur CLI — "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
            )
            if "Generated by Rapporteur CLI" not in formatted_md:
                formatted_md = formatted_md.rstrip() + "\n" + footer_line

            # Update table counter from the formatted output
            table_counter = self._extract_last_table_num(formatted_md)

            chapter_path.write_text(formatted_md, encoding="utf-8")
            self._logger.info(
                "    Formatted (%d chars → %d chars)",
                len(chapter_md), len(formatted_md),
            )

            # Mark chapter as formatted in manifest and save incrementally
            entry["format_status"] = "done"
            self._save_manifest_incremental(
                manifest_path, manifest, fmt,
                total_input_tokens, total_output_tokens, total_thinking_tokens,
            )

        # Final manifest update + cost summary
        if not self.dry_run:
            self._save_manifest_incremental(
                manifest_path, manifest, fmt,
                total_input_tokens, total_output_tokens, total_thinking_tokens,
            )
            cost = self._calculate_cost(
                fmt.model, total_input_tokens, total_output_tokens,
            )
            self._logger.info("  ─── Cost Summary ───")
            self._logger.info("    Model:           %s", fmt.model)
            self._logger.info("    Input tokens:    %s", f"{total_input_tokens:,}")
            self._logger.info("    Output tokens:   %s", f"{total_output_tokens:,}")
            self._logger.info("    Thinking tokens: %s", f"{total_thinking_tokens:,}")
            self._logger.info("    Estimated cost:  $%.4f", cost)
            self._logger.info("  Updated manifest.json with format metadata.")

    # -- private helpers ----------------------------------------------------

    def _save_manifest_incremental(
        self, manifest_path: Path, manifest: dict, fmt,
        in_tok: int, out_tok: int, think_tok: int,
    ) -> None:
        """Persist manifest after each chapter so progress survives crashes."""
        cost = self._calculate_cost(fmt.model, in_tok, out_tok)
        manifest["format"] = {
            "formatted": True,
            "model": fmt.model,
            "provider": fmt.provider,
            "chapters_formatted": sum(
                1 for c in manifest["chapters"] if c.get("format_status") == "done"
            ),
            "usage": {
                "input_tokens": in_tok,
                "output_tokens": out_tok,
                "thinking_tokens": think_tok,
                "total_tokens": in_tok + out_tok,
                "estimated_cost_usd": round(cost, 4),
            },
        }
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2, ensure_ascii=False)

    def _generate_with_usage(
        self, client, prompt: str, *, max_tokens: int,
    ) -> tuple[str, dict]:
        """Call Gemini and return (text, usage_dict).

        Uses the underlying google-genai client directly so we can read
        ``response.usage_metadata`` for token counts.
        Checks Redis cache first; stores responses on miss.
        """
        # --- Redis cache check ---
        cached = _llm_cache_get(client.model_name, prompt)
        if cached is not None:
            self._logger.info("    ⚡ Cache hit — skipping LLM call")
            return cached, {"input": 0, "output": 0, "thinking": 0}

        from google.genai import types as genai_types
        config = genai_types.GenerateContentConfig(
            temperature=0.2,
            top_p=client.top_p,
            max_output_tokens=max_tokens,
        )
        response = client.client.models.generate_content(
            model=client.model_name,
            contents=prompt,
            config=config,
        )
        text = response.text if response.text else ""
        usage: dict = {"input": 0, "output": 0, "thinking": 0}
        um = getattr(response, "usage_metadata", None)
        if um:
            usage["input"] = getattr(um, "prompt_token_count", 0) or 0
            usage["output"] = getattr(um, "candidates_token_count", 0) or 0
            usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0

        # --- Redis cache store ---
        if text:
            _llm_cache_set(client.model_name, prompt, text)

        return text, usage

    def _log_usage(self, label: str, usage: dict, model: str) -> None:
        cost = self._calculate_cost(model, usage["input"], usage["output"])
        self._logger.info(
            "    %s — in: %s  out: %s  think: %s  ($%.4f)",
            label,
            f"{usage['input']:,}",
            f"{usage['output']:,}",
            f"{usage['thinking']:,}",
            cost,
        )

    def _calculate_cost(
        self, model: str, input_tokens: int, output_tokens: int,
    ) -> float:
        rates = self._PRICING.get(model, self._PRICING.get("gemini-2.5-pro"))
        return (
            input_tokens / 1_000_000 * rates["input"]
            + output_tokens / 1_000_000 * rates["output"]
        )

    def _load_style_rules(self, path: Path) -> str:
        """Extract STYLE CONVENTIONS section from the style guide."""
        if not path.exists():
            self._logger.warning("Style guide not found: %s", path)
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r'(## STYLE CONVE[NT]+IONS.*?)(?=\n## |\Z)',
            text, re.DOTALL,
        )
        return match.group(1).strip() if match else text.strip()

    def _build_prompt(self, chapter_md: str, style_rules: str,
                      table_counter: int,
                      chapter_idx: int, total_chapters: int) -> str:
        return (
            "Apply the following style conventions to this chapter markdown.\n"
            "Preserve ALL content, citations, references, and meaning exactly.\n"
            "Only restructure headings, seriation, and table labels.\n\n"
            "CRITICAL CONSTRAINTS (do NOT violate):\n"
            "- Keep the blockquote abstract (lines starting with \">\") at the top of each chapter.\n"
            "- Keep the footer line \"---\\n*Generated by Rapporteur CLI ...\" at the end — do NOT remove or alter it.\n"
            "- Use exactly ONE # heading per chapter (the chapter title). All other headings use ## or ###.\n"
            "- Do NOT add numeric prefixes to headings — the build system numbers them automatically.\n"
            "- Maximum heading depth is 3 levels: # (chapter), ## (section), ### (subsection).\n\n"
            "STYLE CONVENTIONS:\n"
            f"{style_rules}\n\n"
            "CONTINUITY CONTEXT:\n"
            f"- Running table count: {table_counter}\n"
            f"- This is chapter {chapter_idx + 1} of {total_chapters}\n\n"
            "CHAPTER MARKDOWN:\n"
            f"{chapter_md}\n\n"
            "Return ONLY the reformatted markdown. Do not add or remove content."
        )

    @staticmethod
    def _build_continuation_prompt(partial_output: str) -> str:
        """Build a continuation prompt from the last ~500 chars of partial output."""
        tail = partial_output[-500:] if len(partial_output) > 500 else partial_output
        return (
            "Your previous response was truncated. Continue EXACTLY where you "
            "left off. Here is the end of your previous output:\n\n"
            f"...{tail}\n\n"
            "Continue from that exact point. Return ONLY the remaining markdown. "
            "Do not repeat any content already provided."
        )

    @staticmethod
    def _strip_code_fencing(text: str) -> str:
        """Strip leading/trailing markdown code fences if present."""
        stripped = text.strip()
        if stripped.startswith("```"):
            first_nl = stripped.index("\n") if "\n" in stripped else len(stripped)
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3].rstrip()
        return stripped

    @classmethod
    def _split_refs(cls, md: str) -> tuple[str, str]:
        """Split chapter markdown into (body, references).

        Returns (body, refs) where *refs* contains everything from the first
        ``## Top Sources`` or ``## References`` heading to the end of the file
        (excluding the footer).  If neither heading is found, *refs* is empty.
        """
        m = cls._REF_SPLIT_RE.search(md)
        if not m:
            return md, ""
        split_pos = m.start()
        body = md[:split_pos].rstrip()
        refs = md[split_pos:]
        # Strip trailing footer from refs — it will be re-injected later
        refs = re.sub(
            r'\n---\n\*Generated by Rapporteur CLI[^*]*\*\s*$', '', refs,
        )
        return body, refs.strip()

    @staticmethod
    def _renumber_headings(md: str, chapter_num: int) -> str:
        """Fix heading numbering so the top-level heading matches *chapter_num*.

        Handles patterns like:
        - ``1. Title``        → ``{chapter_num}. Title``
        - ``1.2 Sub``         → ``{chapter_num}.2 Sub``
        - ``1.2.3 Sub-sub``   → ``{chapter_num}.2.3 Sub-sub``
        - ``1.``  (bare)      → ``{chapter_num}.``   (number-only heading line)
        - ``1.3.1``  (bare)   → ``{chapter_num}.3.1`` (number-only sub-heading)

        Only replaces lines where the *first* number component does not
        already equal *chapter_num*.  Skips markdown numbered-list items
        (lines like ``1.  **Bold`` or ``2.  Text``).
        """
        def _replace(m: re.Match) -> str:
            old_major = m.group("major")
            rest = m.group("rest")
            if int(old_major) == chapter_num:
                return m.group(0)
            return f"{chapter_num}{rest}"

        # Pass 1: sub-headings with title on same line  (N.N+ Title)
        md = re.sub(
            r'^(?P<major>\d+)(?P<rest>(?:\.\d+)+ .+)$',
            _replace, md, flags=re.MULTILINE,
        )
        # Pass 2: top-level heading with title on same line  (N. Capital-letter...)
        md = re.sub(
            r'^(?P<major>\d+)(?P<rest>\. [A-Z].+)$',
            _replace, md, flags=re.MULTILINE,
        )
        # Pass 3: bare sub-heading on its own line  (N.N or N.N.N etc., nothing else)
        md = re.sub(
            r'^(?P<major>\d+)(?P<rest>(?:\.\d+)+)\s*$',
            _replace, md, flags=re.MULTILINE,
        )
        # Pass 4: bare top-level heading on its own line  (N.)
        md = re.sub(
            r'^(?P<major>\d+)(?P<rest>\.)\s*$',
            _replace, md, flags=re.MULTILINE,
        )
        return md

    @staticmethod
    def _add_heading_markers(md: str) -> str:
        """Add markdown heading markers (#, ##, ###) based on numbering depth.

        Determines heading level from dot-separated numeric components:
        - 1 component (``1. Title``)   → ``# 1. Title``
        - 2 components (``1.2 Sub``)   → ``## 1.2 Sub``
        - 3 components (``1.2.3 Sub``) → ``### 1.2.3 Sub``

        Also handles bare headings (number on its own line).
        Idempotent — lines already starting with ``#`` are not matched.
        """
        def _add_prefix(m: re.Match) -> str:
            num = m.group("num")
            depth = num.count('.') + 1
            prefix = '#' * min(depth, 6)
            return f"{prefix} {m.group(0)}"

        # Pass 1: sub-headings with title  (N.N+ Title)
        md = re.sub(
            r'^(?P<num>\d+(?:\.\d+)+) .+$',
            _add_prefix, md, flags=re.MULTILINE,
        )
        # Pass 2: top-level heading with title  (N. Capital-letter...)
        md = re.sub(
            r'^(?P<num>\d+)\. [A-Z].+$',
            _add_prefix, md, flags=re.MULTILINE,
        )
        # Pass 3: bare sub-heading  (N.N or N.N.N, nothing else on line)
        md = re.sub(
            r'^(?P<num>\d+(?:\.\d+)+)\s*$',
            _add_prefix, md, flags=re.MULTILINE,
        )
        # Pass 4: bare top-level  (N.)
        md = re.sub(
            r'^(?P<num>\d+)\.\s*$',
            _add_prefix, md, flags=re.MULTILINE,
        )
        return md

    @staticmethod
    def _extract_last_heading_num(md: str) -> int:
        """Find the last top-level numbered heading (e.g. '# 5 ...' → 5)."""
        last = 0
        for m in re.finditer(r'^#\s+(\d+)\b', md, re.MULTILINE):
            last = int(m.group(1))
        return last

    @staticmethod
    def _extract_last_table_num(md: str) -> int:
        """Find the last Table N label (e.g. 'Table 12. ...' → 12)."""
        last = 0
        for m in re.finditer(r'Table\s+(\d+)\.', md):
            last = int(m.group(1))
        return last


# ---------------------------------------------------------------------------
# Phase 5b — Conclude
# ---------------------------------------------------------------------------

class ConcludeStage(Base):
    """Phase 5b — Generate conclusion chapter via two-call LLM synthesis.

    Uses a conclude → reflect pattern: the first call synthesises the full
    document into a conclusion chapter; the second call critically reviews
    and strengthens it.  Supports OpenAI (GPT-5.x, default) and Gemini
    providers via ``ConcludeConfig.provider``.
    """

    # Pricing per 1M tokens — OpenAI GPT-5.x & Gemini 2.5
    _PRICING: dict[str, dict[str, float]] = {
        "gpt-5.4":               {"input": 2.50,  "output": 10.00},
        "gpt-5.1":               {"input": 2.50,  "output": 10.00},
        "gpt-5":                 {"input": 2.00,  "output":  8.00},
        "gpt-5-mini":            {"input": 0.40,  "output":  1.60},
        "gemini-2.5-pro":        {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":      {"input": 0.30,  "output":  2.50},
        "gemini-3.1-pro-preview":{"input": 1.25,  "output": 10.00},
    }

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # -- public -------------------------------------------------------------

    def run(self) -> None:
        co = self.cfg.conclude
        if not co.enabled:
            self._logger.info("  Conclusion generation disabled — skipping.")
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to conclude.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Idempotent — skip if already generated
        if manifest.get("conclude", {}).get("generated") and not self.cfg.force:
            self._logger.info("  Conclusion already generated — skipping.")
            return

        # Load style rules
        style_rules = self._load_style_rules(Path(co.style_guide))
        conclusion_rules = self._load_conclusion_rules(Path(co.style_guide))

        # Build full document context
        document_context = self._build_document_context(
            chapters_dir, chapter_entries, co.include_kaliya,
        )

        self._logger.info(
            "  Generating conclusion with %s (%s, reasoning: %s)",
            co.model, co.provider, co.reasoning_effort,
        )

        if self.dry_run:
            self._logger.info("  (dry-run) would generate conclusion chapter")
            return

        # Fetch EO grounding scoped to only EOs cited in document chapters
        eo_grounding = self._fetch_document_eo_grounding(
            chapters_dir, chapter_entries, co.rag_service_url,
        )
        if eo_grounding:
            self._logger.info("  Scoped EO grounding (%d chars)", len(eo_grounding))
        else:
            self._logger.warning("  No EO grounding available (RAG service unreachable or no EO URLs found)")

        # Cost tracking accumulators
        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0

        # ── Call 1: Conclude ──
        self._logger.info("  Call 1/2: CONCLUDE")
        conclude_prompt = self._build_conclude_prompt(
            document_context, conclusion_rules, style_rules, eo_grounding,
        )
        prompt_file = chapters_dir / "_prompt-overall-conclude.txt"
        prompt_file.write_text(conclude_prompt, encoding="utf-8")
        self._logger.info("  Prompt saved to %s (%d chars)", prompt_file.name, len(conclude_prompt))
        conclusion_draft, usage_1 = self._generate_with_usage(co, conclude_prompt)
        total_input_tokens += usage_1.get("input", 0)
        total_output_tokens += usage_1.get("output", 0)
        total_thinking_tokens += usage_1.get("thinking", 0)
        self._log_usage("conclude", usage_1, co.model)

        if not conclusion_draft or not conclusion_draft.strip():
            self._logger.warning("  Empty conclusion draft — aborting.")
            return

        conclusion_draft = self._strip_code_fencing(conclusion_draft)

        # ── Call 2: Reflect ──
        self._logger.info("  Call 2/2: REFLECT")
        reflect_prompt = self._build_reflect_prompt(
            conclusion_draft, document_context, conclusion_rules, eo_grounding,
        )
        prompt_file = chapters_dir / "_prompt-overall-reflect.txt"
        prompt_file.write_text(reflect_prompt, encoding="utf-8")
        self._logger.info("  Prompt saved to %s (%d chars)", prompt_file.name, len(reflect_prompt))
        refined_conclusion, usage_2 = self._generate_with_usage(co, reflect_prompt)
        total_input_tokens += usage_2.get("input", 0)
        total_output_tokens += usage_2.get("output", 0)
        total_thinking_tokens += usage_2.get("thinking", 0)
        self._log_usage("reflect", usage_2, co.model)

        if not refined_conclusion or not refined_conclusion.strip():
            self._logger.warning("  Empty reflect response — using draft.")
            refined_conclusion = conclusion_draft

        refined_conclusion = self._strip_code_fencing(refined_conclusion)

        # Add footer
        footer = (
            f"\n---\n*Generated by Rapporteur CLI — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
        )
        refined_conclusion = refined_conclusion.rstrip() + "\n" + footer

        # Write conclusion chapter — remove any prior *overall* conclusion entry first
        non_conclusion = [c for c in chapter_entries if not ("conclusion" in c["filename"] and "part-" not in c["filename"])]
        chapter_num = len([c for c in non_conclusion if "introduction" not in c["filename"] and "part-" not in c["filename"]]) + 1
        filename = f"{chapter_num:02d}-conclusion.md"
        # Clean up stale overall conclusion files (preserve section part conclusions)
        for old in chapter_entries:
            fn = old["filename"]
            if "conclusion" in fn and fn != filename and "part-" not in fn:
                stale = chapters_dir / fn
                if stale.exists():
                    stale.unlink()
        conclusion_path = chapters_dir / filename
        conclusion_path.write_text(refined_conclusion, encoding="utf-8")
        self._logger.info(
            "  Wrote %s (%d chars)", filename, len(refined_conclusion),
        )

        # Update manifest — replace any prior conclusion entry
        chapter_entries[:] = non_conclusion
        chapter_entries.append({
            "filename": filename,
            "title": "Conclusion",
            "conclude_status": "done",
        })

        cost = self._calculate_cost(
            co.model, total_input_tokens, total_output_tokens,
        )
        manifest["conclude"] = {
            "generated": True,
            "model": co.model,
            "provider": co.provider,
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "thinking_tokens": total_thinking_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 4),
            },
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        self._logger.info("  ─── Cost Summary ───")
        self._logger.info("    Model:           %s", co.model)
        self._logger.info("    Provider:        %s", co.provider)
        self._logger.info("    Input tokens:    %s", f"{total_input_tokens:,}")
        self._logger.info("    Output tokens:   %s", f"{total_output_tokens:,}")
        self._logger.info("    Thinking tokens: %s", f"{total_thinking_tokens:,}")
        self._logger.info("    Estimated cost:  $%.4f", cost)
        self._logger.info("  Updated manifest.json with conclude metadata.")

    # -- LLM generation -----------------------------------------------------

    def _generate_with_usage(
        self, co: ConcludeConfig, prompt: str,
    ) -> tuple[str, dict]:
        """Call the configured LLM provider and return (text, usage_dict).

        Supports dual-provider: OpenAI (Responses API) or Gemini.
        Checks Redis cache first; stores responses on miss.
        """
        # --- Redis cache check ---
        cached = _llm_cache_get(co.model, prompt)
        if cached is not None:
            self._logger.info("    ⚡ Cache hit — skipping LLM call")
            return cached, {"input": 0, "output": 0, "thinking": 0}

        import asyncio

        usage: dict = {"input": 0, "output": 0, "thinking": 0}

        if co.provider == "openai":
            from src._clients.llm_openai_client import OpenAILLMClient

            client = OpenAILLMClient(model_name=co.model)
            max_tokens = 16_000

            # Run async generate_content in sync context
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(
                    client.client.responses.create(
                        model=co.model,
                        input=prompt,
                        reasoning={"effort": co.reasoning_effort},
                        max_output_tokens=max_tokens,
                    )
                )
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

            text = response.output_text if hasattr(response, "output_text") else ""
            # Extract usage from the response object
            resp_usage = getattr(response, "usage", None)
            if resp_usage:
                usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
                usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0
                # Reasoning tokens are nested under output_tokens_details
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = getattr(details, "reasoning_tokens", 0) or 0

            if text:
                _llm_cache_set(co.model, prompt, text)
            return text, usage

        else:
            # Gemini provider
            from google.genai import types as genai_types

            from src._clients.llm_gcp_client import GeminiLLMClient

            client = GeminiLLMClient(model_name=co.model, location=_gemini_location(co.model))
            max_tokens = 16_000

            config = genai_types.GenerateContentConfig(
                temperature=0.2,
                top_p=client.top_p,
                max_output_tokens=max_tokens,
            )
            response = client.client.models.generate_content(
                model=client.model_name,
                contents=prompt,
                config=config,
            )
            text = response.text if response.text else ""
            um = getattr(response, "usage_metadata", None)
            if um:
                usage["input"] = getattr(um, "prompt_token_count", 0) or 0
                usage["output"] = getattr(um, "candidates_token_count", 0) or 0
                usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0

            if text:
                _llm_cache_set(co.model, prompt, text)
            return text, usage

    # -- prompt builders ----------------------------------------------------

    def _build_conclude_prompt(
        self, document_context: str, conclusion_rules: str, style_rules: str,
        eo_grounding: str = "",
    ) -> str:
        """Build the CONCLUDE prompt with full document context and optional EO grounding.

        Phase 8.1: eo_grounding parameter adds primary-source legal framework data.
        """
        parts = [
            "You are writing the conclusion chapter for a published analysis document.\n\n",
            "CRITICAL CONSTRAINTS:\n",
            "- Use exactly ONE # heading: '# Conclusion'\n",
            "- All other headings use ## or ### (max 3 levels)\n",
            "- Do NOT add numeric prefixes to headings — the build system numbers them automatically\n",
            "- Favour free-flowing prose over seriated lists\n\n",
            f"CONCLUSION GUIDELINES:\n{conclusion_rules}\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
        ]

        if eo_grounding:
            parts.extend([
                f"{eo_grounding}\n\n",
                "IMPORTANT — LEGAL FRAMEWORK INTEGRATION:\n",
                "The Executive Order documents above establish the PRIMARY LEGAL FRAMEWORK ",
                "for this analysis. These are the actual policy documents referenced throughout ",
                "the chapters. When synthesizing your conclusion:\n",
                "1. CITE these Executive Orders by number and title when discussing legal/policy foundations\n",
                "2. REFERENCE specific provisions from the EO text to support your analysis\n",
                "3. CONNECT the chapter discussions back to these primary-source documents\n",
                "4. GROUND policy interpretations in the actual EO language provided above\n",
                "5. CITE EOs as: 'Executive Order 14151 (\"Ending Radical and Wasteful Government DEI Programs\")'\n\n",
                "These EOs are NOT just references — they are the legal bedrock of the events and ",
                "policy debates analyzed in the document chapters.\n\n",
            ])

        parts.extend([
            f"FULL DOCUMENT CONTEXT:\n{document_context}\n\n",
            "Write a conclusion chapter that synthesises the entire document.\n",
        ])

        if eo_grounding:
            parts.append(
                "Ensure your conclusion demonstrates how the chapter analyses connect to and "
                "illuminate the Executive Orders' implementation and impact.\n"
            )

        parts.extend([
            "Include a Mermaid timeline diagram where date/event data is available.\n",
            "Return ONLY the conclusion markdown."
        ])

        return "".join(parts)

    def _build_reflect_prompt(
        self,
        conclusion_draft: str,
        document_context: str,
        conclusion_rules: str,
        eo_grounding: str = "",
    ) -> str:
        """Build the REFLECT prompt to refine the conclusion draft.

        Phase 8.1: eo_grounding parameter ensures reflection maintains EO citations.
        """
        parts = [
            "Review and refine this conclusion chapter for a published analysis.\n\n",
            f"CONCLUSION GUIDELINES:\n{conclusion_rules}\n\n",
            "REVIEW CRITERIA:\n",
            "1. Are all conclusions grounded in evidence from the document?\n",
            "2. Is the protective-function framing consistent and respectful?\n",
            "3. Are media intentions assessed for medium to long-term impact?\n",
            "4. Is the information domain analysis connected to democratic implications?\n",
            "5. Is the Mermaid timeline accurate and complete?\n",
            "6. Are headings using #/##/### without numeric prefixes?\n",
        ]

        if eo_grounding:
            parts.extend([
                "7. Are Executive Orders cited correctly with number and title?\n",
                "8. Are policy conclusions grounded in the actual EO text provided?\n",
                "9. Are chapter analyses connected to the legal framework established by the EOs?\n",
            ])

        if eo_grounding:
            parts.extend([
                "\n",
                f"{eo_grounding}\n\n",
                "The Executive Orders above establish the legal framework. Ensure the conclusion:\n",
                "- CITES EOs using: Executive Order [number] (\"[title]\")\n",
                "- GROUNDS policy statements in specific EO provisions\n",
                "- CONNECTS chapter content to these primary-source documents\n\n",
            ])

        parts.extend([
            f"DOCUMENT CONTEXT:\n{document_context}\n\n",
            f"CONCLUSION DRAFT:\n{conclusion_draft}\n\n",
            "Return the refined conclusion chapter markdown.\n",
            "Strengthen weak arguments with specific evidence references.\n",
        ])

        if eo_grounding:
            parts.append(
                "Add or strengthen Executive Order citations where policy/legal analysis appears.\n"
            )

        parts.append("Correct any factual inconsistencies.")

        return "".join(parts)

    # -- context helpers ----------------------------------------------------

    def _fetch_document_eo_grounding(
        self, chapters_dir: Path, chapter_entries: list[dict], rag_url: str,
    ) -> str:
        """Fetch EO grounding scoped to only EOs cited across all document chapters."""
        if not rag_url:
            return ""
        url_pattern = re.compile(r'https://www\.federalregister\.gov/documents/[^\s\)\]"]+')
        doc_urls: set[str] = set()
        for entry in chapter_entries:
            fn = entry.get("filename", "")
            if "conclusion" in fn:
                continue
            path = chapters_dir / fn
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in url_pattern.finditer(text):
                doc_urls.add(m.group(0).rstrip(".,;:"))

        if not doc_urls:
            return ""

        self._logger.info("  Found %d unique EO URLs across chapters", len(doc_urls))

        import requests as _requests
        try:
            ready = _requests.get(f"{rag_url}/ready", timeout=5)
            active = ready.json().get("active_folder", "")
            if "eo" not in active.lower():
                resp = _requests.post(
                    f"{rag_url}/switch-index",
                    json={"folder": "x_indices_eo"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    return ""
        except Exception:
            pass

        grounding_parts: list[str] = []
        for url in sorted(doc_urls):
            try:
                resp = _requests.post(
                    f"{rag_url}/query_metadata",
                    json={
                        "source_url": url,
                        "include_summaries": True,
                        "include_qa": True,
                        "max_results": 100,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                retrieved_docs = data.get("retrieved_docs", [])
                retrieved_qa = data.get("retrieved_qa", [])
                if not retrieved_docs and not retrieved_qa:
                    continue
                if retrieved_docs:
                    doc = retrieved_docs[0]
                    doc_title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                    summary_text = doc.get("summary_text", "")
                    parts = [f"### {doc_title}\n", f"**Source:** {url}\n"]
                    if summary_text:
                        parts.append(f"\n**Summary:**\n{summary_text}\n")
                    if retrieved_qa:
                        parts.append("\n**Detailed Content (Federal Register):**\n")
                        for qa in retrieved_qa:
                            context = qa.get("context", "")
                            if context:
                                parts.append(f"\n{context}\n")
                    grounding_parts.append("".join(parts))
            except Exception as exc:
                self._logger.debug("Failed to fetch EO from %s: %s", url[:80], exc)

        if not grounding_parts:
            return ""
        header = (
            "## LEGAL FRAMEWORK GROUNDING\n\n"
            f"Executive Order context ({len(grounding_parts)} documents) "
            "covering all EOs cited in the document:\n\n"
        )
        return header + "\n\n".join(grounding_parts)

    def _build_document_context(
        self,
        chapters_dir: Path,
        chapter_entries: list[dict],
        include_kaliya: bool,
    ) -> str:
        """Concatenate chapter text, optionally with Kaliya analysis.

        When sections are defined, uses only section introductions and
        conclusions as context (much smaller) instead of all chapters.
        """
        parts: list[str] = []

        # If sections are defined, use section intros + conclusions for a
        # compact representation instead of all chapter text.
        sects = self.cfg.sections
        if sects.enabled and sects.definitions:
            self._logger.info("  Using section intros/conclusions as document context")
            for entry in chapter_entries:
                fn = entry["filename"]
                if "part-" in fn and ("-introduction" in fn or "-conclusion" in fn):
                    path = chapters_dir / fn
                    if path.exists():
                        parts.append(path.read_text(encoding="utf-8"))
        else:
            for entry in chapter_entries:
                if "conclusion" in entry["filename"]:
                    continue  # exclude prior conclusion from context
                path = chapters_dir / entry["filename"]
                if path.exists():
                    parts.append(path.read_text(encoding="utf-8"))

        if include_kaliya:
            kaliya_dir = self.cfg.build_dir / "kaliya"
            if kaliya_dir.is_dir():
                for report in sorted(kaliya_dir.glob("*/report.json")):
                    try:
                        data = json.loads(report.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError):
                        continue
                    if "narrative" in data:
                        cluster = data.get("cluster", report.parent.name)
                        parts.append(
                            f"## Kaliya Analysis: {cluster}\n\n{data['narrative']}"
                        )

        return "\n\n---\n\n".join(parts)

    def _load_style_rules(self, path: Path) -> str:
        """Extract STYLE CONVENTIONS section from the style guide."""
        if not path.exists():
            self._logger.warning("Style guide not found: %s", path)
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r'(## STYLE CONVE[NT]+IONS.*?)(?=\n## |\Z)',
            text, re.DOTALL,
        )
        return match.group(1).strip() if match else text.strip()

    def _load_conclusion_rules(self, path: Path) -> str:
        """Extract CONCLUSION PRO section from the style guide."""
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r'(## CONCLUSION.*?)(?=\n## |\Z)',
            text, re.DOTALL,
        )
        return match.group(1).strip() if match else ""

    def _extract_eo_references(
        self, chapters_dir: Path, chapter_entries: list[dict]
    ) -> list[str]:
        """Extract unique Executive Order reference URLs from all chapter files.

        Scans citation sections for Federal Register and whitehouse.gov URLs that
        reference Executive Orders. These URLs can be queried directly via RAG's
        /query_metadata endpoint to retrieve full EO documents.

        Phase 8.1: EO grounding enhancement.

        Returns: Sorted list of unique EO URLs (Federal Register, whitehouse.gov)
        """
        # Pattern to extract markdown link URLs: [text](url)
        # This captures just the URL part inside parentheses
        markdown_url_pattern = re.compile(
            r'\[([^\]]+)\]\((https://(?:www\.)?(?:federalregister\.gov|whitehouse\.gov)[^\)]+)\)',
            re.IGNORECASE
        )

        eo_urls = set()
        for entry in chapter_entries:
            if "conclusion" in entry.get("filename", ""):
                continue  # Don't extract from prior conclusion
            path = chapters_dir / entry["filename"]
            if path.exists():
                text = path.read_text(encoding="utf-8")
                for match in markdown_url_pattern.finditer(text):
                    url = match.group(2)  # Group 2 is the URL
                    # Filter for INDIVIDUAL EO document URLs (not list pages)
                    # Valid: /documents/YYYY/MM/DD/document-id/title
                    # Invalid: /presidential-documents/executive-orders/... (list page)
                    if '/documents/' in url and re.search(r'/documents/\d{4}/\d{2}/\d{2}/', url):
                        # This is an individual document with date pattern
                        eo_urls.add(url)
                    elif 'executive-order' in url.lower() and '/documents/' in url:
                        # Catch any other executive order document URLs
                        eo_urls.add(url)

        return sorted(eo_urls)

    def _fetch_eo_grounding(self, eo_urls: list[str], rag_url: str) -> str:
        """Query RAG service for full EO documents by URL and return formatted grounding text.

        Uses /query_metadata endpoint to retrieve documents by exact source_url match.
        This requires the RAG service to have the EO index loaded as the active index.

        Phase 8.1: EO grounding enhancement.

        Returns empty string if no URLs or if RAG service unavailable.
        """
        if not eo_urls:
            return ""

        import requests
        grounding_parts = []

        # Check which index is currently active
        try:
            ready_response = requests.get(f"{rag_url}/ready", timeout=5)
            ready_data = ready_response.json()
            active_folder = ready_data.get("active_folder", "")

            # If not on EO index, try to switch
            if "eo" not in active_folder.lower():
                self._logger.info(f"  RAG service using {active_folder}, switching to EO index...")
                try:
                    switch_response = requests.post(
                        f"{rag_url}/switch-index",
                        json={"folder": "x_indices_eo"},
                        timeout=30
                    )
                    if switch_response.status_code == 200:
                        self._logger.info("  Switched to EO index")
                    else:
                        self._logger.warning(f"  Could not switch to EO index: {switch_response.status_code}")
                        return ""  # Can't proceed without EO index
                except Exception as e:
                    self._logger.warning(f"  Failed to switch index: {e}")
                    return ""
        except Exception as e:
            self._logger.warning(f"  Could not check RAG service status: {e}")
            # Continue anyway, might still work

        for url in eo_urls:
            try:
                # Query by exact source URL to get full document
                response = requests.post(
                    f"{rag_url}/query_metadata",
                    json={
                        "source_url": url,
                        "include_summaries": True,
                        "include_qa": True,
                        "max_results": 100  # Get all QA pairs for this document
                    },
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                # Extract summary (high-level overview)
                retrieved_docs = data.get("retrieved_docs", [])
                retrieved_qa = data.get("retrieved_qa", [])

                if not retrieved_docs and not retrieved_qa:
                    self._logger.debug(f"  No content for URL: {url[:80]}")
                    continue

                # Build grounding section for this EO
                if retrieved_docs:
                    doc = retrieved_docs[0]
                    title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                    summary_text = doc.get("summary_text", "")

                    # Format: title + summary + QA context samples
                    parts = [f"### {title}\n"]

                    if summary_text:
                        parts.append(f"\n**Summary:**\n{summary_text}\n")

                    # Add detailed content from QA pairs (source context gives full text)
                    if retrieved_qa:
                        parts.append(f"\n**Detailed Content:**\n")
                        # Take first N QA pairs to show detailed sections
                        for i, qa in enumerate(retrieved_qa[:5], 1):
                            context = qa.get("context", "")
                            if context:
                                parts.append(f"\n{context}\n")

                    grounding_parts.append("".join(parts))
                    self._logger.debug(f"  Retrieved EO: {title[:60]}... ({len(retrieved_qa)} QA pairs)")

            except Exception as e:
                self._logger.warning(f"  Failed to fetch EO from {url[:60]}...: {e}")

        if grounding_parts:
            header = (
                "## LEGAL FRAMEWORK GROUNDING\n\n"
                "Full Executive Order documents referenced in this analysis:\n\n"
            )
            return header + "\n\n".join(grounding_parts)
        return ""

    # -- shared helpers (mirror FormatStage) ---------------------------------

    @staticmethod
    def _strip_code_fencing(text: str) -> str:
        """Strip leading/trailing markdown code fences if present."""
        stripped = text.strip()
        if stripped.startswith("```"):
            first_nl = stripped.index("\n") if "\n" in stripped else len(stripped)
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3].rstrip()
        return stripped

    def _log_usage(self, label: str, usage: dict, model: str) -> None:
        cost = self._calculate_cost(model, usage["input"], usage["output"])
        self._logger.info(
            "    %s — in: %s  out: %s  think: %s  ($%.4f)",
            label,
            f"{usage['input']:,}",
            f"{usage['output']:,}",
            f"{usage['thinking']:,}",
            cost,
        )

    def _calculate_cost(
        self, model: str, input_tokens: int, output_tokens: int,
    ) -> float:
        rates = self._PRICING.get(model, {"input": 2.50, "output": 10.00})
        return (
            input_tokens / 1_000_000 * rates["input"]
            + output_tokens / 1_000_000 * rates["output"]
        )


# ---------------------------------------------------------------------------
# Phase 6 — Publish
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase 9 — Editorial Review
# ---------------------------------------------------------------------------

class EditStage(Base):
    """Phase 9 — Editorial review via two-call LLM pattern.

    Uses a review → apply pattern: the first call assesses the full document
    holistically, producing a structured edit log with scores, executive
    summaries, and specific edit recommendations. The second call applies
    the recommended edits per chapter while preserving content integrity.

    Supports OpenAI (GPT-5.x, default) and Gemini providers via
    ``EditConfig.provider``.
    """

    # Pricing per 1M tokens — same table as ConcludeStage
    _PRICING: dict[str, dict[str, float]] = {
        "gpt-5.4":               {"input": 2.50,  "output": 10.00},
        "gpt-5.1":               {"input": 2.50,  "output": 10.00},
        "gpt-5":                 {"input": 2.00,  "output":  8.00},
        "gpt-5-mini":            {"input": 0.40,  "output":  1.60},
        "gemini-2.5-pro":        {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":      {"input": 0.30,  "output":  2.50},
        "gemini-3.1-pro-preview":{"input": 1.25,  "output": 10.00},
    }

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # -- public -------------------------------------------------------------

    def run(self) -> None:
        ec = self.cfg.edit
        if not ec.enabled:
            self._logger.info("  Editorial review disabled — skipping.")
            return

        # Targeted single-chapter edit mode
        if ec.target_chapter:
            self._run_targeted()
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to review.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Idempotent — skip if already reviewed
        if manifest.get("edit", {}).get("reviewed") and not self.cfg.force:
            self._logger.info("  Editorial review already done — skipping.")
            return

        # Load style rules and editorial principles
        style_rules = self._load_style_rules(Path(ec.style_guide))
        editorial_principles = self._load_editorial_principles(Path(ec.style_guide))

        # Build full document context (all chapters incl. conclusion)
        document_context = self._build_document_context(
            chapters_dir, chapter_entries,
        )

        self._logger.info(
            "  Editorial review with %s (%s, reasoning: %s)",
            ec.model, ec.provider, ec.reasoning_effort,
        )

        if self.dry_run:
            self._logger.info("  (dry-run) would perform editorial review")
            return

        # Cost tracking
        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0

        # ── Call 1: Review ──
        self._logger.info("  Call 1: REVIEW (full document)")
        review_prompt = self._build_review_prompt(
            document_context, editorial_principles, style_rules,
            chapter_entries,
        )
        review_response, usage_1 = self._generate_with_usage(ec, review_prompt)
        total_input_tokens += usage_1.get("input", 0)
        total_output_tokens += usage_1.get("output", 0)
        total_thinking_tokens += usage_1.get("thinking", 0)
        self._log_usage("review", usage_1, ec.model)

        if not review_response or not review_response.strip():
            self._logger.warning("  Empty review response — aborting.")
            return

        # Parse edit log JSON
        edit_log = self._parse_edit_log(review_response)
        if not edit_log:
            self._logger.warning("  Could not parse edit log — aborting.")
            return

        self._logger.info(
            "  Review complete — scores: consistency=%s, cohesiveness=%s, "
            "bias_risk=%s, coherence=%s",
            edit_log.get("document_score", {}).get("stylistic_consistency", "?"),
            edit_log.get("document_score", {}).get("argument_cohesiveness", "?"),
            edit_log.get("document_score", {}).get("bias_risk", "?"),
            edit_log.get("document_score", {}).get("logical_coherence", "?"),
        )
        self._logger.info(
            "  Edits recommended: %d", len(edit_log.get("edits", [])),
        )

        # Write edit log to build dir
        if ec.edit_log:
            log_path = self.cfg.build_dir / "editorial_edit_log.json"
            log_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "model": ec.model,
                "provider": ec.provider,
                **edit_log,
            }
            log_path.write_text(
                json.dumps(log_data, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            self._logger.info("  Wrote editorial_edit_log.json")

        # Build executive summary lookup
        exec_summaries = {}
        for es in edit_log.get("executive_summaries", []):
            ch_num = es.get("chapter")
            if ch_num is not None:
                exec_summaries[ch_num] = es.get("summary", "")

        # Build per-chapter edit lookup
        edits_by_chapter: dict[int, list[dict]] = {}
        for edit in edit_log.get("edits", []):
            ch_num = edit.get("chapter")
            if ch_num is not None:
                edits_by_chapter.setdefault(ch_num, []).append(edit)

        # ── Call 2+: Apply (per chapter with edits) ──
        chapters_with_edits = set(edits_by_chapter.keys())
        if ec.executive_summary:
            chapters_with_edits |= set(exec_summaries.keys())

        if not chapters_with_edits:
            self._logger.info("  No edits or summaries to apply.")
        else:
            self._logger.info(
                "  Call 2: APPLY edits to %d chapter(s)", len(chapters_with_edits),
            )

        chapters_revised = 0
        for idx, entry in enumerate(chapter_entries, 1):
            if idx not in chapters_with_edits:
                continue

            chapter_path = chapters_dir / entry["filename"]
            if not chapter_path.exists():
                continue

            chapter_text = chapter_path.read_text(encoding="utf-8")
            chapter_edits = edits_by_chapter.get(idx, [])
            chapter_summary = exec_summaries.get(idx, "")

            self._logger.info(
                "    Chapter %d (%s): %d edits%s",
                idx, entry["filename"],
                len(chapter_edits),
                " + exec summary" if chapter_summary else "",
            )

            apply_prompt = self._build_apply_prompt(
                chapter_text, chapter_edits, chapter_summary,
                ec.executive_summary,
            )
            revised_text, usage_n = self._generate_with_usage(ec, apply_prompt)
            total_input_tokens += usage_n.get("input", 0)
            total_output_tokens += usage_n.get("output", 0)
            total_thinking_tokens += usage_n.get("thinking", 0)
            self._log_usage(f"apply-ch{idx}", usage_n, ec.model)

            if revised_text and revised_text.strip():
                revised_text = self._strip_code_fencing(revised_text)
                chapter_path.write_text(revised_text, encoding="utf-8")
                chapters_revised += 1
                entry["edit_status"] = "done"
            else:
                self._logger.warning(
                    "    Empty response for chapter %d — keeping original.", idx,
                )

        # Update manifest
        cost = self._calculate_cost(
            ec.model, total_input_tokens, total_output_tokens,
        )
        manifest["edit"] = {
            "reviewed": True,
            "model": ec.model,
            "provider": ec.provider,
            "chapters_revised": chapters_revised,
            "edits_recommended": len(edit_log.get("edits", [])),
            "document_score": edit_log.get("document_score", {}),
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "thinking_tokens": total_thinking_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 4),
            },
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        self._logger.info("  ─── Cost Summary ───")
        self._logger.info("    Model:            %s", ec.model)
        self._logger.info("    Provider:         %s", ec.provider)
        self._logger.info("    Chapters revised: %d", chapters_revised)
        self._logger.info("    Input tokens:     %s", f"{total_input_tokens:,}")
        self._logger.info("    Output tokens:    %s", f"{total_output_tokens:,}")
        self._logger.info("    Thinking tokens:  %s", f"{total_thinking_tokens:,}")
        self._logger.info("    Estimated cost:   $%.4f", cost)
        self._logger.info("  Updated manifest.json with edit metadata.")

    # -- targeted (single-chapter) edit ------------------------------------

    def _run_targeted(self) -> None:
        """Run two-call editorial review on a single chapter.

        Call 1 (REVIEW): produces JSON edit log with scores, bias assessment,
        executive summary, and specific edit recommendations.
        Call 2 (APPLY): applies the recommended edits to the chapter text.
        """
        ec = self.cfg.edit
        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to review.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])

        # Find target chapter
        target_entry = None
        target_idx = None
        for i, entry in enumerate(chapter_entries):
            if ec.target_chapter in entry["filename"]:
                target_entry = entry
                target_idx = i
                break

        if target_entry is None:
            self._logger.warning("Chapter not found: %s", ec.target_chapter)
            return

        chapter_path = chapters_dir / target_entry["filename"]
        if not chapter_path.exists():
            self._logger.warning("Chapter file missing: %s", chapter_path)
            return

        chapter_text = chapter_path.read_text(encoding="utf-8")

        # Load style context
        style_rules = self._load_style_rules(Path(ec.style_guide))
        editorial_principles = self._load_editorial_principles(Path(ec.style_guide))

        # Build citable EO list for LLM-generated chapters
        citable_eo_list = ""
        if self._is_llm_generated_chapter(target_entry, target_entry["filename"]):
            citable_eo_list = self._build_citable_eo_list(chapters_dir, chapter_entries)
            if citable_eo_list:
                self._logger.info("  Built citable EO index (%d entries)",
                                  citable_eo_list.count("- Exec."))

        self._logger.info(
            "  Targeted editorial: %s (%s, %s, reasoning: %s)",
            target_entry["filename"], ec.model, ec.provider,
            ec.reasoning_effort,
        )
        if ec.custom_instructions:
            self._logger.info("  Custom instructions: %s", ec.custom_instructions)

        if self.dry_run:
            self._logger.info("  (dry-run) would edit %s", target_entry["filename"])
            return

        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0

        # ── Call 1: REVIEW ──
        self._logger.info("  Call 1: REVIEW (%s)", target_entry["filename"])
        review_prompt = self._build_targeted_review_prompt(
            chapter_text, editorial_principles, style_rules,
            target_entry, ec.custom_instructions, citable_eo_list,
        )
        review_response, usage_1 = self._generate_with_usage(ec, review_prompt)
        total_input_tokens += usage_1.get("input", 0)
        total_output_tokens += usage_1.get("output", 0)
        total_thinking_tokens += usage_1.get("thinking", 0)
        self._log_usage("review", usage_1, ec.model)

        if not review_response or not review_response.strip():
            self._logger.warning("  Empty review response — aborting.")
            return

        edit_log = self._parse_edit_log(review_response)
        if not edit_log:
            self._logger.warning("  Could not parse edit log — aborting.")
            return

        scores = edit_log.get("document_score", {})
        self._logger.info(
            "  Review complete — scores: consistency=%s, cohesiveness=%s, "
            "bias_risk=%s, coherence=%s",
            scores.get("stylistic_consistency", "?"),
            scores.get("argument_cohesiveness", "?"),
            scores.get("bias_risk", "?"),
            scores.get("logical_coherence", "?"),
        )
        edits = edit_log.get("edits", [])
        self._logger.info("  Edits recommended: %d", len(edits))

        # Write edit log
        if ec.edit_log:
            log_path = self.cfg.build_dir / "editorial_edit_log.json"
            log_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "model": ec.model,
                "provider": ec.provider,
                "target_chapter": target_entry["filename"],
                **edit_log,
            }
            log_path.write_text(
                json.dumps(log_data, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            self._logger.info("  Wrote editorial_edit_log.json")

        exec_summary = ""
        for es in edit_log.get("executive_summaries", []):
            if es.get("summary"):
                exec_summary = es["summary"]
                break

        # ── Call 2: APPLY ──
        self._logger.info("  Call 2: APPLY edits to %s", target_entry["filename"])
        apply_prompt = self._build_apply_prompt(
            chapter_text, edits, exec_summary, ec.executive_summary,
        )
        # Inject custom instructions and citation constraint into apply prompt
        extra_parts = []
        if ec.custom_instructions:
            extra_parts.append(f"ADDITIONAL INSTRUCTIONS:\n{ec.custom_instructions}")
        if citable_eo_list:
            extra_parts.append(
                f"{citable_eo_list}\n\n"
                "CITATION CONSTRAINT:\n"
                "- You may ONLY reference Executive Orders from the CITABLE list above.\n"
                "- Do NOT introduce any Executive Order from your own knowledge.\n"
                "- Do NOT remove or alter existing citation links."
            )
        if extra_parts:
            apply_prompt = apply_prompt.rstrip() + "\n\n" + "\n\n".join(extra_parts) + "\n"

        revised_text, usage_2 = self._generate_with_usage(ec, apply_prompt)
        total_input_tokens += usage_2.get("input", 0)
        total_output_tokens += usage_2.get("output", 0)
        total_thinking_tokens += usage_2.get("thinking", 0)
        self._log_usage("apply", usage_2, ec.model)

        if revised_text and revised_text.strip():
            revised_text = self._strip_code_fencing(revised_text)
            chapter_path.write_text(revised_text, encoding="utf-8")
            self._logger.info("  Updated %s (%s chars)",
                              target_entry["filename"], f"{len(revised_text):,}")
        else:
            self._logger.warning("  Empty apply response — keeping original.")
            return

        cost = self._calculate_cost(
            ec.model, total_input_tokens, total_output_tokens,
        )
        self._logger.info("  ─── Cost Summary ───")
        self._logger.info("    Model:           %s", ec.model)
        self._logger.info("    Input tokens:    %s", f"{total_input_tokens:,}")
        self._logger.info("    Output tokens:   %s", f"{total_output_tokens:,}")
        self._logger.info("    Thinking tokens: %s", f"{total_thinking_tokens:,}")
        self._logger.info("    Estimated cost:  $%.4f", cost)

    def _build_targeted_review_prompt(
        self,
        chapter_text: str,
        editorial_principles: str,
        style_rules: str,
        chapter_entry: dict,
        custom_instructions: str | None,
        citable_eo_list: str = "",
    ) -> str:
        """Build the REVIEW prompt for a single-chapter editorial assessment."""
        parts = [
            "You are performing an editorial review of a single chapter "
            "within a published analysis document.\n\n",
            f"EDITORIAL PRINCIPLES:\n{editorial_principles}\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
            f"CHAPTER: {chapter_entry.get('title', chapter_entry['filename'])}\n\n",
            f"CHAPTER TEXT:\n{chapter_text}\n\n",
        ]

        if custom_instructions:
            parts.append(
                f"ADDITIONAL CONTEXT:\n{custom_instructions}\n\n"
            )

        if citable_eo_list:
            parts.extend([
                f"{citable_eo_list}\n\n",
                "CITATION CONSTRAINT:\n"
                "- This chapter may ONLY reference Executive Orders from the "
                "CITABLE list above.\n"
                "- Flag any Executive Order reference that does NOT appear in "
                "that list as a \"hallucinated_reference\" edit of severity high.\n\n",
            ])

        parts.append(
            "Perform a thorough editorial review of this chapter. Assess:\n"
            "1. Stylistic consistency within the chapter\n"
            "2. Whether facts are logically connected to conclusions\n"
            "3. Whether there is confirmation bias in the arguments\n"
            "4. Whether alternative hypotheses have been considered\n"
            "5. Whether the protective-function framing is consistent and respectful\n"
            "6. Whether the argument flows logically within the chapter\n"
            "7. Whether clear principles and facts are established from sources\n\n"
            "Return a JSON object with exactly this structure:\n"
            "{\n"
            '  "document_score": {\n'
            '    "stylistic_consistency": <1-10>,\n'
            '    "argument_cohesiveness": <1-10>,\n'
            '    "bias_risk": <1-10, where 1=low risk, 10=high risk>,\n'
            '    "logical_coherence": <1-10>\n'
            "  },\n"
            '  "executive_summaries": [\n'
            '    {"chapter": 1, "summary": "Clear, lucid 2-3 sentence highlight..."}\n'
            "  ],\n"
            '  "edits": [\n'
            "    {\n"
            '      "chapter": 1,\n'
            '      "section": "<section heading where issue appears>",\n'
            '      "issue": "<description of the problem>",\n'
            '      "recommendation": "<specific edit recommendation>",\n'
            '      "severity": "<high|medium|low>",\n'
            '      "type": "<stylistic_consistency|logical_coherence|confirmation_bias|'
            'alternative_hypothesis|protective_function|argument_flow>"\n'
            "    },\n"
            "    ...\n"
            "  ]\n"
            "}\n\n"
            "IMPORTANT:\n"
            "- Return ONLY valid JSON. Do not include markdown fencing or explanatory text.\n"
            "- Only recommend edits where there is a genuine issue. Do not invent problems.\n"
            "- Respect the protective-function premise: the government acts in the people's interest.\n"
            "- Guard against confirmation bias: flag unsupported conclusions.\n"
            "- Consider alternative hypotheses and note where they should be addressed.\n"
        )

        return "".join(parts)

    def _build_targeted_apply_prompt(
        self,
        chapter_text: str,
        style_rules: str,
        editorial_principles: str,
        custom_instructions: str | None,
    ) -> str:
        """Build a targeted editorial prompt for a single chapter (legacy)."""
        parts = [
            "You are performing a targeted editorial revision of a single chapter "
            "within a published analysis document.\n\n",
            f"EDITORIAL PRINCIPLES:\n{editorial_principles}\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
            "RULES:\n",
            "- Preserve ALL existing content, citations, references, and meaning\n",
            "- Do NOT remove or alter any citation links "
            "(e.g. [Exec. Order No. ...](url))\n",
            "- Maintain the same # heading for the chapter title\n",
            "- Improve readability and structural clarity\n",
            "- Ensure consistent tone and style\n",
            "- Do NOT add numeric prefixes to headings\n\n",
        ]

        if custom_instructions:
            parts.append(f"SPECIFIC INSTRUCTIONS:\n{custom_instructions}\n\n")

        parts.extend([
            f"CHAPTER MARKDOWN:\n{chapter_text}\n\n",
            "Return ONLY the revised chapter markdown. "
            "No explanations, no code fencing.",
        ])

        return "".join(parts)

    # -- LLM generation -----------------------------------------------------

    def _generate_with_usage(
        self, ec: "EditConfig", prompt: str,
    ) -> tuple[str, dict]:
        """Call the configured LLM provider and return (text, usage_dict).

        Supports dual-provider: OpenAI (Responses API) or Gemini.
        Checks Redis cache first; stores responses on miss.
        """
        # --- Redis cache check ---
        cached = _llm_cache_get(ec.model, prompt)
        if cached is not None:
            self._logger.info("    ⚡ Cache hit — skipping LLM call")
            return cached, {"input": 0, "output": 0, "thinking": 0}

        import asyncio

        from src.models.publisher import EditConfig as _EC  # noqa: F811

        usage: dict = {"input": 0, "output": 0, "thinking": 0}

        if ec.provider == "openai":
            from src._clients.llm_openai_client import OpenAILLMClient

            client = OpenAILLMClient(model_name=ec.model)
            max_tokens = 16_000

            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(
                    client.client.responses.create(
                        model=ec.model,
                        input=prompt,
                        reasoning={"effort": ec.reasoning_effort},
                        max_output_tokens=max_tokens,
                    )
                )
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

            text = response.output_text if hasattr(response, "output_text") else ""
            resp_usage = getattr(response, "usage", None)
            if resp_usage:
                usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
                usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = getattr(details, "reasoning_tokens", 0) or 0

            if text:
                _llm_cache_set(ec.model, prompt, text)
            return text, usage

        else:
            # Gemini provider
            from google.genai import types as genai_types

            from src._clients.llm_gcp_client import GeminiLLMClient

            client = GeminiLLMClient(model_name=ec.model, location=_gemini_location(ec.model))
            max_tokens = 16_000

            config = genai_types.GenerateContentConfig(
                temperature=0.2,
                top_p=client.top_p,
                max_output_tokens=max_tokens,
            )
            response = client.client.models.generate_content(
                model=client.model_name,
                contents=prompt,
                config=config,
            )
            text = response.text if response.text else ""
            um = getattr(response, "usage_metadata", None)
            if um:
                usage["input"] = getattr(um, "prompt_token_count", 0) or 0
                usage["output"] = getattr(um, "candidates_token_count", 0) or 0
                usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0

            if text:
                _llm_cache_set(ec.model, prompt, text)
            return text, usage

    # -- prompt builders ----------------------------------------------------

    def _build_review_prompt(
        self,
        document_context: str,
        editorial_principles: str,
        style_rules: str,
        chapter_entries: list[dict],
    ) -> str:
        """Build the REVIEW prompt for full-document editorial assessment."""
        chapter_list = "\n".join(
            f"  Chapter {i}: {e.get('title', e['filename'])}"
            for i, e in enumerate(chapter_entries, 1)
        )

        return (
            "You are performing a final editorial review of a published analysis document.\n\n"
            f"EDITORIAL PRINCIPLES:\n{editorial_principles}\n\n"
            f"STYLE CONVENTIONS:\n{style_rules}\n\n"
            f"CHAPTERS:\n{chapter_list}\n\n"
            f"FULL DOCUMENT:\n{document_context}\n\n"
            "Perform a thorough editorial review. For each chapter, assess:\n"
            "1. Stylistic consistency with the rest of the document\n"
            "2. Whether facts are logically connected to conclusions\n"
            "3. Whether there is confirmation bias in the arguments\n"
            "4. Whether alternative hypotheses have been considered\n"
            "5. Whether the protective-function framing is consistent and respectful\n"
            "6. Whether the argument flows logically from chapter to chapter\n"
            "7. Whether clear principles and facts are established from sources\n\n"
            "Return a JSON object with exactly this structure:\n"
            "{\n"
            '  "document_score": {\n'
            '    "stylistic_consistency": <1-10>,\n'
            '    "argument_cohesiveness": <1-10>,\n'
            '    "bias_risk": <1-10, where 1=low risk, 10=high risk>,\n'
            '    "logical_coherence": <1-10>\n'
            "  },\n"
            '  "executive_summaries": [\n'
            '    {"chapter": 1, "summary": "Clear, lucid 2-3 sentence highlight of findings..."},\n'
            "    ...\n"
            "  ],\n"
            '  "edits": [\n'
            "    {\n"
            '      "chapter": <chapter number>,\n'
            '      "section": "<section heading where issue appears>",\n'
            '      "issue": "<description of the problem>",\n'
            '      "recommendation": "<specific edit recommendation>",\n'
            '      "severity": "<high|medium|low>",\n'
            '      "type": "<stylistic_consistency|logical_coherence|confirmation_bias|'
            'alternative_hypothesis|protective_function|argument_flow>"\n'
            "    },\n"
            "    ...\n"
            "  ]\n"
            "}\n\n"
            "IMPORTANT:\n"
            "- Return ONLY valid JSON. Do not include markdown fencing or explanatory text.\n"
            "- Include an executive summary for EVERY chapter.\n"
            "- Only recommend edits where there is a genuine issue. Do not invent problems.\n"
            "- Respect the protective-function premise: the government acts in the people's interest.\n"
            "- Guard against confirmation bias: flag unsupported conclusions.\n"
            "- Consider alternative hypotheses and note where they should be addressed.\n"
        )

    def _build_apply_prompt(
        self,
        chapter_text: str,
        chapter_edits: list[dict],
        executive_summary: str,
        include_executive_summary: bool,
    ) -> str:
        """Build the APPLY prompt to revise a single chapter."""
        parts = [
            "Apply the following editorial edits to this chapter.\n\n",
            "RULES:\n",
            "- Preserve ALL existing content, citations, references, and meaning\n",
            "- Only make the specific changes recommended in the edit log\n",
            "- Do NOT add numeric prefixes to headings\n",
            "- Use exactly ONE # heading per chapter (keep the existing one)\n",
            "- Maintain the same markdown structure and formatting\n",
            "- Do NOT remove or alter any citation links\n\n",
        ]

        if chapter_edits:
            parts.append(f"EDIT LOG:\n{json.dumps(chapter_edits, indent=2)}\n\n")
        else:
            parts.append("EDIT LOG: No structural edits needed.\n\n")

        if include_executive_summary and executive_summary:
            parts.extend([
                "EXECUTIVE SUMMARY TO PREPEND:\n",
                "After the chapter's # heading, insert a blockquote executive summary.\n",
                "Format it as:\n",
                "> **Executive Summary:** [summary text]\n\n",
                f"Summary text: {executive_summary}\n\n",
            ])

        parts.extend([
            f"CHAPTER MARKDOWN:\n{chapter_text}\n\n",
            "Return ONLY the revised chapter markdown. No explanations.",
        ])

        return "".join(parts)

    # -- context & style helpers --------------------------------------------

    def _build_document_context(
        self,
        chapters_dir: Path,
        chapter_entries: list[dict],
    ) -> str:
        """Concatenate all chapter text including conclusion."""
        parts: list[str] = []
        for entry in chapter_entries:
            path = chapters_dir / entry["filename"]
            if path.exists():
                parts.append(path.read_text(encoding="utf-8"))
        return "\n\n---\n\n".join(parts)

    def _load_style_rules(self, path: Path) -> str:
        """Extract STYLE CONVENTIONS section from the style guide."""
        if not path.exists():
            self._logger.warning("Style guide not found: %s", path)
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r'(## STYLE CONVE[NT]+IONS.*?)(?=\n## |\Z)',
            text, re.DOTALL,
        )
        return match.group(1).strip() if match else text.strip()

    def _load_editorial_principles(self, path: Path) -> str:
        """Extract EDITORIAL PRINCIPLES section from the style guide."""
        if not path.exists():
            self._logger.warning("Style guide not found: %s", path)
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r'(## EDITORIAL PRINCIPLES.*?)(?=\n## |\Z)',
            text, re.DOTALL,
        )
        return match.group(1).strip() if match else ""

    @staticmethod
    def _is_llm_generated_chapter(entry: dict, filename: str) -> bool:
        """Return True if a chapter is LLM-generated (intro, conclusion, etc.)."""
        source = entry.get("source", "")
        if source in ("generated", "llm"):
            return True
        base = filename.split("-")[0] if "-" in filename else ""
        if base in ("00", "16", "17", "18", "19", "99"):
            return True
        if any(kw in filename for kw in ("conclusion", "introduction", "appendix", "synthesis")):
            return True
        return False

    def _build_citable_eo_list(
        self, chapters_dir: Path, chapter_entries: list[dict],
    ) -> str:
        """Build a formatted list of EOs found in source chapters.

        Scans non-LLM chapters for linked EO references so the LLM knows
        exactly which Executive Orders it may reference during edits.
        """
        pattern = re.compile(
            r'\[Exec\. Order No\. ([\d,]+),\s*(\d{4})\]'
            r'\((https://[^\)]+)\)',
        )
        pattern_apa = re.compile(
            r'\[Exec\. Order No\. ([\d,]+),\s+[^\]]+?\((\d{4})\)\]'
            r'\((https://[^\)]+)\)',
        )
        pattern_plain = re.compile(
            r'Executive Order\s+(\d{4,5})[,:.]*\s*["\u201c]([^"\u201d]+)["\u201d]',
            re.IGNORECASE,
        )
        eos: dict[str, dict] = {}
        for entry in chapter_entries:
            if self._is_llm_generated_chapter(entry, entry.get("filename", "")):
                continue
            path = chapters_dir / entry["filename"]
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in pattern.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2)}
            for m in pattern_apa.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2)}
            for m in pattern_plain.finditer(text):
                num_raw = m.group(1)
                title_frag = m.group(2).strip().rstrip(".")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": num_raw, "year": "", "title": title_frag}

        if not eos:
            return ""

        lines = []
        for num in sorted(eos, key=lambda x: int(x)):
            info = eos[num]
            year_part = f" ({info['year']})" if info.get("year") else ""
            title_part = f' — "{info["title"]}"' if info.get("title") else ""
            lines.append(f"  - Exec. Order No. {info['formatted']}{year_part}{title_part}")

        return (
            "CITABLE EXECUTIVE ORDERS (exhaustive list):\n"
            + "\n".join(lines)
        )

    def _parse_edit_log(self, response_text: str) -> dict | None:
        """Parse JSON edit log from LLM response, handling code fences."""
        text = response_text.strip()
        # Strip code fences if present
        if text.startswith("```"):
            first_nl = text.index("\n") if "\n" in text else len(text)
            text = text[first_nl + 1:]
        if text.endswith("```"):
            text = text[:-3].rstrip()

        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            self._logger.warning("  JSON parse error in edit log: %s", e)
            # Try to extract JSON from response (LLM may have added text around it)
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    pass
            return None

    # -- shared helpers -----------------------------------------------------

    @staticmethod
    def _strip_code_fencing(text: str) -> str:
        """Strip leading/trailing markdown code fences if present."""
        stripped = text.strip()
        if stripped.startswith("```"):
            first_nl = stripped.index("\n") if "\n" in stripped else len(stripped)
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3].rstrip()
        return stripped

    def _log_usage(self, label: str, usage: dict, model: str) -> None:
        cost = self._calculate_cost(model, usage["input"], usage["output"])
        self._logger.info(
            "    %s — in: %s  out: %s  think: %s  ($%.4f)",
            label,
            f"{usage['input']:,}",
            f"{usage['output']:,}",
            f"{usage['thinking']:,}",
            cost,
        )

    def _calculate_cost(
        self, model: str, input_tokens: int, output_tokens: int,
    ) -> float:
        rates = self._PRICING.get(model, {"input": 2.50, "output": 10.00})
        return (
            input_tokens / 1_000_000 * rates["input"]
            + output_tokens / 1_000_000 * rates["output"]
        )


# ---------------------------------------------------------------------------
# Phase 7 — Citation Resolver
# ---------------------------------------------------------------------------

class CiteStage(Base):
    """Resolve unresolved citation tokens and normalise EO links to APA.

    Scans for raw ``(doc_HEXID, eo)`` / ``(doc_HEXID, qa-N)`` /
    ``(req_TIMESTAMP, summary)`` tokens that survived the chapter, format,
    and edit stages.  For EO-sourced citations, renders them in APA style
    (e.g. ``[Exec. Order No. 14,148, Initial Rescissions… (2025)](url)``
    on first mention, ``[Exec. Order No. 14,148 (2025)](url)`` subsequently).

    Also normalises already-resolved Federal Register links (full titles,
    truncated titles, author/date, bare-URL labels) to the same APA format.
    """

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # -- public -------------------------------------------------------------

    def run(self) -> None:
        cc = self.cfg.cite
        if not cc.enabled:
            self._logger.info("  Citation resolver disabled — skipping.")
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to cite-resolve.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Build lookup tables (needed by both primary and downstream passes)
        all_cm = self._build_merged_citation_map()
        eo_title_index = self._build_eo_title_index(all_cm)

        # Idempotent — skip primary resolution if already done
        primary_done = bool(manifest.get("cite", {}).get("resolved")) and not self.cfg.force
        if primary_done:
            self._logger.info("  Primary citation resolution already done — skipping.")
        else:
            self._logger.info(
                "  Citation resolver: %d citation_map entries, %d EO title mappings",
                len(all_cm), len(eo_title_index),
            )

            if self.dry_run:
                self._logger.info("  (dry-run) would resolve citations in %d chapters", len(chapter_entries))
                return

            # Write EO title index for debugging
            parsed_dir = self.cfg.build_dir / "parsed"
            eo_index_path = parsed_dir / "eo_title_index.json"
            eo_index_path.write_text(
                json.dumps(eo_title_index, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            self._logger.info("  Wrote eo_title_index.json (%d entries)", len(eo_title_index))

            # Resolve citations in each chapter
            total_tokens = 0
            total_resolved = 0
            total_eo = 0
            total_web = 0
            total_normalised = 0
            total_deduped = 0
            total_consolidated = 0
            total_sec_merged = 0
            chapters_modified = 0

            for entry in chapter_entries:
                fname = entry["filename"]
                ch_path = chapters_dir / fname
                if not ch_path.exists():
                    continue

                # Skip already-resolved chapters (recovery support)
                if entry.get("cite_status") == "done":
                    self._logger.info("    %s — already resolved, skipping", fname)
                    continue

                original = ch_path.read_text(encoding="utf-8")
                result, stats = self._resolve_chapter(original, all_cm, eo_title_index, cc)

                total_tokens += stats["tokens"]
                total_resolved += stats["resolved"]
                total_eo += stats["eo"]
                total_web += stats["web"]
                total_normalised += stats.get("normalised", 0)
                total_deduped += stats.get("deduped", 0)
                total_consolidated += stats.get("consolidated", 0)
                total_sec_merged += stats.get("sec_merged", 0)

                changed = (stats["resolved"] > 0 or stats.get("normalised", 0) > 0
                           or stats.get("deduped", 0) > 0
                           or stats.get("consolidated", 0) > 0
                           or stats.get("sec_merged", 0) > 0)
                if changed:
                    ch_path.write_text(result, encoding="utf-8")
                    chapters_modified += 1
                    self._logger.info(
                        "    %s — %d/%d resolved (%d EO, %d web), "
                        "%d normalised, %d deduped, %d consolidated",
                        fname, stats["resolved"], stats["tokens"],
                        stats["eo"], stats["web"],
                        stats.get("normalised", 0), stats.get("deduped", 0),
                        stats.get("consolidated", 0),
                    )
                else:
                    self._logger.info("    %s — no unresolved tokens", fname)

                entry["cite_status"] = "done"

            # Update manifest
            manifest["cite"] = {
                "resolved": datetime.now(timezone.utc).isoformat(),
                "total_tokens": total_tokens,
                "resolved_count": total_resolved,
                "unresolved_count": total_tokens - total_resolved,
                "eo_citations": total_eo,
                "web_citations": total_web,
                "normalised_links": total_normalised,
                "deduped_sections": total_deduped,
                "consolidated_groups": total_consolidated,
                "sections_merged": total_sec_merged,
                "chapters_modified": chapters_modified,
            }

            self._logger.info(
                "  Citation resolution complete: %d/%d tokens resolved "
                "(%d EO, %d web), %d normalised, %d section-dupes merged, "
                "%d groups consolidated, %d sections inlined, across %d chapters",
                total_resolved, total_tokens, total_eo, total_web,
                total_normalised, total_deduped, total_consolidated,
                total_sec_merged, chapters_modified,
            )

        # ── Downstream (LLM-generated) chapter citation resolution ──
        # Runs after primary resolution so the reverse index includes all
        # ground-truth corrections from the chapter scan.
        self._resolve_downstream_chapters(manifest, chapters_dir, cc)

        # Re-write manifest (includes both cite and cite_downstream blocks)
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    # -- internal -----------------------------------------------------------

    def _build_merged_citation_map(self) -> dict:
        """Merge citation data from conversations, x_indices, and workflow caches.

        Priority (lowest wins on conflict):
            1. x_indices JSONL files (research_summaries + research_qa_pairs)
            2. Workflow cache conversations (media_linking / eo_linking batches)
            3. Raw conversations.json ``citation_map`` (highest fidelity)
        """
        merged: dict = {}

        # ── 1. x_indices JSONL files ──
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        for folder in (self.cfg.cite.index_folders or []):
            idx_dir = project_root / folder
            if not idx_dir.is_dir():
                continue
            # research_summaries: {document_id, source_url, doc_metadata: {title, authors[], publication_date, publisher}}
            summaries_path = idx_dir / "research_summaries_combined.jsonl"
            if summaries_path.exists():
                count = 0
                for line in summaries_path.open(encoding="utf-8"):
                    d = json.loads(line)
                    did = d.get("document_id", "")
                    if not did:
                        continue
                    dm = d.get("doc_metadata") or {}
                    authors = dm.get("authors") or []
                    merged[did] = {
                        "url": d.get("source_url", ""),
                        "author": ", ".join(authors) if authors else "",
                        "date": dm.get("publication_date", ""),
                        "title": dm.get("title", ""),
                        "publisher": dm.get("publisher", ""),
                    }
                    count += 1
                self._logger.info("    index %s/summaries: %d entries", folder, count)

            # research_qa_pairs: {document_metadata: {document_id, source_url, title, author, creation_date}}
            qa_path = idx_dir / "research_qa_pairs_combined.jsonl"
            if qa_path.exists():
                count = 0
                for line in qa_path.open(encoding="utf-8"):
                    d = json.loads(line)
                    dm = d.get("document_metadata") or {}
                    did = dm.get("document_id", "")
                    if not did or did in merged:
                        continue
                    merged[did] = {
                        "url": dm.get("source_url", ""),
                        "author": dm.get("author", ""),
                        "date": dm.get("creation_date", ""),
                        "title": dm.get("title", ""),
                    }
                    count += 1
                self._logger.info("    index %s/qa_pairs: %d new entries", folder, count)

        # ── 2. Workflow cache conversations ──
        for wf_dir_str in (self.cfg.cite.workflow_dirs or []):
            wf_dir = Path(wf_dir_str)
            if not wf_dir.is_dir():
                continue
            count = 0
            for batch in sorted(wf_dir.iterdir()):
                if not batch.is_dir():
                    continue
                for fname in ("conversations.json", "conversation.json"):
                    conv_path = batch / fname
                    if not conv_path.exists():
                        continue
                    data = json.loads(conv_path.read_text(encoding="utf-8"))
                    convs = data if isinstance(data, list) else [data]
                    for c in convs:
                        meta = c.get("metadata") or {}
                        did = meta.get("document_id", "")
                        if not did or did in merged:
                            continue
                        authors = meta.get("authors") or []
                        merged[did] = {
                            "url": meta.get("article_url", ""),
                            "author": ", ".join(authors) if isinstance(authors, list) else str(authors),
                            "date": meta.get("date", ""),
                            "title": meta.get("article_title", ""),
                            "publisher": meta.get("publisher", ""),
                        }
                        count += 1
            self._logger.info("    workflow %s: %d new entries", wf_dir.name, count)

        # ── 3. Raw conversations.json (highest priority — overwrites) ──
        raw_dir = self.cfg.build_dir / "raw"
        conv_path = raw_dir / "conversations.json"
        if conv_path.exists():
            conversations = json.loads(conv_path.read_text(encoding="utf-8"))
            count = 0
            for conv in conversations:
                cm = (conv.get("metadata") or {}).get("citation_map") or {}
                merged.update(cm)
                count += len(cm)
            self._logger.info("    conversations.json: %d citation_map entries", count)

        if not merged:
            self._logger.warning("No citation data found from any source.")

        return merged

    def _build_eo_title_index(self, citation_map: dict) -> dict:
        """Build ``{doc_id → {title, url, eo_number, year}}`` from EO metadata.

        Cross-references ``eo_results`` in conversations against citation_map
        entries by URL to map ``doc_*`` IDs to EO titles.  Also stores
        ``_url_to_eo`` on *self* for the normalisation pass.
        """
        raw_dir = self.cfg.build_dir / "raw"
        conv_path = raw_dir / "conversations.json"
        if not conv_path.exists():
            self._url_to_eo: dict[str, dict] = {}
            return {}

        conversations = json.loads(conv_path.read_text(encoding="utf-8"))

        # Collect URL → EO info from eo_results across all conversations
        url_to_eo: dict[str, dict] = {}
        for conv in conversations:
            for eo in conv.get("eo_results") or []:
                url = eo.get("source_url", "")
                dm = eo.get("doc_metadata") or {}
                title = dm.get("title", "")
                if url and title:
                    norm = _normalise_url(url)
                    # Prefer eo_ids field (reliable); fall back to title regex
                    eo_ids = eo.get("eo_ids") or []
                    eo_number = eo_ids[0] if eo_ids else self._extract_eo_number(title)
                    pub_date = dm.get("publication_date", "")
                    year = pub_date[:4] if pub_date else ""
                    url_to_eo[norm] = {
                        "title": title,
                        "eo_number": eo_number,
                        "year": year,
                        "url": url,
                    }

        # Fix / fill eo_number values by scanning chapter text for
        # authoritative patterns like  Executive Order 14265, "Title…"
        # This corrects wrong eo_ids from source data AND fills missing ones.
        _eo_title_re = re.compile(
            r'Executive Order\s+(\d{4,5})[,:.]*\s*"([^"]+)"', re.IGNORECASE,
        )
        chapters_dir = self.cfg.build_dir / "chapters"
        if chapters_dir.is_dir():
            # Build title → EO number map from all chapter text (ground truth)
            title_to_num: dict[str, str] = {}
            for ch_path in sorted(chapters_dir.glob("[0-9]*.md")):
                for mt in _eo_title_re.finditer(ch_path.read_text(encoding="utf-8")):
                    title_to_num[mt.group(2).strip().rstrip(".")] = mt.group(1)

            for info in url_to_eo.values():
                # Try exact match, then match after stripping trailing period
                canon = info["title"].rstrip(".")
                num = title_to_num.get(info["title"]) or title_to_num.get(canon)
                if num and num != info.get("eo_number"):
                    info["eo_number"] = num

        # Expose for the normalisation pass
        self._url_to_eo = url_to_eo

        def _make_entry(eo_info: dict) -> dict:
            return {
                "title": eo_info["title"],
                "eo_number": eo_info["eo_number"],
                "year": eo_info["year"],
                "url": eo_info["url"],
            }

        # Match doc_* citation_map entries to EO titles by URL
        index: dict[str, dict] = {}
        for key, entry in citation_map.items():
            if not re.match(r"^doc_[0-9a-f]+$", key):
                continue
            url = entry.get("url", "")
            if not url:
                continue
            norm = _normalise_url(url)
            eo_info = url_to_eo.get(norm)
            if eo_info:
                index[key] = _make_entry(eo_info)

        # Also map req_* keys that are EO-type
        for key, entry in citation_map.items():
            if not re.match(r"^req_\w+$", key):
                continue
            if entry.get("card_type") != "eo":
                continue
            url = entry.get("url", "")
            if not url:
                continue
            norm = _normalise_url(url)
            eo_info = url_to_eo.get(norm)
            if eo_info:
                index[key] = _make_entry(eo_info)

        return index

    @staticmethod
    def _extract_eo_number(title: str) -> str:
        """Extract an EO number from a title string.

        Looks for 5-digit numbers (e.g. 14151) or "Executive Order NNNNN".
        Falls back to empty string.
        """
        m = re.search(r"Executive Order\s+(\d+)", title, re.IGNORECASE)
        if m:
            return m.group(1)
        # Check for standalone 5-digit EO number (e.g. "12396")
        m = re.search(r"\b(1[2-9]\d{3}|[2-9]\d{4})\b", title)
        if m:
            return m.group(1)
        return ""

    def _resolve_chapter(
        self,
        text: str,
        citation_map: dict,
        eo_title_index: dict,
        cc: CiteConfig,
    ) -> tuple[str, dict]:
        """Resolve citation tokens in a single chapter.

        Returns ``(resolved_text, stats_dict)``.
        """
        stats = {"tokens": 0, "resolved": 0, "eo": 0, "web": 0, "normalised": 0}
        seen_eo_urls: set[str] = set()   # first-mention tracking per chapter

        def _eo_link(eo_info: dict, url: str) -> str:
            """Build an APA-formatted EO markdown link, tracking first mention."""
            norm = _normalise_url(url)
            is_first = norm not in seen_eo_urls
            if is_first:
                seen_eo_urls.add(norm)
            label = self._format_eo_label(
                eo_info["title"],
                eo_info["eo_number"],
                cc.eo_name_max_chars,
                year=eo_info.get("year", ""),
                first_mention=is_first and cc.apa_first_mention_title,
            )
            return f"[{label}]({url})"

        def _replace(m: re.Match) -> str:
            group_text = m.group(1)
            parts = [p.strip() for p in group_text.split(";") if p.strip()]
            links: list[str | None] = []

            for part in parts:
                stats["tokens"] += 1
                key = _cite_token_to_cm_key(part)

                # Try EO title index first (for rich EO formatting)
                eo_info = eo_title_index.get(key) if key else None
                if eo_info:
                    links.append(_eo_link(eo_info, eo_info["url"]))
                    stats["resolved"] += 1
                    stats["eo"] += 1
                    continue

                # Try citation_map (direct key or fallback base key)
                entry = citation_map.get(key) if key else None
                if not entry:
                    base = (key or "").split("__")[0]
                    entry = citation_map.get(base)

                if not entry:
                    links.append(None)
                    continue

                # Check if this is a Federal Register URL → try EO format
                url = entry.get("url", "")
                if "federalregister.gov" in url:
                    # Try to find EO info by URL match
                    norm = _normalise_url(url)
                    matched = False
                    for _eo_key, eo_info in eo_title_index.items():
                        if _normalise_url(eo_info["url"]) == norm:
                            links.append(_eo_link(eo_info, url))
                            stats["resolved"] += 1
                            stats["eo"] += 1
                            matched = True
                            break
                    if matched:
                        continue
                    # Federal Register but no EO title → use generic format
                    author = entry.get("author", "Federal Register")
                    from src.components.publisher.rapporteur_docs import \
                        _format_date_month_year
                    date = _format_date_month_year(entry.get("date", ""))
                    label = f"{author}, {date}" if date else author
                    links.append(f"[{label}]({url})")
                    stats["resolved"] += 1
                    stats["web"] += 1
                    continue

                # Web citation
                if cc.resolve_web_citations and url:
                    from src.components.publisher.rapporteur_docs import (
                        _format_date_month_year, _short_author)
                    author = _short_author(entry.get("author") or "")
                    date = _format_date_month_year(entry.get("date", ""))
                    label = f"{author}, {date}" if author and date else (author or date or "?")
                    links.append(f"[{label}]({url})")
                    stats["resolved"] += 1
                    stats["web"] += 1
                else:
                    links.append(None)

            # If every token failed, leave the original group intact
            if all(lnk is None for lnk in links):
                return m.group(0)

            resolved_parts = []
            for lnk, part in zip(links, parts):
                resolved_parts.append(lnk if lnk is not None else part)

            return "(" + "; ".join(resolved_parts) + ")"

        resolved = _CITE_GROUP_RE.sub(_replace, text)

        # ── Normalise already-resolved Federal Register links to APA ──
        normalised, n_count = self._normalise_existing_eo_links(
            resolved, cc, seen_eo_urls,
        )
        stats["normalised"] = n_count

        # ── Deduplicate Section-reference patterns ──
        deduped, d_count = self._dedup_section_citations(normalised)
        stats["deduped"] = d_count

        # ── Consolidate adjacent parentheticals & remove same-EO repeats ──
        consolidated, c_count = self._consolidate_adjacent_citations(deduped)
        stats["consolidated"] = c_count

        # ── Merge trailing Section refs into the parenthetical ──
        sec_merged, s_count = self._merge_trailing_sections(consolidated)
        stats["sec_merged"] = s_count

        return sec_merged, stats

    # -- APA normalisation of pre-existing markdown links -------------------

    # Matches markdown links whose URL points to federalregister.gov
    _FR_LINK_RE = re.compile(
        r'\[([^\]]+)\]'                     # [link text]
        r'\((https?://(?:www\.)?'            # (https://www. or https://
        r'federalregister\.gov/[^)]+)\)',     # federalregister.gov/...)
    )

    def _normalise_existing_eo_links(
        self,
        text: str,
        cc: CiteConfig,
        seen_eo_urls: set[str],
    ) -> tuple[str, int]:
        """Rewrite existing ``[...](federalregister.gov/...)`` links to APA.

        Handles all the common label formats produced by ChapterStage:
        full titles, truncated titles, author/date combos, bare URLs, etc.

        Respects the per-chapter *seen_eo_urls* set so first-mention logic
        stays consistent even for links that were resolved before CiteStage.

        Returns ``(normalised_text, rewrite_count)``.
        """
        url_to_eo = getattr(self, "_url_to_eo", {})
        if not url_to_eo:
            return text, 0

        # Per-chapter refinement: extract EO numbers from this chapter's
        # text to fix/fill any remaining eo_number gaps (supplements the
        # global pre-scan in _build_eo_title_index).
        _eo_title_re = re.compile(
            r'Executive Order\s+(\d{4,5})[,:.]*\s*"([^"]+)"', re.IGNORECASE,
        )
        title_to_num: dict[str, str] = {}
        for mt in _eo_title_re.finditer(text):
            title_to_num[mt.group(2).strip().rstrip(".")] = mt.group(1)

        if title_to_num:
            for info in url_to_eo.values():
                canon = info["title"].rstrip(".")
                num = title_to_num.get(info["title"]) or title_to_num.get(canon)
                if num and num != info.get("eo_number"):
                    info["eo_number"] = num

        count = 0

        def _rewrite(m: re.Match) -> str:
            nonlocal count
            old_label = m.group(1)
            url = m.group(2)
            norm = _normalise_url(url)
            eo_info = url_to_eo.get(norm)
            if not eo_info:
                return m.group(0)  # Not a known EO — leave as-is

            # Skip if already in correct APA format WITH the right EO number
            if old_label.startswith("Exec. Order No.") and re.search(r", \d{4}(, Sections? .+)?$", old_label):
                # Verify the EO number matches — eo_ids can be wrong
                fmt_num = CiteStage._format_eo_number(eo_info["eo_number"])
                if fmt_num and f"Exec. Order No. {fmt_num}" in old_label:
                    # Already correct — but still track for first-mention
                    seen_eo_urls.add(norm)
                    return m.group(0)
                # Wrong EO number — fall through to rewrite

            is_first = norm not in seen_eo_urls
            if is_first:
                seen_eo_urls.add(norm)

            # Preserve any trailing ", Section(s) …" suffix from old label
            sec_suffix = ""
            sec_m = re.search(r',\s*(Sections?\s+.+)$', old_label)
            if sec_m:
                sec_suffix = ", " + sec_m.group(1)

            label = self._format_eo_label(
                eo_info["title"],
                eo_info["eo_number"],
                cc.eo_name_max_chars,
                year=eo_info.get("year", ""),
                first_mention=is_first and cc.apa_first_mention_title,
            )
            label += sec_suffix
            if label != old_label:
                count += 1
            return f"[{label}]({url})"

        result = self._FR_LINK_RE.sub(_rewrite, text)
        return result, count

    # -- Deduplicate redundant Section-reference citations ------------------

    # Section-number sub-pattern: handles 2, 2(c), 3(b)(i), 1-2, 6(c), 6(f), etc.
    _SEC_ITEM = r'\d+(?:\([a-zA-Z0-9]+\))*(?:[-\u2013]\d+(?:\([a-zA-Z0-9]+\))*)?'
    _SEC_LIST = _SEC_ITEM + r'(?:,\s*' + _SEC_ITEM + r')*'
    _SEC_REF  = r'Sections?\s+' + _SEC_LIST

    # Matches: ([link](url)), Section(s) X[, Y]; ([link](url))
    _DEDUP_SECTION_RE = re.compile(
        r'\('
        r'(\[[^\]]+\])'                         # group 1: first link text
        r'\((https?://[^\s)]+)\)'               # group 2: first URL
        r'\)'
        r',?\s*'
        r'(' + _SEC_REF + r')'                     # group 3: section ref(s)
        r'\s*;\s*'
        r'\('
        r'\[[^\]]+\]'                            # second link text (not captured)
        r'\((https?://[^\s)]+)\)'               # group 4: second URL
        r'\)',
    )

    def _dedup_section_citations(self, text: str) -> tuple[str, int]:
        """Collapse ``([EO](url)), Section X; ([EO](url))`` → ``([EO, Section X](url))``.

        When the same EO is cited twice with a Section reference between them,
        merge into a single citation with Section inside the link text.

        Returns ``(text, dedup_count)``.
        """
        count = 0

        def _merge(m: re.Match) -> str:
            nonlocal count
            url1, url2 = m.group(2), m.group(4)
            if _normalise_url(url1) != _normalise_url(url2):
                return m.group(0)  # Different EOs — leave as-is
            count += 1
            link_text = m.group(1)  # [Exec. Order No. ...]
            return f"({link_text[:-1]}, {m.group(3)}]({url1}))"

        result = self._DEDUP_SECTION_RE.sub(_merge, text)
        return result, count

    # -- Consolidate adjacent parenthetical citations -----------------------

    # Matches a single parenthetical EO citation: ([...](url))
    _SINGLE_EO_PAREN_RE = re.compile(
        r'\('
        r'(\[Exec\. Order No\.[^\]]+\]\(https?://[^\s)]+\))'  # link
        r'\)',
    )

    def _consolidate_adjacent_citations(self, text: str) -> tuple[str, int]:
        """Merge adjacent ``(EO1); (EO2)`` into ``(EO1; EO2)`` and deduplicate.

        Handles chains of any length.  After merging, removes duplicate
        citations for the same EO within the consolidated group (keeps the
        first/richest occurrence).

        Returns ``(text, merge_count)``.
        """
        # Step 1: merge adjacent parentheticals into single groups.
        # Match runs of: (EO_LINK);? (EO_LINK) ...
        run_pat = re.compile(
            r'\('
            r'(\[Exec\. Order No\.[^\]]+\]\(https?://[^\s)]+\))'
            r'\)'
            r'(?:'
            r'\s*[;,]?\s*'
            r'\('
            r'(\[Exec\. Order No\.[^\]]+\]\(https?://[^\s)]+\))'
            r'\)'
            r')+'
        )

        count = 0

        def _merge_run(m: re.Match) -> str:
            nonlocal count
            # Re-extract all individual citations from the matched run
            items = list(self._SINGLE_EO_PAREN_RE.finditer(m.group(0)))
            if len(items) < 2:
                return m.group(0)

            links = [item.group(1) for item in items]

            # Step 2: deduplicate — extract the EO number from each link
            # and keep only the first (richest) occurrence of each number
            seen_nums: dict[str, str] = {}  # eo_number → link text
            unique: list[str] = []
            num_pat = re.compile(r'\[Exec\. Order No\. ([\d,]+)')
            for lnk in links:
                nm = num_pat.search(lnk)
                eo_num = nm.group(1) if nm else lnk
                if eo_num not in seen_nums:
                    seen_nums[eo_num] = lnk
                    unique.append(lnk)

            count += 1
            return "(" + "; ".join(unique) + ")"

        result = run_pat.sub(_merge_run, text)
        return result, count

    # -- Merge trailing Section references into the parenthetical -----------

    # Unmerged: section follows OUTSIDE the outer paren — ([link](url)), Section X
    _TRAILING_SEC_RE = re.compile(
        r'\('                                                  # opening (
        r'(\[[^\]]+\])'                                        # group 1: link text [...]
        r'\((https?://[^\s)]+)\)'                              # group 2: URL
        r'\)'                                                  # closing )
        r',?\s*'                                               # optional comma/space
        r'(' + _SEC_REF + r')'                                 # group 3: Section(s) ref
    )

    # Prev-pass format: section inside outer parens but outside link text
    # ([link](url), Section X)
    _SEC_OUTSIDE_LINK_RE = re.compile(
        r'\('                                                  # opening (
        r'(\[[^\]]+\])'                                        # group 1: link text [...]
        r'\((https?://[^\s)]+)\)'                              # group 2: URL
        r',\s*'                                                # comma space
        r'(' + _SEC_REF + r')'                                 # group 3: Section(s) ref
        r'\)'                                                  # closing )
    )

    def _merge_trailing_sections(self, text: str) -> tuple[str, int]:
        """Merge section refs into the link text of EO citations.

        Handles two patterns:
        - ``([EO](url)), Section X`` → section outside parens (unmerged)
        - ``([EO](url), Section X)`` → section inside parens but outside link

        Both become ``([EO, Section X](url))``.

        Returns ``(text, merge_count)``.
        """
        count = 0

        def _merge(m: re.Match) -> str:
            nonlocal count
            count += 1
            link_text = m.group(1)  # [Exec. Order No. ...]
            url = m.group(2)
            section = m.group(3)
            return f"({link_text[:-1]}, {section}]({url}))"

        # Pass A: fix prev-pass format (section outside link text)
        result = self._SEC_OUTSIDE_LINK_RE.sub(_merge, text)
        # Pass B: merge unmerged trailing sections
        result = self._TRAILING_SEC_RE.sub(_merge, result)
        return result, count

    @staticmethod
    def _format_eo_number(eo_number: str) -> str:
        """Format an EO number with comma separators: ``14148`` → ``14,148``."""
        try:
            return f"{int(eo_number):,}"
        except (ValueError, TypeError):
            return eo_number or ""

    @staticmethod
    def _format_eo_label(
        title: str,
        eo_number: str,
        max_chars: int,
        year: str = "",
        first_mention: bool = False,
    ) -> str:
        """Format an APA-style EO citation label.

        First mention::

            Exec. Order No. 14,148, Initial Rescissions of Harmful…, 2025

        Subsequent::

            Exec. Order No. 14,148, 2025

        Falls back to title-only when no EO number is available.
        """
        fmt_num = CiteStage._format_eo_number(eo_number)
        year_part = f", {year}" if year else ""

        if fmt_num:
            if first_mention and title:
                # Include truncated title after the EO number
                prefix = f"Exec. Order No. {fmt_num}, "
                budget = max_chars - len(prefix) - len(year_part)
                if budget > 5 and len(title) > budget:
                    title = title[:budget - 1].rstrip() + "…"
                elif budget <= 5:
                    # Title doesn't fit — fall back to short form
                    return f"Exec. Order No. {fmt_num}{year_part}"
                return f"{prefix}{title}{year_part}"
            else:
                return f"Exec. Order No. {fmt_num}{year_part}"
        else:
            # No EO number — use truncated title only
            if len(title) > max_chars:
                title = title[:max_chars - 1].rstrip() + "…"
            return f"{title}{year_part}"

    # -- Downstream (LLM-generated) chapter citation resolution -------------

    # Matches plain-text "Executive Order NNNNN" NOT already inside a markdown
    # link.  Uses a negative lookbehind for `](` and a negative lookbehind for
    # `[` to avoid rewriting text already wrapped in `[...](...)`.
    _DOWNSTREAM_EO_RE = re.compile(
        r'(?<!\[)'                              # not preceded by [
        r'(Executive Order\s+(\d{4,5}))'        # "Executive Order 14265"
        r'(?![^\[]*\]\()',                       # not followed (before next [) by ](
    )

    def _build_eo_reverse_index(self) -> dict[str, dict]:
        """Build ``{eo_number → {url, title, year}}`` from ``_url_to_eo``.

        This gives us a lookup from plain EO number strings to full metadata,
        enabling resolution of downstream LLM-generated text that references
        EO numbers without hyperlinks.
        """
        url_to_eo = getattr(self, "_url_to_eo", {})
        reverse: dict[str, dict] = {}
        for _norm_url, info in url_to_eo.items():
            eo_num = info.get("eo_number", "")
            if not eo_num:
                continue
            # First entry wins (preserves the ground-truth-corrected value)
            if eo_num not in reverse:
                reverse[eo_num] = {
                    "url": info["url"],
                    "title": info["title"],
                    "year": info.get("year", ""),
                }
        return reverse

    @staticmethod
    def _is_downstream_chapter(entry: dict, filename: str) -> bool:
        """Return *True* if a chapter is LLM-generated (no citation map)."""
        source = entry.get("source", "")
        if source in ("generated", "llm"):
            return True
        # Filename convention fallback
        base = filename.split("-")[0] if "-" in filename else ""
        if base in ("00", "16", "17", "18", "19", "99"):
            return True
        if any(kw in filename for kw in ("conclusion", "introduction", "appendix", "synthesis")):
            return True
        return False

    def _resolve_downstream_chapters(
        self,
        manifest: dict,
        chapters_dir,
        cc: CiteConfig,
    ) -> None:
        """Scan LLM-generated chapters for plain-text EO refs and add links.

        Uses the reverse index built from source chapters as ground truth.
        Writes a structured changelog and updates manifest per-chapter.
        """
        reverse_index = self._build_eo_reverse_index()
        if not reverse_index:
            self._logger.info("  Downstream citation: no EO reverse index — skipping.")
            return

        chapter_entries = manifest.get("chapters", [])
        parsed_dir = self.cfg.build_dir / "parsed"
        parsed_dir.mkdir(parents=True, exist_ok=True)

        total_linked = 0
        total_unresolvable = 0
        total_warnings = 0
        chapters_modified = 0

        for entry in chapter_entries:
            fname = entry["filename"]
            ch_path = chapters_dir / fname

            if not ch_path.exists():
                continue

            # Idempotent — skip if already done
            if entry.get("cite_downstream_status") == "done" and not self.cfg.force:
                self._logger.info("    %s — downstream citation already done, skipping", fname)
                continue

            text = ch_path.read_text(encoding="utf-8")
            result, changelog = self._resolve_downstream_text(
                text, reverse_index, cc, fname,
            )

            linked = changelog["summary"]["linked"]
            unresolvable = changelog["summary"]["unresolvable"]
            warnings = changelog["summary"]["hallucination_warnings"]

            total_linked += linked
            total_unresolvable += unresolvable
            total_warnings += warnings

            if linked > 0:
                ch_path.write_text(result, encoding="utf-8")
                chapters_modified += 1

            # Write per-chapter changelog
            cl_name = fname.replace(".md", "_cite_downstream.json")
            cl_path = parsed_dir / cl_name
            cl_path.write_text(
                json.dumps(changelog, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            entry["cite_downstream_status"] = "done"
            self._logger.info(
                "    %s — downstream: %d linked, %d unresolvable, %d warnings",
                fname, linked, unresolvable, warnings,
            )

        # Update manifest top-level
        manifest["cite_downstream"] = {
            "resolved": datetime.now(timezone.utc).isoformat(),
            "total_linked": total_linked,
            "total_unresolvable": total_unresolvable,
            "hallucination_warnings": total_warnings,
            "chapters_modified": chapters_modified,
        }

        self._logger.info(
            "  Downstream citation complete: %d linked, %d unresolvable, "
            "%d hallucination warnings across %d chapters",
            total_linked, total_unresolvable, total_warnings, chapters_modified,
        )

    def _resolve_downstream_text(
        self,
        text: str,
        reverse_index: dict[str, dict],
        cc: CiteConfig,
        filename: str,
    ) -> tuple[str, dict]:
        """Resolve plain-text EO references in a downstream chapter.

        Returns ``(resolved_text, changelog_dict)``.
        """
        changes: list[dict] = []
        linked_count = 0
        unresolvable_count = 0
        warning_count = 0

        seen_unresolvable: set[str] = set()  # deduplicate changelog entries
        lines = text.split("\n")

        # --- Pass 1: Hallucination check (number ↔ title verification) ---
        _eo_title_pair_re = re.compile(
            r'Executive Order\s+(\d{4,5})[,:.]*\s*'
            r'["\u201c(]([^"\u201d)]+)["\u201d)]',
            re.IGNORECASE,
        )
        for line_no, line in enumerate(lines, 1):
            for m in _eo_title_pair_re.finditer(line):
                eo_num = m.group(1)
                text_title = m.group(2).strip().rstrip(".")
                known = reverse_index.get(eo_num)
                if known:
                    known_title = known["title"].rstrip(".")
                    # Fuzzy compare: check if the core title matches
                    if text_title.lower() not in known_title.lower() and known_title.lower() not in text_title.lower():
                        changes.append({
                            "line": line_no,
                            "eo_number": eo_num,
                            "action": "hallucination_warning",
                            "confidence": "low",
                            "reason": f"Title mismatch: text has \"{text_title[:50]}\" but index has \"{known_title[:50]}\"",
                        })
                        warning_count += 1

        # --- Pass 2: Link injection (first-mention per EO) ---
        def _inject_link(m: re.Match, current_line: int) -> str:
            """Replace a plain EO reference with a hyperlink if resolvable."""
            nonlocal linked_count, unresolvable_count
            full_match = m.group(1)   # "Executive Order 14265"
            eo_num = m.group(2)       # "14265"

            info = reverse_index.get(eo_num)
            if not info:
                # Not in our corpus — leave plain text
                if eo_num not in seen_unresolvable:
                    seen_unresolvable.add(eo_num)
                    changes.append({
                        "line": current_line,
                        "eo_number": eo_num,
                        "action": "unresolvable",
                        "confidence": None,
                        "reason": "EO not in source corpus",
                    })
                unresolvable_count += 1
                return full_match

            # Build short-format link — downstream text already has the
            # title inline so we only hyperlink the EO number reference.
            fmt_num = CiteStage._format_eo_number(eo_num)
            year = info.get("year", "")
            label = f"Exec. Order No. {fmt_num}, {year}" if year else f"Exec. Order No. {fmt_num}"
            linked_count += 1
            changes.append({
                "line": current_line,
                "eo_number": eo_num,
                "action": "linked",
                "confidence": "high",
                "old_text": full_match,
                "new_label": label,
            })
            return f"[{label}]({info['url']})"

        # Process line-by-line to track line numbers for changelog
        result_lines: list[str] = []
        for line_no, line in enumerate(lines, 1):
            # Check if this line is inside a markdown link already
            new_line = self._DOWNSTREAM_EO_RE.sub(
                lambda m: _inject_link(m, line_no), line,
            )
            result_lines.append(new_line)

        changelog = {
            "chapter": filename,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "changes": changes,
            "summary": {
                "linked": linked_count,
                "unresolvable": unresolvable_count,
                "hallucination_warnings": warning_count,
            },
        }

        return "\n".join(result_lines), changelog


# ---------------------------------------------------------------------------
# Phase 8 — Introduction Generation
# ---------------------------------------------------------------------------

class IntroduceStage(Base):
    """Phase 8 — Generate introduction chapter via two-call LLM synthesis.

    Uses a draft → reflect pattern mirroring ConcludeStage: the first call
    generates the introduction from chapter summaries; the second call
    strengthens it.  Supports OpenAI and Gemini providers.
    """

    _PRICING: dict[str, dict[str, float]] = {
        "gpt-5.4":               {"input": 2.50,  "output": 10.00},
        "gpt-5.1":               {"input": 2.50,  "output": 10.00},
        "gpt-5":                 {"input": 2.00,  "output":  8.00},
        "gpt-5-mini":            {"input": 0.40,  "output":  1.60},
        "gemini-2.5-pro":        {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":      {"input": 0.30,  "output":  2.50},
        "gemini-3.1-pro-preview":{"input": 1.25,  "output": 10.00},
    }

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # -- public -------------------------------------------------------------

    def run(self) -> None:
        ic = self.cfg.introduce
        if not ic.enabled:
            self._logger.info("  Introduction generation disabled — skipping.")
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to introduce.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Idempotent — skip if already generated
        if manifest.get("introduce", {}).get("generated") and not self.cfg.force:
            self._logger.info("  Introduction already generated — skipping.")
            return

        # Load style rules
        style_rules = self._load_style_rules(Path(ic.style_guide))

        # Build chapter summaries (title + first paragraphs)
        chapter_summaries = self._build_chapter_summaries(
            chapters_dir, chapter_entries,
        )

        self._logger.info(
            "  Generating introduction with %s (%s, reasoning: %s)",
            ic.model, ic.provider, ic.reasoning_effort,
        )

        if self.dry_run:
            self._logger.info("  (dry-run) would generate introduction chapter")
            return

        # Scoped EO grounding — only EOs cited in chapters
        eo_grounding = self._fetch_document_eo_grounding(
            chapters_dir, chapter_entries, ic.rag_service_url,
        )
        if eo_grounding:
            self._logger.info("  Scoped EO grounding (%d chars)", len(eo_grounding))
        else:
            self._logger.warning("  No EO grounding available (RAG service unreachable or index empty)")

        # Build citable EO list from chapters (constrains LLM to known EOs)
        citable_eo_list = self._build_citable_eo_list(chapters_dir, chapter_entries)
        if citable_eo_list:
            self._logger.info("  Built citable EO index (%d entries)",
                              citable_eo_list.count("- Exec."))

        # Cost tracking
        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0

        # ── Call 1: Draft introduction ──
        self._logger.info("  Call 1/2: INTRODUCE")
        intro_prompt = self._build_introduce_prompt(
            chapter_summaries, style_rules, eo_grounding,
            citable_eo_list,
        )
        intro_draft, usage_1 = self._generate_with_usage(ic, intro_prompt)
        total_input_tokens += usage_1.get("input", 0)
        total_output_tokens += usage_1.get("output", 0)
        total_thinking_tokens += usage_1.get("thinking", 0)
        self._log_usage("introduce", usage_1, ic.model)

        if not intro_draft or not intro_draft.strip():
            self._logger.warning("  Empty introduction draft — aborting.")
            return

        intro_draft = self._strip_code_fencing(intro_draft)

        # ── Call 2: Reflect and strengthen ──
        self._logger.info("  Call 2/2: REFLECT")
        reflect_prompt = self._build_reflect_prompt(
            intro_draft, chapter_summaries, style_rules, eo_grounding,
            citable_eo_list,
        )
        refined_intro, usage_2 = self._generate_with_usage(ic, reflect_prompt)
        total_input_tokens += usage_2.get("input", 0)
        total_output_tokens += usage_2.get("output", 0)
        total_thinking_tokens += usage_2.get("thinking", 0)
        self._log_usage("reflect", usage_2, ic.model)

        if not refined_intro or not refined_intro.strip():
            self._logger.warning("  Empty reflect response — using draft.")
            refined_intro = intro_draft

        refined_intro = self._strip_code_fencing(refined_intro)

        # Add footer
        footer = (
            f"\n---\n*Generated by Rapporteur CLI — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
        )
        refined_intro = refined_intro.rstrip() + "\n" + footer

        # Write introduction chapter — clean up any prior *overall* introduction file
        filename = "00-introduction.md"
        for old in chapter_entries:
            fn = old["filename"]
            if "introduction" in fn and fn != filename and "part-" not in fn:
                stale = chapters_dir / fn
                if stale.exists():
                    stale.unlink()
        intro_path = chapters_dir / filename
        intro_path.write_text(refined_intro, encoding="utf-8")
        self._logger.info(
            "  Wrote %s (%d chars)", filename, len(refined_intro),
        )

        # Update manifest — insert introduction at index 0, remove prior overall intro only
        chapter_entries[:] = [c for c in chapter_entries if not ("introduction" in c["filename"] and "part-" not in c["filename"])]
        chapter_entries.insert(0, {
            "filename": filename,
            "title": "Introduction",
            "introduce_status": "done",
        })

        # Rebuild index.md with introduction first
        self._rebuild_index_md(chapters_dir, chapter_entries)

        cost = self._calculate_cost(
            ic.model, total_input_tokens, total_output_tokens,
        )
        manifest["introduce"] = {
            "generated": True,
            "model": ic.model,
            "provider": ic.provider,
            "chapters_in_scope": len([c for c in chapter_entries if "introduction" not in c["filename"]]),
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "thinking_tokens": total_thinking_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 4),
            },
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        self._logger.info("  ─── Cost Summary ───")
        self._logger.info("    Model:           %s", ic.model)
        self._logger.info("    Provider:        %s", ic.provider)
        self._logger.info("    Input tokens:    %s", f"{total_input_tokens:,}")
        self._logger.info("    Output tokens:   %s", f"{total_output_tokens:,}")
        self._logger.info("    Thinking tokens: %s", f"{total_thinking_tokens:,}")
        self._logger.info("    Estimated cost:  $%.4f", cost)
        self._logger.info("  Updated manifest.json with introduce metadata.")

    # -- LLM generation (mirrors ConcludeStage) -----------------------------

    def _generate_with_usage(
        self, ic: IntroduceConfig, prompt: str,
    ) -> tuple[str, dict]:
        """Call the configured LLM provider and return (text, usage_dict).
        Checks Redis cache first; stores responses on miss."""
        # --- Redis cache check ---
        cached = _llm_cache_get(ic.model, prompt)
        if cached is not None:
            self._logger.info("    ⚡ Cache hit — skipping LLM call")
            return cached, {"input": 0, "output": 0, "thinking": 0}

        import asyncio

        usage: dict = {"input": 0, "output": 0, "thinking": 0}

        if ic.provider == "openai":
            from src._clients.llm_openai_client import OpenAILLMClient

            client = OpenAILLMClient(model_name=ic.model)

            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(
                    client.client.responses.create(
                        model=ic.model,
                        input=prompt,
                        reasoning={"effort": ic.reasoning_effort},
                        max_output_tokens=16_000,
                    )
                )
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

            text = response.output_text if hasattr(response, "output_text") else ""
            resp_usage = getattr(response, "usage", None)
            if resp_usage:
                usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
                usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = getattr(details, "reasoning_tokens", 0) or 0

            if text:
                _llm_cache_set(ic.model, prompt, text)
            return text, usage

        else:
            from google.genai import types as genai_types

            from src._clients.llm_gcp_client import GeminiLLMClient

            client = GeminiLLMClient(model_name=ic.model, location=_gemini_location(ic.model))

            config = genai_types.GenerateContentConfig(
                temperature=0.2,
                top_p=client.top_p,
                max_output_tokens=16_000,
            )
            response = client.client.models.generate_content(
                model=client.model_name,
                contents=prompt,
                config=config,
            )
            text = response.text if response.text else ""
            um = getattr(response, "usage_metadata", None)
            if um:
                usage["input"] = getattr(um, "prompt_token_count", 0) or 0
                usage["output"] = getattr(um, "candidates_token_count", 0) or 0
                usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0

            if text:
                _llm_cache_set(ic.model, prompt, text)
            return text, usage

    # -- prompt builders ----------------------------------------------------

    def _build_introduce_prompt(
        self, chapter_summaries: str, style_rules: str,
        eo_grounding: str = "", citable_eo_list: str = "",
    ) -> str:
        """Build the INTRODUCE prompt with chapter summaries."""
        title = self.cfg.project.title
        author = self.cfg.project.author

        parts = [
            f"You are writing the introduction chapter for a publication titled "
            f"'{title}' by {author}.\n\n",
            "CRITICAL CONSTRAINTS:\n",
            "- Use exactly ONE # heading: '# Introduction'\n",
            "- All other headings use ## or ### (max 3 levels)\n",
            "- Do NOT add numeric prefixes to headings — the build system numbers them automatically\n",
            "- Favour free-flowing prose over seriated lists\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
            "INTRODUCTION GUIDELINES:\n",
            "- Describe the scope and purpose of this work\n",
            "- Provide a roadmap of chapters without summarising them in detail\n",
            "- Establish why this work matters to the reader\n",
            "- Set an authoritative, professional, and favourable tone\n",
            "- This introduction should work as a standalone executive summary "
            "suitable for external promotion of the work\n",
            "- The administration's actions serve a protective function and have "
            "the interests of the people at heart — frame accordingly\n",
            "- Reference chapter titles and themes but do not duplicate content\n",
            "- Engage the audience and explain why this publication matters\n\n",
        ]

        if citable_eo_list:
            parts.extend([
                f"{citable_eo_list}\n\n",
                "CITATION CONSTRAINT:\n",
                "- You may ONLY reference Executive Orders that appear in the "
                "CITABLE EXECUTIVE ORDERS list above.\n",
                "- Do NOT reference any Executive Order from your own knowledge "
                "that is not in that list.\n",
                "- If an Executive Order is not in the list, do not mention it — "
                "even if you know it exists.\n",
                "- Cite in the format: "
                "'Executive Order 14151, \"Ending Radical and Wasteful "
                "Government DEI Programs\"'\n\n",
            ])

        if eo_grounding:
            parts.extend([
                f"{eo_grounding}\n\n",
            ])

        parts.extend([
            f"CHAPTER SUMMARIES:\n{chapter_summaries}\n\n",
            "Write an introduction chapter that sets the scene for the entire document.\n",
            "Return ONLY the introduction markdown.",
        ])

        return "".join(parts)

    def _build_reflect_prompt(
        self, intro_draft: str, chapter_summaries: str,
        style_rules: str, eo_grounding: str = "",
        citable_eo_list: str = "",
    ) -> str:
        """Build the REFLECT prompt to refine the introduction draft."""
        parts = [
            "Review and refine this introduction chapter for a published analysis.\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
            "REVIEW CRITERIA:\n",
            "1. Does the introduction accurately represent the document's scope?\n",
            "2. Is there a compelling hook and clear value proposition?\n",
            "3. Is the tone professional, authoritative, and favourable to the administration?\n",
            "4. Is it self-contained as an executive summary?\n",
            "5. Does it avoid duplicating chapter content while effectively summarising themes?\n",
            "6. Are headings using #/##/### without numeric prefixes?\n",
        ]

        if citable_eo_list:
            parts.extend([
                "7. Are ALL Executive Order references found in the citable list below? "
                "Remove any that are not.\n",
                "\n",
                f"{citable_eo_list}\n\n",
            ])
        elif eo_grounding:
            parts.extend([
                "7. Are Executive Orders cited correctly with number and title?\n",
                "\n",
            ])

        if eo_grounding:
            parts.append(f"{eo_grounding}\n\n")

        parts.extend([
            f"CHAPTER SUMMARIES:\n{chapter_summaries}\n\n",
            f"INTRODUCTION DRAFT:\n{intro_draft}\n\n",
            "Return the refined introduction chapter markdown.\n",
            "Strengthen the hook, tighten prose, and ensure standalone quality.\n",
            "Correct any factual inconsistencies.",
        ])

        return "".join(parts)

    # -- context helpers ----------------------------------------------------

    def _build_chapter_summaries(
        self,
        chapters_dir: Path,
        chapter_entries: list[dict],
    ) -> str:
        """Build a summary of each chapter: title + first few paragraphs."""
        parts: list[str] = []
        for entry in chapter_entries:
            fn = entry["filename"]
            if "introduction" in fn or "conclusion" in fn:
                continue
            path = chapters_dir / fn
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            title = entry.get("title", fn)
            # Extract first 3 non-empty paragraphs after the heading
            lines = text.split("\n")
            paragraphs: list[str] = []
            in_content = False
            current_para: list[str] = []
            for line in lines:
                if line.startswith("# ") and not in_content:
                    in_content = True
                    continue
                if not in_content:
                    continue
                if line.strip() == "":
                    if current_para:
                        paragraphs.append(" ".join(current_para))
                        current_para = []
                        if len(paragraphs) >= 3:
                            break
                else:
                    # Skip headings, images, code fences from summary
                    if not line.startswith(("#", "![", "```", "---")):
                        current_para.append(line.strip())
            if current_para:
                paragraphs.append(" ".join(current_para))

            summary_text = "\n\n".join(paragraphs[:3])
            parts.append(f"### {title}\n\n{summary_text}")

        return "\n\n---\n\n".join(parts)

    def _load_style_rules(self, path: Path) -> str:
        """Extract STYLE CONVENTIONS section from the style guide."""
        if not path.exists():
            self._logger.warning("Style guide not found: %s", path)
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r'(## STYLE CONVE[NT]+IONS.*?)(?=\n## |\Z)',
            text, re.DOTALL,
        )
        return match.group(1).strip() if match else text.strip()

    def _build_citable_eo_list(
        self, chapters_dir: Path, chapter_entries: list[dict],
    ) -> str:
        """Build a formatted list of EOs found in chapters for prompt injection.

        Scans existing chapter files for linked EO references and returns a
        formatted block listing each unique EO once, so the LLM knows exactly
        which Executive Orders it may reference.
        """
        pattern = re.compile(
            r'\[Exec\. Order No\. ([\d,]+),\s*(\d{4})\]'
            r'\((https://[^\)]+)\)',
        )
        # Also match APA-style: [Exec. Order No. 14,148, Title... (2025)](url)
        pattern_apa = re.compile(
            r'\[Exec\. Order No\. ([\d,]+),\s+[^\]]+?\((\d{4})\)\]'
            r'\((https://[^\)]+)\)',
        )
        # Also find "Executive Order NNNNN, \"Title...\"" patterns with EO number
        pattern_plain = re.compile(
            r'Executive Order\s+(\d{4,5})[,:.]*\s*["\u201c]([^"\u201d]+)["\u201d]',
            re.IGNORECASE,
        )
        eos: dict[str, dict] = {}  # eo_num -> {num, year, title_fragment}
        for entry in chapter_entries:
            if "introduction" in entry.get("filename", ""):
                continue
            path = chapters_dir / entry["filename"]
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in pattern.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2)}
            for m in pattern_apa.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2)}
            for m in pattern_plain.finditer(text):
                num_raw = m.group(1)
                title_frag = m.group(2).strip().rstrip(".")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": num_raw, "year": "", "title": title_frag}

        if not eos:
            return ""

        lines = []
        for num in sorted(eos, key=lambda x: int(x)):
            info = eos[num]
            year_part = f" ({info['year']})" if info.get("year") else ""
            title_part = f' — "{info["title"]}"' if info.get("title") else ""
            lines.append(f"  - Exec. Order No. {info['formatted']}{year_part}{title_part}")

        return (
            "CITABLE EXECUTIVE ORDERS (exhaustive list):\n"
            + "\n".join(lines)
        )

    def _extract_eo_references(
        self, chapters_dir: Path, chapter_entries: list[dict],
    ) -> list[str]:
        """Extract unique EO reference URLs from chapter files."""
        pattern = re.compile(
            r'\[([^\]]+)\]\((https://(?:www\.)?(?:federalregister\.gov|whitehouse\.gov)[^\)]+)\)',
            re.IGNORECASE,
        )
        eo_urls: set[str] = set()
        for entry in chapter_entries:
            if "introduction" in entry.get("filename", ""):
                continue
            path = chapters_dir / entry["filename"]
            if path.exists():
                text = path.read_text(encoding="utf-8")
                for m in pattern.finditer(text):
                    url = m.group(2)
                    if '/documents/' in url and re.search(r'/documents/\d{4}/\d{2}/\d{2}/', url):
                        eo_urls.add(url)
        return sorted(eo_urls)

    def _fetch_eo_grounding(self, eo_urls: list[str], rag_url: str) -> str:
        """Query RAG service for EO grounding text. Returns empty string on failure."""
        if not eo_urls or not rag_url:
            return ""

        import requests

        grounding_parts: list[str] = []
        try:
            ready = requests.get(f"{rag_url}/ready", timeout=5)
            ready_data = ready.json()
            active = ready_data.get("active_folder", "")
            if "eo" not in active.lower():
                self._logger.info("  RAG not on EO index, switching...")
                try:
                    requests.post(f"{rag_url}/switch-index", json={"folder": "x_indices_eo"}, timeout=30)
                except Exception:
                    return ""
        except Exception:
            pass  # try anyway

        for url in eo_urls[:20]:  # cap at 20 to avoid excessive requests
            try:
                resp = requests.post(
                    f"{rag_url}/query_metadata",
                    json={"source_url": url, "include_summaries": True, "include_qa": True, "max_results": 50},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                docs = data.get("retrieved_docs", [])
                if docs:
                    doc = docs[0]
                    title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                    summary = doc.get("summary_text", "")
                    if summary:
                        grounding_parts.append(f"### {title}\n\n**Summary:**\n{summary}")
            except Exception:
                continue

        if grounding_parts:
            return (
                "## LEGAL FRAMEWORK GROUNDING\n\n"
                "Executive Order documents referenced in this analysis:\n\n"
                + "\n\n".join(grounding_parts)
            )
        return ""

    def _fetch_document_eo_grounding(
        self, chapters_dir: Path, chapter_entries: list[dict], rag_url: str,
    ) -> str:
        """Fetch EO grounding scoped to only EOs cited across all document chapters."""
        if not rag_url:
            return ""
        url_pattern = re.compile(r'https://www\.federalregister\.gov/documents/[^\s\)\]"]+')
        doc_urls: set[str] = set()
        for entry in chapter_entries:
            fn = entry.get("filename", "")
            if "conclusion" in fn or "introduction" in fn:
                continue
            path = chapters_dir / fn
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in url_pattern.finditer(text):
                doc_urls.add(m.group(0).rstrip(".,;:"))

        if not doc_urls:
            return ""

        self._logger.info("  Found %d unique EO URLs across chapters", len(doc_urls))

        import requests as _requests
        try:
            ready = _requests.get(f"{rag_url}/ready", timeout=5)
            active = ready.json().get("active_folder", "")
            if "eo" not in active.lower():
                resp = _requests.post(
                    f"{rag_url}/switch-index",
                    json={"folder": "x_indices_eo"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    return ""
        except Exception:
            pass

        grounding_parts: list[str] = []
        for url in sorted(doc_urls):
            try:
                resp = _requests.post(
                    f"{rag_url}/query_metadata",
                    json={
                        "source_url": url,
                        "include_summaries": True,
                        "include_qa": True,
                        "max_results": 100,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                retrieved_docs = data.get("retrieved_docs", [])
                retrieved_qa = data.get("retrieved_qa", [])
                if not retrieved_docs and not retrieved_qa:
                    continue
                if retrieved_docs:
                    doc = retrieved_docs[0]
                    doc_title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                    summary_text = doc.get("summary_text", "")
                    parts = [f"### {doc_title}\n", f"**Source:** {url}\n"]
                    if summary_text:
                        parts.append(f"\n**Summary:**\n{summary_text}\n")
                    if retrieved_qa:
                        parts.append("\n**Detailed Content (Federal Register):**\n")
                        for qa in retrieved_qa:
                            context = qa.get("context", "")
                            if context:
                                parts.append(f"\n{context}\n")
                    grounding_parts.append("".join(parts))
            except Exception as exc:
                self._logger.debug("Failed to fetch EO from %s: %s", url[:80], exc)

        if not grounding_parts:
            return ""
        header = (
            "## LEGAL FRAMEWORK GROUNDING\n\n"
            f"Executive Order context ({len(grounding_parts)} documents) "
            "covering all EOs cited in the document:\n\n"
        )
        return header + "\n\n".join(grounding_parts)

    def _rebuild_index_md(
        self, chapters_dir: Path, chapter_entries: list[dict],
    ) -> None:
        """Regenerate index.md with the introduction as the first entry."""
        parts = [
            f"# {self.cfg.project.title}\n",
            f"**Author:** {self.cfg.project.author}  ",
            f"**Date:** {self.cfg.project.date}\n",
            "## Table of Contents\n",
        ]
        for i, entry in enumerate(chapter_entries):
            title = entry.get("title", entry["filename"])
            wc = entry.get("word_count", 0)
            wc_str = f" — {wc:,} words" if wc else ""
            parts.append(
                f"{i}. [{title}]({entry['filename']}){wc_str}"
            )
        parts.append("")
        parts.append(
            f"\n---\n*Generated by Rapporteur CLI — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
        )
        index_path = chapters_dir / "index.md"
        index_path.write_text("\n".join(parts), encoding="utf-8")
        self._logger.info("  Rebuilt index.md with introduction.")

    # -- shared helpers -----------------------------------------------------

    @staticmethod
    def _strip_code_fencing(text: str) -> str:
        stripped = text.strip()
        if stripped.startswith("```"):
            first_nl = stripped.index("\n") if "\n" in stripped else len(stripped)
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3].rstrip()
        return stripped

    def _log_usage(self, label: str, usage: dict, model: str) -> None:
        cost = self._calculate_cost(model, usage["input"], usage["output"])
        self._logger.info(
            "    %s — in: %s  out: %s  think: %s  ($%.4f)",
            label,
            f"{usage['input']:,}",
            f"{usage['output']:,}",
            f"{usage['thinking']:,}",
            cost,
        )

    def _calculate_cost(
        self, model: str, input_tokens: int, output_tokens: int,
    ) -> float:
        rates = self._PRICING.get(model, {"input": 2.50, "output": 10.00})
        return (
            input_tokens / 1_000_000 * rates["input"]
            + output_tokens / 1_000_000 * rates["output"]
        )


# ---------------------------------------------------------------------------
# Section Conclude — per-section conclusions for multi-part volumes
# ---------------------------------------------------------------------------

class SectionConcludeStage(Base):
    """Generate a conclusion for each section defined in ``sections.definitions``.

    Reuses the LLM generation, prompt-building, and EO-grounding logic from
    :class:`ConcludeStage` but scopes each call to the chapters in a single
    section.  Output files are inserted into the manifest immediately after
    the last chapter in each section.
    """

    _PRICING: dict[str, dict[str, float]] = {
        "gpt-5.4":               {"input": 2.50,  "output": 10.00},
        "gpt-5.1":               {"input": 2.50,  "output": 10.00},
        "gpt-5":                 {"input": 2.00,  "output":  8.00},
        "gpt-5-mini":            {"input": 0.40,  "output":  1.60},
        "gemini-2.5-pro":        {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":      {"input": 0.30,  "output":  2.50},
        "gemini-3.1-pro-preview":{"input": 1.25,  "output": 10.00},
    }

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        sects = self.cfg.sections
        if not sects.enabled or not sects.definitions:
            self._logger.info("  No sections defined — skipping section conclude.")
            return

        co = self.cfg.conclude
        if not co.enabled:
            self._logger.info("  Conclusion generation disabled — skipping.")
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to conclude.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Idempotent check
        if manifest.get("section_conclude", {}).get("generated") and not self.cfg.force:
            self._logger.info("  Section conclusions already generated — skipping.")
            return

        # Load style rules
        style_rules = self._load_style_rules(Path(co.style_guide))
        conclusion_rules = self._load_conclusion_rules(Path(co.style_guide))

        if self.dry_run:
            for sec in sects.definitions:
                self._logger.info("  (dry-run) would generate conclusion for: %s", sec.title)
                if sec.prompt:
                    self._logger.info("    custom prompt: %s", sec.prompt[:120])
            return

        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0
        conclusions_written = 0

        # Build a map of 1-based chapter number → entry for quick lookup
        # Content chapters are those with an "index" key (excludes intro/conclusion)
        content_entries = [e for e in chapter_entries if "index" in e]

        for sec_idx, sec in enumerate(sects.definitions):
            part_num = sec_idx + 1
            self._logger.info("")
            self._logger.info("  ── Section %d: %s (chapters %s) ──",
                              part_num, sec.title,
                              ", ".join(str(c) for c in sec.chapters))

            # Select chapter entries for this section (1-based chapter numbers)
            section_entries = []
            for ch_num in sec.chapters:
                # ch_num is 1-based; content_entries[0] is chapter 1
                idx = ch_num - 1
                if 0 <= idx < len(content_entries):
                    section_entries.append(content_entries[idx])
                else:
                    self._logger.warning("    Chapter %d out of range — skipped", ch_num)

            if not section_entries:
                self._logger.warning("    No valid chapters for section — skipping.")
                continue

            # Build document context scoped to this section
            doc_context = self._build_section_context(
                chapters_dir, section_entries, co.include_kaliya,
            )

            # Build citable EO list from section chapters only
            citable_eo_list = self._build_citable_eo_list(chapters_dir, section_entries)
            if citable_eo_list:
                self._logger.info("    Scoped EO citations: %d EOs from section chapters",
                                  citable_eo_list.count("\n"))

            # Fetch scoped EO grounding — only EOs referenced in this section
            eo_grounding = self._fetch_section_eo_grounding(
                chapters_dir, section_entries, co.rag_service_url,
            )
            if eo_grounding:
                self._logger.info("    Scoped EO grounding (%d chars)", len(eo_grounding))

            if sec.prompt:
                self._logger.info("    Custom prompt: %s", sec.prompt[:120])

            self._logger.info("    Generating with %s (%s)", co.model, co.provider)

            # Call 1: Conclude
            self._logger.info("    Call 1/2: CONCLUDE")
            conclude_prompt = self._build_conclude_prompt(
                sec.title, part_num, len(sects.definitions),
                doc_context, conclusion_rules, style_rules, eo_grounding,
                citable_eo_list, sec.prompt,
            )
            prompt_file = chapters_dir / f"_prompt-part-{part_num}-conclude.txt"
            prompt_file.write_text(conclude_prompt, encoding="utf-8")
            self._logger.info("    Prompt saved to %s (%d chars)", prompt_file.name, len(conclude_prompt))
            conclusion_draft, usage_1 = self._generate_with_usage(co, conclude_prompt)
            total_input_tokens += usage_1.get("input", 0)
            total_output_tokens += usage_1.get("output", 0)
            total_thinking_tokens += usage_1.get("thinking", 0)
            self._log_usage(f"conclude-part-{part_num}", usage_1, co.model)

            if not conclusion_draft or not conclusion_draft.strip():
                self._logger.warning("    Empty conclusion draft — skipping section.")
                continue

            conclusion_draft = self._strip_code_fencing(conclusion_draft)

            # Call 2: Reflect
            self._logger.info("    Call 2/2: REFLECT")
            reflect_prompt = self._build_reflect_prompt(
                sec.title, conclusion_draft, doc_context,
                conclusion_rules, eo_grounding, citable_eo_list,
            )
            prompt_file = chapters_dir / f"_prompt-part-{part_num}-reflect.txt"
            prompt_file.write_text(reflect_prompt, encoding="utf-8")
            self._logger.info("    Prompt saved to %s (%d chars)", prompt_file.name, len(reflect_prompt))
            refined, usage_2 = self._generate_with_usage(co, reflect_prompt)
            total_input_tokens += usage_2.get("input", 0)
            total_output_tokens += usage_2.get("output", 0)
            total_thinking_tokens += usage_2.get("thinking", 0)
            self._log_usage(f"reflect-part-{part_num}", usage_2, co.model)

            if not refined or not refined.strip():
                refined = conclusion_draft
            refined = self._strip_code_fencing(refined)

            # Add footer
            footer = (
                f"\n---\n*Generated by Rapporteur CLI — "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
            )
            refined = refined.rstrip() + "\n" + footer

            # Write section conclusion file
            last_ch = sec.chapters[-1]
            filename = f"{last_ch:02d}z-part-{part_num}-conclusion.md"
            (chapters_dir / filename).write_text(refined, encoding="utf-8")
            self._logger.info("    Wrote %s (%d chars)", filename, len(refined))
            conclusions_written += 1

            # Insert into chapter_entries after the last chapter in section
            insert_after_fn = content_entries[last_ch - 1]["filename"] if last_ch <= len(content_entries) else None
            if insert_after_fn:
                # Find position in the full chapter_entries list
                for pos, e in enumerate(chapter_entries):
                    if e.get("filename") == insert_after_fn:
                        chapter_entries.insert(pos + 1, {
                            "filename": filename,
                            "title": f"Part {part_num} Conclusion — {sec.title}",
                            "section_conclude_status": "done",
                            "section_index": sec_idx,
                        })
                        break

        # Write manifest
        cost = self._calculate_cost(co.model, total_input_tokens, total_output_tokens)
        manifest["section_conclude"] = {
            "generated": True,
            "model": co.model,
            "provider": co.provider,
            "sections": len(sects.definitions),
            "conclusions_written": conclusions_written,
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "thinking_tokens": total_thinking_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 4),
            },
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        self._logger.info("  ─── Section Conclude Summary ───")
        self._logger.info("    Sections:        %d", len(sects.definitions))
        self._logger.info("    Written:         %d", conclusions_written)
        self._logger.info("    Model:           %s", co.model)
        self._logger.info("    Input tokens:    %s", f"{total_input_tokens:,}")
        self._logger.info("    Output tokens:   %s", f"{total_output_tokens:,}")
        self._logger.info("    Estimated cost:  $%.4f", cost)

    # -- LLM generation (reuse pattern from ConcludeStage) -----------------

    def _generate_with_usage(self, co: ConcludeConfig, prompt: str) -> tuple[str, dict]:
        # --- Redis cache check ---
        cached = _llm_cache_get(co.model, prompt)
        if cached is not None:
            self._logger.info("    ⚡ Cache hit — skipping LLM call")
            return cached, {"input": 0, "output": 0, "thinking": 0}

        import asyncio
        usage: dict = {"input": 0, "output": 0, "thinking": 0}

        if co.provider == "openai":
            from src._clients.llm_openai_client import OpenAILLMClient
            client = OpenAILLMClient(model_name=co.model)
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(
                    client.client.responses.create(
                        model=co.model, input=prompt,
                        reasoning={"effort": co.reasoning_effort},
                        max_output_tokens=16_000,
                    )
                )
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
            text = response.output_text if hasattr(response, "output_text") else ""
            resp_usage = getattr(response, "usage", None)
            if resp_usage:
                usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
                usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = getattr(details, "reasoning_tokens", 0) or 0
            if text:
                _llm_cache_set(co.model, prompt, text)
            return text, usage
        else:
            from google.genai import types as genai_types

            from src._clients.llm_gcp_client import GeminiLLMClient
            client = GeminiLLMClient(model_name=co.model, location=_gemini_location(co.model))
            config = genai_types.GenerateContentConfig(
                temperature=0.2, top_p=client.top_p, max_output_tokens=16_000,
            )
            response = client.client.models.generate_content(
                model=client.model_name, contents=prompt, config=config,
            )
            text = response.text if response.text else ""
            um = getattr(response, "usage_metadata", None)
            if um:
                usage["input"] = getattr(um, "prompt_token_count", 0) or 0
                usage["output"] = getattr(um, "candidates_token_count", 0) or 0
                usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0
            if text:
                _llm_cache_set(co.model, prompt, text)
            return text, usage

    # -- prompt builders ---------------------------------------------------

    def _build_conclude_prompt(
        self, section_title: str, part_num: int, total_parts: int,
        doc_context: str, conclusion_rules: str, style_rules: str,
        eo_grounding: str = "", citable_eo_list: str = "",
        custom_prompt: str = "",
    ) -> str:
        title = self.cfg.project.title
        author = self.cfg.project.author
        parts = [
            f"You are writing the conclusion for Part {part_num} of {total_parts} "
            f"in a multi-part published analysis titled '{title}' by {author}.\n\n",
            f"This section is titled: '{section_title}'\n\n",
            "CRITICAL CONSTRAINTS:\n",
            f"- Use exactly ONE # heading: '# Part {part_num} Conclusion — {section_title}'\n",
            "- All other headings use ## or ### (max 3 levels)\n",
            "- Do NOT add numeric prefixes to headings — the build system numbers them automatically\n",
            "- Favour free-flowing prose over seriated lists\n",
            "- This conclusion covers ONLY the chapters in this section, not the whole document\n",
            "- Reference ONLY Executive Orders that appear in this section's chapters\n\n",
        ]
        if custom_prompt:
            parts.extend([
                f"AUTHOR GUIDANCE:\n{custom_prompt}\n\n",
            ])
        parts.extend([
            f"CONCLUSION GUIDELINES:\n{conclusion_rules}\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
        ])
        if citable_eo_list:
            parts.extend([
                f"{citable_eo_list}\n\n",
                "CITATION CONSTRAINT:\n",
                "- You may ONLY reference Executive Orders in the CITABLE list above.\n",
                "- Do NOT cite or mention any EOs not in this list.\n",
                "- Cite as: 'Executive Order 14151, \"Title\"'\n\n",
            ])
        if eo_grounding:
            parts.extend([
                f"{eo_grounding}\n\n",
                "LEGAL FRAMEWORK:\n",
                "Cite Executive Orders by number and title. Ground conclusions in EO provisions.\n\n",
            ])
        parts.extend([
            f"SECTION CHAPTERS:\n{doc_context}\n\n",
            "Write a conclusion that synthesises the themes and analysis from this section's chapters.\n",
            "Return ONLY the conclusion markdown.",
        ])
        return "".join(parts)

    def _build_reflect_prompt(
        self, section_title: str, conclusion_draft: str, doc_context: str,
        conclusion_rules: str, eo_grounding: str = "", citable_eo_list: str = "",
    ) -> str:
        parts = [
            f"Review and refine this section conclusion for '{section_title}'.\n\n",
            f"CONCLUSION GUIDELINES:\n{conclusion_rules}\n\n",
            "REVIEW CRITERIA:\n",
            "1. Are conclusions grounded in evidence from the section's chapters?\n",
            "2. Is the tone professional, authoritative, and favourable?\n",
            "3. Are headings using #/##/### without numeric prefixes?\n",
            "4. Does it stay scoped to this section (not the full document)?\n",
        ]
        if citable_eo_list:
            parts.extend([
                "5. Does it ONLY reference Executive Orders from the citable list?\n",
                "6. Remove any references to EOs not in the citable list.\n\n",
                f"{citable_eo_list}\n\n",
            ])
        elif eo_grounding:
            parts.extend([
                "5. Are Executive Orders cited correctly with number and title?\n\n",
            ])
        if eo_grounding:
            parts.append(f"{eo_grounding}\n\n")
        parts.extend([
            f"SECTION CHAPTERS:\n{doc_context}\n\n",
            f"CONCLUSION DRAFT:\n{conclusion_draft}\n\n",
            "Return the refined conclusion markdown. Strengthen weak arguments.",
        ])
        return "".join(parts)

    # -- helpers -----------------------------------------------------------

    def _build_section_context(
        self, chapters_dir: Path, entries: list[dict], include_kaliya: bool,
    ) -> str:
        parts: list[str] = []
        for entry in entries:
            path = chapters_dir / entry["filename"]
            if path.exists():
                parts.append(path.read_text(encoding="utf-8"))
        return "\n\n---\n\n".join(parts)

    def _load_style_rules(self, path: Path) -> str:
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(r'(## STYLE CONVE[NT]+IONS.*?)(?=\n## |\Z)', text, re.DOTALL)
        return match.group(1).strip() if match else text.strip()

    def _load_conclusion_rules(self, path: Path) -> str:
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(r'(## CONCLUSION.*?)(?=\n## |\Z)', text, re.DOTALL)
        return match.group(1).strip() if match else ""

    def _build_citable_eo_list(self, chapters_dir: Path, entries: list[dict]) -> str:
        """Extract EO references from section chapters and return a constrained list."""
        pattern = re.compile(r'\[Exec\. Order No\. ([\d,]+),\s*(\d{4})\]\((https://[^\)]+)\)')
        pattern_apa = re.compile(r'\[Exec\. Order No\. ([\d,]+),\s+[^\]]+?\((\d{4})\)\]\((https://[^\)]+)\)')
        pattern_plain = re.compile(r'Executive Order\s+(\d{4,5})[,:.]*\s*["\u201c]([^"\u201d]+)["\u201d]', re.IGNORECASE)
        eos: dict[str, dict] = {}
        for entry in entries:
            if "introduction" in entry.get("filename", "") or "conclusion" in entry.get("filename", ""):
                continue
            path = chapters_dir / entry["filename"]
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in pattern.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2), "url": m.group(3)}
            for m in pattern_apa.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2), "url": m.group(3)}
            for m in pattern_plain.finditer(text):
                num_raw = m.group(1)
                title_frag = m.group(2).strip().rstrip(".")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": num_raw, "year": "", "title": title_frag}
        if not eos:
            return ""
        lines = []
        for num in sorted(eos, key=lambda x: int(x)):
            info = eos[num]
            year_part = f" ({info['year']})" if info.get("year") else ""
            title_part = f' — "{info["title"]}"' if info.get("title") else ""
            lines.append(f"  - Exec. Order No. {info['formatted']}{year_part}{title_part}")
        return "CITABLE EXECUTIVE ORDERS (exhaustive list):\n" + "\n".join(lines)

    def _fetch_section_eo_grounding(
        self, chapters_dir: Path, entries: list[dict], rag_url: str,
    ) -> str:
        """Fetch EO grounding scoped to only the EOs referenced in section chapters."""
        if not rag_url:
            return ""
        # Extract unique Federal Register URLs from section chapters
        url_pattern = re.compile(r'https://www\.federalregister\.gov/documents/[^\s\)\]"]+')
        section_urls: set[str] = set()
        for entry in entries:
            if "introduction" in entry.get("filename", "") or "conclusion" in entry.get("filename", ""):
                continue
            path = chapters_dir / entry["filename"]
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in url_pattern.finditer(text):
                section_urls.add(m.group(0).rstrip(".,;:"))

        if not section_urls:
            return ""

        import requests as _requests

        # Ensure EO index is active
        try:
            ready = _requests.get(f"{rag_url}/ready", timeout=5)
            active = ready.json().get("active_folder", "")
            if "eo" not in active.lower():
                resp = _requests.post(
                    f"{rag_url}/switch-index",
                    json={"folder": "x_indices_eo"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    return ""
        except Exception:
            pass  # continue — might already be on the right index

        grounding_parts: list[str] = []
        for url in sorted(section_urls):
            try:
                resp = _requests.post(
                    f"{rag_url}/query_metadata",
                    json={
                        "source_url": url,
                        "include_summaries": True,
                        "include_qa": True,
                        "max_results": 100,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                retrieved_docs = data.get("retrieved_docs", [])
                retrieved_qa = data.get("retrieved_qa", [])
                if not retrieved_docs and not retrieved_qa:
                    continue
                if retrieved_docs:
                    doc = retrieved_docs[0]
                    doc_title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                    summary_text = doc.get("summary_text", "")
                    parts = [f"### {doc_title}\n", f"**Source:** {url}\n"]
                    if summary_text:
                        parts.append(f"\n**Summary:**\n{summary_text}\n")
                    if retrieved_qa:
                        parts.append("\n**Detailed Content (Federal Register):**\n")
                        for qa in retrieved_qa:
                            context = qa.get("context", "")
                            if context:
                                parts.append(f"\n{context}\n")
                    grounding_parts.append("".join(parts))
            except Exception as exc:
                self._logger.debug("Failed to fetch EO from %s: %s", url[:80], exc)

        if not grounding_parts:
            return ""
        header = (
            "## LEGAL FRAMEWORK GROUNDING\n\n"
            f"Executive Order context ({len(grounding_parts)} documents) "
            "scoped to this section's chapters:\n\n"
        )
        return header + "\n\n".join(grounding_parts)

    @staticmethod
    def _strip_code_fencing(text: str) -> str:
        stripped = text.strip()
        if stripped.startswith("```"):
            first_nl = stripped.index("\n") if "\n" in stripped else len(stripped)
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3].rstrip()
        return stripped

    def _log_usage(self, label: str, usage: dict, model: str) -> None:
        cost = self._calculate_cost(model, usage["input"], usage["output"])
        self._logger.info(
            "    %s — in: %s  out: %s  think: %s  ($%.4f)",
            label, f"{usage['input']:,}", f"{usage['output']:,}",
            f"{usage['thinking']:,}", cost,
        )

    def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        rates = self._PRICING.get(model, {"input": 2.50, "output": 10.00})
        return input_tokens / 1_000_000 * rates["input"] + output_tokens / 1_000_000 * rates["output"]


# ---------------------------------------------------------------------------
# Section Introduce — per-section introductions for multi-part volumes
# ---------------------------------------------------------------------------

class SectionIntroduceStage(Base):
    """Generate an introduction for each section defined in ``sections.definitions``.

    Mirrors :class:`SectionConcludeStage` but generates introductions.  Output
    files are inserted into the manifest immediately before the first chapter
    in each section.
    """

    _PRICING: dict[str, dict[str, float]] = {
        "gpt-5.4":               {"input": 2.50,  "output": 10.00},
        "gpt-5.1":               {"input": 2.50,  "output": 10.00},
        "gpt-5":                 {"input": 2.00,  "output":  8.00},
        "gpt-5-mini":            {"input": 0.40,  "output":  1.60},
        "gemini-2.5-pro":        {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":      {"input": 0.30,  "output":  2.50},
        "gemini-3.1-pro-preview":{"input": 1.25,  "output": 10.00},
    }

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        sects = self.cfg.sections
        if not sects.enabled or not sects.definitions:
            self._logger.info("  No sections defined — skipping section introduce.")
            return

        ic = self.cfg.introduce
        if not ic.enabled:
            self._logger.info("  Introduction generation disabled — skipping.")
            return

        chapters_dir = self.cfg.build_dir / "chapters"
        manifest_path = chapters_dir / "manifest.json"
        if not manifest_path.exists():
            self._logger.warning("No manifest.json — nothing to introduce.")
            return

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chapter_entries = manifest.get("chapters", [])
        if not chapter_entries:
            self._logger.warning("Manifest has no chapters — skipping.")
            return

        # Idempotent check
        if manifest.get("section_introduce", {}).get("generated") and not self.cfg.force:
            self._logger.info("  Section introductions already generated — skipping.")
            return

        # Load style rules
        style_rules = self._load_style_rules(Path(ic.style_guide))

        if self.dry_run:
            for sec in sects.definitions:
                self._logger.info("  (dry-run) would generate introduction for: %s", sec.title)
                if sec.prompt:
                    self._logger.info("    custom prompt: %s", sec.prompt[:120])
            return

        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0
        intros_written = 0

        content_entries = [e for e in chapter_entries if "index" in e]

        for sec_idx, sec in enumerate(sects.definitions):
            part_num = sec_idx + 1
            self._logger.info("")
            self._logger.info("  ── Section %d: %s (chapters %s) ──",
                              part_num, sec.title,
                              ", ".join(str(c) for c in sec.chapters))

            # Select chapter entries for this section
            section_entries = []
            for ch_num in sec.chapters:
                idx = ch_num - 1
                if 0 <= idx < len(content_entries):
                    section_entries.append(content_entries[idx])

            if not section_entries:
                self._logger.warning("    No valid chapters for section — skipping.")
                continue

            # Skip if this section's intro already exists on disk
            first_ch = sec.chapters[0]
            intro_filename = f"{first_ch:02d}a-part-{part_num}-introduction.md"
            if (chapters_dir / intro_filename).exists():
                self._logger.info("    %s already exists — skipping.", intro_filename)
                intros_written += 1
                continue

            # Build chapter summaries scoped to this section
            chapter_summaries = self._build_chapter_summaries(chapters_dir, section_entries)

            # Build citable EO list from section chapters
            citable_eo_list = self._build_citable_eo_list(chapters_dir, section_entries)
            if citable_eo_list:
                self._logger.info("    Scoped EO citations: %d EOs from section chapters",
                                  citable_eo_list.count("\n"))

            # Fetch scoped EO grounding — only EOs referenced in this section
            eo_grounding = self._fetch_section_eo_grounding(
                chapters_dir, section_entries, ic.rag_service_url,
            )
            if eo_grounding:
                self._logger.info("    Scoped EO grounding (%d chars)", len(eo_grounding))

            if sec.prompt:
                self._logger.info("    Custom prompt: %s", sec.prompt[:120])

            self._logger.info("    Generating with %s (%s)", ic.model, ic.provider)

            # Call 1: Introduce
            self._logger.info("    Call 1/2: INTRODUCE")
            intro_prompt = self._build_introduce_prompt(
                sec.title, part_num, len(sects.definitions),
                chapter_summaries, style_rules, eo_grounding, citable_eo_list,
                sec.prompt,
            )
            prompt_file = chapters_dir / f"_prompt-part-{part_num}-introduce.txt"
            prompt_file.write_text(intro_prompt, encoding="utf-8")
            self._logger.info("    Prompt saved to %s (%d chars)", prompt_file.name, len(intro_prompt))
            intro_draft, usage_1 = self._generate_with_usage(ic, intro_prompt)
            total_input_tokens += usage_1.get("input", 0)
            total_output_tokens += usage_1.get("output", 0)
            total_thinking_tokens += usage_1.get("thinking", 0)
            self._log_usage(f"introduce-part-{part_num}", usage_1, ic.model)

            if not intro_draft or not intro_draft.strip():
                self._logger.warning("    Empty introduction draft — skipping section.")
                continue

            intro_draft = self._strip_code_fencing(intro_draft)

            # Call 2: Reflect
            self._logger.info("    Call 2/2: REFLECT")
            reflect_prompt = self._build_reflect_prompt(
                sec.title, intro_draft, chapter_summaries,
                style_rules, eo_grounding, citable_eo_list,
            )
            prompt_file = chapters_dir / f"_prompt-part-{part_num}-intro-reflect.txt"
            prompt_file.write_text(reflect_prompt, encoding="utf-8")
            self._logger.info("    Prompt saved to %s (%d chars)", prompt_file.name, len(reflect_prompt))
            refined, usage_2 = self._generate_with_usage(ic, reflect_prompt)
            total_input_tokens += usage_2.get("input", 0)
            total_output_tokens += usage_2.get("output", 0)
            total_thinking_tokens += usage_2.get("thinking", 0)
            self._log_usage(f"reflect-part-{part_num}", usage_2, ic.model)

            if not refined or not refined.strip():
                refined = intro_draft
            refined = self._strip_code_fencing(refined)

            footer = (
                f"\n---\n*Generated by Rapporteur CLI — "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
            )
            refined = refined.rstrip() + "\n" + footer

            # Write section introduction file (sorts before first chapter)
            first_ch = sec.chapters[0]
            filename = f"{first_ch:02d}a-part-{part_num}-introduction.md"
            (chapters_dir / filename).write_text(refined, encoding="utf-8")
            self._logger.info("    Wrote %s (%d chars)", filename, len(refined))
            intros_written += 1

            # Insert into chapter_entries before the first chapter in section
            first_fn = content_entries[first_ch - 1]["filename"] if first_ch <= len(content_entries) else None
            if first_fn:
                for pos, e in enumerate(chapter_entries):
                    if e.get("filename") == first_fn:
                        chapter_entries.insert(pos, {
                            "filename": filename,
                            "title": f"Part {part_num} Introduction — {sec.title}",
                            "section_introduce_status": "done",
                            "section_index": sec_idx,
                        })
                        break

        # Write manifest
        cost = self._calculate_cost(ic.model, total_input_tokens, total_output_tokens)
        manifest["section_introduce"] = {
            "generated": True,
            "model": ic.model,
            "provider": ic.provider,
            "sections": len(sects.definitions),
            "introductions_written": intros_written,
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "thinking_tokens": total_thinking_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 4),
            },
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        # Rebuild index.md
        self._rebuild_index_md(chapters_dir, chapter_entries)

        self._logger.info("  ─── Section Introduce Summary ───")
        self._logger.info("    Sections:        %d", len(sects.definitions))
        self._logger.info("    Written:         %d", intros_written)
        self._logger.info("    Model:           %s", ic.model)
        self._logger.info("    Input tokens:    %s", f"{total_input_tokens:,}")
        self._logger.info("    Output tokens:   %s", f"{total_output_tokens:,}")
        self._logger.info("    Estimated cost:  $%.4f", cost)

    # -- LLM generation ---------------------------------------------------

    def _generate_with_usage(self, ic: IntroduceConfig, prompt: str) -> tuple[str, dict]:
        # --- Redis cache check ---
        cached = _llm_cache_get(ic.model, prompt)
        if cached is not None:
            self._logger.info("    ⚡ Cache hit — skipping LLM call")
            return cached, {"input": 0, "output": 0, "thinking": 0}

        import asyncio
        usage: dict = {"input": 0, "output": 0, "thinking": 0}

        if ic.provider == "openai":
            from src._clients.llm_openai_client import OpenAILLMClient
            client = OpenAILLMClient(model_name=ic.model)
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(
                    client.client.responses.create(
                        model=ic.model, input=prompt,
                        reasoning={"effort": ic.reasoning_effort},
                        max_output_tokens=16_000,
                    )
                )
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
            text = response.output_text if hasattr(response, "output_text") else ""
            resp_usage = getattr(response, "usage", None)
            if resp_usage:
                usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
                usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = getattr(details, "reasoning_tokens", 0) or 0
            if text:
                _llm_cache_set(ic.model, prompt, text)
            return text, usage
        else:
            from google.genai import types as genai_types

            from src._clients.llm_gcp_client import GeminiLLMClient
            client = GeminiLLMClient(model_name=ic.model, location=_gemini_location(ic.model))
            config = genai_types.GenerateContentConfig(
                temperature=0.2, top_p=client.top_p, max_output_tokens=16_000,
            )
            response = client.client.models.generate_content(
                model=client.model_name, contents=prompt, config=config,
            )
            text = response.text if response.text else ""
            um = getattr(response, "usage_metadata", None)
            if um:
                usage["input"] = getattr(um, "prompt_token_count", 0) or 0
                usage["output"] = getattr(um, "candidates_token_count", 0) or 0
                usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0
            if text:
                _llm_cache_set(ic.model, prompt, text)
            return text, usage

    # -- prompt builders ---------------------------------------------------

    def _build_introduce_prompt(
        self, section_title: str, part_num: int, total_parts: int,
        chapter_summaries: str, style_rules: str,
        eo_grounding: str = "", citable_eo_list: str = "",
        custom_prompt: str = "",
    ) -> str:
        title = self.cfg.project.title
        author = self.cfg.project.author
        parts = [
            f"You are writing the introduction for Part {part_num} of {total_parts} "
            f"in a multi-part published analysis titled '{title}' by {author}.\n\n",
            f"This section is titled: '{section_title}'\n\n",
            "CRITICAL CONSTRAINTS:\n",
            f"- Use exactly ONE # heading: '# Part {part_num} — {section_title}'\n",
            "- All other headings use ## or ### (max 3 levels)\n",
            "- Do NOT add numeric prefixes to headings\n",
            "- Favour free-flowing prose over seriated lists\n",
            "- This introduction covers ONLY the chapters in this section\n",
            "- Reference ONLY Executive Orders that appear in this section's chapters\n\n",
        ]
        if custom_prompt:
            parts.extend([
                f"AUTHOR GUIDANCE:\n{custom_prompt}\n\n",
            ])
        parts.extend([
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
            "INTRODUCTION GUIDELINES:\n",
            "- Describe the scope and purpose of this section\n",
            "- Provide a roadmap of the chapters within this part\n",
            "- Explain how this section fits into the broader multi-part publication\n",
            "- Set an authoritative, professional, and favourable tone\n",
            "- The administration's actions serve a protective function — frame accordingly\n\n",
        ])
        if citable_eo_list:
            parts.extend([
                f"{citable_eo_list}\n\n",
                "CITATION CONSTRAINT:\n",
                "- You may ONLY reference Executive Orders in the CITABLE list above.\n",
                "- Cite as: 'Executive Order 14151, \"Title\"'\n\n",
            ])
        if eo_grounding:
            parts.append(f"{eo_grounding}\n\n")
        parts.extend([
            f"CHAPTER SUMMARIES:\n{chapter_summaries}\n\n",
            "Write a section introduction. Return ONLY the markdown.",
        ])
        return "".join(parts)

    def _build_reflect_prompt(
        self, section_title: str, intro_draft: str,
        chapter_summaries: str, style_rules: str,
        eo_grounding: str = "", citable_eo_list: str = "",
    ) -> str:
        parts = [
            f"Review and refine this section introduction for '{section_title}'.\n\n",
            f"STYLE CONVENTIONS:\n{style_rules}\n\n",
            "REVIEW CRITERIA:\n",
            "1. Does it accurately represent this section's scope?\n",
            "2. Is the tone professional and authoritative?\n",
            "3. Does it stay scoped to this section only?\n",
            "4. Are headings using #/##/### without numeric prefixes?\n",
        ]
        if citable_eo_list:
            parts.extend([
                "5. Are all EO references in the citable list?\n\n",
                f"{citable_eo_list}\n\n",
            ])
        if eo_grounding:
            parts.append(f"{eo_grounding}\n\n")
        parts.extend([
            f"CHAPTER SUMMARIES:\n{chapter_summaries}\n\n",
            f"INTRODUCTION DRAFT:\n{intro_draft}\n\n",
            "Return the refined introduction markdown.",
        ])
        return "".join(parts)

    # -- helpers -----------------------------------------------------------

    def _build_chapter_summaries(self, chapters_dir: Path, entries: list[dict]) -> str:
        parts: list[str] = []
        for entry in entries:
            fn = entry["filename"]
            if "introduction" in fn or "conclusion" in fn:
                continue
            path = chapters_dir / fn
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            title = entry.get("title", fn)
            lines = text.split("\n")
            paragraphs: list[str] = []
            in_content = False
            current_para: list[str] = []
            for line in lines:
                if line.startswith("# ") and not in_content:
                    in_content = True
                    continue
                if not in_content:
                    continue
                if line.strip() == "":
                    if current_para:
                        paragraphs.append(" ".join(current_para))
                        current_para = []
                        if len(paragraphs) >= 3:
                            break
                else:
                    if not line.startswith(("#", "![", "```", "---")):
                        current_para.append(line.strip())
            if current_para:
                paragraphs.append(" ".join(current_para))
            summary_text = "\n\n".join(paragraphs[:3])
            parts.append(f"### {title}\n\n{summary_text}")
        return "\n\n---\n\n".join(parts)

    def _build_citable_eo_list(self, chapters_dir: Path, entries: list[dict]) -> str:
        pattern = re.compile(r'\[Exec\. Order No\. ([\d,]+),\s*(\d{4})\]\((https://[^\)]+)\)')
        pattern_apa = re.compile(r'\[Exec\. Order No\. ([\d,]+),\s+[^\]]+?\((\d{4})\)\]\((https://[^\)]+)\)')
        pattern_plain = re.compile(r'Executive Order\s+(\d{4,5})[,:.]*\s*["\u201c]([^"\u201d]+)["\u201d]', re.IGNORECASE)
        eos: dict[str, dict] = {}
        for entry in entries:
            if "introduction" in entry.get("filename", ""):
                continue
            path = chapters_dir / entry["filename"]
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in pattern.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2)}
            for m in pattern_apa.finditer(text):
                num_raw = m.group(1).replace(",", "")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": m.group(1), "year": m.group(2)}
            for m in pattern_plain.finditer(text):
                num_raw = m.group(1)
                title_frag = m.group(2).strip().rstrip(".")
                if num_raw not in eos:
                    eos[num_raw] = {"formatted": num_raw, "year": "", "title": title_frag}
        if not eos:
            return ""
        lines = []
        for num in sorted(eos, key=lambda x: int(x)):
            info = eos[num]
            year_part = f" ({info['year']})" if info.get("year") else ""
            title_part = f' — "{info["title"]}"' if info.get("title") else ""
            lines.append(f"  - Exec. Order No. {info['formatted']}{year_part}{title_part}")
        return "CITABLE EXECUTIVE ORDERS (exhaustive list):\n" + "\n".join(lines)

    def _fetch_section_eo_grounding(
        self, chapters_dir: Path, entries: list[dict], rag_url: str,
    ) -> str:
        """Fetch EO grounding scoped to only the EOs referenced in section chapters."""
        if not rag_url:
            return ""
        url_pattern = re.compile(r'https://www\.federalregister\.gov/documents/[^\s\)\]"]+')
        section_urls: set[str] = set()
        for entry in entries:
            if "introduction" in entry.get("filename", "") or "conclusion" in entry.get("filename", ""):
                continue
            path = chapters_dir / entry["filename"]
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for m in url_pattern.finditer(text):
                section_urls.add(m.group(0).rstrip(".,;:"))

        if not section_urls:
            return ""

        import requests as _requests
        try:
            ready = _requests.get(f"{rag_url}/ready", timeout=5)
            active = ready.json().get("active_folder", "")
            if "eo" not in active.lower():
                resp = _requests.post(
                    f"{rag_url}/switch-index",
                    json={"folder": "x_indices_eo"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    return ""
        except Exception:
            pass

        grounding_parts: list[str] = []
        for url in sorted(section_urls):
            try:
                resp = _requests.post(
                    f"{rag_url}/query_metadata",
                    json={
                        "source_url": url,
                        "include_summaries": True,
                        "include_qa": True,
                        "max_results": 100,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                retrieved_docs = data.get("retrieved_docs", [])
                retrieved_qa = data.get("retrieved_qa", [])
                if not retrieved_docs and not retrieved_qa:
                    continue
                if retrieved_docs:
                    doc = retrieved_docs[0]
                    doc_title = doc.get("doc_metadata", {}).get("title", "Executive Order")
                    summary_text = doc.get("summary_text", "")
                    parts = [f"### {doc_title}\n", f"**Source:** {url}\n"]
                    if summary_text:
                        parts.append(f"\n**Summary:**\n{summary_text}\n")
                    if retrieved_qa:
                        parts.append("\n**Detailed Content (Federal Register):**\n")
                        for qa in retrieved_qa:
                            context = qa.get("context", "")
                            if context:
                                parts.append(f"\n{context}\n")
                    grounding_parts.append("".join(parts))
            except Exception as exc:
                self._logger.debug("Failed to fetch EO from %s: %s", url[:80], exc)

        if not grounding_parts:
            return ""
        header = (
            "## LEGAL FRAMEWORK GROUNDING\n\n"
            f"Executive Order context ({len(grounding_parts)} documents) "
            "scoped to this section's chapters:\n\n"
        )
        return header + "\n\n".join(grounding_parts)

    def _load_style_rules(self, path: Path) -> str:
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        match = re.search(r'(## STYLE CONVE[NT]+IONS.*?)(?=\n## |\Z)', text, re.DOTALL)
        return match.group(1).strip() if match else text.strip()

    def _rebuild_index_md(self, chapters_dir: Path, chapter_entries: list[dict]) -> None:
        parts = [
            f"# {self.cfg.project.title}\n",
            f"**Author:** {self.cfg.project.author}  ",
            f"**Date:** {self.cfg.project.date}\n",
            "## Table of Contents\n",
        ]
        for i, entry in enumerate(chapter_entries):
            title = entry.get("title", entry["filename"])
            wc = entry.get("word_count", 0)
            wc_str = f" — {wc:,} words" if wc else ""
            parts.append(f"{i}. [{title}]({entry['filename']}){wc_str}")
        parts.append("")
        parts.append(
            f"\n---\n*Generated by Rapporteur CLI — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
        )
        (chapters_dir / "index.md").write_text("\n".join(parts), encoding="utf-8")
        self._logger.info("  Rebuilt index.md with section introductions.")

    @staticmethod
    def _strip_code_fencing(text: str) -> str:
        stripped = text.strip()
        if stripped.startswith("```"):
            first_nl = stripped.index("\n") if "\n" in stripped else len(stripped)
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3].rstrip()
        return stripped

    def _log_usage(self, label: str, usage: dict, model: str) -> None:
        cost = self._calculate_cost(model, usage["input"], usage["output"])
        self._logger.info(
            "    %s — in: %s  out: %s  think: %s  ($%.4f)",
            label, f"{usage['input']:,}", f"{usage['output']:,}",
            f"{usage['thinking']:,}", cost,
        )

    def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        rates = self._PRICING.get(model, {"input": 2.50, "output": 10.00})
        return input_tokens / 1_000_000 * rates["input"] + output_tokens / 1_000_000 * rates["output"]


# ---------------------------------------------------------------------------
# Phase 6 — Publish
# ---------------------------------------------------------------------------

class PublishStage(Base):
    """Phase 6 — Build PDF / HTML via Sphinx (MyST‑parser)."""

    # Regex to strip the trailing footer line from each chapter
    _FOOTER_RE = re.compile(
        r'\n---\n\*Generated by Rapporteur CLI[^*]*\*\s*$'
    )

    # Regex to match the ## References section (and everything after it until
    # the next ## heading or ## Implementation Record, whichever comes first)
    _REFS_SECTION_RE = re.compile(
        r'\n## References\n.*?(?=\n## (?!References)|\Z)',
        re.DOTALL,
    )

    # Regex to match the ## Top Sources section
    _TOP_SOURCES_SECTION_RE = re.compile(
        r'\n## Top Sources\n.*?(?=\n## |\Z)',
        re.DOTALL,
    )

    # Regex to match ## Implementation Record and everything below it
    _IMPL_RECORD_RE = re.compile(
        r'\n## Implementation Record\n.*',
        re.DOTALL,
    )

    # Parse a single reference line:
    # - **Author** (Date). *Title*. Publisher. [url](url)
    _REF_LINE_RE = re.compile(
        r'^- \*\*(?P<author>[^*]+)\*\*'
        r'(?:\s*\((?P<date>[^)]*)\))?\.?'
        r'(?:\s*\*(?P<title>[^*]+)\*\.?)?'
        r'(?:\s*(?P<publisher>[^[]+?)\.)?'
        r'(?:\s*\[(?P<url>[^\]]+)\]\([^)]+\))?',
    )

    _PRICING = {
        "gemini-2.5-pro":            {"input": 1.25,  "output": 10.00},
        "gemini-2.5-flash":          {"input": 0.30,  "output":  2.50},
        "gemini-2.5-flash-lite":     {"input": 0.10,  "output":  0.40},
        "gemini-3.1-pro-preview":    {"input": 1.25,  "output": 10.00},
    }

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
    # Sphinx-based PDF builder
    # ------------------------------------------------------------------

    def _build_pdf(
        self,
        build_dir: Path,
        chapters_dir: Path,
        chapter_entries: list[dict],
    ) -> None:
        """Build PDF via sphinx-build -M latexpdf using MyST-parser."""
        slug = re.sub(r'[^a-z0-9]+', '_', self.cfg.project.title.lower()).strip('_')

        # -- 1. Prepare Sphinx source directory ----------------------------
        # Work on copies so the original chapter files stay unmodified.
        import shutil as _shutil

        src_dir = build_dir / "_sphinx_src"
        if src_dir.exists():
            _shutil.rmtree(src_dir)
        src_dir.mkdir()

        # Sort chapter entries: intro (00-) first, then numbered, then
        # conclusion (typically 10- or highest-numbered), references last.
        sorted_entries = self._sort_chapter_entries(chapter_entries)

        chapter_stems: list[str] = []
        all_ref_lines: list[str] = []
        word_count = 0
        for entry in sorted_entries:
            chapter_file = chapters_dir / entry["filename"]
            if not chapter_file.exists():
                self._logger.warning(
                    "  Chapter file missing: %s", entry["filename"]
                )
                continue
            text = chapter_file.read_text(encoding="utf-8")
            text = self._FOOTER_RE.sub('', text).rstrip()
            text = self._strip_leading_blockquote(text)
            text = re.sub(r'^\*\*(?:Date|Subject):\*\*.*\n?', '', text, flags=re.MULTILINE)
            # Strip baked numbers from ALL heading levels so Sphinx auto-numbers
            text = re.sub(r'^(#{1,4} )\d+(?:\.\d+)*\.?\s*', r'\1', text, flags=re.MULTILINE)
            # Demote extra # headings to ## (only the first # stays as chapter)
            text = self._demote_extra_chapter_headings(text)

            # --- Collect and strip per-chapter references -----------------
            if self.cfg.publish.collect_references:
                text, refs = self._extract_references(text)
                all_ref_lines.extend(refs)

            # --- Strip Implementation Record prose, but keep diagrams --
            impl_match = self._IMPL_RECORD_RE.search(text)
            if impl_match:
                impl_section = impl_match.group(0)
                # Extract any mermaid blocks from the Implementation Record
                impl_diagrams = self._MERMAID_BLOCK_RE.findall(impl_section)
                text = self._IMPL_RECORD_RE.sub('', text).rstrip()
                # Re-insert diagrams after exec summary / first heading
                if impl_diagrams:
                    diagram_block = '\n\n'.join(
                        f'```mermaid\n{diag}```' for diag in impl_diagrams
                    )
                    text = self._insert_after_first_heading(text, diagram_block)

            # --- Generate LLM captions for mermaid diagrams ---------------
            if self.cfg.publish.diagram_captions and not self.dry_run:
                text = self._add_diagram_captions(text)

            # Render ```mermaid fences to PNG images for PDF inclusion
            text = self._render_mermaid_blocks(text, src_dir)
            (src_dir / entry["filename"]).write_text(text + '\n', encoding="utf-8")
            chapter_stems.append(chapter_file.stem)
            word_count += len(text.split())

        if not chapter_stems:
            self._logger.error("  No chapter files found")
            return

        # -- 1b. Write collected references chapter ------------------------
        if self.cfg.publish.collect_references and all_ref_lines:
            refs_md = self._format_apa_references(all_ref_lines)
            refs_filename = "99-references.md"
            (src_dir / refs_filename).write_text(refs_md + '\n', encoding="utf-8")
            chapter_stems.append("99-references")
            self._logger.info(
                "  Collected %d unique references into %s",
                refs_md.count('\n- '), refs_filename,
            )

        self._logger.info(
            "  Prepared %d chapters (%d words)", len(chapter_stems), word_count
        )

        # Symlink or copy kaliya images so Sphinx can resolve relative paths
        kaliya_src = build_dir / "kaliya"
        kaliya_link = src_dir / "kaliya"
        if kaliya_src.is_dir() and not kaliya_link.exists():
            try:
                kaliya_link.symlink_to(kaliya_src.resolve())
            except OSError:
                _shutil.copytree(kaliya_src, kaliya_link)

        # -- 2. Write conf.py ----------------------------------------------
        conf_py = self._build_conf_py(slug)
        conf_path = src_dir / "conf.py"
        conf_path.write_text(conf_py, encoding="utf-8")
        self._logger.info("  Written: conf.py")

        # -- 3. Write index.md (MyST toctree) ------------------------------
        index_md = self._build_index_md(chapter_stems)
        index_path = src_dir / "index.md"
        index_path.write_text(index_md, encoding="utf-8")
        self._logger.info("  Written: index.md")

        if self.dry_run:
            self._logger.info(
                "  [dry-run] Would invoke sphinx-build -M latexpdf %s",
                src_dir,
            )
            return

        # -- 4. Invoke sphinx-build ----------------------------------------
        latex_build_dir = build_dir / "_build" / "latex"
        cmd = [
            "sphinx-build", "-M", "latexpdf",
            str(src_dir),                # sourcedir
            str(build_dir / "_build"),   # outputdir
            "-q",                        # quiet
        ]
        self._logger.info("  Running: %s", ' '.join(cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(src_dir),
            timeout=600,
        )

        if result.returncode != 0:
            # latexmk may exit non-zero even when a usable PDF was produced
            # (e.g. segfault on a later pass).  Check before aborting.
            pdf_in_latex = latex_build_dir / f"{slug}.pdf"
            if not pdf_in_latex.exists():
                for pdf_cand in latex_build_dir.glob("*.pdf"):
                    pdf_in_latex = pdf_cand
                    break
            if not pdf_in_latex.exists():
                self._logger.error("  sphinx-build failed (exit %d)", result.returncode)
                stderr = result.stderr.strip()
                if stderr:
                    for line in stderr.splitlines()[-15:]:
                        self._logger.error("    %s", line)
                return
            self._logger.warning(
                "  sphinx-build exited %d but PDF was produced — continuing",
                result.returncode,
            )

        if result.stderr:
            for line in result.stderr.strip().splitlines():
                self._logger.debug("  sphinx: %s", line)

        # -- 5. Copy PDF to build root with expected name ------------------
        pdf_in_latex = latex_build_dir / f"{slug}.pdf"
        if not pdf_in_latex.exists():
            # Sphinx may use the first latex_documents entry name
            for pdf_cand in latex_build_dir.glob("*.pdf"):
                pdf_in_latex = pdf_cand
                break

        output_pdf = build_dir / f"{slug}.pdf"
        if pdf_in_latex.exists():
            import shutil
            shutil.copy2(pdf_in_latex, output_pdf)
        else:
            self._logger.error("  sphinx-build completed but no PDF found in %s", latex_build_dir)
            return

        # -- 6. Report -----------------------------------------------------
        if output_pdf.exists():
            size = _human_size(output_pdf)
            pages = self._pdf_page_count(output_pdf)
            page_info = f", {pages} pages" if pages else ""
            self._logger.info("  PDF written: %s (%s%s)", output_pdf.name, size, page_info)
        else:
            self._logger.error("  PDF copy failed")
            return

        # -- 7. Update manifest with publish metadata ----------------------
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

    # ------------------------------------------------------------------
    # Chapter ordering
    # ------------------------------------------------------------------

    @staticmethod
    def _sort_chapter_entries(entries: list[dict]) -> list[dict]:
        """Sort chapter entries by filename so intros/conclusions stay
        interleaved with their content chapters.

        The filenames use a naming convention that sorts naturally:
        00-introduction, 01a-part-1-introduction, 01-..., 04z-part-1-conclusion,
        05a-part-2-introduction, 05-..., etc.
        """
        import re as _re

        def _sort_key(entry: dict) -> tuple:
            fn = entry.get("filename", "")
            # Extract leading number and optional letter suffix
            m = _re.match(r'^(\d+)([a-z]?)', fn)
            if m:
                num = int(m.group(1))
                suffix = m.group(2) or 'm'  # 'm' sorts between 'a' and 'z'
                return (num, suffix, fn)
            return (9999, 'z', fn)
        return sorted(entries, key=_sort_key)

    # ------------------------------------------------------------------
    # Reference collection and APA formatting
    # ------------------------------------------------------------------

    # Regex to extract inline citations like [Author, Month YYYY](url)
    # Also matches date-only variants: [Month YYYY](url) or [YYYY](url)
    _INLINE_CITE_RE = re.compile(
        r'\[(?P<label>[^\]]*?'
        r'(?:January|February|March|April|May|June'
        r'|July|August|September|October|November|December)?'
        r'\s*\d{4})\]'
        r'\((?P<url>https?://[^)]+)\)',
    )

    def _extract_references(self, text: str) -> tuple[str, list[str]]:
        """Strip ``## References`` and ``## Top Sources`` from chapter text.

        Returns (cleaned_text, list_of_reference_lines) where each
        reference line starts with ``- ``.

        When no ``## References`` section exists, inline citations of the
        form ``[Author, Month YYYY](url)`` are harvested instead.
        """
        ref_lines: list[str] = []

        # Extract reference bullet lines from ## References section
        refs_match = self._REFS_SECTION_RE.search(text)
        if refs_match:
            section_text = refs_match.group(0)
            for line in section_text.splitlines():
                line = line.strip()
                if line.startswith('- '):
                    ref_lines.append(line)
            text = self._REFS_SECTION_RE.sub('', text)

        # Also strip ## Top Sources (duplicates references)
        text = self._TOP_SOURCES_SECTION_RE.sub('', text)

        # Fallback: harvest inline [Author, Month YYYY](url) citations
        if not ref_lines:
            seen_urls: set[str] = set()
            for m in self._INLINE_CITE_RE.finditer(text):
                url = m.group("url").strip()
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                label = m.group("label").strip()
                # Split "Author, Month YYYY" into author and date
                parts = label.rsplit(", ", 1)
                if len(parts) == 2:
                    author, date = parts[0].strip(), parts[1].strip()
                else:
                    author, date = label, "n.d."
                ref_lines.append(
                    f"- **{author}** ({date}). [{url}]({url})"
                )

        return text.rstrip(), ref_lines

    @staticmethod
    def _parse_ref_line(line: str) -> dict[str, str]:
        """Parse a reference bullet line into structured fields.

        Expected format:
        ``- **Author** (Date). *Title*. Publisher. [url](url)``
        """
        result: dict[str, str] = {"raw": line}
        # Strip leading "- "
        body = line[2:].strip() if line.startswith('- ') else line.strip()

        # Author: **...**
        m = re.match(r'\*\*([^*]+)\*\*', body)
        if m:
            result["author"] = m.group(1).strip()
            body = body[m.end():].strip()

        # Date: (...)
        m = re.match(r'\(([^)]*)\)', body)
        if m:
            result["date"] = m.group(1).strip()
            body = body[m.end():].strip()

        # Strip leading period/dot
        body = body.lstrip('.').strip()

        # Title: *...*
        m = re.match(r'\*([^*]+)\*', body)
        if m:
            result["title"] = m.group(1).strip().rstrip('.')
            body = body[m.end():].strip().lstrip('.')

        # URL: [url](url) — extract from the end
        url_m = re.search(r'\[([^\]]+)\]\(([^)]+)\)', body)
        if url_m:
            result["url"] = url_m.group(2).strip()
            # Publisher is what's left before the URL
            before_url = body[:url_m.start()].strip().rstrip('.')
            if before_url:
                result["publisher"] = before_url.strip()
        else:
            # No URL — remaining text is publisher
            leftover = body.strip().rstrip('.')
            if leftover:
                result["publisher"] = leftover.strip()

        return result

    def _format_apa_references(self, ref_lines: list[str]) -> str:
        """Deduplicate and format all references in APA 7th edition style.

        Returns a complete Markdown chapter for the references appendix.
        """
        # Parse all reference lines
        parsed: list[dict[str, str]] = []
        for line in ref_lines:
            if line.startswith('- '):
                parsed.append(self._parse_ref_line(line))

        # Deduplicate by URL (prefer entry with most fields filled)
        seen_urls: dict[str, dict[str, str]] = {}
        no_url: list[dict[str, str]] = []
        for ref in parsed:
            url = ref.get("url", "")
            if url:
                norm_url = _normalise_url(url)
                existing = seen_urls.get(norm_url)
                if not existing or len(ref) > len(existing):
                    seen_urls[norm_url] = ref
            else:
                no_url.append(ref)

        unique_refs = list(seen_urls.values()) + no_url

        # Sort alphabetically by author surname, then date
        unique_refs.sort(key=lambda r: (
            r.get("author", "Unknown").split()[-1].lower()
            if r.get("author") else "zzz",
            r.get("date", ""),
        ))

        # Format in APA 7th style
        lines = [
            "# References",
            "",
            "*All sources cited in this document, deduplicated and "
            "formatted in APA 7th edition style.*",
            "",
        ]
        for ref in unique_refs:
            author = ref.get("author", "Unknown")
            date = ref.get("date", "n.d.")
            title = ref.get("title", "")
            publisher = ref.get("publisher", "")
            url = ref.get("url", "")

            # APA format: Author. (Date). *Title*. Publisher. URL
            parts = [f"- {author}. ({date})."]
            if title:
                parts.append(f" *{title}.*")
            if publisher:
                parts.append(f" {publisher}.")
            if url:
                parts.append(f" [{url}]({url})")
            lines.append("".join(parts))

        self._logger.info("  APA references: %d unique from %d total",
                         len(unique_refs), len(ref_lines))
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Diagram repositioning
    # ------------------------------------------------------------------

    def _insert_after_first_heading(self, text: str, block: str) -> str:
        """Insert *block* after the first ``## `` section's content.

        The diagram is placed just before the *second* ``## `` heading,
        so it appears at the end of the executive-summary / overview
        section.  Falls back to appending at the end if fewer than two
        ``## `` headings exist.
        """
        lines = text.split('\n')
        h2_count = 0
        for i, line in enumerate(lines):
            if line.startswith('## '):
                h2_count += 1
                if h2_count == 2:
                    # Insert just before the second ## heading
                    lines.insert(i, block + '\n')
                    return '\n'.join(lines)
        # Fallback: append at end
        return text + '\n\n' + block

    def _reposition_diagrams(self, text: str) -> str:
        """Move ``## Implementation Record`` section from the end of the
        chapter to directly after the executive summary / abstract.

        The exec summary is identified as the blockquote paragraph(s) that
        immediately follow the chapter ``# `` heading.  If no exec summary
        is found, diagrams stay in their original position.
        """
        impl_match = self._IMPL_RECORD_RE.search(text)
        if not impl_match:
            return text

        impl_section = impl_match.group(0).strip()
        text_without_impl = self._IMPL_RECORD_RE.sub('', text).rstrip()

        # Find insertion point: after the exec summary blockquote(s)
        # Pattern: lines starting with '> ' immediately after the # heading
        lines = text_without_impl.split('\n')
        insert_idx = None
        in_heading = False
        in_blockquote = False

        for i, line in enumerate(lines):
            if line.startswith('# ') and not line.startswith('## '):
                in_heading = True
                continue
            if in_heading:
                stripped = line.strip()
                if stripped.startswith('>'):
                    in_blockquote = True
                    continue
                if in_blockquote and stripped == '':
                    # Blank line after blockquote — could be between
                    # consecutive blockquote paragraphs or the end
                    # Look ahead: if next non-blank line is also a blockquote, continue
                    for j in range(i + 1, min(i + 3, len(lines))):
                        if lines[j].strip().startswith('>'):
                            break
                        if lines[j].strip():
                            insert_idx = i
                            break
                    else:
                        insert_idx = i
                    if insert_idx is not None:
                        break
                    continue
                if in_blockquote and not stripped.startswith('>'):
                    insert_idx = i
                    break
                if not in_blockquote and stripped:
                    # No blockquote found after heading — insert after heading
                    insert_idx = i
                    break

        if insert_idx is None:
            # Fallback: append at end (original behaviour)
            return text_without_impl + '\n\n' + impl_section

        # Insert the implementation record section
        lines.insert(insert_idx, '\n' + impl_section + '\n')
        return '\n'.join(lines)

    # ------------------------------------------------------------------
    # LLM diagram captions
    # ------------------------------------------------------------------

    _MERMAID_BLOCK_RE = re.compile(
        r'```mermaid\n(.*?)```', re.DOTALL,
    )

    def _add_diagram_captions(self, text: str) -> str:
        """Find mermaid blocks and append an LLM-generated caption below each.

        The full chapter text is passed to the LLM for context so captions
        are specific and descriptive.
        """
        # Use the full chapter as context (sans mermaid blocks themselves)
        chapter_context = self._MERMAID_BLOCK_RE.sub('[diagram]', text)

        def _replace_with_caption(match: re.Match) -> str:
            mermaid_code = match.group(1)
            original = match.group(0)
            try:
                caption = self._generate_diagram_caption(mermaid_code, chapter_context)
            except Exception as exc:
                self._logger.warning("  Caption generation failed: %s", exc)
                return original
            # Escape inner asterisks so they don't break markdown italic
            safe_caption = caption.replace('*', '\\*')
            return f"{original}\n\n*{safe_caption}*"

        return self._MERMAID_BLOCK_RE.sub(_replace_with_caption, text)

    def _generate_diagram_caption(
        self, mermaid_code: str, eo_context: str,
    ) -> str:
        """Use Gemini Flash to generate a figure caption explaining the
        EO dependency diagram.

        Returns a caption string, or a fallback if the LLM call fails.
        """
        model = self.cfg.publish.caption_model
        prompt = (
            "You are writing a descriptive figure caption for a policy "
            "document aimed at an informed general audience. The chapter "
            "below analyses Executive Orders and the mermaid diagram shows "
            "how they relate.\n\n"
            "## Chapter Content\n\n"
            f"{eo_context[:12000]}\n\n"
            "## Mermaid Diagram\n\n"
            f"```mermaid\n{mermaid_code}\n```\n\n"
            "Write a 3-4 sentence figure caption that:\n"
            "1. Refers to each Executive Order by its SHORT COMMON NAME "
            "(e.g. 'Unleashing Prosperity Through Deregulation') — do NOT "
            "cite EO numbers like 14148 or 14319\n"
            "2. Explains in plain language how they connect — which order "
            "enabled, replaced, or built upon the others\n"
            "3. Summarises the real-world significance for a layman — "
            "what changed in practice\n\n"
            "Respond with ONLY the caption text. No heading, no prefix "
            "like 'Figure:', no bullet points, no asterisks, no markdown "
            "formatting. Write in accessible formal prose, 60-100 words."
        )
        try:
            from src._clients.llm_gcp_client import GeminiLLMClient
            client = GeminiLLMClient(
                model_name=model,
                location=_gemini_location(model),
            )
            from google.genai import types as genai_types
            config = genai_types.GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=2048,
                thinking_config=genai_types.ThinkingConfig(
                    thinking_budget=0,
                ),
            )
            response = client.client.models.generate_content(
                model=client.model_name,
                contents=prompt,
                config=config,
            )
            caption = (response.text or "").strip()
            if caption:
                # Track cost
                um = getattr(response, "usage_metadata", None)
                in_tok = getattr(um, "prompt_token_count", 0) or 0 if um else 0
                out_tok = getattr(um, "candidates_token_count", 0) or 0 if um else 0
                cost = self._calculate_cost(model, in_tok, out_tok)
                self._logger.info(
                    "    Caption — in: %s  out: %s  ($%.4f)",
                    f"{in_tok:,}", f"{out_tok:,}", cost,
                )
                return caption
        except Exception as e:
            self._logger.warning("  Caption generation failed: %s", e)

        return ("This diagram shows how these executive orders connect. "
                "The central order enabled or replaced others, reshaping "
                "federal policy across the areas discussed in this chapter.")

    def _calculate_cost(
        self, model: str, input_tokens: int, output_tokens: int,
    ) -> float:
        rates = self._PRICING.get(model, {"input": 0.30, "output": 2.50})
        return (
            input_tokens / 1_000_000 * rates["input"]
            + output_tokens / 1_000_000 * rates["output"]
        )

    # ------------------------------------------------------------------
    # Sphinx configuration generators
    # ------------------------------------------------------------------

    def _build_conf_py(self, slug: str) -> str:
        """Generate a Sphinx conf.py matching the Kaliya reference style."""
        title = self.cfg.project.title
        author = self.cfg.project.author
        date = self.cfg.project.date
        # Escape for Python string literals
        title_esc = title.replace('"', '\\"')
        author_esc = author.replace('"', '\\"')
        # Escape LaTeX special characters for latex_documents
        latex_title = title.replace('&', r'\&').replace('%', r'\%').replace('$', r'\$').replace('#', r'\#').replace('_', r'\_')

        return f'''# -- Sphinx configuration (auto-generated by Rapporteur CLI) --
import os, shutil

project = "{title_esc}"
author = "{author_esc}"
copyright = "2026, {author_esc}"
release = "1.0"

# -- Extensions ---------------------------------------------------------------
extensions = [
    "myst_parser",
]

# -- MyST settings ------------------------------------------------------------
myst_enable_extensions = ["colon_fence", "deflist"]
source_suffix = {{".md": "markdown"}}

# -- HTML output ---------------------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_title = "{title_esc}"

# -- LaTeX / PDF settings ------------------------------------------------------
latex_elements = {{
    "papersize": "a4paper",
    "pointsize": "11pt",
    "sphinxsetup": (
        "TitleColor={{RGB}}{{34,34,34}},"
        "InnerLinkColor={{RGB}}{{26,115,232}},"
        "OuterLinkColor={{RGB}}{{26,115,232}},"
        "VerbatimBorderColor={{RGB}}{{224,224,224}},"
        "noteBorderColor={{RGB}}{{66,133,244}},"
        "noteBgColor={{RGB}}{{245,247,250}},"
    ),
    "preamble": r"""
% --- Fonts (Palatino body + Helvetica headings) ------------------------------
\\usepackage{{fontspec}}
\\setmainfont{{TeX Gyre Pagella}}
\\setsansfont{{TeX Gyre Heros}}
\\setmonofont{{TeX Gyre Cursor}}[Scale=0.88]

% --- Line spacing 1.5x (generous, airy feel) --------------------------------
\\usepackage{{setspace}}
\\onehalfspacing

% --- Block paragraphs: skip between paras, no indent ------------------------
\\setlength{{\\parskip}}{{0.6em plus 0.2em minus 0.1em}}
\\setlength{{\\parindent}}{{0pt}}

% --- Sans-serif section headings (keep fncychap chapter decoration) ----------
\\usepackage{{titlesec}}
\\titleformat{{\\section}}
  {{\\normalfont\\sffamily\\bfseries\\Large}}{{\\thesection}}{{1em}}{{}}
\\titleformat{{\\subsection}}
  {{\\normalfont\\sffamily\\bfseries\\large}}{{\\thesubsection}}{{1em}}{{}}
\\titleformat{{\\subsubsection}}
  {{\\normalfont\\sffamily\\bfseries\\normalsize}}{{\\thesubsubsection}}{{1em}}{{}}

% --- Body text in near-black -------------------------------------------------
\\usepackage{{xcolor}}
\\color[RGB]{{34,34,34}}

% --- Images, tables, captions ------------------------------------------------
\\usepackage{{graphicx}}
\\setkeys{{Gin}}{{width=\\linewidth,height=0.85\\textheight,keepaspectratio=true}}
\\usepackage{{float}}
\\usepackage{{booktabs}}
\\usepackage{{caption}}
\\captionsetup{{font=small,labelfont=bf,skip=4pt}}

% --- Full-width tables with smaller font ------------------------------------
\\usepackage{{tabularx}}
\\usepackage{{longtable}}
\\usepackage{{etoolbox}}
% Shrink all table environments to \\footnotesize with tighter column padding
\\AtBeginEnvironment{{tabulary}}{{\\footnotesize\\tabcolsep=3pt}}
\\AtBeginEnvironment{{longtable}}{{\\footnotesize\\tabcolsep=3pt}}
\\AtBeginEnvironment{{tabular}}{{\\footnotesize\\tabcolsep=3pt}}
% Force Sphinx tabulary to use full textwidth
\\renewcommand{{\\tymin}}{{20pt}}
\\renewcommand{{\\tymax}}{{\\textwidth}}
% Let long URLs wrap inside table cells
\\usepackage{{seqsplit}}

% --- Section numbering depth -------------------------------------------------
\\setcounter{{secnumdepth}}{{2}}
\\setcounter{{tocdepth}}{{2}}

% --- Fix running headers: truncate long titles -------------------------------
\\usepackage[breakall]{{truncate}}
\\renewcommand{{\\chaptermark}}[1]{{%
  \\markboth{{\\truncate{{.7\\textwidth}}{{#1}}}}{{}}%
}}
\\renewcommand{{\\sectionmark}}[1]{{%
  \\markright{{\\truncate{{.7\\textwidth}}{{#1}}}}%
}}

% --- Fix fancyhdr headheight warning -----------------------------------------
\\setlength{{\\headheight}}{{14pt}}
\\addtolength{{\\topmargin}}{{-2pt}}
""",
    "figure_align": "H",
    "geometry": r"\\usepackage[a4paper,top=2.5cm,bottom=2.5cm,left=2cm,right=2cm]{{geometry}}",
}}

# Full-width tables, coloured header rows
latex_table_style = ["booktabs", "colorrows"]

# Sphinx auto-numbers sections (secnumdepth=2); figure numbering off
numfig = False
latex_engine = "xelatex"
latex_documents = [
    ("index", "{slug}.tex",
     r"{latex_title}",
     r"{author_esc}", "manual"),
]
'''

    def _build_index_md(self, chapter_stems: list[str]) -> str:
        """Generate a MyST index.md with toctree entries."""
        title = self.cfg.project.title
        lines = [
            f"# {title}",
            "",
            "```{toctree}",
            ":maxdepth: 2",
            ":caption: Chapters",
            "",
        ]
        for stem in chapter_stems:
            lines.append(stem)
        lines.append("```")
        lines.append("")
        return '\n'.join(lines)

    @staticmethod
    def _strip_leading_blockquote(text: str) -> str:
        """Remove the leading blockquote (duplicate abstract) before the first heading."""
        lines = text.split('\n')
        first_heading = None
        for i, line in enumerate(lines):
            if line.startswith('#'):
                first_heading = i
                break
        if first_heading is not None and first_heading > 0:
            pre = '\n'.join(lines[:first_heading]).strip()
            if pre.startswith('>'):
                return '\n'.join(lines[first_heading:])
        return text

    @staticmethod
    def _demote_extra_chapter_headings(text: str) -> str:
        """Ensure only the first ``#`` heading stays at chapter level; extras become ``##``."""
        lines = text.split('\n')
        seen_first = False
        for i, line in enumerate(lines):
            if line.startswith('# ') and not line.startswith('## '):
                if seen_first:
                    lines[i] = '#' + line  # '# X' → '## X'
                else:
                    seen_first = True
        return '\n'.join(lines)

    _mmdc_counter: int = 0  # class-level counter for unique diagram filenames

    def _render_mermaid_blocks(self, text: str, src_dir: Path) -> str:
        """Render ```mermaid blocks to PNG via mmdc and replace with image refs.

        Falls back to a placeholder if mmdc is unavailable or fails.
        """
        import shutil as _shutil
        import tempfile

        if not _shutil.which('mmdc'):
            self._logger.warning("  mmdc not found — stripping mermaid blocks")
            return re.sub(
                r'```mermaid\n.*?```',
                '*[Diagram omitted from PDF — see online version]*',
                text, flags=re.DOTALL,
            )

        images_dir = src_dir / "_diagrams"
        images_dir.mkdir(exist_ok=True)

        # Find puppeteer config
        puppeteer_cfg = None
        for candidate in [
            Path(self.cfg.paths.build_dir) / "puppeteer-config.json",
            Path(__file__).parent.parent.parent.parent / "docs" / "AtrumVolentia" / "puppeteer-config.json",
        ]:
            if candidate.exists():
                puppeteer_cfg = candidate
                break

        def _render_one(match: re.Match) -> str:
            PublishStage._mmdc_counter += 1
            mermaid_code = match.group(0)
            # Extract just the code between fences
            inner = re.search(r'```mermaid\n(.*?)```', mermaid_code, re.DOTALL)
            if not inner:
                return mermaid_code

            img_name = f"diagram_{PublishStage._mmdc_counter:03d}.png"
            img_path = images_dir / img_name

            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.mmd', delete=False, encoding='utf-8'
            ) as tmp:
                tmp.write(inner.group(1))
                tmp_path = tmp.name

            try:
                cmd = ['mmdc', '-i', tmp_path, '-o', str(img_path),
                       '-b', 'white', '-w', '1200']
                if puppeteer_cfg:
                    cmd.extend(['-p', str(puppeteer_cfg)])
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=60,
                )
                if result.returncode != 0 or not img_path.exists():
                    self._logger.warning(
                        "  mmdc failed for diagram %d: %s",
                        PublishStage._mmdc_counter,
                        result.stderr.strip()[:200] if result.stderr else "unknown error",
                    )
                    return '*[Diagram could not be rendered]*'

                self._logger.info("  Rendered diagram: %s", img_name)
                # Use relative path from Sphinx source
                rel = f"_diagrams/{img_name}"
                return f"```{{image}} {rel}\n:width: 100%\n:align: center\n```"
            except (subprocess.TimeoutExpired, OSError) as exc:
                self._logger.warning("  mmdc error: %s", exc)
                return '*[Diagram could not be rendered]*'
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        return re.sub(
            r'```mermaid\n.*?```',
            _render_one,
            text,
            flags=re.DOTALL,
        )

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
    ("export",             ExportStage),
    ("parse",              ParseStage),
    ("chapters",           ChapterStage),
    ("kaliya",             KaliyaStage),
    ("eo_link",            EOLinkStage),
    ("media_link",         MediaLinkStage),
    ("format",             FormatStage),
    ("conclude",           ConcludeStage),
    ("section_conclude",   SectionConcludeStage),
    ("edit",               EditStage),
    ("cite",               CiteStage),
    ("introduce",          IntroduceStage),
    ("section_introduce",  SectionIntroduceStage),
    ("analytics_conclude", AnalyticsConcludeStage),
    ("fuse_analysis",       FuseAnalysisStage),
    ("publish",            PublishStage),
]


def run_pipeline(cfg: PipelineConfig, selected: list[str], dry_run: bool) -> None:
    """Run the selected stages in sequence."""
    log.info("=" * 70)
    log.info("Rapporteur CLI — %s", cfg.project.title)
    log.info("  Build dir: %s", cfg.paths.build_dir)
    log.info("  Dry run:   %s", dry_run)
    if cfg.force:
        log.info("  Force:     True (idempotency guards overridden)")
    log.info("=" * 70)

    for stage_name, stage_cls in STAGES:
        if stage_name in selected:
            log.info("")
            log.info("── Stage: %s ──", stage_name.upper())
            stage = stage_cls(cfg, dry_run=dry_run)
            stage.run()

    # Cost rollup
    _write_cost_rollup(cfg, selected)

    log.info("")
    log.info("Pipeline complete.")


# ---------------------------------------------------------------------------
# Cost rollup
# ---------------------------------------------------------------------------

_COST_STAGES = ("format", "conclude", "section_conclude", "edit", "cite", "introduce", "section_introduce", "eo_link", "media_link", "analytics_conclude", "fuse_analysis")


def _write_cost_rollup(cfg: PipelineConfig, selected: list[str]) -> None:
    """Aggregate per-stage costs into a top-level ``costs`` manifest block."""
    manifest_path = Path(cfg.paths.build_dir) / "chapters" / "manifest.json"
    if not manifest_path.exists():
        return

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    by_stage: dict[str, dict] = {}
    total_cost = 0.0
    total_in = 0
    total_out = 0

    for stage_name in _COST_STAGES:
        stage_data = manifest.get(stage_name)
        if not stage_data or not isinstance(stage_data, dict):
            continue
        # Cost may be nested under "usage" or at top level
        usage = stage_data.get("usage", stage_data)
        cost = float(usage.get("estimated_cost_usd", 0))
        in_tok = int(usage.get("input_tokens", 0))
        out_tok = int(usage.get("output_tokens", 0))
        model = stage_data.get("model", "")
        by_stage[stage_name] = {
            "model": model,
            "cost_usd": round(cost, 4),
            "input_tokens": in_tok,
            "output_tokens": out_tok,
        }
        total_cost += cost
        total_in += in_tok
        total_out += out_tok

    if not by_stage:
        return

    manifest["costs"] = {
        "total_estimated_usd": round(total_cost, 4),
        "total_input_tokens": total_in,
        "total_output_tokens": total_out,
        "by_stage": by_stage,
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + '\n',
        encoding="utf-8",
    )
    log.info("")
    log.info(
        "  Total pipeline cost: $%.2f (%d stages)",
        total_cost, len(by_stage),
    )


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
    stages.add_argument("--format",   action="store_true", default=None,
                        help="Stage 5: Apply LLM style formatter to chapters")
    stages.add_argument("--no-format", dest="format", action="store_false",
                        help="Skip style formatter even if enabled in config")
    stages.add_argument("--conclude",  action="store_true", default=None,
                        help="Stage 5b: Generate conclusion chapter via LLM")
    stages.add_argument("--no-conclude", dest="conclude", action="store_false",
                        help="Skip conclusion generation even if enabled in config")
    stages.add_argument("--edit",      action="store_true", default=None,
                        help="Stage 7: Editorial review via LLM")
    stages.add_argument("--no-edit",   dest="edit", action="store_false",
                        help="Skip editorial review even if enabled in config")
    stages.add_argument("--cite",      action="store_true", default=None,
                        help="Stage 7b: Resolve unresolved citation tokens")
    stages.add_argument("--no-cite",   dest="cite", action="store_false",
                        help="Skip citation resolver even if enabled in config")
    stages.add_argument("--introduce", action="store_true", default=None,
                        help="Stage 8: Generate introduction chapter via LLM")
    stages.add_argument("--no-introduce", dest="introduce", action="store_false",
                        help="Skip introduction generation even if enabled in config")
    stages.add_argument("--section-conclude", action="store_true", default=None,
                        help="Generate per-section conclusions for multi-part volumes")
    stages.add_argument("--section-introduce", action="store_true", default=None,
                        help="Generate per-section introductions for multi-part volumes")
    stages.add_argument("--eo-link",  action="store_true", default=None,
                        help="EO Linking: Map .gov/.mil comms to EO framework")
    stages.add_argument("--no-eo-link", dest="eo_link", action="store_false",
                        help="Skip EO linking even if enabled in config")
    stages.add_argument("--media-link", action="store_true", default=None,
                        help="Media Linkage: Link media articles to EO framework")
    stages.add_argument("--no-media-link", dest="media_link", action="store_false",
                        help="Skip media linkage even if enabled in config")
    stages.add_argument("--analytics-conclude", action="store_true", default=None,
                        help="Analytics Conclusion: integrate cluster + Kaliya analyses into overarching findings")
    stages.add_argument("--fuse-analysis", action="store_true", default=None,
                        help="Fuse Analysis: rewrite Phase 4 + Kaliya per cluster into readable prose")
    stages.add_argument("--publish",  action="store_true", help="Stage 8: Build PDF / HTML / Confluence output")
    stages.add_argument("--all",      action="store_true", help="Run all stages (1-6)")

    opts = parser.add_argument_group("options")
    opts.add_argument("--force",        action="store_true", help="Override idempotency guards and re-run stages from scratch")
    opts.add_argument("--dry-run",      action="store_true", help="Preview actions without writing files")
    opts.add_argument("--build-dir",    default=None,        help="Override build directory from config")
    opts.add_argument("--frontend-url", default=None,        help="Override frontend URL from config")
    opts.add_argument("--edit-chapter", default=None,        help="Target a specific chapter for editorial review (filename substring)")
    opts.add_argument("--edit-instructions", default=None,   help="Custom editorial instructions for targeted chapter edit")
    opts.add_argument("--exclude-eos",  nargs="*", default=None,
                      help="EO numbers to exclude from diagrams and tables (e.g. --exclude-eos 14165 14370)")
    opts.add_argument("--clusters", nargs="*", default=None,
                      help="Restrict analytics-conclude to specific clusters (e.g. --clusters A B C E)")
    opts.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        # Keep noisy HTTP/transport libraries at WARNING to avoid
        # dumping huge request/response payloads (grounding data, etc.)
        for _lib in ("httpx", "httpcore", "openai", "urllib3",
                      "requests", "google", "grpc"):
            logging.getLogger(_lib).setLevel(logging.WARNING)

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

    # Handle --format/--no-format override
    if args.format is False:
        cfg.format.enabled = False
    elif args.format is True and "format" not in selected:
        cfg.format.enabled = True
        selected.append("format")
        # Ensure correct ordering
        stage_order = [name for name, _ in STAGES]
        selected.sort(key=lambda s: stage_order.index(s))

    # Handle --conclude/--no-conclude override
    if args.conclude is False:
        cfg.conclude.enabled = False
    elif args.conclude is True:
        cfg.conclude.enabled = True
        if "conclude" not in selected:
            selected.append("conclude")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --edit/--no-edit override
    if args.edit is False:
        cfg.edit.enabled = False
    elif args.edit is True:
        cfg.edit.enabled = True
        if "edit" not in selected:
            selected.append("edit")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --edit-chapter (implies --edit)
    if args.edit_chapter:
        cfg.edit.enabled = True
        cfg.edit.target_chapter = args.edit_chapter
        cfg.edit.custom_instructions = args.edit_instructions
        if "edit" not in selected:
            selected.append("edit")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --cite/--no-cite override
    if args.cite is False:
        cfg.cite.enabled = False
    elif args.cite is True:
        cfg.cite.enabled = True
        if "cite" not in selected:
            selected.append("cite")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --introduce/--no-introduce override
    if args.introduce is False:
        cfg.introduce.enabled = False
    elif args.introduce is True:
        cfg.introduce.enabled = True
        if "introduce" not in selected:
            selected.append("introduce")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --section-conclude
    if args.section_conclude is True:
        cfg.conclude.enabled = True
        cfg.sections.enabled = True
        if "section_conclude" not in selected:
            selected.append("section_conclude")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --section-introduce
    if args.section_introduce is True:
        cfg.introduce.enabled = True
        cfg.sections.enabled = True
        if "section_introduce" not in selected:
            selected.append("section_introduce")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --eo-link/--no-eo-link override
    if args.eo_link is False:
        cfg.eo_linking.enabled = False
    elif args.eo_link is True:
        cfg.eo_linking.enabled = True
        if "eo_link" not in selected:
            selected.append("eo_link")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --media-link/--no-media-link override
    if args.media_link is False:
        cfg.media_linking.enabled = False
    elif args.media_link is True:
        cfg.media_linking.enabled = True
        if "media_link" not in selected:
            selected.append("media_link")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --analytics-conclude
    if args.analytics_conclude is True:
        cfg.analytics_conclude.enabled = True
        if args.clusters:
            cfg.analytics_conclude.clusters = [c.upper() for c in args.clusters]
        if "analytics_conclude" not in selected:
            selected.append("analytics_conclude")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --fuse-analysis
    if getattr(args, 'fuse_analysis', None) is True:
        cfg.fuse_analysis.enabled = True
        if args.clusters:
            cfg.fuse_analysis.clusters = [c.upper() for c in args.clusters]
        if "fuse_analysis" not in selected:
            selected.append("fuse_analysis")
            stage_order = [name for name, _ in STAGES]
            selected.sort(key=lambda s: stage_order.index(s))

    # Handle --force
    if args.force:
        cfg.force = True

    # Handle --exclude-eos
    if args.exclude_eos is not None:
        cfg.eo_linking.exclude_eos = args.exclude_eos

    if not selected:
        parser.error("No stages selected. Use --export, --parse, --chapters, --kaliya, --format, --conclude, --edit, --cite, --introduce, --eo-link, --media-link, --analytics-conclude, --fuse-analysis, --publish, or --all.")

    run_pipeline(cfg, selected, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
