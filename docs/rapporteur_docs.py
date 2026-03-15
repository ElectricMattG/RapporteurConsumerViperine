#!/usr/bin/env python3
"""
rapporteur_docs.py — General-purpose agent-state-to-Markdown document builder.

Resolves citation references and retrieved documents from RapporteurApp
conversation state (bundled JSON array or split files) and renders each
conversation into a structured Markdown research document.

Usage
-----
    # Run from a manifest:
    python3 docs/rapporteur_docs.py <manifest.json> [--dry-run]

    # Import as a library:
    from docs.rapporteur_docs import RefResolver, DocumentRenderer, ConversationState

Manifest schema
---------------
{
  "output_dir": "docs/midnight_hammer",   // relative to manifest or absolute
  "raw_dir":    "docs/midnight_hammer/raw", // default dir for relative source paths
  "documents": [
    {
      "filename": "01_foo.md",
      "title":    "The Title",
      "source": {
        "type":  "bundled",
        "file":  "conversations.json",
        "index": 0
      }
    },
    {
      "filename": "02_bar.md",
      "title":    "Another Title",
      "source": {
        "type":              "split",
        "response_file":     "response_1.txt",
        "docs_file":         "docs_1.json",
        "citation_map_file": "citation_map_1.json",
        "user_query":        "Optional query text"
      }
    },
    {
      "filename": "03_merged.md",
      "title":    "Bundled source with sidecar overrides",
      "source": {
        "type":              "bundled",
        "file":              "conversations.json",
        "index":             2,
        "citation_map_file": "citation_map_2.json",
        "docs_file":         "docs_2.json"
      }
    }
  ],
  "render_options": {
    "top_sources_n":      10,
    "abstract_max_words": 220,
    "postprocess":        true
  }
}

Ref resolution strategy
-----------------------
For **bundled** sources the primary refs come from the conversation object itself:
  - citation_map  ← conv["metadata"]["citation_map"]
  - retrieved_docs ← conv["retrieved_docs"]

Optional sidecar files (``citation_map_file``, ``docs_file``) can be added to any
bundled entry; their contents are merged in, with sidecars taking priority on URL
collisions. This lets you progressively enrich agent state without re-exporting the
full conversation export.

For **split** sources all three artefacts (response text, docs, citation map) are
loaded from separate files. ``citation_map_file`` may be a JSON dict *or* a JSON
array; an array is automatically re-keyed by position.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Normalised conversation state
# ---------------------------------------------------------------------------

class ConversationState:
    """Resolved, normalised representation of a single agent-conversation turn."""

    __slots__ = (
        "user_query",
        "response",
        "retrieved_docs",
        "retrieved_qa",
        "citation_map",
        "enrichment_results",
        "timestamp",
    )

    def __init__(
        self,
        user_query: str,
        response: str,
        retrieved_docs: list,
        citation_map: dict,
        retrieved_qa: list | None = None,
        enrichment_results: list | None = None,
        timestamp: str | None = None,
    ) -> None:
        self.user_query        = (user_query or "").strip()
        self.response          = (response or "").strip()
        self.retrieved_docs    = retrieved_docs or []
        self.retrieved_qa      = retrieved_qa or []
        self.citation_map      = citation_map or {}
        self.enrichment_results = enrichment_results or []
        self.timestamp         = timestamp


# ---------------------------------------------------------------------------
# Reference resolver
# ---------------------------------------------------------------------------

class RefResolver:
    """
    Resolves citation_map and retrieved_docs from agent state into a
    ConversationState regardless of whether the state is stored as a
    bundled conversations JSON or as split artefact files.

    Parameters
    ----------
    raw_dir:
        Base directory used to resolve relative paths in source specs.
    """

    def __init__(self, raw_dir: str | Path = ".") -> None:
        self.raw_dir = Path(raw_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, source: dict) -> ConversationState:
        """
        Resolve a source spec dict into a ConversationState.

        The ``source`` dict must contain ``"type"`` of either
        ``"bundled"`` or ``"split"``.
        """
        kind = source.get("type", "bundled")
        if kind == "bundled":
            return self._resolve_bundled(source)
        if kind == "split":
            return self._resolve_split(source)
        raise ValueError(
            f"Unknown source type {kind!r}. Expected 'bundled' or 'split'."
        )

    # ------------------------------------------------------------------
    # Internal loaders
    # ------------------------------------------------------------------

    def _abs(self, relative: str | Path) -> Path:
        p = Path(relative)
        return p if p.is_absolute() else self.raw_dir / p

    def _load_json(self, path: Path) -> Any:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _normalise_citation_map(self, raw: Any) -> dict:
        """Accept both dict and list forms of a citation map."""
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list):
            return {str(i): entry for i, entry in enumerate(raw)}
        return {}

    # ------------------------------------------------------------------
    # Bundled loader
    # ------------------------------------------------------------------

    def _resolve_bundled(self, source: dict) -> ConversationState:
        file_path  = self._abs(source["file"])
        data       = self._load_json(file_path)

        if not isinstance(data, list):
            raise TypeError(
                f"{file_path}: expected a JSON array, got {type(data).__name__}"
            )

        index = source["index"]
        if index >= len(data):
            raise IndexError(
                f"Index {index} is out of range — file has {len(data)} conversation(s)"
            )

        conv = data[index]

        # Primary refs come from the bundled conversation object
        primary_cm   = (conv.get("metadata") or {}).get("citation_map") or {}
        primary_docs = conv.get("retrieved_docs") or []

        # Optional sidecars can override / enrich the bundled refs
        merged_cm, merged_docs = self._merge_sidecars(
            source, primary_cm, primary_docs
        )

        return ConversationState(
            user_query        = conv.get("user_query") or "",
            response          = conv.get("assistant_response") or "",
            retrieved_docs    = merged_docs,
            citation_map      = merged_cm,
            retrieved_qa      = conv.get("retrieved_qa") or [],
            enrichment_results = conv.get("enrichment_results") or [],
            timestamp         = conv.get("timestamp"),
        )

    # ------------------------------------------------------------------
    # Split loader
    # ------------------------------------------------------------------

    def _resolve_split(self, source: dict) -> ConversationState:
        response_file = source.get("response_file")
        if not response_file:
            raise ValueError("split source requires 'response_file'")

        response      = self._abs(response_file).read_text(encoding="utf-8")
        retrieved_docs = []
        citation_map   = {}

        if source.get("docs_file"):
            retrieved_docs = self._load_json(self._abs(source["docs_file"]))

        if source.get("citation_map_file"):
            raw_cm       = self._load_json(self._abs(source["citation_map_file"]))
            citation_map = self._normalise_citation_map(raw_cm)

        return ConversationState(
            user_query        = source.get("user_query") or "",
            response          = response,
            retrieved_docs    = retrieved_docs,
            citation_map      = citation_map,
            enrichment_results = [],
        )

    # ------------------------------------------------------------------
    # Sidecar merge
    # ------------------------------------------------------------------

    def _merge_sidecars(
        self,
        source: dict,
        base_cm: dict,
        base_docs: list,
    ) -> tuple[dict, list]:
        """
        If ``citation_map_file`` or ``docs_file`` are listed on a bundled source,
        load them and merge into the base refs.

        Merge rules
        -----------
        - citation_map: sidecar entries win on key collision (sidecar takes priority).
        - retrieved_docs: merged by URL; sidecar version of a URL replaces the base
          version; docs without a URL are appended.
        """
        cm   = dict(base_cm)
        docs = list(base_docs)

        cm_file   = source.get("citation_map_file")
        docs_file = source.get("docs_file")

        if cm_file:
            path = self._abs(cm_file)
            if path.exists():
                raw_cm    = self._load_json(path)
                sidecar   = self._normalise_citation_map(raw_cm)
                cm        = {**cm, **sidecar}     # sidecar wins

        if docs_file:
            path = self._abs(docs_file)
            if path.exists():
                sidecar_docs = self._load_json(path)
                url_map      = {
                    d["source_url"]: d
                    for d in docs
                    if d.get("source_url")
                }
                no_url = [d for d in docs if not d.get("source_url")]
                for d in sidecar_docs:
                    url = d.get("source_url")
                    if url:
                        url_map[url] = d          # sidecar wins
                    else:
                        no_url.append(d)
                docs = list(url_map.values()) + no_url

        return cm, docs


# ---------------------------------------------------------------------------
# Inline citation resolution
# ---------------------------------------------------------------------------

# Matches a parenthesised group that contains at least one doc_ or req_ token.
# Examples:
#   (doc_9e9e11d1c65c, qa-8)
#   (req_20260302_193819_004, summary; req_20260302_194249_006, summary)
#   (req_20260302_194549_000, summary; doc_c7682f0c01f2, qa-34)
_CITE_GROUP_RE = re.compile(
    r"\("
    r"((?:(?:doc_[0-9a-f]+|req_\w+)[^;)]*)"
    r"(?:;\s*(?:doc_[0-9a-f]+|req_\w+)[^;)]*)*)"
    r"\)"
)


def _cite_token_to_cm_key(part: str) -> str | None:
    """Convert one semicolon-split part of a citation group to a citation_map key.

    ``(doc_HEXID, qa-N)``        → ``doc_HEXID__qa-N``
    ``(req_TIMESTAMP, summary)`` → ``req_TIMESTAMP``
    """
    part = part.strip()
    m = re.match(r"(doc_[0-9a-f]+),\s*(qa-\d+)", part)
    if m:
        return f"{m.group(1)}__{m.group(2)}"
    m = re.match(r"(req_\w+),\s*\w+", part)
    if m:
        return m.group(1)
    return None


def _short_author(author: str) -> str:
    """Return a brief author label suitable for inline citation text.

    * ``@Handle``         → ``@Handle``
    * ``First Last``      → ``Last``
    * ``A, B, C``         → ``A et al.``  (multiple authors, comma-separated)
    * Multi-word name     → last token
    * Institution string  → kept as-is (no obvious surname to extract)
    """
    if not author or author == "Unknown":
        return author
    # Social / institutional handle
    if author.startswith("@"):
        return author
    # Multiple authors separated by commas → first surname + et al.
    parts = [p.strip() for p in author.split(",") if p.strip()]
    if len(parts) >= 3:
        return f"{parts[0].split()[-1]} et al."
    # Two authors "A and B" or "A, B"
    if " and " in author:
        first = author.split(" and ")[0].strip()
        return first.split()[-1]
    # Single name: return last token
    tokens = author.split()
    if len(tokens) >= 2:
        return tokens[-1]
    return author


def resolve_inline_citations(text: str, citation_map: dict) -> str:
    """Replace raw agent-state citation tokens with linked inline citations.

    Raw form in response text::

        ... policy (doc_9e9e11d1c65c, qa-8) and funding
            (req_20260302_193819_004, summary; req_20260302_194249_006, summary).

    Resolved form::

        ... policy ([Wehner, June 2025](https://...)) and funding
            ([Senate Armed Services, January 2025](https://...); [Lopez, March 2025](https://...)).

    Each token that cannot be resolved is left as-is.  If *none* of the tokens
    in a group resolve, the entire parenthesised group is left unchanged.
    """
    def _replace(m: re.Match) -> str:
        group_text = m.group(1)
        parts      = [p.strip() for p in group_text.split(";") if p.strip()]
        links      = []
        for part in parts:
            key   = _cite_token_to_cm_key(part)
            entry = citation_map.get(key) if key else None
            if not entry:
                # Fallback: try without the __subtype suffix
                base = (key or "").split("__")[0]
                entry = citation_map.get(base)
            if not entry:
                links.append(None)   # mark as unresolvable
                continue
            author = _short_author(entry.get("author") or "")
            date   = entry.get("date") or ""
            url    = entry.get("url") or ""
            label  = f"{author}, {date}" if author and date else (author or date or "?")
            if url:
                links.append(f"[{label}]({url})")
            else:
                links.append(label)

        # If every token failed, leave the original group intact
        if all(lnk is None for lnk in links):
            return m.group(0)

        # Replace unresolvable tokens with their original text
        resolved_parts = []
        for lnk, part in zip(links, parts):
            resolved_parts.append(lnk if lnk is not None else part)

        return "(" + "; ".join(resolved_parts) + ")"

    return _CITE_GROUP_RE.sub(_replace, text)


# ---------------------------------------------------------------------------
# Markdown helpers
# ---------------------------------------------------------------------------

def _authors_str(authors) -> str:
    if not authors:
        return ""
    if isinstance(authors, list):
        if len(authors) == 1:
            return authors[0]
        if len(authors) == 2:
            return f"{authors[0]} and {authors[1]}"
        return ", ".join(authors[:-1]) + f", and {authors[-1]}"
    return str(authors)


def _domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except Exception:
        return url


def _risk_label(rating: str) -> str:
    r = (rating or "").upper()
    if r == "HIGH":
        return "🔴 HIGH"
    if r == "MEDIUM":
        return "🟡 MEDIUM"
    if r == "LOW":
        return "🟢 LOW"
    return rating or "—"


def build_top_sources(retrieved_docs: list, n: int = 10) -> str:
    """Markdown section: top-N retrieved docs ranked by similarity score."""
    sorted_docs = sorted(
        retrieved_docs, key=lambda d: d.get("similarity_score", 0), reverse=True
    )[:n]
    lines = ["## Top Sources\n"]
    lines.append(
        "*The following sources were retrieved as most relevant to this query, "
        "ranked by semantic similarity.*\n"
    )
    for rank, doc in enumerate(sorted_docs, start=1):
        meta       = doc.get("doc_metadata") or {}
        title      = meta.get("title") or "Untitled"
        authors    = _authors_str(meta.get("authors") or [])
        pub_date   = meta.get("publication_date") or doc.get("publication_date") or ""
        pub_year   = pub_date[:4] if pub_date and len(pub_date) >= 4 else pub_date
        publisher  = meta.get("publisher") or meta.get("publication_venue") or ""
        url        = doc.get("source_url") or ""
        score      = doc.get("similarity_score", 0)

        author_part    = authors if authors else "Unknown"
        date_part      = f" ({pub_year})" if pub_year else ""
        publisher_part = f" *{publisher}*." if publisher else ""
        url_part       = f" [{_domain(url)}]({url})" if url else ""
        score_part     = f" — similarity: {score:.3f}"

        lines.append(
            f"{rank}. **{title}**  \n"
            f"   {author_part}{date_part}.{publisher_part}{url_part}{score_part}\n"
        )
    return "\n".join(lines)


def build_references(
    citation_map: dict,
    retrieved_docs: list,
    retrieved_qa: list | None = None,
) -> str:
    """
    Markdown section: deduplicated reference list sorted alphabetically by author.

    Resolution strategy
    -------------------
    For every URL in ``citation_map``, we attempt to enrich the entry with
    ``title`` and ``publisher`` from the matching doc in ``retrieved_docs``
    OR ``retrieved_qa`` (matched by ``source_url``).  ``retrieved_qa`` entries
    carry full ``doc_metadata`` for sources cited inline by the agent that did
    not appear in the top-N retrieval results, giving 100% title coverage when
    both corpora are supplied.
    """
    # Build URL → full doc lookup: retrieved_docs first, qa fills the gaps
    url_to_doc: dict[str, dict] = {
        d["source_url"]: d
        for d in (retrieved_qa or [])
        if d.get("source_url")
    }
    # retrieved_docs takes priority over qa on URL collision
    url_to_doc.update({
        d["source_url"]: d
        for d in retrieved_docs
        if d.get("source_url")
    })

    # Deduplicate citation_map by URL, keeping the richest entry per URL
    seen: dict[str, dict] = {}
    for entry in citation_map.values():
        url = entry.get("url") or ""
        if not url:
            continue
        if url not in seen:
            seen[url] = entry
        elif not seen[url].get("author") and entry.get("author"):
            seen[url] = entry   # upgrade to entry that has an author

    sorted_refs = sorted(
        seen.values(),
        key=lambda e: (e.get("author") or "").lower() or e.get("url", "").lower(),
    )

    lines = ["## References\n"]
    lines.append(
        "*All sources cited during this section. "
        "Deduplicated and sorted alphabetically by author.*\n"
    )

    for ref in sorted_refs:
        author    = ref.get("author") or "Unknown"
        date      = ref.get("date") or ""
        url       = ref.get("url") or ""

        doc       = url_to_doc.get(url)
        meta      = (doc.get("doc_metadata") or {}) if doc else {}
        title     = meta.get("title") or ""
        # retrieved_docs uses "publisher"; retrieved_qa entries may not have it
        publisher = meta.get("publisher") or meta.get("publication_venue") or ""

        author_part    = f"**{author}**"
        date_part      = f" ({date})" if date else ""
        title_part     = f" *{title}*." if title else ""
        publisher_part = f" {publisher}." if publisher else ""
        url_part       = f" [{url}]({url})" if url else ""

        lines.append(
            f"- {author_part}{date_part}.{title_part}{publisher_part}{url_part}"
        )

    return "\n".join(lines)


def build_enrichment_section(enrichment_results: list) -> str:
    """Markdown section: BBOA media enrichment records sorted by consumer salience."""
    if not enrichment_results:
        return ""

    lines = ["## Media Enrichment (BBOA Analysis)\n"]
    lines.append(
        "*Records drawn from the BBOA media analysis index. "
        "Each entry reflects independently assessed consumer salience, media framing, "
        "and risk ratings for articles directly linked to the retrieved source corpus.*\n"
    )

    sorted_er = sorted(
        enrichment_results, key=lambda r: r.get("consumer_salience") or 0, reverse=True
    )
    linked   = [r for r in sorted_er if r.get("event_key")]
    unlinked = [r for r in sorted_er if not r.get("event_key")]

    def render_record(idx: int, rec: dict) -> str:
        parts = []
        pub         = rec.get("publisher") or rec.get("source_name") or ""
        author      = rec.get("author") or "Unknown"
        date        = rec.get("date_publication") or ""
        link        = rec.get("link") or ""
        salience    = rec.get("consumer_salience")
        time_window = rec.get("time_window") or ""
        event_desc  = rec.get("event_descriptor") or ""
        eo          = rec.get("eo_identifier") or ""
        summary     = rec.get("summary") or ""
        mf          = rec.get("media_framing") or {}

        header = f"### {idx}. "
        if pub:
            header += pub
        if author and author != "Unknown":
            header += f" — {author}"
        if date:
            header += f" ({date})"
        parts.append(header)

        if link:
            parts.append(f"**URL:** [{link}]({link})")
        if time_window:
            parts.append(f"**Time window:** {time_window}")
        if salience is not None:
            parts.append(f"**Consumer salience:** {salience:.4f}")
        if event_desc:
            parts.append(f"**Event:** {event_desc}")
        if eo:
            parts.append(f"**Executive Order:** {eo}")
        if summary:
            short = (
                summary[:600].rsplit(" ", 1)[0] + "\u2026"
                if len(summary) > 600
                else summary
            )
            parts.append(f"\n> {short}")

        if isinstance(mf, dict) and mf:
            risk = mf.get("risk") or {}
            framing_lines = [
                f"  - **{k.replace('_', ' ').title()}:** {v}"
                for k, v in mf.items()
                if k != "risk" and v
            ]
            if framing_lines:
                parts.append("\n**Media Framing:**")
                parts.extend(framing_lines)
            if isinstance(risk, dict) and risk:
                rating    = risk.get("rating") or ""
                reasoning = risk.get("reasoning") or ""
                parts.append(f"\n**Risk:** {_risk_label(rating)}")
                if reasoning:
                    parts.append(f"> *{reasoning}*")
        elif isinstance(mf, str) and mf:
            parts.append(f"\n**Media Framing:** {mf}")

        return "\n".join(parts)

    if linked:
        lines.append(f"### Event-Linked Records ({len(linked)} records)\n")
        for idx, rec in enumerate(linked, start=1):
            lines.append(render_record(idx, rec))
            lines.append("")

    if unlinked:
        lines.append(f"### Unlinked Media Records ({len(unlinked)} records)\n")
        for idx, rec in enumerate(unlinked, start=1):
            lines.append(render_record(idx, rec))
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Response post-processing
# ---------------------------------------------------------------------------

def postprocess_response(text: str, long_threshold: int = 60) -> str:
    """Promote bold-headed bullet items with long bodies to subheadings.

    Pattern matched: ``[indent]- **Heading**: <body of >= long_threshold chars>``
    Continuation lines indented by 3+ spaces are folded into the body.
    """
    lines = text.split("\n")
    out   = []
    i     = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"^(\s*)([-*])\s+\*\*([^*]{3,}?)\*\*:?\s+(.*)", line)
        if m:
            indent_str   = m.group(1)
            heading_text = m.group(3).strip().rstrip(":")
            body         = m.group(4).strip()
            # Fold indented continuation lines
            j = i + 1
            while j < len(lines):
                nxt = lines[j]
                if nxt.startswith("   ") or nxt.startswith("\t"):
                    body += " " + nxt.strip()
                    j    += 1
                else:
                    break
            if len(body) >= long_threshold:
                depth  = len(indent_str) // 2
                hashes = "###" + "#" * depth
                out.extend(["", f"{hashes} {heading_text}", "", body])
                i = j
                continue
        out.append(line)
        i += 1
    return "\n".join(out)


def extract_abstract(text: str, max_words: int = 220) -> str:
    """Extract the first substantive prose as an auto-generated abstract.

    Skips headings, blockquotes, code fences, list items, and pure
    metadata-label lines (``**Label:**``).
    """
    collected  = []
    word_count = 0
    for line in text.split("\n"):
        s = line.strip()
        if not s:
            continue
        if s.startswith(("#", ">", "```", "-", "*", "|")):
            continue
        if re.match(r"^\*\*[^*]+:\*\*", s):
            continue
        collected.append(s)
        word_count += len(s.split())
        if word_count >= max_words:
            break

    if not collected:
        return ""
    abstract = " ".join(collected)
    words    = abstract.split()
    if len(words) > max_words:
        abstract = " ".join(words[:max_words]) + "\u2026"
    return abstract


def _slug(heading: str) -> str:
    s = heading.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return s


def build_toc(text: str) -> str:
    """Build a nested Markdown table of contents from ## and deeper headings."""
    toc = ["## Table of Contents\n"]
    for line in text.split("\n"):
        m = re.match(r"^(#{2,6})\s+(.*)", line)
        if m:
            level  = len(m.group(1))   # 2 = ##, 3 = ###, …
            htext  = m.group(2).strip()
            anchor = _slug(htext)
            indent = "  " * (level - 2)
            toc.append(f"{indent}- [{htext}](#{anchor})")
    return "\n".join(toc)


# ---------------------------------------------------------------------------
# Document renderer
# ---------------------------------------------------------------------------

class DocumentRenderer:
    """
    Renders a (title, ConversationState) pair to a full Markdown document.

    Document structure
    ------------------
    1. Title + header block (generated date, source, counts)
    2. Abstract (auto-extracted from first substantive prose)
    3. Table of Contents (derived from all ## and deeper headings in body)
    4. Research Query
    5. Analysis (optionally post-processed)
    6. Top Sources
    7. Media Enrichment (if present)
    8. References
    """

    def __init__(
        self,
        top_sources_n: int = 10,
        abstract_max_words: int = 220,
        postprocess: bool = True,
        generated_date: str | None = None,
    ) -> None:
        self.top_sources_n      = top_sources_n
        self.abstract_max_words = abstract_max_words
        self.postprocess        = postprocess
        self.generated_date     = (
            generated_date
            or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

    def render(self, title: str, state: ConversationState) -> str:
        response = state.response
        if self.postprocess:
            response = postprocess_response(response)

        # Resolve raw agent-state citation tokens to linked inline citations
        response = resolve_inline_citations(response, state.citation_map)

        abstract_text  = extract_abstract(response, max_words=self.abstract_max_words)
        abstract_block = f"> {abstract_text}" if abstract_text else ""

        top_src_md  = build_top_sources(state.retrieved_docs, n=self.top_sources_n)
        enrich_md   = build_enrichment_section(state.enrichment_results)
        refs_md     = build_references(
            state.citation_map, state.retrieved_docs, state.retrieved_qa
        )

        enrich_block = f"\n---\n\n{enrich_md}\n" if enrich_md else ""

        # Build body first so the ToC can reference its headings
        body = (
            f"## Research Query\n\n"
            f"> {state.user_query}\n\n"
            f"---\n\n"
            f"## Analysis\n\n"
            f"{response}\n\n"
            f"---\n\n"
            f"{top_src_md}"
            f"{enrich_block}"
            f"\n---\n\n"
            f"{refs_md}\n"
        )

        toc = build_toc(body)

        n_enrichment = len(state.enrichment_results)
        n_docs       = len(state.retrieved_docs)

        return (
            f"# {title}\n\n"
            f"> **Generated:** {self.generated_date}  \n"
            f"> **Source:** RapporteurApp — AI Research Assistant  \n"
            f"> **Enrichment records:** {n_enrichment} | "
            f"**Retrieved documents:** {n_docs}\n\n"
            f"---\n\n"
            f"## Abstract\n\n"
            f"{abstract_block}\n\n"
            f"---\n\n"
            f"{toc}\n\n"
            f"---\n\n"
            f"{body}"
        )


# ---------------------------------------------------------------------------
# Manifest runner
# ---------------------------------------------------------------------------

class ManifestRunner:
    """
    Reads a manifest JSON file and generates Markdown documents.

    Parameters
    ----------
    manifest_path:
        Path to the manifest JSON file. All relative paths inside the manifest
        are resolved relative to the manifest file's own directory.
    dry_run:
        If True, print what would be written without touching the filesystem.
    """

    def __init__(self, manifest_path: str | Path, dry_run: bool = False) -> None:
        self.manifest_path = Path(manifest_path).resolve()
        self.manifest_dir  = self.manifest_path.parent
        self.dry_run       = dry_run
        self._manifest     = self._load_manifest()

    def _load_manifest(self) -> dict:
        with open(self.manifest_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _resolve_dir(self, key: str, default: str = ".") -> Path:
        raw = self._manifest.get(key, default)
        p   = Path(raw)
        return (p if p.is_absolute() else self.manifest_dir / p).resolve()

    def run(self) -> list[tuple[str, int]]:
        """
        Generate all documents described in the manifest.

        Returns a list of ``(output_path, word_count)`` tuples for successfully
        written files.
        """
        output_dir = self._resolve_dir("output_dir", ".")
        raw_dir    = self._resolve_dir("raw_dir", "raw")
        opts       = self._manifest.get("render_options") or {}
        doc_specs  = self._manifest.get("documents") or []

        resolver = RefResolver(raw_dir)
        renderer = DocumentRenderer(
            top_sources_n      = opts.get("top_sources_n", 10),
            abstract_max_words = opts.get("abstract_max_words", 220),
            postprocess        = opts.get("postprocess", True),
        )

        if not self.dry_run:
            output_dir.mkdir(parents=True, exist_ok=True)

        results: list[tuple[str, int]] = []

        for spec in doc_specs:
            filename = spec["filename"]
            title    = spec["title"]
            source   = spec.get("source") or {}

            try:
                state = resolver.resolve(source)
            except Exception as exc:
                print(f"  ERROR resolving {filename!r}: {exc}", file=sys.stderr)
                continue

            doc      = renderer.render(title, state)
            out_path = output_dir / filename

            if self.dry_run:
                n_er   = len(state.enrichment_results)
                n_docs = len(state.retrieved_docs)
                print(
                    f"  [dry-run] {filename}  "
                    f"({len(doc.split()):,} words, {n_er} enrichment, {n_docs} docs)"
                )
            else:
                out_path.write_text(doc, encoding="utf-8")
                word_count = len(doc.split())
                n_er       = len(state.enrichment_results)
                n_docs     = len(state.retrieved_docs)
                print(
                    f"  Written: {filename}  "
                    f"({word_count:,} words, {n_er} enrichment, {n_docs} docs)"
                )
                results.append((str(out_path), word_count))

        return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rapporteur_docs",
        description=(
            "Generate Markdown research documents from RapporteurApp agent state."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="See module docstring for the full manifest schema.",
    )
    parser.add_argument(
        "manifest",
        help="Path to the manifest JSON file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without creating any files.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args    = _build_parser().parse_args(argv)
    runner  = ManifestRunner(args.manifest, dry_run=args.dry_run)
    results = runner.run()
    noun    = "document" if len(results) == 1 else "documents"
    print(f"\nDone. {len(results)} {noun} generated.")


if __name__ == "__main__":
    main()
