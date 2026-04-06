#!/usr/bin/env python3
"""Dispatch Kaliya with supplementary grounding packages for all 4 categories.

Categories: artificial_intelligence, energy_and_minerals,
            national_security_domestic, national_security_foreign

Steps:
  1. Parse Phase 4 markdown per-cluster article tables → URLs
  2. Load EO policy summaries per category
  3. Build supplementary grounding packages
  4. Dispatch to Kafka via KaliyaHandoff
  5. Poll for completion and collect narratives to clustering dir
"""
import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────
CLUSTERING_DIR = Path("/mnt/workspace_2/bboa/documents/clustering")
KALIYA_CACHE_DIR = Path("/mnt/workspace_2/bboa/cache/kaliya_agenda_cache")
EO_INDEX = Path("x_indices_eo/research_summaries_with_embeddings_combined.jsonl")

# ── Category configuration ─────────────────────────────────────────────────
CATEGORIES = {
    "culture_and_media": {
        "phase4": CLUSTERING_DIR / "phase4_culture_and_media_20260328_183618.md",
        "clusters": ["A", "B", "C", "D", "E"],
        "eo_anchors": ["13950", "13963", "13989", "14148", "14151", "14173", "14183", "14184", "14185", "14347"],
    },
    "artificial_intelligence": {
        "phase4": CLUSTERING_DIR / "phase4_artificial_intelligence_20260328_201342.md",
        "clusters": ["A", "B", "C", "D", "E", "F", "G"],
        "eo_anchors": ["13859", "13960", "14110", "14179", "14319"],
    },
    "energy_and_minerals": {
        "phase4": CLUSTERING_DIR / "phase4_energy_and_minerals_20260328_203200.md",
        "clusters": ["A", "B", "C", "D", "E"],
        "eo_anchors": ["14154", "14192", "14241", "14387"],
    },
    "national_security_domestic": {
        "phase4": CLUSTERING_DIR / "phase4_national_security_domestic_20260328_202404.md",
        "clusters": ["A", "B", "C", "D"],
        "eo_anchors": ["14157", "14161", "14165", "14167", "14269", "14367"],
    },
    "national_security_foreign": {
        "phase4": CLUSTERING_DIR / "phase4_national_security_foreign_20260328_162639.md",
        "clusters": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "eo_anchors": ["14175", "14186", "14199", "14305", "14346", "14357", "14369", "14383"],
    },
}


# ── Phase 4 parsing ───────────────────────────────────────────────────────

def parse_phase4_clusters(phase4_path: Path) -> dict[str, dict]:
    """Parse Phase 4 markdown into per-cluster data.

    Returns {cluster_letter: {"description": str, "section_text": str, "articles": [...]}}

    Handles two formats:
      - ``## Cluster A — Title`` (AI, EM, NSD)
      - ``## Cluster A. Title`` (NSF)
    """
    text = phase4_path.read_text(encoding="utf-8")

    # Match both formats: "## Cluster A — Title" and "## Cluster A. Title"
    cluster_pat = re.compile(
        r"^##\s+Cluster\s+([A-Z])[.\s]+[—–\-]?\s*(.+)", re.MULTILINE,
    )
    starts: list[tuple[int, str, str]] = []
    seen: set[str] = set()
    for m in cluster_pat.finditer(text):
        cid = m.group(1)
        # Take only the FIRST (detailed) occurrence if a cluster appears twice
        if cid in seen:
            continue
        seen.add(cid)
        starts.append((m.start(), cid, m.group(2).strip()))

    clusters: dict[str, dict] = {}
    for idx, (start, cid, desc) in enumerate(starts):
        # End at next *first-occurrence* cluster heading or EOF
        end = starts[idx + 1][0] if idx + 1 < len(starts) else len(text)
        section = text[start:end].strip()

        # Extract article table URLs from this section
        articles = _extract_articles_from_section(section)

        clusters[cid] = {
            "description": desc,
            "section_text": section,
            "articles": articles,
        }

    return clusters


def _extract_articles_from_section(section_text: str) -> list[dict]:
    """Extract article metadata from markdown table in a cluster section."""
    # Find table with URL column
    table_match = re.search(
        r'(\|[^\n]*(?:URL|url|Url)[^\n]*\|\n\|[-| :]+\|\n(?:\|[^\n]*\|\n)+)',
        section_text,
    )
    if not table_match:
        return []

    table_text = table_match.group(1).strip()
    lines = table_text.split("\n")
    if len(lines) < 3:
        return []

    # Parse header
    header = [h.strip() for h in lines[0].split("|") if h.strip()]
    # Normalize header names
    col_map = {}
    for i, h in enumerate(header):
        hl = h.lower()
        if "author" in hl:
            col_map["author"] = i
        elif "date" in hl:
            col_map["date"] = i
        elif "publication" in hl or "source" in hl:
            col_map["publication"] = i
        elif "title" in hl:
            col_map["title"] = i
        elif "url" in hl:
            col_map["url"] = i

    if "url" not in col_map:
        return []

    articles = []
    for line in lines[2:]:  # Skip header + separator
        cols = [c.strip() for c in line.split("|")]
        # Remove empty first/last from leading/trailing |
        if cols and cols[0] == "":
            cols = cols[1:]
        if cols and cols[-1] == "":
            cols = cols[:-1]

        if len(cols) <= col_map["url"]:
            continue

        url = cols[col_map["url"]].strip()
        if not url.startswith("http"):
            continue

        article = {"url": url}
        for key in ("author", "date", "publication", "title"):
            if key in col_map and col_map[key] < len(cols):
                article[key] = cols[col_map[key]].strip()
            else:
                article[key] = ""

        # Map to the field names KaliyaHandoff expects
        articles.append({
            "url": url,
            "author": article.get("author", ""),
            "date_publication": article.get("date", ""),
            "publisher": article.get("publication", ""),
            "title": article.get("title", ""),
        })

    return articles


# ── EO loading ─────────────────────────────────────────────────────────

def load_eo_policy_summaries(eo_numbers: list[str]) -> list[dict[str, str]]:
    """Load EO policy summaries from the local EO index JSONL."""
    if not EO_INDEX.exists():
        log.warning("EO index not found: %s", EO_INDEX)
        return []

    target = set(eo_numbers)
    found: dict[str, dict[str, str]] = {}

    with open(EO_INDEX, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            url = rec.get("source_url", "")
            summary = rec.get("summary_text", "")
            doc_meta = rec.get("doc_metadata", {})
            title = doc_meta.get("title", "")
            for m in re.findall(r"(?:EO|Executive Order)\s+(\d{5})", summary + " " + title):
                if m in target and m not in found:
                    found[m] = {
                        "eo_number": m,
                        "title": title,
                        "url": url,
                        "summary_text": summary,
                    }

    return [found[eo] for eo in eo_numbers if eo in found]


# ── Supplementary package builder ──────────────────────────────────────

def build_supplementary(
    category: str,
    cluster_data: dict[str, dict],
    eo_summaries: list[dict[str, str]],
    hypothesis_section: str = "",
) -> dict[str, str]:
    """Build per-cluster supplementary grounding packages.

    Returns {cluster_id: path_to_supp_file}.
    """
    supp_dir = KALIYA_CACHE_DIR / f"kaliya_supplementary_{category}"
    supp_dir.mkdir(parents=True, exist_ok=True)

    supp_files: dict[str, str] = {}

    for cid, cdata in cluster_data.items():
        parts: list[str] = [
            f"# Supplementary Grounding — Cluster {cid}\n",
            f"**Category:** {category.replace('_', ' ').title()}\n",
            f"**Cluster:** {cid} — {cdata['description']}\n\n",
            "---\n\n",
        ]

        # 1. Cluster analysis from Phase 4
        if cdata["section_text"]:
            parts.append("## Cluster Analysis (from Phase 4 Threat Clustering)\n\n")
            parts.append(
                "The following is the threat cluster analysis produced by the "
                "Phase 4 model. Use these findings as priors — test, refine, "
                "and challenge them with the deep multi-modal analysis of the "
                "individual articles. Pay attention to the hypotheses, "
                "alternative explanations, and convergence/divergence patterns.\n\n"
            )
            parts.append(cdata["section_text"])
            parts.append("\n\n---\n\n")

        # 2. Hypothesis testing priors
        if hypothesis_section:
            parts.append("## Hypothesis Testing Priors\n\n")
            parts.append(
                "The following hypotheses were tested in the clustering phase. "
                "Evaluate whether your deep analysis supports, refutes, or "
                "adds nuance to these findings.\n\n"
            )
            parts.append(hypothesis_section)
            parts.append("\n\n---\n\n")

        # 3. Executive Order policy context
        if eo_summaries:
            parts.append("## Executive Orders — Policy Context\n\n")
            parts.append(
                "The following executive orders are the authoritative legal and "
                "policy framework relevant to this category. They represent the "
                "government's stated justification and legal basis for the actions "
                "under analysis. Evaluate whether media articles acknowledge, "
                "omit, or misrepresent these policy positions.\n\n"
            )
            for eo in eo_summaries:
                parts.append(
                    f"### EO {eo['eo_number']}: {eo['title']}\n\n"
                    f"Source: {eo['url']}\n\n"
                    f"{eo['summary_text']}\n\n"
                    "---\n\n"
                )

        # 4. Article metadata table
        articles = cdata.get("articles", [])
        if articles:
            parts.append("## Article Metadata for This Cluster\n\n")
            parts.append("| art_# | Author | Date | Publication | URL |\n")
            parts.append("|-------|--------|------|-------------|-----|\n")
            for i, a in enumerate(articles, 1):
                author = (a.get("author") or "").replace("|", "/")
                date = (a.get("date_publication") or "").replace("|", "/")
                pub = (a.get("publisher") or "").replace("|", "/")
                url = a.get("url", "")
                parts.append(f"| art_{i} | {author} | {date} | {pub} | {url} |\n")
            parts.append("\n")

        # 5. Citation rules
        parts.append("## Citation Rules\n\n")
        parts.append(
            "IMPORTANT: Every document in the Grounding Data has a "
            "\"Cite as\" field immediately below its URL. You MUST use this "
            "exact citation label for ALL in-text references. "
            "The format is: (document_id, summary) — for example: "
            "(req_20260107_221154_000, summary).\n\n"
            "Rules:\n"
            "- Always copy the document_id EXACTLY as shown in the "
            "\"Cite as\" field — it will look like req_YYYYMMDD_HHMMSS_NNN.\n"
            "- NEVER use incremental numbers like [1], [2], [3] or "
            "(doc_1) or (art_1). These are meaningless to the reader.\n"
            "- NEVER use author names, publication names, or URLs as citations.\n"
            "- NEVER hyperlink the citation — render as plain text in parentheses.\n"
            "- When quoting or paraphrasing from a document, place the citation "
            "immediately after the quoted passage.\n"
            "- Multiple citations for the same claim: "
            "(req_20260303_090925_005, summary)(req_20260303_090925_006, summary).\n\n"
        )

        content = "".join(parts)
        cluster_id = f"cluster_{cid}"
        fname = f"supp_cluster_{cluster_id}.md"
        filepath = supp_dir / fname
        filepath.write_text(content, encoding="utf-8")
        supp_files[cluster_id] = str(filepath)
        log.info(
            "  Built supp: %s (%d bytes, %d articles)",
            fname, len(content), len(articles),
        )

    return supp_files


# ── Extract hypothesis section ─────────────────────────────────────────

def extract_hypothesis_section(phase4_path: Path) -> str:
    text = phase4_path.read_text(encoding="utf-8")
    m = re.search(
        r"(^##.*[Tt]esting\s+.*[Hh]ypothes.*?\n.*?)(?=\n## |\Z)",
        text, re.MULTILINE | re.DOTALL,
    )
    return m.group(1).strip() if m else ""


# ── Build cluster_results structure for KaliyaHandoff ──────────────────

def build_cluster_results(
    category: str,
    cluster_data: dict[str, dict],
) -> list[dict]:
    """Build the cluster_results list expected by KaliyaHandoff."""
    clusters = []
    for cid, cdata in sorted(cluster_data.items()):
        clusters.append({
            "cluster_id": f"cluster_{cid}",
            "cluster_description": cdata["description"],
            "cluster_number": ord(cid) - ord("A") + 1,
            "cluster_text": cdata["section_text"][:500],
            "article_count": len(cdata["articles"]),
            "articles": cdata["articles"],
        })
    return [{"category": category, "clusters": clusters}]


# ── Main ───────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv
    categories_filter = [a for a in sys.argv[1:] if not a.startswith("--")]

    # Import Kaliya components
    sys.path.insert(0, str(Path.cwd()))
    from src.components.analysis.media_linkage import KaliyaHandoff

    handoff = KaliyaHandoff()
    all_sent: list[dict] = []

    for category, cfg in CATEGORIES.items():
        if categories_filter and category not in categories_filter:
            continue

        log.info("=" * 70)
        log.info("CATEGORY: %s", category)
        log.info("=" * 70)

        # 1. Parse Phase 4
        phase4_path = cfg["phase4"]
        if not phase4_path.exists():
            log.error("Phase 4 file not found: %s", phase4_path)
            continue

        cluster_data = parse_phase4_clusters(phase4_path)
        log.info("Parsed %d clusters from Phase 4", len(cluster_data))
        for cid, cdata in sorted(cluster_data.items()):
            log.info(
                "  Cluster %s: %s (%d articles)",
                cid, cdata["description"][:60], len(cdata["articles"]),
            )

        # 2. Load EO policy summaries
        eo_summaries = load_eo_policy_summaries(cfg["eo_anchors"])
        log.info("Loaded %d EO policy summaries", len(eo_summaries))

        # 3. Extract hypothesis section
        hyp_section = extract_hypothesis_section(phase4_path)
        log.info("Hypothesis section: %d chars", len(hyp_section))

        # 4. Build supplementary packages
        log.info("Building supplementary packages...")
        supp_files = build_supplementary(
            category, cluster_data, eo_summaries, hyp_section,
        )
        log.info("Built %d supplementary files", len(supp_files))

        # 5. Build handoff payloads
        cluster_results = build_cluster_results(category, cluster_data)
        payloads = handoff.build_all_handoffs(
            cluster_results,
            min_articles=1,  # Don't skip small clusters
            supplementary_files=supp_files,
        )
        log.info("Built %d handoff payloads", len(payloads))
        for p in payloads:
            log.info(
                "  → %s/%s: %d URLs, supp=%s",
                p["category"], p["cluster_id"],
                p["url_count"],
                bool(p.get("supplementary_file")),
            )

        if dry_run:
            log.info("DRY RUN — skipping Kafka dispatch")
            continue

        # 6. Dispatch to Kafka
        log.info("Dispatching to Kafka...")
        sent = handoff.send_to_kafka(payloads)
        log.info("Sent %d/%d messages", len(sent), len(payloads))
        all_sent.extend(sent)

    if dry_run or not all_sent:
        log.info("\nDone. %s", "DRY RUN" if dry_run else "No messages sent")
        return

    # 7. Poll for completion
    log.info("\n" + "=" * 70)
    log.info("POLLING FOR COMPLETION (%d total requests)", len(all_sent))
    log.info("=" * 70)
    completed = handoff.poll_results(all_sent, timeout=7200, poll_interval=30)
    log.info("Completed: %d/%d", len(completed), len(all_sent))

    # 8. Collect narratives to clustering dir
    if completed:
        written = handoff.collect_narratives(completed, CLUSTERING_DIR)
        log.info("Collected %d narrative files to %s", len(written), CLUSTERING_DIR)
        for w in written:
            log.info("  → %s", w)
    else:
        log.warning("No Kaliya results completed")


if __name__ == "__main__":
    main()
