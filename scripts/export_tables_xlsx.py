#!/usr/bin/env python3
"""Export culture_and_media bundle + grounding events to a linked XLSX workbook.

Primary data source: bundle JSONL (293 articles with full media analysis).
Joins with:
  - Cluster tables (A–E) article assignments
  - Grounding events (445 nested [G] records)

Sheets produced:
  1. Articles          — master: all 293 articles with cluster, EO, media framing,
                         risk rating, stance, spin, factualness, salience
  2. Grounding Events  — 445 government-action records, EO-linked
  3. Cluster A .. E    — per-cluster article subsets (filtered view of Articles)
  4. Risk Summary      — pivot: cluster × risk rating counts
  5. EO Summary        — pivot: EO × article count, mean salience
  6. Publisher Summary  — pivot: publisher × article count
  7. Stance Summary    — pivot: cluster × supportive/neutral/unsupportive
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

BUILD_DIR = Path("/mnt/workspace_2/bboa/documents/culture_and_media")
TABLES_DIR = BUILD_DIR / "tables"
BUNDLE = Path("/home/electric/workspace/RapporteurApp/cache/"
              "phase4_culture_and_media_20260328_183618.bundle.jsonl")
OUTPUT = BUILD_DIR / "culture_and_media_tables.xlsx"

# ── Styles ───────────────────────────────────────────────────────────────────

HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
LINK_FONT = Font(color="0563C1", underline="single")
WRAP_TOP = Alignment(wrap_text=True, vertical="top")

RISK_FILLS = {
    "LOW":    PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
    "MEDIUM": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
    "HIGH":   PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
    "EXTREME": PatternFill(start_color="FF4444", end_color="FF4444", fill_type="solid"),
}

CLUSTER_FILLS = {
    "A": PatternFill(start_color="DAEEF3", end_color="DAEEF3", fill_type="solid"),
    "B": PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid"),
    "C": PatternFill(start_color="FCE4D6", end_color="FCE4D6", fill_type="solid"),
    "D": PatternFill(start_color="EDEDED", end_color="EDEDED", fill_type="solid"),
    "E": PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid"),
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _auto_width(ws, min_w=10, max_w=65):
    for col_cells in ws.columns:
        col_letter = get_column_letter(col_cells[0].column)
        lengths = [min(len(str(c.value or "")), max_w) for c in col_cells]
        ws.column_dimensions[col_letter].width = min(
            max(min_w, max(lengths, default=min_w) + 2), max_w
        )


def _style_header(ws, ncols):
    for col in range(1, ncols + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    ws.freeze_panes = "A2"
    if ws.max_row > 1:
        ws.auto_filter.ref = ws.dimensions


def _hyperlink_col(ws, col_idx, start_row=2):
    """Make URLs in a column clickable."""
    for r in range(start_row, ws.max_row + 1):
        cell = ws.cell(row=r, column=col_idx)
        url = str(cell.value or "").strip()
        if url.startswith("http"):
            cell.hyperlink = url
            cell.font = LINK_FONT


def _colour_risk_col(ws, col_idx, start_row=2):
    for r in range(start_row, ws.max_row + 1):
        cell = ws.cell(row=r, column=col_idx)
        fill = RISK_FILLS.get(str(cell.value or "").upper())
        if fill:
            cell.fill = fill


# ── Load bundle JSONL ────────────────────────────────────────────────────────

def load_bundle(path: Path) -> list[dict]:
    """Load bundle and flatten media_framing / doc_metadata into top-level."""
    records = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            raw = json.loads(line)
            mf = raw.get("media_framing") or {}
            risk_obj = mf.get("risk") or {}
            rec = {
                "#": i,
                "title": raw.get("title", ""),
                "url": raw.get("url", ""),
                "publisher": raw.get("publisher", ""),
                "author": raw.get("author", ""),
                "date": raw.get("date_publication", ""),
                "eo": raw.get("eo_identifier", ""),
                "source_type": raw.get("source_type", ""),
                "qa_count": raw.get("qa_count", 0),
                "salience": raw.get("consumer_salience"),
                "search_rank": raw.get("mean_search_engine_rank", 0.0),
                # Media framing fields
                "risk_rating": risk_obj.get("rating", ""),
                "risk_reasoning": risk_obj.get("reasoning", ""),
                "stance": mf.get("supportive_unsupportive", ""),
                "spin": mf.get("spin", ""),
                "factualness": mf.get("factualness", ""),
                "convergence": mf.get("convergence_divergence", ""),
                "linked_to_dow": mf.get("linked_to_dow", ""),
                # Cluster filled later
                "cluster": "",
            }
            records.append(rec)
    return records


# ── Load cluster assignments from markdown tables ────────────────────────────

def load_cluster_map(tables_dir: Path) -> dict[int, str]:
    """Return {article_number: cluster_letter} from _full_table_cluster_*.md."""
    mapping: dict[int, str] = {}
    for f in sorted(tables_dir.glob("_full_table_cluster_*.md")):
        cluster = f.stem[-1]  # A, B, C, D, E
        for line in f.read_text(encoding="utf-8").splitlines():
            m = re.match(r'^\|\s*(\d+)\s*\|', line.strip())
            if m:
                mapping[int(m.group(1))] = cluster
    return mapping


# ── Parse grounding events from nested markdown blocks ───────────────────────

_GE_HEADER = re.compile(
    r'^##\s*\[(?P<tag>G\d+)\]\s*(?:EO\s*(?P<eo>\d+))?[:\s]*(?P<eo_title>.*?)\s*$'
)
_GE_FIELD = re.compile(r'^-\s*\*\*(?P<key>[^*]+)\*\*:?\s*(?P<val>.*)$')


def parse_grounding_events(text: str) -> list[dict]:
    records: list[dict] = []
    current: dict | None = None
    for line in text.splitlines():
        m = _GE_HEADER.match(line.strip())
        if m:
            if current:
                records.append(current)
            current = {
                "Tag": m.group("tag"),
                "EO": m.group("eo") or "",
                "EO Title": (m.group("eo_title") or "").strip(" —–-"),
            }
            continue
        if current is None:
            continue
        fm = _GE_FIELD.match(line.strip())
        if fm:
            current[fm.group("key").strip()] = fm.group("val").strip()
    if current:
        records.append(current)
    return records


# ── Classify stance into bucket ──────────────────────────────────────────────

def _stance_bucket(stance_text: str) -> str:
    s = stance_text.lower()
    if "unsupportive" in s:
        return "Unsupportive"
    if "supportive" in s:
        return "Supportive"
    if "neutral" in s:
        return "Neutral"
    return "Unknown"


# ── Sheet: Articles (master) ─────────────────────────────────────────────────

ARTICLE_HEADERS = [
    "#", "Cluster", "EO", "Title", "Publisher", "Author", "Date",
    "Source Type", "Risk Rating", "Stance Bucket", "Salience",
    "Search Rank", "QA Count",
    "Stance (full)", "Spin", "Factualness", "Convergence/Divergence",
    "Linked to DoW", "Risk Reasoning", "URL",
]


def write_articles_sheet(wb: Workbook, articles: list[dict], sheet_name: str = "Articles",
                         cluster_filter: str | None = None):
    ws = wb.create_sheet(title=sheet_name[:31])
    ws.append(ARTICLE_HEADERS)

    recs = articles if not cluster_filter else [
        a for a in articles if a["cluster"] == cluster_filter
    ]

    for a in recs:
        ws.append([
            a["#"],
            a["cluster"],
            a["eo"],
            a["title"],
            a["publisher"],
            a["author"],
            a["date"],
            a["source_type"],
            a["risk_rating"],
            _stance_bucket(a["stance"]),
            round(a["salience"], 3) if a["salience"] else None,
            round(a["search_rank"], 2) if a["search_rank"] else None,
            a["qa_count"],
            a["stance"],
            a["spin"],
            a["factualness"],
            a["convergence"],
            a["linked_to_dow"],
            a["risk_reasoning"],
            a["url"],
        ])

    _style_header(ws, len(ARTICLE_HEADERS))

    # Hyperlink Title col with URL
    title_idx = ARTICLE_HEADERS.index("Title") + 1
    url_idx = ARTICLE_HEADERS.index("URL") + 1
    for r in range(2, ws.max_row + 1):
        url_val = str(ws.cell(row=r, column=url_idx).value or "")
        if url_val.startswith("http"):
            ws.cell(row=r, column=title_idx).hyperlink = url_val
            ws.cell(row=r, column=title_idx).font = LINK_FONT

    # Colour risk column
    risk_idx = ARTICLE_HEADERS.index("Risk Rating") + 1
    _colour_risk_col(ws, risk_idx)

    # Colour cluster column
    cluster_idx = ARTICLE_HEADERS.index("Cluster") + 1
    for r in range(2, ws.max_row + 1):
        cell = ws.cell(row=r, column=cluster_idx)
        fill = CLUSTER_FILLS.get(str(cell.value or ""))
        if fill:
            cell.fill = fill

    # Wrap long text columns
    for col_name in ("Stance (full)", "Spin", "Factualness",
                     "Convergence/Divergence", "Linked to DoW", "Risk Reasoning"):
        idx = ARTICLE_HEADERS.index(col_name) + 1
        for r in range(2, ws.max_row + 1):
            ws.cell(row=r, column=idx).alignment = WRAP_TOP
        ws.column_dimensions[get_column_letter(idx)].width = 55

    _auto_width(ws, max_w=40)
    # Force wide columns back after auto
    for col_name in ("Stance (full)", "Spin", "Factualness",
                     "Convergence/Divergence", "Linked to DoW", "Risk Reasoning"):
        idx = ARTICLE_HEADERS.index(col_name) + 1
        ws.column_dimensions[get_column_letter(idx)].width = 55

    return len(recs)


# ── Sheet: Grounding Events ─────────────────────────────────────────────────

def write_grounding_events_sheet(wb: Workbook, ge_records: list[dict]):
    all_keys = []
    seen = set()
    for r in ge_records:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    ws = wb.create_sheet(title="Grounding Events")
    ws.append(all_keys)
    for rec in ge_records:
        ws.append([rec.get(k, "") for k in all_keys])

    _style_header(ws, len(all_keys))
    if "Source URL" in all_keys:
        _hyperlink_col(ws, all_keys.index("Source URL") + 1)
    _auto_width(ws)
    return len(ge_records)


# ── Sheet: Pivots / Summaries ────────────────────────────────────────────────

def write_risk_summary(wb: Workbook, articles: list[dict]):
    ws = wb.create_sheet(title="Risk Summary")
    ratings = sorted({a["risk_rating"] for a in articles if a["risk_rating"]})
    clusters = sorted({a["cluster"] for a in articles if a["cluster"]})
    headers = ["Cluster"] + ratings + ["Total"]
    ws.append(headers)
    counts = defaultdict(Counter)
    for a in articles:
        if a["cluster"] and a["risk_rating"]:
            counts[a["cluster"]][a["risk_rating"]] += 1
    for c in clusters:
        row = [c] + [counts[c].get(r, 0) for r in ratings]
        row.append(sum(counts[c].values()))
        ws.append(row)
    # Totals row
    totals = ["Total"] + [sum(counts[c].get(r, 0) for c in clusters) for r in ratings]
    totals.append(sum(totals[1:]))
    ws.append(totals)
    _style_header(ws, len(headers))
    _auto_width(ws)


def write_eo_summary(wb: Workbook, articles: list[dict]):
    ws = wb.create_sheet(title="EO Summary")
    headers = ["EO", "Articles", "Mean Salience", "HIGH Risk", "Supportive", "Unsupportive", "Neutral"]
    ws.append(headers)
    eo_groups = defaultdict(list)
    for a in articles:
        eo_groups[a["eo"] or "(unlinked)"].append(a)
    for eo in sorted(eo_groups, key=lambda x: (-len(eo_groups[x]), x)):
        arts = eo_groups[eo]
        sals = [a["salience"] for a in arts if a["salience"]]
        mean_sal = round(sum(sals) / len(sals), 3) if sals else None
        high = sum(1 for a in arts if a["risk_rating"] == "HIGH")
        sup = sum(1 for a in arts if _stance_bucket(a["stance"]) == "Supportive")
        unsup = sum(1 for a in arts if _stance_bucket(a["stance"]) == "Unsupportive")
        neut = sum(1 for a in arts if _stance_bucket(a["stance"]) == "Neutral")
        ws.append([eo, len(arts), mean_sal, high, sup, unsup, neut])
    _style_header(ws, len(headers))
    _auto_width(ws)


def write_publisher_summary(wb: Workbook, articles: list[dict]):
    ws = wb.create_sheet(title="Publisher Summary")
    headers = ["Publisher", "Articles", "HIGH Risk", "Supportive", "Unsupportive"]
    ws.append(headers)
    pub_groups = defaultdict(list)
    for a in articles:
        pub_groups[a["publisher"] or "(unknown)"].append(a)
    for pub in sorted(pub_groups, key=lambda x: (-len(pub_groups[x]), x)):
        arts = pub_groups[pub]
        high = sum(1 for a in arts if a["risk_rating"] == "HIGH")
        sup = sum(1 for a in arts if _stance_bucket(a["stance"]) == "Supportive")
        unsup = sum(1 for a in arts if _stance_bucket(a["stance"]) == "Unsupportive")
        ws.append([pub, len(arts), high, sup, unsup])
    _style_header(ws, len(headers))
    _auto_width(ws)


def write_stance_summary(wb: Workbook, articles: list[dict]):
    ws = wb.create_sheet(title="Stance Summary")
    buckets = ["Supportive", "Neutral", "Unsupportive", "Unknown"]
    clusters = sorted({a["cluster"] for a in articles if a["cluster"]})
    headers = ["Cluster"] + buckets + ["Total"]
    ws.append(headers)
    counts = defaultdict(Counter)
    for a in articles:
        if a["cluster"]:
            counts[a["cluster"]][_stance_bucket(a["stance"])] += 1
    for c in clusters:
        row = [c] + [counts[c].get(b, 0) for b in buckets]
        row.append(sum(counts[c].values()))
        ws.append(row)
    totals = ["Total"] + [sum(counts[c].get(b, 0) for c in clusters) for b in buckets]
    totals.append(sum(totals[1:]))
    ws.append(totals)
    _style_header(ws, len(headers))
    _auto_width(ws)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Loading bundle JSONL...")
    articles = load_bundle(BUNDLE)
    print(f"  {len(articles)} articles loaded")

    # Join cluster assignments
    cluster_map = load_cluster_map(TABLES_DIR)
    for a in articles:
        a["cluster"] = cluster_map.get(a["#"], "")
    clustered = sum(1 for a in articles if a["cluster"])
    print(f"  {clustered}/{len(articles)} articles assigned to clusters")

    # Parse grounding events
    ge_file = TABLES_DIR / "23-appendix_grounding_events_reference.md"
    ge_records = []
    if ge_file.exists():
        ge_records = parse_grounding_events(ge_file.read_text(encoding="utf-8"))
        print(f"  {len(ge_records)} grounding events parsed")

    # Build workbook
    wb = Workbook()
    wb.remove(wb.active)

    # 1. Master Articles sheet
    n = write_articles_sheet(wb, articles, "Articles")
    print(f"  ✓ Articles: {n} rows")

    # 2. Per-cluster sheets
    for c in "ABCDE":
        n = write_articles_sheet(wb, articles, f"Cluster {c}", cluster_filter=c)
        print(f"  ✓ Cluster {c}: {n} rows")

    # 3. Grounding Events
    n = write_grounding_events_sheet(wb, ge_records)
    print(f"  ✓ Grounding Events: {n} rows")

    # 4. Summaries
    write_risk_summary(wb, articles)
    print("  ✓ Risk Summary")
    write_eo_summary(wb, articles)
    print("  ✓ EO Summary")
    write_publisher_summary(wb, articles)
    print("  ✓ Publisher Summary")
    write_stance_summary(wb, articles)
    print("  ✓ Stance Summary")

    wb.save(OUTPUT)
    size_kb = OUTPUT.stat().st_size / 1024
    print(f"\n  Saved: {OUTPUT} ({size_kb:.1f} KB)")
    print(f"  Sheets: {len(wb.sheetnames)}")
    for s in wb.sheetnames:
        ws = wb[s]
        print(f"    {s}: {ws.max_row - 1} rows × {ws.max_column} cols")


if __name__ == "__main__":
    main()
