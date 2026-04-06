"""Curate the integrated EO implementation events table.

Reads  eo_implementation_cache_integrated.jsonl  and applies:

1. DROP null-comm stubs (conf=0, no URL) — move precedence to a side-car
2. DEDUP on (url, eo_number) — keep highest-confidence record, merge fields
3. NORMALIZE eo_number — resolve free-text identifiers to 5-digit numbers
4. BACKFILL eo_title, eo_date from x_indices_eo reference data
5. BACKFILL comm_title for bboa-only records from bboa descriptor
6. FLAG remaining quality issues in a QA column

Writes:
  eo_implementation_cache_curated.jsonl   — clean events
  eo_implementation_cache_curated.csv     — same, flat
  eo_precedence_sidecar.jsonl             — precedence from dropped stubs
  curation_report.json                    — stats and audit trail
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

BBOA_ROOT = Path("/mnt/workspace_2/bboa")
EO_LINKING_DIR = BBOA_ROOT / "workflows/eo_linking"
INPUT_PATH = EO_LINKING_DIR / "eo_implementation_cache_integrated.jsonl"
OUTPUT_JSONL = EO_LINKING_DIR / "eo_implementation_cache_curated.jsonl"
OUTPUT_CSV = EO_LINKING_DIR / "eo_implementation_cache_curated.csv"
PRECEDENCE_PATH = EO_LINKING_DIR / "eo_precedence_sidecar.jsonl"
REPORT_PATH = EO_LINKING_DIR / "curation_report.json"

EO_INDEX_JSONL = Path("/home/electric/workspace/RapporteurApp/x_indices_eo/research_summaries_with_embeddings_combined.jsonl")

# ── Known EO title → number mappings for free-text identifiers ─────────
KNOWN_EO_TITLES: dict[str, str] = {
    "modernizing defense acquisitions and spurring innovation in the defense industrial base": "14265",
    "reforming foreign defense sales to improve speed and accountability": "14268",
    "restoring the united states department of war": "14191",
    "the iron dome for america": "14186",
    "strengthening efforts to protect u.s. nationals from wrongful detention abroad": "14347",
}


def load_integrated() -> list[dict]:
    with open(INPUT_PATH, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def load_eo_reference() -> dict[str, dict]:
    """Build EO number → {title, date, url} from x_indices_eo."""
    ref: dict[str, dict] = {}
    if not EO_INDEX_JSONL.exists():
        return ref
    with open(EO_INDEX_JSONL, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            meta = rec.get("doc_metadata", {})
            title = meta.get("title", "")
            summary = rec.get("summary_text", "")
            url = rec.get("source_url", "")
            date = meta.get("date", "")
            for m in re.findall(r"(?:EO|Executive Order)\s+(\d{5})", title + " " + summary):
                if m not in ref:
                    ref[m] = {"title": title, "url": url, "date": date}
    return ref


def normalize_eo_number(eo_id: str) -> str:
    """Resolve free-text EO identifiers to 5-digit numbers."""
    # Already standard
    if re.match(r"^\d{5}$", eo_id):
        return eo_id
    # Try extracting from "EO 14265" or "Executive Order 14265"
    m = re.search(r"(\d{5})", eo_id)
    if m:
        return m.group(1)
    # Try known title lookup
    key = re.sub(r"^(executive order[:\s]*)", "", eo_id, flags=re.IGNORECASE).strip().lower()
    if key in KNOWN_EO_TITLES:
        return KNOWN_EO_TITLES[key]
    return eo_id


def curate() -> None:
    recs = load_integrated()
    eo_ref = load_eo_reference()
    report: dict = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "input_records": len(recs),
        "actions": [],
    }

    # ── Step 1: Separate stubs from real records ──────────────────────
    stubs: list[dict] = []
    real: list[dict] = []
    for r in recs:
        comm = r.get("communication") or {}
        if not comm.get("url") and r.get("confidence", 1) == 0:
            stubs.append(r)
        else:
            real.append(r)
    report["actions"].append({
        "step": "drop_stubs",
        "description": "Removed null-communication stub records (conf=0, no URL)",
        "dropped": len(stubs),
        "remaining": len(real),
    })

    # Save precedence from stubs as sidecar
    precedence_records = []
    for r in stubs:
        prec = r.get("precedence") or {}
        if any(prec.get(k, []) for k in ("rescinds", "derived_from", "read_with")):
            precedence_records.append({
                "eo_number": r.get("eo_number", ""),
                "eo_title": r.get("eo_title", ""),
                "record_id": r.get("record_id", ""),
                "precedence": prec,
            })
    with open(PRECEDENCE_PATH, "w", encoding="utf-8") as f:
        for pr in precedence_records:
            f.write(json.dumps(pr, ensure_ascii=False) + "\n")
    report["actions"].append({
        "step": "save_precedence_sidecar",
        "records_with_precedence": len(precedence_records),
        "path": str(PRECEDENCE_PATH),
    })

    # ── Step 2: Normalize EO numbers ──────────────────────────────────
    normalized_count = 0
    for r in real:
        old_eo = r.get("eo_number", "")
        new_eo = normalize_eo_number(old_eo)
        if new_eo != old_eo:
            r["eo_number"] = new_eo
            r["_eo_number_original"] = old_eo
            normalized_count += 1
    report["actions"].append({
        "step": "normalize_eo_numbers",
        "normalized": normalized_count,
    })

    # ── Step 3: Backfill eo_title, eo_date from reference ────────────
    title_filled = 0
    date_filled = 0
    for r in real:
        eo = r.get("eo_number", "")
        ref = eo_ref.get(eo, {})
        if not r.get("eo_title") and ref.get("title"):
            r["eo_title"] = ref["title"]
            title_filled += 1
        if not r.get("eo_date") and ref.get("date"):
            r["eo_date"] = ref["date"]
            date_filled += 1
    # Also cross-fill eo_title from sibling records
    eo_title_map: dict[str, str] = {}
    for r in real:
        eo = r.get("eo_number", "")
        t = r.get("eo_title", "")
        if eo and t and eo not in eo_title_map:
            eo_title_map[eo] = t
    for r in real:
        eo = r.get("eo_number", "")
        if not r.get("eo_title") and eo in eo_title_map:
            r["eo_title"] = eo_title_map[eo]
            title_filled += 1
    report["actions"].append({
        "step": "backfill_eo_metadata",
        "titles_filled": title_filled,
        "dates_filled": date_filled,
    })

    # ── Step 4: Backfill comm_title for bboa-only records ────────────
    title_from_desc = 0
    for r in real:
        comm = r.get("communication") or {}
        if not comm.get("title"):
            enr = r.get("bboa_enrichment") or {}
            desc = enr.get("descriptor", "") or comm.get("action_description", "")
            if desc:
                # Use first sentence or first 120 chars as title
                first_sent = re.split(r"[.!?]\s", desc, maxsplit=1)[0]
                comm["title"] = first_sent[:150].strip()
                r["communication"] = comm
                title_from_desc += 1
    report["actions"].append({
        "step": "backfill_comm_title",
        "filled_from_descriptor": title_from_desc,
    })

    # ── Step 5: Dedup on (url, eo_number) ────────────────────────────
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    no_url_recs: list[dict] = []
    for r in real:
        url = (r.get("communication") or {}).get("url", "")
        eo = r.get("eo_number", "")
        if url:
            groups[(url, eo)].append(r)
        else:
            no_url_recs.append(r)

    deduped: list[dict] = []
    merge_count = 0
    for key, group in groups.items():
        if len(group) == 1:
            deduped.append(group[0])
            continue
        # Pick the best record and merge fields from others
        group.sort(key=lambda r: r.get("confidence", 0), reverse=True)
        best = dict(group[0])
        for other in group[1:]:
            _merge_into(best, other)
        best["_dedup_merged_from"] = len(group)
        deduped.append(best)
        merge_count += len(group) - 1

    deduped.extend(no_url_recs)
    report["actions"].append({
        "step": "dedup_url_eo",
        "groups_with_duplicates": sum(1 for g in groups.values() if len(g) > 1),
        "records_merged_away": merge_count,
        "remaining": len(deduped),
    })

    # ── Step 6: QA flags ─────────────────────────────────────────────
    for r in deduped:
        flags: list[str] = []
        comm = r.get("communication") or {}
        if not comm.get("url"):
            flags.append("no_url")
        if not r.get("eo_title"):
            flags.append("no_eo_title")
        if not re.match(r"^\d{5}$", str(r.get("eo_number", ""))):
            flags.append("non_standard_eo")
        if r.get("confidence", 1) < 0.5 and r.get("confidence", 1) > 0:
            flags.append("low_confidence")
        if not comm.get("action_description") and not (r.get("bboa_enrichment") or {}).get("descriptor"):
            flags.append("no_description")
        r["qa_flags"] = flags

    flagged = sum(1 for r in deduped if r.get("qa_flags"))
    flag_counts = Counter()
    for r in deduped:
        for f in r.get("qa_flags", []):
            flag_counts[f] += 1
    report["actions"].append({
        "step": "qa_flags",
        "records_flagged": flagged,
        "flag_counts": dict(flag_counts),
    })

    # ── Sort by date, then EO ────────────────────────────────────────
    def sort_key(r: dict) -> tuple:
        comm = r.get("communication") or {}
        d = comm.get("date", "")
        if not d:
            enr = r.get("bboa_enrichment") or {}
            d = enr.get("window_start", "")
        return (d or "9999", r.get("eo_number", ""))

    deduped.sort(key=sort_key)

    # ── Write outputs ────────────────────────────────────────────────
    with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
        for r in deduped:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    _write_csv(deduped)

    report["output_records"] = len(deduped)
    report["reduction"] = f"{len(recs)} → {len(deduped)} ({len(recs) - len(deduped)} removed)"
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"=== Curation Complete ===")
    print(f"  Input:              {len(recs)}")
    print(f"  Stubs dropped:      {len(stubs)} (precedence saved: {len(precedence_records)})")
    print(f"  EO# normalized:     {normalized_count}")
    print(f"  Titles backfilled:  {title_filled}")
    print(f"  Dates backfilled:   {date_filled}")
    print(f"  Titles from desc:   {title_from_desc}")
    print(f"  Duplicates merged:  {merge_count}")
    print(f"  QA flagged:         {flagged}")
    print(f"  Output:             {len(deduped)}")
    print(f"  Files:")
    print(f"    {OUTPUT_JSONL}")
    print(f"    {OUTPUT_CSV}")
    print(f"    {PRECEDENCE_PATH}")
    print(f"    {REPORT_PATH}")


def _merge_into(best: dict, other: dict) -> None:
    """Merge non-empty fields from *other* into *best*."""
    # Merge communication fields
    bc = best.get("communication") or {}
    oc = other.get("communication") or {}
    for k in ("title", "official", "action_type", "action_description", "subsection"):
        if not bc.get(k) and oc.get(k):
            bc[k] = oc[k]
    best["communication"] = bc

    # Merge precedence (union)
    bp = best.get("precedence") or {"rescinds": [], "derived_from": [], "read_with": []}
    op = other.get("precedence") or {}
    for k in ("rescinds", "derived_from", "read_with"):
        existing = set(bp.get(k, []))
        for v in op.get(k, []):
            if v not in existing:
                bp.setdefault(k, []).append(v)
    best["precedence"] = bp

    # Merge cross_eo
    existing_xeo = set(best.get("cross_eo", []))
    for v in other.get("cross_eo", []):
        if v not in existing_xeo:
            best.setdefault("cross_eo", []).append(v)

    # Merge bboa_enrichment (prefer the one with data)
    if not best.get("bboa_enrichment") and other.get("bboa_enrichment"):
        best["bboa_enrichment"] = other["bboa_enrichment"]

    # Backfill eo_title, eo_date
    if not best.get("eo_title") and other.get("eo_title"):
        best["eo_title"] = other["eo_title"]
    if not best.get("eo_date") and other.get("eo_date"):
        best["eo_date"] = other["eo_date"]


def _write_csv(records: list[dict]) -> None:
    headers = [
        "eo_number", "eo_title", "eo_date",
        "comm_url", "comm_title", "comm_date", "comm_domain",
        "comm_official", "comm_action_type", "comm_action_description",
        "comm_subsection",
        "rescinds", "derived_from", "read_with",
        "confidence", "batch_id", "record_id", "cross_eo",
        "bboa_source", "bboa_event_key",
        "bboa_window_start", "bboa_window_end",
        "bboa_descriptor", "bboa_media_count",
        "bboa_risk_low", "bboa_risk_medium", "bboa_risk_high", "bboa_risk_extreme",
        "bboa_exec_official",
        "qa_flags",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in records:
            comm = r.get("communication") or {}
            prec = r.get("precedence") or {}
            enr = r.get("bboa_enrichment") or {}
            risk = enr.get("risk_counts") or {}
            w.writerow({
                "eo_number": r.get("eo_number", ""),
                "eo_title": r.get("eo_title", ""),
                "eo_date": r.get("eo_date", ""),
                "comm_url": comm.get("url", ""),
                "comm_title": comm.get("title", ""),
                "comm_date": comm.get("date", ""),
                "comm_domain": comm.get("domain", ""),
                "comm_official": comm.get("official", ""),
                "comm_action_type": comm.get("action_type", ""),
                "comm_action_description": comm.get("action_description", ""),
                "comm_subsection": comm.get("subsection", ""),
                "rescinds": "|".join(prec.get("rescinds", [])),
                "derived_from": "|".join(prec.get("derived_from", [])),
                "read_with": "|".join(prec.get("read_with", [])),
                "confidence": r.get("confidence", ""),
                "batch_id": r.get("batch_id", ""),
                "record_id": r.get("record_id", ""),
                "cross_eo": "|".join(r.get("cross_eo", [])),
                "bboa_source": enr.get("source", ""),
                "bboa_event_key": enr.get("event_key", ""),
                "bboa_window_start": enr.get("window_start", ""),
                "bboa_window_end": enr.get("window_end", ""),
                "bboa_descriptor": enr.get("descriptor", ""),
                "bboa_media_count": enr.get("media_count", ""),
                "bboa_risk_low": risk.get("LOW", ""),
                "bboa_risk_medium": risk.get("MEDIUM", ""),
                "bboa_risk_high": risk.get("HIGH", ""),
                "bboa_risk_extreme": risk.get("EXTREME", ""),
                "bboa_exec_official": enr.get("exec_official", ""),
                "qa_flags": "|".join(r.get("qa_flags", [])),
            })


if __name__ == "__main__":
    curate()
