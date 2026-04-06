"""Reconcile EO implementation events from two complementary sources.

Source A — eo_implementation_cache.jsonl (workflows/eo_linking/)
    Structured EO-linkage records with action types, precedence chains,
    confidence scores, and cross-EO references.

Source B — eo_events_bboa_*.jsonl (cache/bboa_*/)
    BBOA-derived event records with time windows, media counts,
    risk assessments, and narrative descriptors.

Strategy:
    1. URL-match:  Join on source_url where both sources reference
       the same government communication.  Merge complementary fields.
    2. bboa-only:  Records with URLs not in impl_cache are converted
       to impl_cache format so they can participate in grounding.
    3. impl-only:  Records already in impl_cache that have no bboa
       counterpart are kept as-is (they already work for grounding).

Output:  eo_implementation_cache_integrated.jsonl  — unified dataset
         in impl_cache schema, enriched with bboa media/risk metadata.
"""
from __future__ import annotations

import glob
import json
import re
import sys
from pathlib import Path

BBOA_ROOT = Path("/mnt/workspace_2/bboa")
IMPL_PATH = BBOA_ROOT / "workflows/eo_linking/eo_implementation_cache.jsonl"
BBOA_GLOB = str(BBOA_ROOT / "cache/bboa_*/eo_events_bboa_*.jsonl")
OUTPUT_PATH = BBOA_ROOT / "workflows/eo_linking/eo_implementation_cache_integrated.jsonl"
OUTPUT_CSV = BBOA_ROOT / "workflows/eo_linking/eo_implementation_cache_integrated.csv"


def load_impl() -> list[dict]:
    with open(IMPL_PATH, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def load_bboa_deduped() -> list[dict]:
    """Load all bboa event files, deduplicate by event_key."""
    all_recs: list[dict] = []
    for path in sorted(glob.glob(BBOA_GLOB)):
        with open(path, encoding="utf-8") as f:
            all_recs.extend(json.loads(line) for line in f if line.strip())
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_recs:
        k = r.get("event_key", "")
        if k and k not in seen:
            seen.add(k)
            deduped.append(r)
    return deduped


def normalize_eo_number(eo_id: str) -> str:
    """Extract 5-digit EO number from various formats."""
    m = re.search(r"(\d{5})", eo_id)
    return m.group(1) if m else ""


def bboa_to_impl_format(bboa_rec: dict) -> dict:
    """Convert a bboa event record into impl_cache schema."""
    eo_id = bboa_rec.get("eo_identifier", "")
    eo_num = normalize_eo_number(eo_id)

    return {
        "eo_number": eo_num or eo_id,
        "eo_title": bboa_rec.get("eo_title", ""),
        "eo_date": bboa_rec.get("eo_date", ""),
        "communication": {
            "url": bboa_rec.get("source_url", ""),
            "title": "",
            "date": bboa_rec.get("window_start", ""),
            "domain": _extract_domain(bboa_rec.get("source_url", "")),
            "official": bboa_rec.get("exec_official", ""),
            "action_type": "",
            "action_description": bboa_rec.get("descriptor", ""),
            "subsection": "",
        },
        "precedence": {
            "rescinds": [],
            "derived_from": [],
            "read_with": [],
        },
        "confidence": 0.0,
        "batch_id": "bboa_import",
        "record_id": bboa_rec.get("event_key", ""),
        "cross_eo": [],
        # --- bboa enrichment fields ---
        "bboa_enrichment": {
            "event_key": bboa_rec.get("event_key", ""),
            "window_start": bboa_rec.get("window_start", ""),
            "window_end": bboa_rec.get("window_end", ""),
            "descriptor": bboa_rec.get("descriptor", ""),
            "media_count": bboa_rec.get("media_count", 0),
            "risk_counts": bboa_rec.get("risk_counts", {}),
            "exec_official": bboa_rec.get("exec_official", ""),
            "source": "bboa_only",
        },
    }


def enrich_impl_with_bboa(impl_rec: dict, bboa_rec: dict) -> dict:
    """Add bboa enrichment fields to an existing impl_cache record."""
    rec = dict(impl_rec)
    rec["bboa_enrichment"] = {
        "event_key": bboa_rec.get("event_key", ""),
        "window_start": bboa_rec.get("window_start", ""),
        "window_end": bboa_rec.get("window_end", ""),
        "descriptor": bboa_rec.get("descriptor", ""),
        "media_count": bboa_rec.get("media_count", 0),
        "risk_counts": bboa_rec.get("risk_counts", {}),
        "exec_official": bboa_rec.get("exec_official", ""),
        "source": "url_matched",
    }
    return rec


def _extract_domain(url: str) -> str:
    m = re.search(r"https?://([^/]+)", url)
    return m.group(1) if m else ""


def reconcile() -> None:
    impl = load_impl()
    bboa = load_bboa_deduped()

    # Index impl by URL
    impl_by_url: dict[str, list[int]] = {}
    for i, r in enumerate(impl):
        comm = r.get("communication")
        if comm and comm.get("url"):
            impl_by_url.setdefault(comm["url"], []).append(i)

    # Index bboa by URL (many-to-one possible)
    bboa_by_url: dict[str, list[dict]] = {}
    for r in bboa:
        url = r.get("source_url", "")
        if url:
            bboa_by_url.setdefault(url, []).append(r)

    # --- Pass 1: Enrich existing impl records with bboa metadata ---
    enriched_impl: list[dict] = []
    impl_enriched_indices: set[int] = set()
    bboa_matched_keys: set[str] = set()

    for url, impl_indices in impl_by_url.items():
        bboa_recs = bboa_by_url.get(url, [])
        if not bboa_recs:
            continue
        # Match: pick the bboa record closest in EO number
        for idx in impl_indices:
            ir = impl[idx]
            eo_num = str(ir.get("eo_number", ""))
            best_bboa = None
            for br in bboa_recs:
                br_eo = normalize_eo_number(br.get("eo_identifier", ""))
                if br_eo == eo_num:
                    best_bboa = br
                    break
            if best_bboa is None:
                best_bboa = bboa_recs[0]
            enriched_impl.append(enrich_impl_with_bboa(ir, best_bboa))
            impl_enriched_indices.add(idx)
            bboa_matched_keys.add(best_bboa.get("event_key", ""))

    # --- Pass 2: Keep un-enriched impl records as-is ---
    for i, r in enumerate(impl):
        if i not in impl_enriched_indices:
            rec = dict(r)
            rec["bboa_enrichment"] = None
            enriched_impl.append(rec)

    # --- Pass 3: Add bboa-only records (no URL in impl) ---
    bboa_only: list[dict] = []
    for r in bboa:
        key = r.get("event_key", "")
        url = r.get("source_url", "")
        if key not in bboa_matched_keys and url not in impl_by_url:
            bboa_only.append(bboa_to_impl_format(r))

    integrated = enriched_impl + bboa_only

    # Sort by date
    def sort_key(r: dict) -> str:
        comm = r.get("communication") or {}
        d = comm.get("date", "")
        if not d:
            enr = r.get("bboa_enrichment") or {}
            d = enr.get("window_start", "")
        return d or "9999"

    integrated.sort(key=sort_key)

    # Write JSONL
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for rec in integrated:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Write CSV
    _write_csv(integrated)

    # Stats
    n_enriched = sum(1 for r in integrated if (r.get("bboa_enrichment") or {}).get("source") == "url_matched")
    n_bboa_only = sum(1 for r in integrated if (r.get("bboa_enrichment") or {}).get("source") == "bboa_only")
    n_impl_only = sum(1 for r in integrated if r.get("bboa_enrichment") is None)
    print(f"=== Reconciliation Complete ===")
    print(f"  impl_cache records:     {len(impl)}")
    print(f"  bboa events (deduped):  {len(bboa)}")
    print(f"  ---")
    print(f"  URL-matched (enriched): {n_enriched}")
    print(f"  impl-only (no bboa):    {n_impl_only}")
    print(f"  bboa-only (new):        {n_bboa_only}")
    print(f"  ---")
    print(f"  TOTAL integrated:       {len(integrated)}")
    print(f"  Output JSONL: {OUTPUT_PATH}")
    print(f"  Output CSV:   {OUTPUT_CSV}")


def _write_csv(records: list[dict]) -> None:
    import csv
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
            })


if __name__ == "__main__":
    reconcile()
