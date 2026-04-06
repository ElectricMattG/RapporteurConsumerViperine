#!/usr/bin/env python3
"""Standalone demo of the BM25 + Fuzzy ArticleRetriever component.

Loads 2,127 tagged BBOA media articles and runs structured queries across
title, topic, media, operation, person, country fields.

Usage:
    python -m src.demo.demo_article_retriever [--top-k 10]
"""
from __future__ import annotations

import argparse
import time

from src.components.analysis.article_retriever import (ArticleQuery,
                                                       ArticleRetriever)


def _print_results(label: str, results: list[dict], show_max: int = 5) -> None:
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"  {len(results)} result(s)")
    print(f"{'='*70}")
    for i, art in enumerate(results[:show_max], 1):
        tags = art.get("tags", {})
        score_key = "_combined_score" if "_combined_score" in art else "_score"
        score = art.get(score_key, 0)
        fields = art.get("_field_hits", "")
        bm25 = art.get("_bm25_score", "")
        rag = art.get("_rag_score", "")
        score_str = f"score={score:.2f}"
        if bm25 != "":
            score_str += f"  bm25={bm25:.2f}  rag={rag:.3f}"
        if fields != "":
            score_str += f"  fields={fields}"
        print(f"\n  [{i}] {score_str}")
        print(f"      {art.get('title', 'Untitled')}")
        print(f"      {art.get('url', '')[:90]}")
        print(f"      topics:     {', '.join(tags.get('topic', []))}")
        persons = tags.get("person", [])
        if persons:
            print(f"      persons:    {', '.join(persons[:6])}")
        countries = tags.get("country", [])
        if countries:
            print(f"      countries:  {', '.join(countries[:6])}")
        ops = tags.get("operation", [])
        if ops:
            print(f"      operations: {', '.join(ops)}")
    if len(results) > show_max:
        print(f"\n  ... +{len(results) - show_max} more")


def main() -> None:
    parser = argparse.ArgumentParser(description="ArticleRetriever demo")
    parser.add_argument("--top-k", type=int, default=10, help="Results per query")
    args = parser.parse_args()
    top_k = args.top_k

    # ── Load ──────────────────────────────────────────────────────────
    print("Loading tagged articles ...")
    t0 = time.perf_counter()
    retriever = ArticleRetriever.from_tagged_files()
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"Loaded {retriever.article_count} articles in {elapsed:.0f} ms")
    print(f"Sources: {retriever.source_counts()}")

    # ── Q1: Hegseth + culture_and_media ───────────────────────────────
    t0 = time.perf_counter()
    q1 = retriever.query(
        person="Hegseth",
        topic="culture_and_media",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q1: Hegseth + culture_and_media  ({ms:.1f} ms)", q1)

    # ── Q2: AI + key tech persons ─────────────────────────────────────
    t0 = time.perf_counter()
    q2 = retriever.query(
        topic="artificial_intelligence",
        person=["Elon Musk", "Sam Altman", "Dario Amodei"],
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q2: AI + key tech persons  ({ms:.1f} ms)", q2)

    # ── Q3: Border / cartels in title ─────────────────────────────────
    t0 = time.perf_counter()
    q3 = retriever.query(
        title="Border Cartels Immigrants",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q3: title=Border Cartels Immigrants  ({ms:.1f} ms)", q3)

    # ── Q4: Southern Border operations (AND mode) ─────────────────────
    t0 = time.perf_counter()
    q4 = retriever.query(
        topic="national_security_domestic",
        operation=["Illegal Immigration", "Southern Border"],
        require_all=True,
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q4: nat_sec_domestic AND ops (require_all)  ({ms:.1f} ms)", q4)

    # ── Q5: Russia/Ukraine + nat_sec_foreign ──────────────────────────
    t0 = time.perf_counter()
    q5 = retriever.query(
        topic="national_security_foreign",
        title="Russia Ukraine",
        person=["Trump", "Zelensky", "Putin"],
        country=["Russia", "Ukraine"],
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q5: Russia/Ukraine + nat_sec_foreign  ({ms:.1f} ms)", q5)

    # ── Q6: Multi-topic, multi-country ────────────────────────────────
    t0 = time.perf_counter()
    q6 = retriever.query(
        topic=["energy", "federal_procurement"],
        country=["China", "Canada", "Mexico"],
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q6: energy|fed_procurement + China|Canada|Mexico  ({ms:.1f} ms)", q6)

    # ── Q7: Epic Fury + Iran ──────────────────────────────────────────
    t0 = time.perf_counter()
    q7 = retriever.query(
        operation="Epic Fury",
        country="Iran",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q7: Operation Epic Fury + Iran  ({ms:.1f} ms)", q7)

    # ── Q8: Midnight Hammer + Iran ────────────────────────────────────
    t0 = time.perf_counter()
    q8 = retriever.query(
        operation="Midnight Hammer",
        country="Iran",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q8: Operation Midnight Hammer + Iran  ({ms:.1f} ms)", q8)

    # ── Q9: Rough Rider + Yemen / Houthis ─────────────────────────────
    t0 = time.perf_counter()
    q9 = retriever.query(
        operation="Rough Rider",
        country="Yemen",
        title="Houthis",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q9: Operation Rough Rider + Yemen + Houthis  ({ms:.1f} ms)", q9)

    # ── Q10: Illegal Immigration + ICE ────────────────────────────────
    t0 = time.perf_counter()
    q10 = retriever.query(
        operation="Illegal Immigration",
        title="ICE",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q10: Operation Illegal Immigration + title ICE  ({ms:.1f} ms)", q10)

    # ── Q11: Intelligence — Gabbard / Patel ───────────────────────────
    t0 = time.perf_counter()
    q11 = retriever.query(
        title="intelligence",
        person=["Tulsi Gabbard", "Kash Patel"],
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Q11: Intelligence + Gabbard/Patel  ({ms:.1f} ms)", q11)

    # ── Source filter: linked only ────────────────────────────────────
    t0 = time.perf_counter()
    linked = retriever.query(
        topic="culture_and_media",
        source_filter="linked",
        top_k=top_k,
    )
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"Source filter: linked-only culture_and_media  ({ms:.1f} ms)", linked)

    # ── ArticleQuery dataclass ────────────────────────────────────────
    spec = ArticleQuery(
        title="tariff trade",
        country=["China", "European Union"],
        topic="federal_procurement",
        top_k=top_k,
    )
    t0 = time.perf_counter()
    q_spec = retriever.query_from_spec(spec)
    ms = (time.perf_counter() - t0) * 1000
    _print_results(f"ArticleQuery spec: tariff trade + China/EU  ({ms:.1f} ms)", q_spec)

    # ── format_for_grounding ──────────────────────────────────────────
    grounding = retriever.format_for_grounding(q1[:3], max_chars=5000)
    print(f"\n{'='*70}")
    print("  format_for_grounding (first 3 from Q1)")
    print(f"{'='*70}")
    print(grounding[:2000])
    if len(grounding) > 2000:
        print(f"\n  ... ({len(grounding)} chars total)")

    # ==================================================================
    # BM25 unlinked-only sweep (deduplicated unique set, no RAG)
    # ==================================================================
    print(f"\n\n{'#'*70}")
    print("  BM25 UNLINKED SWEEP  (deduplicated, no RAG)")
    print(f"{'#'*70}")

    bm25_queries = [
        ("Q1", dict(topic="culture_and_media", person="Hegseth")),
        ("Q2", dict(topic="artificial_intelligence", person=["Elon Musk", "Sam Altman", "Dario Amodei"])),
        ("Q3", dict(title="Border Cartels Immigrants")),
        ("Q4", dict(topic="national_security_domestic", operation=["Illegal Immigration", "Southern Border"], require_all=True)),
        ("Q5", dict(topic="national_security_foreign", title="Russia Ukraine", person=["Trump", "Zelensky", "Putin"], country=["Russia", "Ukraine"])),
        ("Q6", dict(topic=["energy", "federal_procurement"], country=["China", "Canada", "Mexico"])),
        ("Q7", dict(operation="Epic Fury", country="Iran")),
        ("Q8", dict(operation="Midnight Hammer", country="Iran")),
        ("Q9", dict(operation="Rough Rider", country="Yemen", title="Houthis")),
        ("Q10", dict(operation="Illegal Immigration", title="ICE")),
        ("Q11", dict(title="intelligence", person=["Tulsi Gabbard", "Kash Patel"])),
    ]

    seen_urls: set[str] = set()
    unique_results: list[dict] = []

    for label, kwargs in bm25_queries:
        t0 = time.perf_counter()
        results = retriever.query(
            source_filter="unlinked",
            top_k=100,
            **kwargs,
        )
        ms = (time.perf_counter() - t0) * 1000
        new_count = 0
        for r in results:
            url = r.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                r["_from_query"] = label
                unique_results.append(r)
                new_count += 1
        print(f"\n  {label}: {len(results)} results, {new_count} new unique  ({ms:.1f} ms)")
        # Show top 5 from this query
        for i, art in enumerate(results[:5], 1):
            tags = art.get("tags", {})
            score = art.get("_score", 0)
            fields = art.get("_field_hits", 0)
            dedup = " [dup]" if art.get("url", "") in seen_urls and art not in unique_results[-new_count:] else ""
            print(f"    [{i}] score={score:.2f}  fields={fields}{dedup}")
            print(f"        {art.get('title', 'Untitled')}")
            print(f"        {art.get('url', '')[:100]}")
            topics = tags.get("topic", [])
            persons = tags.get("person", [])
            countries = tags.get("country", [])
            ops = tags.get("operation", [])
            if topics:
                print(f"        topics:     {', '.join(topics)}")
            if persons:
                print(f"        persons:    {', '.join(persons[:6])}")
            if countries:
                print(f"        countries:  {', '.join(countries[:6])}")
            if ops:
                print(f"        operations: {', '.join(ops)}")

    # Summary
    print(f"\n{'='*70}")
    print(f"  UNIQUE SET: {len(unique_results)} unlinked articles from {len(bm25_queries)} queries")
    print(f"  (BM25 only, source=unlinked, deduplicated by URL)")
    print(f"{'='*70}")

    # Show top 25 by score
    unique_results.sort(key=lambda x: x.get("_score", 0), reverse=True)
    for i, art in enumerate(unique_results[:25], 1):
        tags = art.get("tags", {})
        score = art.get("_score", 0)
        fields = art.get("_field_hits", 0)
        print(f"\n  [{i}] score={score:.2f}  fields={fields}  from={art.get('_from_query','')}")
        print(f"      {art.get('title', 'Untitled')}")
        print(f"      {art.get('url', '')[:100]}")
        topics = tags.get("topic", [])
        persons = tags.get("person", [])
        countries = tags.get("country", [])
        ops = tags.get("operation", [])
        if topics:
            print(f"      topics:     {', '.join(topics)}")
        if persons:
            print(f"      persons:    {', '.join(persons[:6])}")
        if countries:
            print(f"      countries:  {', '.join(countries[:6])}")
        if ops:
            print(f"      operations: {', '.join(ops)}")
    if len(unique_results) > 25:
        print(f"\n  ... +{len(unique_results) - 25} more")

    print(f"\n{'='*70}")
    print("  Done — all queries complete")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
