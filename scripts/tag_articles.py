#!/usr/bin/env python3
"""
tag_articles.py — Lightweight article tagging via Gemini-2.5-Flash.

Reads unlinked and linked media articles, extracts summaries, sends batches
to Gemini-2.5-Flash for structured tagging, and writes enriched JSONL output.

Tag schema injected per article:
    "tags": {
        "topic": [...],       # Category-level topics
        "media": [...],       # Media classification labels
        "operation": [...],   # Named operations/events
        "person": [...],      # Named individuals
        "country": [...]      # Nation-states mentioned
    }

Output is written incrementally — progress survives Ctrl+C.

Usage:
    python scripts/tag_articles.py                          # dry-run, no LLM
    python scripts/tag_articles.py --live                   # call Gemini
    python scripts/tag_articles.py --live --batch-size 30   # larger batches
    python scripts/tag_articles.py --live --source linked   # only linked
    python scripts/tag_articles.py --live --source unlinked # only unlinked
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
UNLINKED_PATH = Path(
    "/mnt/workspace_2/bboa/workflows/media_linking/unlinked_articles.jsonl"
)
LINKED_PATH = Path(
    "/mnt/workspace_2/bboa/workflows/media_linking/media_linkage_cache.jsonl"
)
OUTPUT_DIR = Path("/mnt/workspace_2/bboa/workflows/media_linking")

MODEL_NAME = "gemini-2.5-flash"

# ---------------------------------------------------------------------------
# Canonical tag values
# ---------------------------------------------------------------------------
TOPICS = [
    "culture_and_media",
    "artificial_intelligence",
    "federal_procurement",
    "energy_and_minerals",
    "national_security_domestic",
    "national_security_foreign",
    "government_and_politics",
    "war_time_narratives",
    "department_of_war",
    "academic_and_research",
    "personal_profile",
]

OPERATIONS = [
    "Rough Rider",        # Yemen strikes
    "Southern Border",    # Border security
    "Midnight Hammer",    # Iran nuclear strikes B2
    "Epic Fury",          # Iran attack
    "Illegal Immigration",# Immigration operations (general)
    "Other Joint Operations",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_summary_from_content(content: str) -> str:
    """Pull the ## Summary section from the unlinked article content block."""
    idx = content.find("## Summary")
    if idx >= 0:
        return content[idx + len("## Summary"):].strip()
    # Fallback: skip the header block (URL/Title/Domain/Date/Publisher lines)
    lines = content.split("\n")
    body_lines = []
    past_header = False
    for line in lines:
        if past_header:
            body_lines.append(line)
        elif line.strip() == "":
            past_header = True
    return "\n".join(body_lines).strip() if body_lines else content


def _build_article_text_unlinked(record: dict) -> str:
    """Build concise text for an unlinked article (summary preferred)."""
    title = record.get("title", "")
    publisher = record.get("publisher", "")
    date = record.get("date", "")
    domain = record.get("domain", "")
    summary = _extract_summary_from_content(record.get("content", ""))

    parts = []
    if title:
        parts.append(f"Title: {title}")
    if publisher:
        parts.append(f"Publisher: {publisher}")
    if date:
        parts.append(f"Date: {date}")
    if domain:
        parts.append(f"Domain: {domain}")
    if summary:
        parts.append(f"Summary: {summary}")
    return "\n".join(parts)


def _build_article_text_linked(record: dict) -> str:
    """Build concise text for a linked article (rationale + metadata)."""
    title = record.get("article_title", "")
    publisher = record.get("publisher", "")
    date = record.get("article_date", "")
    domain = record.get("article_domain", "")
    rationale = record.get("linkage_rationale", "")

    # Also include linked EO info for context
    eo_info = ""
    for eo in record.get("linked_eos", []):
        eo_info += f" EO {eo.get('eo_number', '')}: {eo.get('eo_name', '')}."

    parts = []
    if title:
        parts.append(f"Title: {title}")
    if publisher:
        parts.append(f"Publisher: {publisher}")
    if date:
        parts.append(f"Date: {date}")
    if domain:
        parts.append(f"Domain: {domain}")
    if rationale:
        parts.append(f"Content: {rationale}")
    if eo_info:
        parts.append(f"Related EOs:{eo_info}")
    return "\n".join(parts)


def _build_batch_prompt(articles: list[dict[str, str]]) -> str:
    """Build a batch tagging prompt for N articles.

    articles: list of {"id": str, "text": str}
    Uses simple numeric IDs (1, 2, 3...) so Gemini returns them reliably.
    """
    article_block = ""
    for art in articles:
        article_block += f"--- ARTICLE {art['id']} ---\n{art['text']}\n\n"

    return f"""You are a structured-data tagger. For each article below, extract tags into exactly this JSON schema:

{{
  "<article_id>": {{
    "topic": [],
    "media": [],
    "operation": [],
    "person": [],
    "country": []
  }}
}}

**Tag definitions:**

topic — Select ALL that apply from this fixed list:
  culture_and_media, artificial_intelligence, federal_procurement, energy_and_minerals, national_security_domestic, national_security_foreign
  An article can have multiple topics. Choose based on the subject matter of the article. If one of these doesn't fit consider another tag or tags that are commonly used in news and media.

media — Classify the media based on the information provided. Use common media labels related to the content so that someone could search by label and get relevant content.

operation — Select ALL that apply from this list, or leave empty if none match:
  "Rough Rider" (Yemen strikes), "Southern Border" (border security operations), "Midnight Hammer" (Iran nuclear strikes B2), "Epic Fury" (Iran attack), "Illegal Immigration" (immigration enforcement operations in general), "Other Joint Operations" (any named military operations not in the list above)

person — Extract names of specific individuals mentioned (executive branch, legislative branch, military, defense officials, foreign leaders). Use full names where possible. Empty list if no specific people are named.

country — Extract names of countries/nation-states mentioned in the article. Use standard country names. Empty list if no countries mentioned.

**Rules:**
- Return ONLY valid JSON, no markdown fences, no commentary.
- The JSON keys MUST be exactly the article IDs shown above (e.g. "1", "2", "3").
- Every article ID in the input must appear as a key in the output.
- Lists can be empty [] if nothing applies.
- For topic, at least one must be selected if the article relates to US politics, policy, defense, or governance.

{article_block}

Return the JSON object now:"""


def _parse_llm_response(text: str) -> dict:
    """Parse the LLM JSON response, handling common formatting issues."""
    text = text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find the JSON object
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return {}


def _resolve_tags(tags_map: dict, expected_ids: list[str]) -> dict[str, dict]:
    """Match LLM-returned keys to our expected IDs, handling format variations.

    Gemini may return "1", "ARTICLE 1", "article_1", "unlinked_0", etc.
    We try exact match first, then positional fallback.
    """
    resolved: dict[str, dict] = {}

    # Try exact match first
    for eid in expected_ids:
        if eid in tags_map:
            resolved[eid] = tags_map[eid]

    if len(resolved) == len(expected_ids):
        return resolved

    # Try matching returned keys to expected IDs by position
    # (if Gemini returned all keys but in a different format)
    remaining_expected = [eid for eid in expected_ids if eid not in resolved]
    remaining_returned = {k: v for k, v in tags_map.items() if k not in expected_ids}

    if len(remaining_returned) == len(remaining_expected):
        # Positional match: sort returned keys, match to remaining expected IDs in order
        # Gemini typically returns keys in the same order as the articles
        for eid, (_, tags) in zip(remaining_expected, sorted(remaining_returned.items())):
            resolved[eid] = tags
    elif len(remaining_returned) >= 1:
        # Partial match: try numeric suffix extraction
        # e.g. "ARTICLE 3" -> "3", "article_unlinked_7" -> strip to find match
        key_to_returned = {}
        for rk in remaining_returned:
            # Extract trailing number
            m = re.search(r'(\d+)\s*$', str(rk))
            if m:
                key_to_returned[m.group(1)] = rk
        for eid in remaining_expected:
            m = re.search(r'(\d+)$', eid)
            if m and m.group(1) in key_to_returned:
                resolved[eid] = remaining_returned[key_to_returned[m.group(1)]]

    return resolved


def _empty_tags() -> dict:
    """Return a default empty tags structure."""
    return {"topic": [], "media": [], "operation": [], "person": [], "country": []}


def _get_url(rec: dict, source: str) -> str:
    """Get the URL from a record regardless of source type."""
    if source == "unlinked":
        return rec.get("url", "")
    return rec.get("article_url", "")


def _build_index_row(rec: dict, source: str) -> dict:
    """Build a unified tags index row from a record."""
    if source == "unlinked":
        return {
            "url": rec.get("url", ""),
            "source": "unlinked",
            "title": rec.get("title", ""),
            "publisher": rec.get("publisher", ""),
            "date": rec.get("date", ""),
            "tags": rec.get("tags", _empty_tags()),
        }
    return {
        "url": rec.get("article_url", ""),
        "source": "linked",
        "title": rec.get("article_title", ""),
        "publisher": rec.get("publisher", ""),
        "date": rec.get("article_date", ""),
        "tags": rec.get("tags", _empty_tags()),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_articles(source: str) -> tuple[list[dict], list[dict]]:
    """Load articles from the specified source(s)."""
    unlinked = []
    linked = []

    if source in ("all", "unlinked"):
        with open(UNLINKED_PATH, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    unlinked.append(json.loads(line))

    if source in ("all", "linked"):
        with open(LINKED_PATH, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    linked.append(json.loads(line))

    return unlinked, linked


async def tag_batch_live(
    client,
    batch: list[dict[str, str]],
    batch_num: int,
    total_batches: int,
) -> tuple[dict, dict]:
    """Send a batch to Gemini and return (tags_by_id, usage)."""
    prompt = _build_batch_prompt(batch)

    try:
        # Gemini-2.5-flash uses "thinking" tokens that count against
        # max_output_tokens — need a large budget so thinking (~4k)
        # doesn't crowd out the actual JSON response (~2k for 20 articles).
        response = await client.generate_content_with_usage(
            prompt,
            temperature=0.1,
            max_tokens=65536,
        )
        raw_text = response["text"]
        raw_tags = _parse_llm_response(raw_text)
        usage = response.get("usage", {})
        # Resolve LLM-returned keys to our expected IDs
        expected_ids = [art["id"] for art in batch]
        tags = _resolve_tags(raw_tags, expected_ids)

        return tags, usage
    except Exception as e:
        print(f"  [Batch {batch_num}/{total_batches}] ERROR: {e}")
        return {}, {}


def run(args: argparse.Namespace) -> None:
    """Main entry point."""
    print(f"\n{'=' * 70}")
    print(f"Article Tagging — {MODEL_NAME}")
    print(f"{'=' * 70}")

    unlinked, linked = load_articles(args.source)
    print(f"\n[Loaded]")
    print(f"  Unlinked articles: {len(unlinked)}")
    print(f"  Linked articles:   {len(linked)}")
    print(f"  Total to tag:      {len(unlinked) + len(linked)}")

    # Build text representations
    # Each item: (source, idx, record, text)
    all_items: list[tuple[str, int, dict, str]] = []

    for i, rec in enumerate(unlinked):
        text = _build_article_text_unlinked(rec)
        all_items.append(("unlinked", i, rec, text))

    for i, rec in enumerate(linked):
        text = _build_article_text_linked(rec)
        all_items.append(("linked", i, rec, text))

    # Estimate token cost
    total_chars = sum(len(item[3]) for item in all_items)
    est_tokens = total_chars // 4
    rate = 0.30  # gemini-2.5-flash input rate per 1M
    est_cost = est_tokens / 1_000_000 * rate
    print(f"\n[Estimate]")
    print(f"  Total text: {total_chars:,} chars (~{est_tokens:,} input tokens)")
    print(f"  Prompt overhead: ~{len(all_items) // args.batch_size + 1} batches × ~500 tokens")
    print(f"  Estimated input cost: ~${est_cost:.4f} ({MODEL_NAME} @ ${rate}/1M)")

    # Build batches
    batch_size = args.batch_size
    batches: list[list[tuple[str, int, dict, str]]] = []
    for i in range(0, len(all_items), batch_size):
        batches.append(all_items[i : i + batch_size])

    print(f"  Batches: {len(batches)} (batch size {batch_size})")

    # Output paths
    unlinked_out = str(OUTPUT_DIR / "unlinked_articles_tagged.jsonl")
    linked_out = str(OUTPUT_DIR / "media_linkage_cache_tagged.jsonl")
    index_out = str(OUTPUT_DIR / "article_tags_index.jsonl")

    # Resume support: count already-written records and skip completed batches
    skip_batches = 0
    if args.resume:
        already_written = 0
        for p in [unlinked_out, linked_out]:
            pp = Path(p)
            if pp.exists():
                already_written += sum(1 for _ in open(p, encoding="utf-8"))
        skip_batches = already_written // batch_size
        if skip_batches > 0:
            print(f"\n[Resume] Found {already_written} records — skipping {skip_batches} batches")

    # Open output files: append if resuming, write if starting fresh
    mode = "a" if args.resume and skip_batches > 0 else "w"
    unlinked_fh = open(unlinked_out, mode, encoding="utf-8") if unlinked else None
    linked_fh = open(linked_out, mode, encoding="utf-8") if linked else None
    index_fh = open(index_out, mode, encoding="utf-8")

    # Tag
    total_usage = {"input_tokens": 0, "output_tokens": 0}
    tagged_count = 0
    failed_count = 0

    def _flush_batch(batch_items: list[tuple[str, int, dict, str]]) -> None:
        """Write tagged records from the batch to output files immediately."""
        for source, idx, rec, text in batch_items:
            if source == "unlinked" and unlinked_fh:
                unlinked_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                unlinked_fh.flush()
            elif source == "linked" and linked_fh:
                linked_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                linked_fh.flush()
            # Always write the index row
            row = _build_index_row(rec, source)
            if row["url"]:
                index_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                index_fh.flush()

    try:
        if args.live:
            print(f"\n[Tagging — LIVE MODE]")
            from src._clients.llm_gcp_client import GeminiLLMClient

            client = GeminiLLMClient(model_name=MODEL_NAME)
            loop = asyncio.new_event_loop()

            for batch_num, batch in enumerate(batches, 1):
                # Skip already-completed batches when resuming
                if batch_num <= skip_batches:
                    continue

                # Build batch input with simple numeric IDs (1-based within batch)
                batch_input = []
                for pos, (source, idx, rec, text) in enumerate(batch, 1):
                    batch_input.append({"id": str(pos), "text": text})

                t0 = time.time()
                tags_map, usage = loop.run_until_complete(
                    tag_batch_live(client, batch_input, batch_num, len(batches))
                )
                dt = time.time() - t0

                total_usage["input_tokens"] += usage.get("input_tokens", 0)
                total_usage["output_tokens"] += usage.get("output_tokens", 0)

                # Apply tags back to records using positional IDs
                batch_tagged = 0
                for pos, (source, idx, rec, text) in enumerate(batch, 1):
                    art_id = str(pos)
                    if art_id in tags_map:
                        rec["tags"] = tags_map[art_id]
                        batch_tagged += 1
                    else:
                        rec["tags"] = _empty_tags()
                        failed_count += 1

                tagged_count += batch_tagged

                # Write this batch immediately
                _flush_batch(batch)

                in_tok = usage.get("input_tokens", 0)
                out_tok = usage.get("output_tokens", 0)
                print(
                    f"  Batch {batch_num:3}/{len(batches)} — "
                    f"{batch_tagged}/{len(batch)} tagged, "
                    f"{in_tok:,} in / {out_tok:,} out tokens, "
                    f"{dt:.1f}s"
                )

                # Rate limiting
                if batch_num < len(batches):
                    time.sleep(0.5)

            loop.close()
        else:
            print(f"\n[Tagging — DRY RUN MODE]")
            print(f"  Generating synthetic tags (use --live to call Gemini)")
            for source, idx, rec, text in all_items:
                rec["tags"] = _empty_tags()
                text_lower = text.lower()

                # --- topic ---
                if any(kw in text_lower for kw in ["ai", "artificial intelligence", "machine learning", "algorithm", "deepfake"]):
                    rec["tags"]["topic"].append("artificial_intelligence")
                if any(kw in text_lower for kw in ["border", "immigration", "migrant", "asylum", "deporta", "ice "]):
                    rec["tags"]["topic"].append("national_security_domestic")
                if any(kw in text_lower for kw in ["military", "defense", "nato", "missile", "strike", "troops", "pentagon"]):
                    rec["tags"]["topic"].append("national_security_foreign")
                if any(kw in text_lower for kw in ["oil", "gas", "energy", "mineral", "drill", "pipeline", "lng"]):
                    rec["tags"]["topic"].append("energy_and_minerals")
                if any(kw in text_lower for kw in ["procurement", "contract", "doge", "spending", "budget", "gsa"]):
                    rec["tags"]["topic"].append("federal_procurement")
                if any(kw in text_lower for kw in ["media", "culture", "dei", "woke", "social media", "censorship"]):
                    rec["tags"]["topic"].append("culture_and_media")

                # --- media (content-based labels) ---
                _media_rules = [
                    (["executive order", " eo ", "white house", "oval office", "presidential"], "executive-action"),
                    (["congress", "senate", "house of representatives", "legislation", "bill", "lawmakers"], "legislation"),
                    (["court", "judge", "ruling", "lawsuit", "legal", "injunction", "supreme court"], "legal-judicial"),
                    (["military", "troops", "deployment", "strike", "missile", "navy", "army", "air force", "pentagon", "dod"], "defense-military"),
                    (["trade", "tariff", "sanctions", "export", "import", "commerce"], "trade-economics"),
                    (["election", "vote", "ballot", "campaign", "polling", "primary"], "elections"),
                    (["border", "immigration", "migrant", "deporta", "asylum", "ice "], "immigration"),
                    (["climate", "environment", "epa", "pollution", "carbon", "emission"], "environment-climate"),
                    (["health", "vaccine", "pandemic", "cdc", "fda", "hospital"], "public-health"),
                    (["tech", "cybersecurity", "hack", "data breach", "surveillance", "ai ", "artificial intelligence"], "technology"),
                    (["education", "school", "university", "student", "campus"], "education"),
                    (["economy", "inflation", "gdp", "jobs", "unemployment", "recession", "fed ", "federal reserve"], "economy"),
                    (["russia", "ukraine", "china", "iran", "north korea", "nato", "ally", "allies", "diplomacy", "ambassador"], "foreign-affairs"),
                    (["oil", "gas", "energy", "pipeline", "lng", "solar", "nuclear", "drill"], "energy"),
                    (["gun", "firearm", "shooting", "second amendment", "nra"], "firearms"),
                    (["israel", "gaza", "hamas", "hezbollah", "middle east"], "middle-east"),
                    (["yemen", "houthi"], "yemen-conflict"),
                    (["dei", "diversity", "equity", "inclusion", "woke", "critical race"], "dei-policy"),
                    (["social media", "platform", "facebook", "twitter", " x ", "tiktok", "free speech", "censorship"], "social-media"),
                    (["budget", "deficit", "debt ceiling", "appropriation", "spending", "fiscal"], "fiscal-policy"),
                ]
                for keywords, label in _media_rules:
                    if any(kw in text_lower for kw in keywords):
                        if label not in rec["tags"]["media"]:
                            rec["tags"]["media"].append(label)

                # --- operation ---
                if any(kw in text_lower for kw in ["yemen", "houthi", "rough rider"]):
                    rec["tags"]["operation"].append("Rough Rider")
                if any(kw in text_lower for kw in ["southern border", "border wall", "border patrol", "cbp"]):
                    rec["tags"]["operation"].append("Southern Border")
                if any(kw in text_lower for kw in ["midnight hammer", "b-2", "b2 ", "iran nuclear"]):
                    rec["tags"]["operation"].append("Midnight Hammer")
                if any(kw in text_lower for kw in ["epic fury", "iran attack", "iran retalia"]):
                    rec["tags"]["operation"].append("Epic Fury")
                if any(kw in text_lower for kw in ["illegal immigration", "mass deportation", "ice raid"]):
                    rec["tags"]["operation"].append("Illegal Immigration")

                # --- person (common figures) ---
                _person_rules = [
                    (["trump"], "Donald Trump"),
                    (["biden"], "Joe Biden"),
                    (["hegseth"], "Pete Hegseth"),
                    (["rubio"], "Marco Rubio"),
                    (["vance"], "JD Vance"),
                    (["musk"], "Elon Musk"),
                    (["desantis"], "Ron DeSantis"),
                    (["waltz"], "Mike Waltz"),
                    (["ratcliffe"], "John Ratcliffe"),
                    (["austin"], "Lloyd Austin"),
                    (["sullivan"], "Jake Sullivan"),
                    (["zelensk"], "Volodymyr Zelenskyy"),
                    (["putin"], "Vladimir Putin"),
                    (["netanyahu"], "Benjamin Netanyahu"),
                    (["xi jinping", "xi "], "Xi Jinping"),
                    (["kim jong"], "Kim Jong Un"),
                    (["khamenei"], "Ali Khamenei"),
                    (["noem"], "Kristi Noem"),
                    (["homan"], "Tom Homan"),
                ]
                for keywords, name in _person_rules:
                    if any(kw in text_lower for kw in keywords):
                        if name not in rec["tags"]["person"]:
                            rec["tags"]["person"].append(name)

                # --- country ---
                _country_rules = [
                    (["united states", "u.s.", "us ", "america"], "United States"),
                    (["china", "beijing", "chinese"], "China"),
                    (["russia", "moscow", "russian", "kremlin"], "Russia"),
                    (["ukraine", "kyiv", "ukrainian"], "Ukraine"),
                    (["iran", "tehran", "iranian"], "Iran"),
                    (["israel", "israeli", "jerusalem", "tel aviv"], "Israel"),
                    (["north korea", "pyongyang", "dprk"], "North Korea"),
                    (["yemen", "houthi", "sanaa"], "Yemen"),
                    (["taiwan", "taipei", "taiwanese"], "Taiwan"),
                    (["mexico", "mexican"], "Mexico"),
                    (["canada", "canadian", "ottawa"], "Canada"),
                    (["syria", "syrian", "damascus"], "Syria"),
                    (["afghanistan", "afghan", "kabul"], "Afghanistan"),
                    (["iraq", "iraqi", "baghdad"], "Iraq"),
                    (["japan", "japanese", "tokyo"], "Japan"),
                    (["south korea", "seoul", "korean"], "South Korea"),
                    (["india", "indian", "delhi", "modi"], "India"),
                    (["turkey", "turkish", "ankara", "erdogan"], "Turkey"),
                    (["saudi", "riyadh"], "Saudi Arabia"),
                    (["gaza", "palestinian", "west bank"], "Palestine"),
                    (["philippines", "manila", "filipino"], "Philippines"),
                    (["venezuela", "maduro", "caracas"], "Venezuela"),
                    (["cuba", "cuban", "havana"], "Cuba"),
                ]
                for keywords, country in _country_rules:
                    if any(kw in text_lower for kw in keywords):
                        if country not in rec["tags"]["country"]:
                            rec["tags"]["country"].append(country)

                tagged_count += 1

            # Write all at once in dry-run (fast, no risk)
            _flush_batch(all_items)

    finally:
        # Always close files even on Ctrl+C
        if unlinked_fh:
            unlinked_fh.close()
        if linked_fh:
            linked_fh.close()
        index_fh.close()

    # Report output
    print(f"\n[Output]")
    for p in [unlinked_out, linked_out, index_out]:
        pp = Path(p)
        if pp.exists():
            lines = sum(1 for _ in open(p))
            print(f"  ✓ {p} ({lines} records)")

    # Summary
    in_rate, out_rate = 0.30, 2.50
    total_cost = (
        total_usage["input_tokens"] / 1_000_000 * in_rate
        + total_usage["output_tokens"] / 1_000_000 * out_rate
    )

    print(f"\n[Summary]")
    print(f"  Articles tagged: {tagged_count}")
    print(f"  Failed/empty:    {failed_count}")
    print(f"  Input tokens:    {total_usage['input_tokens']:,}")
    print(f"  Output tokens:   {total_usage['output_tokens']:,}")
    print(f"  Total cost:      ${total_cost:.4f}")
    print(f"  Model:           {MODEL_NAME}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Tag unlinked and linked media articles with topic/operation/person/country."
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Call Gemini for real tagging (default: dry run with keyword heuristics)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=20,
        help="Articles per LLM batch call (default: 20)"
    )
    parser.add_argument(
        "--source", choices=["all", "unlinked", "linked"], default="all",
        help="Which articles to tag (default: all)"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from where a previous run left off (append to existing output)"
    )
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
