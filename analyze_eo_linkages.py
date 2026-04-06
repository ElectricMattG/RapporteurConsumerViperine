#!/usr/bin/env python3
"""
Compare article-to-EO linkages - verify which EOs are most referenced.
Provides insights into media coverage patterns.
"""

import json
from collections import Counter, defaultdict
from pathlib import Path

CACHE_PATH = Path("/mnt/workspace_2/bboa/workflows/media_linking/media_linkage_cache.jsonl")

def analyze_linkages():
    """Detailed analysis of article-to-EO linkage patterns."""

    if not CACHE_PATH.exists():
        print(f"❌ Cache file not found: {CACHE_PATH}")
        return

    # Data structures for analysis
    eo_to_articles = defaultdict(list)  # EO → list of article URLs
    article_to_eos = {}  # article URL → list of EO numbers
    eo_to_domains = defaultdict(set)  # EO → set of domains
    domain_to_eos = defaultdict(set)  # domain → set of EOs

    eo_names = {}  # EO number → name
    total_articles = 0
    total_linkages = 0

    # Parse cache
    print("Loading cache...")
    with open(CACHE_PATH) as f:
        for line in f:
            record = json.loads(line)
            article_url = record.get('article_url')
            linked_eos = record.get('linked_eos', [])

            if not article_url:
                continue

            total_articles += 1
            domain = article_url.split('/')[2] if len(article_url.split('/')) > 2 else 'unknown'

            eo_nums = []
            for eo_link in linked_eos:
                eo_num = eo_link.get('eo_number')
                eo_name = eo_link.get('eo_name', 'N/A')

                if eo_num:
                    eo_nums.append(eo_num)
                    eo_to_articles[eo_num].append(article_url)
                    eo_to_domains[eo_num].add(domain)
                    domain_to_eos[domain].add(eo_num)
                    eo_names[eo_num] = eo_name
                    total_linkages += 1

            if eo_nums:
                article_to_eos[article_url] = eo_nums

    # ========================================================================
    # SUMMARY STATS
    # ========================================================================
    print("\n" + "=" * 80)
    print("ARTICLE-TO-EO LINKAGE ANALYSIS")
    print("=" * 80)

    print(f"\n📊 Overall Statistics:")
    print(f"  Total articles with EO links: {total_articles}")
    print(f"  Total EO linkages: {total_linkages}")
    print(f"  Unique EOs referenced: {len(eo_to_articles)}")
    print(f"  Unique domains: {len(domain_to_eos)}")
    print(f"  Avg linkages per article: {total_linkages/total_articles:.2f}")

    # ========================================================================
    # TOP EOs BY ARTICLE COUNT
    # ========================================================================
    print(f"\n🏆 Top 15 Most-Referenced Executive Orders:")
    print(f"{'Rank':<6} {'EO':<10} {'Articles':<10} {'Domains':<10} {'Name'}")
    print("-" * 80)

    top_eos = sorted(eo_to_articles.items(), key=lambda x: len(x[1]), reverse=True)[:15]
    for rank, (eo_num, articles) in enumerate(top_eos, 1):
        article_count = len(articles)
        domain_count = len(eo_to_domains[eo_num])
        name = eo_names.get(eo_num, 'N/A')[:40]
        print(f"{rank:<6} {eo_num:<10} {article_count:<10} {domain_count:<10} {name}")

    # ========================================================================
    # TOP DOMAINS BY EO COVERAGE
    # ========================================================================
    print(f"\n🌐 Top 15 Domains by EO Coverage:")
    print(f"{'Rank':<6} {'Domain':<40} {'EOs':<10} {'Articles'}")
    print("-" * 80)

    # Count articles per domain
    domain_article_count = Counter()
    for article_url, eos in article_to_eos.items():
        domain = article_url.split('/')[2] if len(article_url.split('/')) > 2 else 'unknown'
        domain_article_count[domain] += 1

    top_domains = sorted(domain_to_eos.items(), key=lambda x: len(x[1]), reverse=True)[:15]
    for rank, (domain, eos) in enumerate(top_domains, 1):
        eo_count = len(eos)
        article_count = domain_article_count[domain]
        print(f"{rank:<6} {domain:<40} {eo_count:<10} {article_count}")

    # ========================================================================
    # ARTICLES WITH MOST EO LINKAGES
    # ========================================================================
    print(f"\n📰 Top 15 Articles by Number of EO Linkages:")
    print(f"{'Rank':<6} {'EOs':<6} {'Domain':<40} {'URL Snippet'}")
    print("-" * 80)

    top_articles = sorted(article_to_eos.items(), key=lambda x: len(x[1]), reverse=True)[:15]
    for rank, (url, eos) in enumerate(top_articles, 1):
        eo_count = len(eos)
        domain = url.split('/')[2] if len(url.split('/')) > 2 else 'unknown'
        url_snippet = url[len(url.split('//', 1)[0])+2:][:60]
        print(f"{rank:<6} {eo_count:<6} {domain:<40}")
        print(f"       → {url_snippet}")
        print(f"       EOs: {', '.join(sorted(eos))}")
        print()

    # ========================================================================
    # EO CO-OCCURRENCE ANALYSIS
    # ========================================================================
    print(f"\n🔗 Most Common EO Co-occurrences (same article):")

    eo_pairs = Counter()
    for article_url, eos in article_to_eos.items():
        if len(eos) >= 2:
            # Count all pairs
            sorted_eos = sorted(eos)
            for i in range(len(sorted_eos)):
                for j in range(i+1, len(sorted_eos)):
                    pair = (sorted_eos[i], sorted_eos[j])
                    eo_pairs[pair] += 1

    print(f"{'Rank':<6} {'Count':<8} {'EO Pair':<25} {'Names'}")
    print("-" * 80)

    top_pairs = eo_pairs.most_common(10)
    for rank, ((eo1, eo2), count) in enumerate(top_pairs, 1):
        name1 = eo_names.get(eo1, 'N/A')[:25]
        name2 = eo_names.get(eo2, 'N/A')[:25]
        print(f"{rank:<6} {count:<8} {eo1} & {eo2:<10} {name1}")
        print(f"{'':>15} {'':>9} {name2}")
        print()

    # ========================================================================
    # DISTRIBUTION ANALYSIS
    # ========================================================================
    print(f"\n📈 Linkage Distribution:")

    linkage_counts = Counter([len(eos) for eos in article_to_eos.values()])
    print(f"{'EOs per article':<20} {'Count':<10} {'Percent'}")
    print("-" * 50)

    for eo_count in sorted(linkage_counts.keys()):
        article_count = linkage_counts[eo_count]
        percent = 100 * article_count / total_articles
        bar = "█" * int(percent)
        print(f"{eo_count:<20} {article_count:<10} {percent:>5.1f}% {bar}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    analyze_linkages()
