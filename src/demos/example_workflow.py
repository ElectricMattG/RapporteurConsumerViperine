#!/usr/bin/env python3
"""
Example: Testing the Kafka Web Scraper locally

This script demonstrates how to test the complete workflow without Kafka.
It simulates the message processing flow.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from components.document_websearcher import DocumentWebSearcher
from components.url_resolver import URLResolver


async def test_workflow():
    """Test the complete workflow without Kafka"""

    print("=" * 80)
    print("🧪 Testing Kafka Web Scraper Workflow (No Kafka Required)")
    print("=" * 80)

    # Sample message (as would come from Kafka)
    sample_message = {
        "uri": "https://www.python.org",
        "title": "Python Official Website",
        "context": [
            "Testing the web scraper workflow",
            "This is a sample context line"
        ],
        "session_id": "test-session-example"
    }

    print("\n📩 Sample Message:")
    print(json.dumps(sample_message, indent=2))

    # Initialize components
    print("\n⏳ Initializing components...")
    url_resolver = URLResolver()
    web_searcher = DocumentWebSearcher()
    print("✅ Components initialized")

    # Step 1: Resolve URL
    print("\n" + "=" * 80)
    print("📍 Step 1: Resolving URL")
    print("=" * 80)

    uri = sample_message['uri']
    resolution_result = await url_resolver.resolve_url(uri)

    print(f"\n📊 Resolution Results:")
    print(f"  Original URL: {resolution_result['original_url']}")
    print(f"  Final URL: {resolution_result['final_url']}")
    print(f"  Redirects: {resolution_result['redirect_count']}")
    print(f"  Success: {resolution_result['success']}")
    print(f"  Time: {resolution_result['resolution_time']:.2f}s")

    if not resolution_result['success']:
        print(f"\n❌ URL resolution failed: {resolution_result['error']}")
        return

    final_url = resolution_result['final_url']

    # Step 2: Extract links
    print("\n" + "=" * 80)
    print("📍 Step 2: Extracting Links")
    print("=" * 80)

    link_result = await url_resolver.extract_links_from_url(final_url, max_links=5)

    print(f"\n📊 Link Extraction Results:")
    print(f"  Total links found: {link_result['link_count']}")
    print(f"  Success: {link_result['success']}")

    if link_result['success'] and link_result['links']:
        print(f"\n  First 5 links:")
        for i, link in enumerate(link_result['links'][:5], 1):
            print(f"    {i}. {link[:80]}...")

    # Step 3: Scrape content (just the main URL for demo)
    print("\n" + "=" * 80)
    print("📍 Step 3: Scraping Content")
    print("=" * 80)

    print(f"\n🌐 Fetching HTML from: {final_url}")

    try:
        response = web_searcher.session.get(final_url, timeout=15)

        if response.status_code == 200:
            html_content = response.content.decode('utf-8', errors='ignore')
            print(f"✅ Fetched {len(html_content)} characters of HTML")

            # Step 4: Convert to markdown
            print("\n" + "=" * 80)
            print("📍 Step 4: Converting to Markdown")
            print("=" * 80)

            # Check if docling is available
            try:
                from docling.document_converter import DocumentConverter

                print("\n🔄 Converting HTML to markdown...")
                extraction_record = web_searcher._convert_html_to_extraction_record(
                    url=final_url,
                    html_content=html_content,
                    source_query=sample_message['title'],
                    storage_dir='./cache/web_scrapes',
                    extraction_id=1
                )

                if extraction_record:
                    print(f"✅ Conversion successful!")
                    print(f"\n📊 Extraction Record:")
                    print(f"  URL: {extraction_record['source_url']}")
                    print(f"  Method: {extraction_record['method']}")
                    print(f"  Text length: {extraction_record['text_length']} chars")
                    print(f"  Title: {extraction_record['metadata']['title']}")
                    print(f"  Links extracted: {extraction_record['metadata']['link_count']}")
                    print(f"  Images extracted: {extraction_record['metadata']['image_count']}")

                    # Show markdown preview
                    print(f"\n📝 Markdown Preview (first 500 chars):")
                    print("-" * 80)
                    print(extraction_record['text'][:500])
                    if len(extraction_record['text']) > 500:
                        print("...")
                    print("-" * 80)

                    # Step 5: Save to cache
                    print("\n" + "=" * 80)
                    print("📍 Step 5: Saving to Cache")
                    print("=" * 80)

                    cache_dir = Path(f"./cache/web_scrapes/{sample_message['session_id']}")
                    cache_dir.mkdir(parents=True, exist_ok=True)

                    from datetime import datetime
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    cache_file = cache_dir / f"scraped_content_{timestamp}.jsonl"

                    # Add session metadata
                    extraction_record['session_id'] = sample_message['session_id']
                    extraction_record['source_title'] = sample_message['title']
                    extraction_record['source_context'] = sample_message['context']

                    with open(cache_file, 'w', encoding='utf-8') as f:
                        f.write(json.dumps(extraction_record, ensure_ascii=False) + '\n')

                    print(f"✅ Saved to: {cache_file}")
                    print(f"   File size: {cache_file.stat().st_size} bytes")

                else:
                    print("❌ Conversion to markdown failed")

            except ImportError:
                print("⚠️  Docling not installed - skipping markdown conversion")
                print("   To enable: pip install docling")
        else:
            print(f"❌ HTTP {response.status_code}")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Summary
    print("\n" + "=" * 80)
    print("✅ Workflow Test Complete!")
    print("=" * 80)
    print("\nThis demonstrates the complete flow that happens when a Kafka message is received.")
    print("To run with actual Kafka:")
    print("  1. Start Kafka: docker-compose up -d kafka")
    print("  2. Start consumer: python run_kafka_consumer.py")
    print("  3. Send message: python test_producer.py --url 'https://example.com'")


if __name__ == "__main__":
    asyncio.run(test_workflow())
