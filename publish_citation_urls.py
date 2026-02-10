#!/usr/bin/env python3
"""
Publish citation URLs to web-scraper Kafka topic in batches.

Reads URLs from extracted_citation_urls.txt and publishes them to the
web-scrape-requests Kafka topic for processing by the web scraper consumer.
"""

import json
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

from confluent_kafka import Producer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = 'web-scrape-requests'
BATCH_SIZE = 10
BATCH_DELAY_SECONDS = 45  # Delay between batches


def create_producer():
    """Create Kafka producer."""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'citation-url-publisher',
        'acks': 'all',
        'retries': 3,
        'max.in.flight.requests.per.connection': 1,
    }
    return Producer(config)


def delivery_callback(err, msg):
    """Kafka delivery callback."""
    if err:
        print(f'❌ Delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')


def publish_url_batch(producer, urls, batch_num, base_session_id, total_urls):
    """
    Publish a batch of URLs to Kafka.

    Args:
        producer: Kafka producer instance
        urls: List of URLs in this batch
        batch_num: Batch number (1-indexed)
        base_session_id: Base session identifier (will append batch number)
        total_urls: Total number of URLs across all batches
    """
    # Each batch gets its own session_id for independent aggregation
    session_id = f"{base_session_id}_{batch_num:03d}"
    batch_start = (batch_num - 1) * BATCH_SIZE + 1

    print(f"\n📦 Publishing batch {batch_num} ({len(urls)} URLs)...")
    print(f"   Session ID: {session_id}")

    for idx, url in enumerate(urls, start=batch_start):
        # Parse URL for domain
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or "unknown"
            path_parts = parsed.path.strip('/').split('/')
            page_title = path_parts[-1].replace('-', ' ').title() if path_parts else "Document"
        except:
            domain = "unknown"
            page_title = "Document"

        # Create message
        request_id = f"{session_id}_{idx:04d}"
        message = {
            "uri": url,
            "title": f"{page_title} - {domain}",
            "context": [
                f"Citation URL {idx} of {total_urls}",
                f"Source: IDF Press Releases Archive",
                f"Batch: {batch_num}, Position: {idx - batch_start + 1}/{len(urls)}"
            ],
            "session_id": session_id,
            "request_id": request_id,
            "total_requests": len(urls),  # Total for THIS batch
            "source": "citation_archive_extraction",
            "query_id": session_id  # Use session_id as query_id (already unique per batch)
        }

        # Publish to Kafka
        try:
            producer.produce(
                KAFKA_TOPIC,
                key=request_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=delivery_callback
            )
            print(f"  [{idx}/{total_urls}] Queued: {url[:80]}...")
        except Exception as e:
            print(f"  ❌ Error publishing URL {idx}: {e}")

    # Wait for messages to be delivered
    producer.flush()
    print(f"✅ Batch {batch_num} published and flushed")


def main():
    """Main execution."""
    # Read URLs from file
    url_file = './extracted_citation_urls.txt'

    print(f"📖 Reading URLs from: {url_file}")

    try:
        with open(url_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Error: File not found: {url_file}")
        sys.exit(1)

    total_urls = len(urls)
    total_batches = (total_urls + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division

    print(f"📊 Total URLs: {total_urls}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print(f"📦 Total batches: {total_batches}")

    # Generate base session ID (batch number will be appended)
    base_session_id = f"citation_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"🔑 Base Session ID: {base_session_id} (each batch gets unique suffix)")

    # Create Kafka producer
    print(f"\n🔌 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    try:
        producer = create_producer()
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Process URLs in batches
    try:
        for batch_num in range(1, total_batches + 1):
            start_idx = (batch_num - 1) * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_urls)
            batch_urls = urls[start_idx:end_idx]

            publish_url_batch(producer, batch_urls, batch_num, base_session_id, total_urls)

            # Delay between batches (except for last batch)
            if batch_num < total_batches:
                print(f"⏳ Waiting {BATCH_DELAY_SECONDS}s before next batch...")
                time.sleep(BATCH_DELAY_SECONDS)

        print(f"\n✅ Successfully published all {total_urls} URLs in {total_batches} batches")
        print(f"📝 Base Session ID: {base_session_id}")
        print(f"📝 Session IDs: {base_session_id}_001 through {base_session_id}_{total_batches:03d}")
        print(f"\n💡 Monitor web-scraper consumer logs to track processing:")
        print(f"   docker logs web-scraper-consumer --tail=50 -f")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during batch processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        producer.flush()
        print("\n🔌 Kafka producer closed")


if __name__ == '__main__':
    main()
