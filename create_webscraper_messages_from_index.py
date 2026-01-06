#!/usr/bin/env python3
"""
Create Web Scraper Messages from Summary Index

Reads a JSONL summary index file and creates web-scraper messages
for testing the pipeline in isolation.

Usage:
    # Print messages to stdout
    python create_webscraper_messages_from_index.py research_summaries_with_embeddings_combined.jsonl

    # Save to file
    python create_webscraper_messages_from_index.py research_summaries_with_embeddings_combined.jsonl --output messages.jsonl

    # Send to Kafka
    python create_webscraper_messages_from_index.py research_summaries_with_embeddings_combined.jsonl --send-kafka

    # Limit number of URLs
    python create_webscraper_messages_from_index.py research_summaries_with_embeddings_combined.jsonl --limit 10
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlparse


def load_summaries(index_path: str) -> List[Dict]:
    """Load summaries from JSONL index file."""
    summaries = []
    with open(index_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                summary = json.loads(line)
                summaries.append(summary)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON at line {line_num}: {e}", file=sys.stderr)
    return summaries


def extract_unique_urls(summaries: List[Dict]) -> List[str]:
    """Extract unique source URLs from summaries."""
    urls = set()
    for summary in summaries:
        source_url = summary.get('source_url')
        if source_url:
            urls.add(source_url)
    return sorted(list(urls))


def create_webscraper_message(url: str, idx: int, total: int, session_id: str) -> Dict:
    """Create a web-scraper message for a given URL."""
    # Parse URL to get domain for title
    parsed = urlparse(url)
    domain = parsed.netloc or "unknown"

    message = {
        "uri": url,
        "title": f"Document from {domain}",
        "context": [f"Re-scraping URL {idx} of {total} from summary index"],
        "session_id": session_id,
        "request_id": f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{idx:03d}",
        "total_requests": total,
        "source": "summary_index_rescrape"
    }

    return message


def print_messages(messages: List[Dict]):
    """Print messages to stdout in JSONL format."""
    for message in messages:
        print(json.dumps(message, ensure_ascii=False))


def save_messages(messages: List[Dict], output_path: str):
    """Save messages to a JSONL file."""
    with open(output_path, 'w', encoding='utf-8') as f:
        for message in messages:
            f.write(json.dumps(message, ensure_ascii=False) + '\n')
    print(f"✅ Saved {len(messages)} messages to {output_path}", file=sys.stderr)


def send_to_kafka(messages: List[Dict], bootstrap_servers: str, topic: str):
    """Send messages to Kafka topic."""
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("Error: confluent-kafka not installed. Run: pip install confluent-kafka", file=sys.stderr)
        sys.exit(1)

    # Configure producer
    config = {
        'bootstrap.servers': bootstrap_servers
    }

    producer = Producer(config)

    print("=" * 80, file=sys.stderr)
    print(f"📤 Sending {len(messages)} messages to Kafka", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"Topic: {topic}", file=sys.stderr)
    print(f"Bootstrap servers: {bootstrap_servers}", file=sys.stderr)
    print("=" * 80, file=sys.stderr)

    sent_count = 0
    failed_count = 0

    for idx, message in enumerate(messages, 1):
        try:
            # Serialize and send
            message_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')
            producer.produce(
                topic,
                value=message_bytes,
                callback=lambda err, msg: delivery_report(err, msg, idx)
            )
            sent_count += 1

            # Flush every 10 messages
            if idx % 10 == 0:
                producer.flush()

        except Exception as e:
            print(f"❌ Failed to send message {idx}: {e}", file=sys.stderr)
            failed_count += 1

    # Final flush
    producer.flush()

    print("=" * 80, file=sys.stderr)
    print(f"✅ Sent: {sent_count}", file=sys.stderr)
    print(f"❌ Failed: {failed_count}", file=sys.stderr)
    print("=" * 80, file=sys.stderr)


def delivery_report(err, msg, idx):
    """Kafka delivery callback."""
    if err is not None:
        print(f"❌ Message {idx} delivery failed: {err}", file=sys.stderr)
    else:
        print(f"✅ Message {idx} delivered to {msg.topic()} [{msg.partition()}]", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Create web-scraper messages from summary index file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Print messages to stdout
  %(prog)s research_summaries_with_embeddings_combined.jsonl

  # Save to file
  %(prog)s research_summaries_with_embeddings_combined.jsonl --output messages.jsonl

  # Send to Kafka (local)
  %(prog)s research_summaries_with_embeddings_combined.jsonl --send-kafka

  # Send to Kafka (external)
  %(prog)s research_summaries_with_embeddings_combined.jsonl --send-kafka --bootstrap-servers kafka.rappoteur.ai:9094

  # Limit to first 10 URLs
  %(prog)s research_summaries_with_embeddings_combined.jsonl --limit 10
        """
    )

    parser.add_argument(
        'index_file',
        type=str,
        help='Path to JSONL summary index file'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output file path (JSONL format). If not specified, prints to stdout'
    )

    parser.add_argument(
        '--send-kafka',
        action='store_true',
        help='Send messages to Kafka instead of printing/saving'
    )

    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )

    parser.add_argument(
        '--topic',
        type=str,
        default='web-scrape-requests',
        help='Kafka topic name (default: web-scrape-requests)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of URLs to process'
    )

    parser.add_argument(
        '--session-id',
        type=str,
        help='Custom session ID (default: auto-generated)'
    )

    args = parser.parse_args()

    # Validate input file
    if not Path(args.index_file).exists():
        print(f"Error: File not found: {args.index_file}", file=sys.stderr)
        sys.exit(1)

    # Generate session ID
    session_id = args.session_id or f"rescrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    print(f"📚 Loading summaries from: {args.index_file}", file=sys.stderr)
    summaries = load_summaries(args.index_file)
    print(f"   Loaded {len(summaries)} summaries", file=sys.stderr)

    print(f"🔗 Extracting unique URLs...", file=sys.stderr)
    urls = extract_unique_urls(summaries)
    print(f"   Found {len(urls)} unique URLs", file=sys.stderr)

    # Apply limit if specified
    if args.limit and args.limit < len(urls):
        urls = urls[:args.limit]
        print(f"   Limited to {len(urls)} URLs", file=sys.stderr)

    # Create messages
    print(f"📝 Creating web-scraper messages...", file=sys.stderr)
    print(f"   Session ID: {session_id}", file=sys.stderr)
    messages = []
    for idx, url in enumerate(urls, 1):
        message = create_webscraper_message(url, idx, len(urls), session_id)
        messages.append(message)

    print(f"   Created {len(messages)} messages", file=sys.stderr)
    print("=" * 80, file=sys.stderr)

    # Output messages
    if args.send_kafka:
        send_to_kafka(messages, args.bootstrap_servers, args.topic)
    elif args.output:
        save_messages(messages, args.output)
    else:
        print_messages(messages)


if __name__ == '__main__':
    main()
