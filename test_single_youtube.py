#!/usr/bin/env python3
"""Send a single YouTube URL to the pipeline"""

import json
from datetime import datetime

from confluent_kafka import Producer


def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}]')

def send_youtube_request(url: str):
    """Send a single YouTube URL to Kafka"""

    # Kafka configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'youtube-test-producer'
    }

    producer = Producer(conf)

    # Generate request ID and session ID
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    session_id = f"youtube-test-{timestamp}"
    request_id = f"req_{timestamp}_000"

    # Create message
    message = {
        "request_id": request_id,
        "session_id": session_id,
        "uri": url,
        "title": "youtube.com",
        "context_snippets": []
    }

    print("=" * 80)
    print("📤 Sending YouTube URL to Kafka")
    print("=" * 80)
    print(f"URL: {url}")
    print(f"Session ID: {session_id}")
    print(f"Request ID: {request_id}")
    print("=" * 80)

    # Send to Kafka
    producer.produce(
        topic='web-scrape-requests',
        value=json.dumps(message).encode('utf-8'),
        callback=delivery_report
    )

    # Wait for delivery
    producer.flush()

    print("=" * 80)
    print("✅ Request sent successfully!")
    print(f"💡 Monitor with: tail -f logs/web_scraper.log")
    print("=" * 80)

if __name__ == '__main__':
    url = "https://www.youtube.com/watch?v=hvZg9Gxu75Q"
    send_youtube_request(url)
