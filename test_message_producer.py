#!/usr/bin/env python3
"""
Test Message Producer for Web Scraper Consumer

Sends a test message to the Kafka web-scrape-requests topic
based on grounding metadata from Gemini.
"""

import json
import os
from datetime import datetime

from confluent_kafka import Producer


def create_test_message():
    """Create a test message based on grounding metadata"""

    # Generate request ID
    request_id = f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    message = {
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFMRaKoEULQOFpDr6r2oy1P6wg6uDxoIAWBxhU73lDOPWTttNa6or9QbsyuQIm_zWVUouQADbe7vnULdtGDYweZvEdo_0Be2quhsVZJzhf2WV05rHFIEh3E82KKC1xD1v6bjmYVHeXbKBAMQFNsG_Y1zbjdMtZs3G5pItTno0U_UlDkkJ62MgzWUYo=",
        "title": "ohchr.org",
        "context": [
            "Corporate human rights obligations are fundamentally guided by the United Nations Guiding Principles on Business and Human Rights (UNGPs), which establish a global standard of expected conduct for all business enterprises."
        ],
        "session_id": "test-session-grounding-001",
        "request_id": request_id
    }

    return message


def send_message(bootstrap_servers='localhost:9092', topic='web-scrape-requests'):
    """Send test message to Kafka"""

    # Configure producer
    config = {
        'bootstrap.servers': bootstrap_servers
    }

    producer = Producer(config)

    # Create message
    message = create_test_message()

    print("=" * 80)
    print("📤 Sending test message to Kafka")
    print("=" * 80)
    print(f"Topic: {topic}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print("\nMessage:")
    print(json.dumps(message, indent=2))
    print("=" * 80)

    # Serialize and send
    message_bytes = json.dumps(message).encode('utf-8')

    try:
        producer.produce(topic, message_bytes)
        producer.flush()
        print("\n✅ Message sent successfully!")
        print(f"   Request ID: {message['request_id']}")
        print(f"   Session ID: {message['session_id']}")
        print(f"   URI: {message['uri']}")
        print(f"   Title: {message['title']}")
        print(f"   Context lines: {len(message['context'])}")

    except Exception as e:
        print(f"\n❌ Error sending message: {e}")
        raise

    return message


if __name__ == "__main__":
    # Get Kafka config from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'web-scrape-requests')

    # Send message
    send_message(bootstrap_servers, topic)
