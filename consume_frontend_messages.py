#!/usr/bin/env python3
"""
Consume and display messages from the frontend-message Kafka topic.
"""

import json
import sys

from confluent_kafka import Consumer, KafkaError


def consume_frontend_messages(timeout=10):
    """Consume messages from frontend-message topic and print them."""

    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'frontend-message-reader',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['frontend-message'])

    print("=" * 80)
    print("📨 Frontend Messages Consumer")
    print("=" * 80)
    print(f"Consuming from topic: frontend-message")
    print(f"Timeout: {timeout}s")
    print("-" * 80)

    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                if message_count == 0:
                    print("⏳ No messages available (timeout reached)")
                else:
                    print(f"\n⏳ No more messages (timeout reached)")
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    continue

            message_count += 1

            # Parse and display the message
            try:
                message_data = json.loads(msg.value().decode('utf-8'))

                print(f"\n📬 Message #{message_count}")
                print(f"   Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                print(f"   Type: {message_data.get('type', 'unknown')}")
                print(f"   Session ID: {message_data.get('session_id', 'unknown')}")
                print(f"   New Summaries: {message_data.get('new_summaries_count', 0)}")
                print(f"   Total Summaries: {message_data.get('total_summaries_count', 0)}")
                print(f"   Timestamp: {message_data.get('timestamp', 'unknown')}")

                # Display new summaries details
                new_summaries = message_data.get('new_summaries', [])
                if new_summaries:
                    print(f"\n   📄 New Summaries Added:")
                    for i, summary in enumerate(new_summaries, 1):
                        print(f"      {i}. {summary.get('title', 'Untitled')}")
                        print(f"         URL: {summary.get('url', 'N/A')}")

                print("-" * 80)

            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON in message: {e}")
                print(f"   Raw: {msg.value()}")

    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")
    finally:
        consumer.close()
        print(f"\n✅ Consumed {message_count} message(s)")
        print("=" * 80)

if __name__ == "__main__":
    timeout = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    consume_frontend_messages(timeout=timeout)
