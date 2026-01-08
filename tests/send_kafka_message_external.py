#!/usr/bin/env python3
"""
External Kafka Client Example

This script demonstrates how to connect to the cloud Kafka deployment
from an external location (e.g., local machine, remote server).

Usage:
    python send_kafka_message_external.py --topic web-scraping-requests --message '{"url": "https://example.com"}'

Requirements:
    pip install confluent-kafka
"""

import argparse
import json
import sys
from typing import Any, Dict

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Configuration for external access
KAFKA_BOOTSTRAP_SERVERS = "kafka.rappoteur.ai:9094"
# Note: Basic auth is handled at the Ingress level
# For Kafka protocol, we use PLAINTEXT (no additional Kafka-level auth)


def get_producer_config() -> Dict[str, Any]:
    """Get producer configuration for external access."""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'external-producer',
        'compression.type': 'snappy',
        'acks': 'all',
        'retries': 3,
        'max.in.flight.requests.per.connection': 5,
        'request.timeout.ms': 30000,
        'socket.timeout.ms': 60000,
    }


def get_consumer_config(group_id: str = 'external-consumer-group') -> Dict[str, Any]:
    """Get consumer configuration for external access."""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'client.id': 'external-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 30000,
        'socket.timeout.ms': 60000,
    }


def get_admin_config() -> Dict[str, Any]:
    """Get admin client configuration for external access."""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'socket.timeout.ms': 60000,
    }


def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f'❌ Message delivery failed: {err}', file=sys.stderr)
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')


def produce_message(topic: str, message: str, key: str = None):
    """
    Produce a message to Kafka.

    Args:
        topic: Kafka topic name
        message: Message payload (JSON string or plain text)
        key: Optional message key
    """
    config = get_producer_config()
    producer = Producer(config)

    try:
        print(f"📤 Producing message to topic '{topic}'...")

        # Try to parse as JSON for pretty printing
        try:
            msg_json = json.loads(message)
            print(f"   Message: {json.dumps(msg_json, indent=2)}")
            message_bytes = message.encode('utf-8')
        except json.JSONDecodeError:
            print(f"   Message: {message}")
            message_bytes = message.encode('utf-8')

        # Produce message
        producer.produce(
            topic,
            value=message_bytes,
            key=key.encode('utf-8') if key else None,
            callback=delivery_report
        )

        # Wait for delivery
        producer.flush(timeout=10)
        print("✅ Message sent successfully")

    except KafkaException as e:
        print(f"❌ Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.flush()


def consume_messages(topic: str, num_messages: int = 10, timeout: int = 30):
    """
    Consume messages from Kafka.

    Args:
        topic: Kafka topic name
        num_messages: Maximum number of messages to consume
        timeout: Timeout in seconds
    """
    config = get_consumer_config()
    consumer = Consumer(config)

    try:
        print(f"📥 Subscribing to topic '{topic}'...")
        consumer.subscribe([topic])

        messages_consumed = 0
        print(f"⏳ Waiting for messages (timeout: {timeout}s)...\n")

        while messages_consumed < num_messages:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"   Reached end of partition {msg.partition()}")
                    continue
                else:
                    print(f"❌ Consumer error: {msg.error()}", file=sys.stderr)
                    break

            # Successfully received message
            messages_consumed += 1
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8') if msg.value() else None

            print(f"📨 Message {messages_consumed}:")
            print(f"   Topic: {msg.topic()}")
            print(f"   Partition: {msg.partition()}")
            print(f"   Offset: {msg.offset()}")
            if key:
                print(f"   Key: {key}")

            # Try to parse as JSON
            try:
                value_json = json.loads(value)
                print(f"   Value: {json.dumps(value_json, indent=2)}")
            except json.JSONDecodeError:
                print(f"   Value: {value}")
            print()

        print(f"✅ Consumed {messages_consumed} messages")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()


def list_topics():
    """List all available topics."""
    config = get_admin_config()
    admin_client = AdminClient(config)

    try:
        print("📋 Fetching topics from Kafka cluster...\n")
        metadata = admin_client.list_topics(timeout=10)

        print(f"Cluster: {metadata.cluster_id}")
        print(f"Brokers: {len(metadata.brokers)}\n")
        print("Topics:")

        for topic in sorted(metadata.topics.keys()):
            if not topic.startswith('__'):  # Skip internal topics
                partitions = len(metadata.topics[topic].partitions)
                print(f"  • {topic} ({partitions} partitions)")

    except Exception as e:
        print(f"❌ Error listing topics: {e}", file=sys.stderr)
        sys.exit(1)


def create_topic(topic: str, partitions: int = 12, replication_factor: int = 1):
    """
    Create a new topic.

    Args:
        topic: Topic name
        partitions: Number of partitions
        replication_factor: Replication factor
    """
    config = get_admin_config()
    admin_client = AdminClient(config)

    try:
        print(f"🔨 Creating topic '{topic}'...")

        new_topic = NewTopic(
            topic,
            num_partitions=partitions,
            replication_factor=replication_factor
        )

        futures = admin_client.create_topics([new_topic])

        # Wait for operation to complete
        for topic_name, future in futures.items():
            try:
                future.result()
                print(f"✅ Topic '{topic_name}' created successfully")
            except Exception as e:
                print(f"❌ Failed to create topic '{topic_name}': {e}", file=sys.stderr)

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='External Kafka Client for kafka.rappoteur.ai',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Produce command
    produce_parser = subparsers.add_parser('produce', help='Produce a message')
    produce_parser.add_argument('--topic', required=True, help='Topic name')
    produce_parser.add_argument('--message', required=True, help='Message content (JSON or plain text)')
    produce_parser.add_argument('--key', help='Optional message key')

    # Consume command
    consume_parser = subparsers.add_parser('consume', help='Consume messages')
    consume_parser.add_argument('--topic', required=True, help='Topic name')
    consume_parser.add_argument('--num-messages', type=int, default=10, help='Number of messages to consume')
    consume_parser.add_argument('--timeout', type=int, default=30, help='Timeout in seconds')

    # List topics command
    subparsers.add_parser('list-topics', help='List all topics')

    # Create topic command
    create_parser = subparsers.add_parser('create-topic', help='Create a new topic')
    create_parser.add_argument('--topic', required=True, help='Topic name')
    create_parser.add_argument('--partitions', type=int, default=12, help='Number of partitions')
    create_parser.add_argument('--replication-factor', type=int, default=1, help='Replication factor')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    print(f"🌐 Connecting to Kafka cluster: {KAFKA_BOOTSTRAP_SERVERS}\n")

    if args.command == 'produce':
        produce_message(args.topic, args.message, args.key)
    elif args.command == 'consume':
        consume_messages(args.topic, args.num_messages, args.timeout)
    elif args.command == 'list-topics':
        list_topics()
    elif args.command == 'create-topic':
        create_topic(args.topic, args.partitions, args.replication_factor)


if __name__ == '__main__':
    main()
