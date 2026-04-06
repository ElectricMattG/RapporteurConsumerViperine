#!/usr/bin/env python3
"""
Send extraction-based threat cluster to Kaliya via Kafka.

Reads the threat_clusters.json from the parsed directory and publishes
a Kaliya analysis request to the kafka-requests topic.
"""

import json
import sys
import uuid
from datetime import datetime
from pathlib import Path

try:
    from confluent_kafka import Producer
except ImportError:
    print("❌ confluent_kafka not installed. Install with:")
    print("   pip install confluent-kafka")
    sys.exit(1)

# Configuration
PARSED_DIR = Path("/mnt/workspace_2/bboa/documents/atrum_volentia/parsed")
CLUSTERS_FILE = PARSED_DIR / "threat_clusters.json"
EXTRACTION_FILE = Path.cwd() / "extraction.json"
OUTPUT_PATH = "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache"
TOPIC = "kaliya-requests"
BOOTSTRAP = "localhost:9092"  # Local Docker Kafka (for testing) or use k8s: kafka.kafka.svc.cluster.local:9092

def delivery_callback(err, msg):
    """Kafka delivery report callback."""
    if err:
        print(f"   ❌ Delivery failed: {err}")
    else:
        print(
            f"   ✅ Delivered to {msg.topic()} "
            f"[partition {msg.partition()}, offset {msg.offset()}]"
        )

def load_clusters():
    """Load threat clusters from JSON."""
    if not CLUSTERS_FILE.exists():
        print(f"❌ Clusters file not found: {CLUSTERS_FILE}")
        sys.exit(1)

    with open(CLUSTERS_FILE) as f:
        return json.load(f)

def load_extraction_data():
    """Load extraction.json for supplementary context (optional)."""
    if not EXTRACTION_FILE.exists():
        print(f"⚠️  Extraction file not found: {EXTRACTION_FILE}")
        return None

    with open(EXTRACTION_FILE) as f:
        return f.read()

def create_producer():
    """Create Kafka producer with connection test."""
    config = {
        "bootstrap.servers": BOOTSTRAP,
        "socket.timeout.ms": 10000,
        "api.version.request.timeout.ms": 10000,
    }

    producer = Producer(config)

    # Test connection by listing topics
    try:
        metadata = producer.list_topics(timeout=5)
        print(f"✅ Connected to Kafka broker at {BOOTSTRAP}")
        print(f"   Available topics: {len(metadata.topics)}")
        if TOPIC in metadata.topics:
            print(f"   ✅ Topic '{TOPIC}' exists")
        else:
            print(f"   ⚠️  Topic '{TOPIC}' not found (will be auto-created)")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print(f"\nTroubleshooting:")
        print(f"  1. Check if Kafka is running: kubectl get pods -n default | grep kafka")
        print(f"  2. Verify NodePort service: kubectl get svc kafka-external -n default")
        print(f"  3. Test connectivity: telnet 192.168.1.3 30094")
        sys.exit(1)

    return producer

def main():
    print("=" * 80)
    print("📨 Kaliya Agent — Send Extraction Data to Kafka")
    print("=" * 80)

    # Load data
    clusters = load_clusters()
    extraction_context = load_extraction_data()

    print(f"\n📂 Loaded data:")
    print(f"   Clusters file:  {CLUSTERS_FILE}")
    print(f"   Total clusters: {len(clusters)}")

    # Create producer
    producer = create_producer()

    # Process each cluster
    messages_sent = 0
    for cluster in clusters:
        cluster_num = cluster["cluster_number"]
        cluster_name = cluster["cluster_name"]
        urls = [row["url"] for row in cluster["rows"] if row.get("url")]

        if not urls:
            print(f"\n⚠️  Cluster {cluster_num} ({cluster_name}): no URLs — skipping")
            continue

        # Generate request ID
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        request_id = f"kaliya-tc{cluster_num}-extraction-{ts}_{uuid.uuid4().hex[:6]}"

        # Build Kaliya request message
        message = {
            "request_id": request_id,
            "urls": urls,
            "output_path": OUTPUT_PATH,
            "analysis_window": "2025-01-01 to 2026-03-16",
        }

        # Add supplementary context if available
        if extraction_context:
            message["supplementary_context"] = extraction_context[:50000]  # Limit size

        print(f"\n{'─' * 70}")
        print(f"📊 Cluster {cluster_num}: {cluster_name}")
        print(f"   Request ID:  {request_id}")
        print(f"   URLs:        {len(urls)}")
        print(f"   Sample URLs:")
        for u in urls[:5]:
            print(f"      • {u}")
        if len(urls) > 5:
            print(f"      ... and {len(urls) - 5} more")

        if extraction_context:
            print(f"   Supplementary: {len(extraction_context):,} chars")

        # Produce message to Kafka
        try:
            message_json = json.dumps(message, ensure_ascii=False)
            message_bytes = message_json.encode("utf-8")

            print(f"   Message size: {len(message_bytes):,} bytes")
            print(f"   Publishing to topic '{TOPIC}'...")

            producer.produce(
                topic=TOPIC,
                value=message_bytes,
                callback=delivery_callback,
            )
            producer.flush(timeout=10)
            messages_sent += 1

        except Exception as e:
            print(f"   ❌ Failed to send message: {e}")
            continue

    print(f"\n{'=' * 80}")
    print(f"✅ Sent {messages_sent} message(s) to '{TOPIC}'")
    print(f"\n📁 Kaliya consumer will write results to:")
    print(f"   {OUTPUT_PATH}/{request_id}/")
    print(f"   • report.json")
    print(f"   • narrative.md")
    print(f"   • figures.json")
    print(f"   • grounding.jsonl")
    print(f"\n⏱️  Expected completion time: 2-5 minutes (depends on cluster size)")
    print()

if __name__ == "__main__":
    main()
