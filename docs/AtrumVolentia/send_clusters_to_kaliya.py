#!/usr/bin/env python3
"""
Send one Kaliya agenda-analysis request per threat cluster.

Reads 203_threat_clusters.json, extracts URLs from each cluster,
and produces a KaliyaRequestMessage to the kaliya-requests Kafka topic
for each cluster (skipping entries with null URLs).
"""

import json
import sys
import uuid
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer

CLUSTERS_FILE = Path(__file__).parent / "203_threat_clusters.json"
SUPPLEMENTARY_FILE = str(Path(__file__).parent / "203_supplementary_context.json")
OUTPUT_PATH = "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache"
TOPIC = "kaliya-requests"
BOOTSTRAP = "localhost:30094"


def delivery_callback(err, msg):
    if err:
        print(f"   ❌ Delivery failed: {err}")
    else:
        print(
            f"   ✅ Delivered to {msg.topic()} "
            f"[partition {msg.partition()}, offset {msg.offset()}]"
        )


def main():
    with open(CLUSTERS_FILE) as f:
        data = json.load(f)

    clusters = data["threat_clusters"]
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    print("=" * 80)
    print("📊 Kaliya — Threat Cluster Batch Send")
    print(f"   Source: {CLUSTERS_FILE.name}")
    print(f"   Clusters: {len(clusters)}")
    print(f"   Supplementary: {SUPPLEMENTARY_FILE}")
    print("=" * 80)

    for cluster in clusters:
        cid = cluster["cluster_id"]
        name = cluster["name"]
        urls = [e["url"] for e in cluster["entries"] if e.get("url")]

        if not urls:
            print(f"\n⚠️  Cluster {cid} ({name}): no URLs — skipping")
            continue

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        request_id = f"tc{cid}-{ts}_{uuid.uuid4().hex[:6]}"

        message = {
            "request_id": request_id,
            "urls": urls,
            "output_path": OUTPUT_PATH,
            "supplementary_file": SUPPLEMENTARY_FILE,
            "analysis_window": "2025-01-01 to 2026-03-16",
        }

        print(f"\n{'─' * 60}")
        print(f"📨 Cluster {cid}: {name}")
        print(f"   Request ID:  {request_id}")
        print(f"   URLs:        {len(urls)}")
        for u in urls:
            print(f"      • {u}")

        producer.produce(
            topic=TOPIC,
            value=json.dumps(message).encode("utf-8"),
            callback=delivery_callback,
        )
        producer.flush(timeout=10)

    print(f"\n{'=' * 80}")
    print(f"✅ All cluster messages sent to '{TOPIC}'")
    print(f"   Output dir: {OUTPUT_PATH}/")
    print()


if __name__ == "__main__":
    main()
