#!/bin/bash
# Send extraction data to k8s Kafka using kubectl exec

CLUSTER_FILE="/mnt/workspace_2/bboa/documents/atrum_volentia/parsed/threat_clusters.json"
TOPIC="kaliya-requests"
REQUEST_ID="kaliya-tc1-extraction-$(date +%Y%m%d_%H%M%S)_$(openssl rand -hex 3)"
OUTPUT_PATH="/mnt/workspace_2/bboa/cache/kaliya_agenda_cache"

echo "================================================================================"
echo "📨 Kaliya Agent — Send to k8s Kafka"
echo "================================================================================"
echo ""
echo "📂 Loading cluster data..."
echo "   File: $CLUSTER_FILE"
echo ""

# Build URLs list from JSON
URLS=$(python3 -c "
import json
with open('$CLUSTER_FILE') as f:
    data = json.load(f)
    urls = [row['url'] for cluster in data for row in cluster['rows'] if row.get('url')]
    print(json.dumps(urls))
")

NUM_URLS=$(echo "$URLS" | python3 -c "import sys, json; print(len(json.loads(sys.stdin.read())))")

echo "📊 Cluster: Document References - Unified Analysis"
echo "   Request ID: $REQUEST_ID"
echo "   URLs: $NUM_URLS"
echo ""

# Build Kaliya request message
MESSAGE=$(python3 -c "
import json
message = {
    'request_id': '$REQUEST_ID',
    'urls': $URLS,
    'output_path': '$OUTPUT_PATH',
    'analysis_window': '2025-01-01 to 2026-03-16'
}
print(json.dumps(message, ensure_ascii=False))
")

echo "📤 Publishing to k8s Kafka topic '$TOPIC'..."
echo ""

# Send message using kafka console producer inside the pod
echo "$MESSAGE" | kubectl -n kafka exec -i kafka-0 -c kafka -- \
    kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC"

if [ $? -eq 0 ]; then
    echo "✅ Message successfully published to k8s Kafka!"
    echo ""
    echo "📁 Kaliya consumer will write results to:"
    echo "   $OUTPUT_PATH/$REQUEST_ID/"
    echo "   • report.json"
    echo "   • narrative.md"
    echo "   • figures.json"
    echo "   • grounding.jsonl"
    echo ""
    echo "⏱️  Expected completion time: 2-5 minutes (166 URLs)"
    echo ""
else
    echo "❌ Failed to publish message"
    exit 1
fi
