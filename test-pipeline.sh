#!/bin/bash

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}================================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================================${NC}"
}

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_header "🧪 Testing Research Indexing Pipeline"

# Step 1: Check current local storage state
print_header "Step 1/5: Checking current local storage on index-aggregator"

INDEX_POD=$(kubectl get pods -n consumers -l app=index-aggregator -o jsonpath='{.items[0].metadata.name}')
print_info "Index aggregator pod: $INDEX_POD"

echo ""
print_info "Current local files:"
kubectl exec -n consumers $INDEX_POD -- ls -lh /app/cache/ 2>/dev/null || echo "No cache directory yet"

# Step 2: Set up Kafka port-forward
print_header "Step 2/5: Setting up Kafka port-forward"

print_info "Starting port-forward to Kafka (localhost:9092 -> kafka:9092)..."
kubectl port-forward -n kafka svc/kafka-direct-headless 9092:9092 > /dev/null 2>&1 &
PORT_FORWARD_PID=$!

# Wait for port-forward to be ready
sleep 3

print_status "Port-forward active (PID: $PORT_FORWARD_PID)"
print_info "You can now connect to Kafka at localhost:9092"

# Step 3: Send URLs through pipeline
print_header "Step 3/5: Sending test URLs to pipeline"

print_info "Running send_urls.py..."
python3 send_urls.py

print_status "URLs sent to Kafka!"

# Step 4: Monitor pipeline processing
print_header "Step 4/5: Monitoring pipeline (60 seconds)"

print_info "Watching consumer logs for activity..."
echo ""

# Function to check logs
check_consumer() {
    local consumer=$1
    local pod=$(kubectl get pods -n consumers -l app=$consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$pod" ]; then
        local recent=$(kubectl logs -n consumers $pod --tail=5 --since=10s 2>/dev/null | grep -i "processing\|received\|message" | head -2 || echo "")
        if [ -n "$recent" ]; then
            echo -e "${GREEN}  ✓ $consumer: Active${NC}"
            echo "$recent" | sed 's/^/      /'
        fi
    fi
}

for i in {1..6}; do
    echo ""
    print_info "Check $i/6 (every 10 seconds)..."
    check_consumer "web-scraper"
    check_consumer "document-chunker"
    check_consumer "chunk-embedder"
    check_consumer "qa-generator"
    check_consumer "qa-embedder"
    check_consumer "index-aggregator"

    if [ $i -lt 6 ]; then
        sleep 10
    fi
done

# Step 5: Check final storage state
print_header "Step 5/5: Checking final storage state"

echo ""
print_info "Local storage on index-aggregator:"
kubectl exec -n consumers $INDEX_POD -- ls -lh /app/cache/ 2>/dev/null || echo "No cache directory"

echo ""
print_info "Checking for generated files:"
kubectl exec -n consumers $INDEX_POD -- find /app/cache -type f -name "*.jsonl" -o -name "*.json" 2>/dev/null | head -20 || echo "No files found yet"

echo ""
print_info "Kafka topic message counts:"
kubectl exec -n kafka kafka-direct-0 -- kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic web-scrape-requests \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print "  web-scrape-requests: " sum " messages"}'

kubectl exec -n kafka kafka-direct-0 -- kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic document-chunking \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print "  document-chunking: " sum " messages"}'

kubectl exec -n kafka kafka-direct-0 -- kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic qa-indexed \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print "  qa-indexed: " sum " messages"}'

# Cleanup
print_header "Cleanup"
print_info "Stopping port-forward..."
kill $PORT_FORWARD_PID 2>/dev/null || true
print_status "Port-forward stopped"

echo ""
print_header "✨ Pipeline Test Complete!"
echo ""
print_info "To view detailed logs:"
echo "  kubectl logs -n consumers -l app=web-scraper -f"
echo "  kubectl logs -n consumers -l app=document-chunker -f"
echo "  kubectl logs -n consumers -l app=index-aggregator -f"
echo ""
print_info "To check storage:"
echo "  kubectl exec -n consumers $INDEX_POD -- ls -lah /app/cache/"
echo ""
