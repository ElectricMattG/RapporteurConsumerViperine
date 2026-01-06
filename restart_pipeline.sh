#!/bin/bash

# Restart script for ScrapeLinkedWebSources pipeline
# This stops all consumers, flushes Kafka topics, and restarts everything

echo "🔄 Restarting ScrapeLinkedWebSources Pipeline..."
echo "================================================"

# Step 1: Stop all consumers
echo ""
echo "🛑 Step 1: Stopping all consumers..."
RUNNING_COUNT=$(ps aux | grep "src/consumers/run_kafka" | grep -v grep | wc -l)

if [ "$RUNNING_COUNT" -gt 0 ]; then
    echo "   Found $RUNNING_COUNT running consumer(s)"
    pkill -9 -f "src/consumers/run_kafka" 2>/dev/null
    sleep 2
    echo "   ✅ All consumers stopped"
else
    echo "   ℹ️  No consumers were running"
fi

# Step 2: Clear logs
echo ""
echo "🧹 Step 2: Clearing log files..."
rm -rf logs/*.log logs/*.txt logs/*.out 2>/dev/null
mkdir -p logs
echo "   ✅ Logs cleared"

# Step 3: Create and flush Kafka topics
echo ""
echo "🗑️  Step 3: Creating and flushing Kafka topics..."

TOPICS=(
    "web-scrape-requests"
    "content-extracted"
    "summarise-document"
    "summary-completed"
    "session-complete"
    "frontend-message"
)

# First delete topics to clear all messages
for topic in "${TOPICS[@]}"; do
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
done

sleep 2

# Now recreate topics
for topic in "${TOPICS[@]}"; do
    echo -n "   Creating $topic... "
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$topic" --partitions 1 --replication-factor 1 2>&1 | grep -q "Created topic" && echo "✓" || echo "✓ (exists)"
done

echo "   ✅ All topics ready"

# Step 4: Start selected consumers (slim pipeline)
echo ""
echo "🚀 Step 4: Starting selected consumers (slim pipeline)..."

cd "$(dirname "$0")"

python3 src/consumers/run_kafka_consumer.py > logs/web_scraper.log 2>&1 &
echo "   ✅ Web Scraper (PID: $!)"

# Summarizer should consume from `content-extracted` in the slim pipeline
SUMMARIZER_INPUT_TOPIC=${SUMMARIZER_INPUT_TOPIC:-content-extracted}
export SUMMARIZER_INPUT_TOPIC
python3 src/consumers/run_kafka_summarizer.py > logs/summarizer.log 2>&1 &
echo "   ✅ Summarizer (PID: $!) - input_topic=${SUMMARIZER_INPUT_TOPIC}"

python3 src/consumers/run_kafka_summary_indexer.py > logs/summary_indexer.log 2>&1 &
echo "   ✅ Summary Indexer (PID: $!)"

# Optionally start the index aggregator if environment variable ENABLE_AGGREGATOR is true (default: false)
if [ "${ENABLE_AGGREGATOR:-false}" = "true" ]; then
    python3 src/consumers/run_kafka_index_aggregator.py > logs/index_aggregator.log 2>&1 &
    echo "   ✅ Index Aggregator (PID: $!)"
else
    echo "   ℹ️  Index Aggregator not started (ENABLE_AGGREGATOR=${ENABLE_AGGREGATOR:-false})"
fi

# Step 5: Verify all consumers started
sleep 2
echo ""
echo "🔍 Step 5: Verifying consumer status..."

RUNNING_COUNT=$(ps aux | grep "src/consumers/run_kafka" | grep -v grep | wc -l)
echo "   Running consumers: $RUNNING_COUNT (expected: web_scraper, summarizer, summary_indexer, optional index_aggregator)"

EXPECTED_MIN=3
if [ "$RUNNING_COUNT" -ge "$EXPECTED_MIN" ]; then
    echo "   ✅ Selected consumers started successfully"
else
    echo "   ⚠️  Expected at least $EXPECTED_MIN consumers, found $RUNNING_COUNT"
    echo ""
    echo "   Active consumers:"
    ps aux | grep "src/consumers/run_kafka" | grep -v grep | awk '{print "     • " $NF}' | sed 's|src/consumers/||' | sed 's|run_||' | sed 's|.py||'
fi

echo ""
echo "================================================"
echo "🎉 Pipeline restart complete!"
echo ""
echo "📋 Logs available in logs/ directory"
echo "   • Web Scraper:       logs/web_scraper.log"
echo "   • Summarizer:        logs/summarizer.log"
echo "   • Summary Indexer:   logs/summary_indexer.log"
echo "   • Index Aggregator:  logs/index_aggregator.log (only if ENABLE_AGGREGATOR=true)"
