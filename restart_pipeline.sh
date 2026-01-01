#!/bin/bash

# Restart script for ScrapeLinkedWebSources pipeline
# This stops all consumers, flushes Kafka topics, and restarts everything

echo "🔄 Restarting ScrapeLinkedWebSources Pipeline..."
echo "================================================"

# Step 1: Stop all consumers
echo ""
echo "🛑 Step 1: Stopping all consumers..."
RUNNING_COUNT=$(ps aux | grep "python3 src/consumers" | grep -v grep | wc -l)

if [ "$RUNNING_COUNT" -gt 0 ]; then
    echo "   Found $RUNNING_COUNT running consumer(s)"
    pkill -9 -f "python3 src/consumers" 2>/dev/null
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
    "summarise-document"
    "content-extracted"
    "citation-extraction"
    "chunk-embedding"
    "qa-generation"
    "qa-embedding"
    "qa-indexed"
    "session-complete"
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

# Step 4: Start all consumers
echo ""
echo "🚀 Step 4: Starting all consumers..."

cd "$(dirname "$0")"

python3 src/consumers/run_kafka_consumer.py > logs/web_scraper.log 2>&1 &
echo "   ✅ Web Scraper (PID: $!)"

ENABLE_LLM_VALIDATION=true python3 src/consumers/run_kafka_chunker.py > logs/chunker.log 2>&1 &
echo "   ✅ Document Chunker (PID: $!) - LLM validation: ON"

python3 src/consumers/run_kafka_citation_extractor.py > logs/citation_extractor.log 2>&1 &
echo "   ✅ Citation Extractor (PID: $!)"

python3 src/consumers/run_kafka_chunk_embedder.py > logs/chunk_embedder.log 2>&1 &
echo "   ✅ Chunk Embedder (PID: $!)"

python3 src/consumers/run_kafka_qa_generator.py > logs/qa_generator.log 2>&1 &
echo "   ✅ Q&A Generator (PID: $!)"

python3 src/consumers/run_kafka_qa_embedder.py > logs/qa_embedder.log 2>&1 &
echo "   ✅ Q&A Embedder (PID: $!)"

python3 src/consumers/run_kafka_summarizer.py > logs/summarizer.log 2>&1 &
echo "   ✅ Summarizer (PID: $!)"

sleep 3

python3 src/consumers/run_kafka_index_aggregator.py > logs/index_aggregator.log 2>&1 &
echo "   ✅ Index Aggregator (PID: $!)"

# Step 5: Verify all consumers started
sleep 2
echo ""
echo "🔍 Step 5: Verifying consumer status..."

RUNNING_COUNT=$(ps aux | grep "python3 src/consumers" | grep -v grep | wc -l)
echo "   Running consumers: $RUNNING_COUNT/8"

if [ "$RUNNING_COUNT" -eq 8 ]; then
    echo "   ✅ All consumers started successfully"
else
    echo "   ⚠️  Expected 8 consumers, found $RUNNING_COUNT"
    echo ""
    echo "   Active consumers:"
    ps aux | grep "python3 src/consumers" | grep -v grep | awk '{print "     • " $NF}' | sed 's|src/consumers/||' | sed 's|run_||' | sed 's|.py||'
fi

echo ""
echo "================================================"
echo "🎉 Pipeline restart complete!"
echo ""
echo "📋 Logs available in logs/ directory"
echo "   • Web Scraper:       logs/web_scraper.log"
echo "   • Chunker:           logs/chunker.log"
echo "   • Citation Extract:  logs/citation_extractor.log"
echo "   • Chunk Embedder:    logs/chunk_embedder.log"
echo "   • Q&A Generator:     logs/qa_generator.log"
echo "   • Q&A Embedder:      logs/qa_embedder.log"
echo "   • Summarizer:        logs/summarizer.log"
echo "   • Index Aggregator:  logs/index_aggregator.log"
