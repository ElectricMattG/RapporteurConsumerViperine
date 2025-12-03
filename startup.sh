#!/bin/bash

# Startup script for ScrapeLinkedWebSources pipeline
# This starts all Kafka consumers in the background

set -e

echo "🚀 Starting ScrapeLinkedWebSources Pipeline..."
echo "================================================"

# Create logs directory if it doesn't exist
mkdir -p logs

# Kill any existing Python processes to avoid conflicts
echo "🧹 Cleaning up any existing processes..."
pkill -9 -f "python3 src/consumers" 2>/dev/null || true
sleep 2

# Start each consumer in background with PYTHONDONTWRITEBYTECODE to avoid .pyc issues
echo ""
echo "📡 Starting Kafka Consumers..."

# 1. Web Scraper - Extracts content from URLs
echo "  ✓ Starting Web Scraper..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/kafka_web_scraper.py > logs/web_scraper.log 2>&1 &
WEB_SCRAPER_PID=$!

# 2. Summarizer - Generates summaries from extracted content
echo "  ✓ Starting Summarizer..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/run_kafka_summarizer.py > logs/summarizer.log 2>&1 &
SUMMARIZER_PID=$!

# 3. Document Chunker - Splits documents into chunks
echo "  ✓ Starting Document Chunker..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/run_kafka_chunker.py > logs/chunker.log 2>&1 &
CHUNKER_PID=$!

# 4. Citation Extractor - Enriches chunks with citations
echo "  ✓ Starting Citation Extractor..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/run_kafka_citation_extractor.py > logs/citation.log 2>&1 &
CITATION_PID=$!

# 5. Chunk Embedder - Generates embeddings for chunks
echo "  ✓ Starting Chunk Embedder..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/run_kafka_chunk_embedder.py > logs/embedder.log 2>&1 &
EMBEDDER_PID=$!

# 6. Q&A Generator - Generates Q&A pairs from chunks
echo "  ✓ Starting Q&A Generator..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/run_kafka_qa_generator.py > logs/qa_generator.log 2>&1 &
QA_GEN_PID=$!

# 7. Q&A Embedder - Generates embeddings for Q&A pairs
echo "  ✓ Starting Q&A Embedder..."
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/run_kafka_qa_embedder.py > logs/qa_embedder.log 2>&1 &
QA_EMB_PID=$!

# Wait a moment for processes to start
sleep 3

# Check which processes are running
echo ""
echo "📊 Checking running processes..."
RUNNING_COUNT=$(ps aux | grep "python3 src/consumers" | grep -v grep | wc -l)

if [ "$RUNNING_COUNT" -eq 7 ]; then
    echo "✅ All 7 consumers started successfully!"
else
    echo "⚠️  Warning: Only $RUNNING_COUNT/7 consumers are running"
fi

echo ""
echo "Process IDs:"
echo "  Web Scraper:        $WEB_SCRAPER_PID"
echo "  Summarizer:         $SUMMARIZER_PID"
echo "  Document Chunker:   $CHUNKER_PID"
echo "  Citation Extractor: $CITATION_PID"
echo "  Chunk Embedder:     $EMBEDDER_PID"
echo "  Q&A Generator:      $QA_GEN_PID"
echo "  Q&A Embedder:       $QA_EMB_PID"

echo ""
echo "📁 Log files location: logs/"
echo "  • logs/web_scraper.log"
echo "  • logs/summarizer.log"
echo "  • logs/chunker.log"
echo "  • logs/citation.log"
echo "  • logs/embedder.log"
echo "  • logs/qa_generator.log"
echo "  • logs/qa_embedder.log"

echo ""
echo "💡 To monitor logs in real-time:"
echo "   tail -f logs/web_scraper.log"
echo ""
echo "💡 To shutdown all consumers:"
echo "   ./shutdown.sh"
echo ""
echo "================================================"
echo "🎉 Pipeline startup complete!"
