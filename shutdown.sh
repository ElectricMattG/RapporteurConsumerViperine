#!/bin/bash

# Shutdown script for ScrapeLinkedWebSources pipeline
# This gracefully stops all Kafka consumers

echo "🛑 Shutting down ScrapeLinkedWebSources Pipeline..."
echo "================================================"

# Count running consumers before shutdown
RUNNING_COUNT=$(ps aux | grep "python3 src/consumers" | grep -v grep | wc -l)

if [ "$RUNNING_COUNT" -eq 0 ]; then
    echo "ℹ️  No consumers are currently running"
    exit 0
fi

echo "📊 Found $RUNNING_COUNT running consumer(s)"
echo ""

# Show which consumers are running
echo "Running consumers:"
ps aux | grep "python3 src/consumers" | grep -v grep | awk '{print "  • " $NF}' | sed 's|src/consumers/||' | sed 's|.py||'

echo ""
echo "🔄 Attempting graceful shutdown (SIGTERM)..."

# First try graceful shutdown with SIGTERM
pkill -15 -f "python3 src/consumers" 2>/dev/null

# Wait up to 10 seconds for graceful shutdown
for i in {1..10}; do
    REMAINING=$(ps aux | grep "python3 src/consumers" | grep -v grep | wc -l)
    if [ "$REMAINING" -eq 0 ]; then
        echo "✅ All consumers stopped gracefully"
        echo ""
        echo "================================================"
        echo "🎉 Pipeline shutdown complete!"
        exit 0
    fi
    echo "  Waiting... ($i/10) - $REMAINING process(es) still running"
    sleep 1
done

# If graceful shutdown failed, force kill
echo ""
echo "⚠️  Graceful shutdown timeout - forcing shutdown (SIGKILL)..."
pkill -9 -f "python3 src/consumers" 2>/dev/null
sleep 1

REMAINING=$(ps aux | grep "python3 src/consumers" | grep -v grep | wc -l)
if [ "$REMAINING" -eq 0 ]; then
    echo "✅ All consumers stopped (forced)"
else
    echo "❌ Failed to stop $REMAINING consumer(s)"
    echo "   Try manually: pkill -9 -f 'python3 src/consumers'"
    exit 1
fi

echo ""
echo "================================================"
echo "🎉 Pipeline shutdown complete!"
