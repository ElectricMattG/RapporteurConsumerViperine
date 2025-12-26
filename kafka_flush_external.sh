#!/bin/bash
# Flush all messages from Kafka topics without deleting the topics
#
# This script temporarily sets retention to 1ms to purge messages,
# then restores the default retention settings.

set -e

NAMESPACE="kafka"
KAFKA_POD="kafka-0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Kafka Topic Flush - Clear All Messages${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl is not installed${NC}"
    exit 1
fi

# Check if Kafka pod exists
echo -e "${YELLOW}🔍 Checking Kafka pod...${NC}"
if ! kubectl get pod "$KAFKA_POD" -n "$NAMESPACE" &> /dev/null; then
    echo -e "${RED}❌ Kafka pod '$KAFKA_POD' not found in namespace '$NAMESPACE'${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Kafka pod found${NC}"
echo ""

# Get list of topics (exclude internal topics)
echo -e "${YELLOW}📋 Getting list of topics...${NC}"
TOPICS=$(kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -c kafka -- \
    kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | \
    grep -v "^__consumer")

if [ -z "$TOPICS" ]; then
    echo -e "${YELLOW}⚠️  No user topics found${NC}"
    exit 0
fi

TOPIC_COUNT=$(echo "$TOPICS" | wc -l | tr -d ' ')
echo -e "${GREEN}✅ Found $TOPIC_COUNT topics to flush${NC}"
echo ""

# Confirm with user
echo -e "${YELLOW}Topics to flush:${NC}"
echo "$TOPICS" | sed 's/^/  - /'
echo ""
echo -e "${RED}⚠️  WARNING: This will delete all messages from these topics!${NC}"
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}❌ Operation cancelled${NC}"
    exit 0
fi
echo ""

# Set retention to 1ms for all topics
echo -e "${YELLOW}🔥 Setting retention to 1ms to purge messages...${NC}"
while IFS= read -r topic; do
    echo -e "  Flushing: ${BLUE}$topic${NC}"
    kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -c kafka -- \
        kafka-configs --bootstrap-server localhost:9092 \
        --entity-type topics \
        --entity-name "$topic" \
        --alter \
        --add-config retention.ms=1 2>&1 | grep -E "Completed|Error" | sed 's/^/    /'
done <<< "$TOPICS"
echo -e "${GREEN}✅ Retention configured${NC}"
echo ""

# Wait for messages to be purged
echo -e "${YELLOW}⏳ Waiting 60 seconds for messages to be purged...${NC}"
sleep 60
echo -e "${GREEN}✅ Purge complete${NC}"
echo ""

# Restore default retention settings
echo -e "${YELLOW}🔄 Restoring default retention settings...${NC}"
while IFS= read -r topic; do
    echo -e "  Restoring: ${BLUE}$topic${NC}"
    kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -c kafka -- \
        kafka-configs --bootstrap-server localhost:9092 \
        --entity-type topics \
        --entity-name "$topic" \
        --alter \
        --delete-config retention.ms 2>&1 | grep -E "Completed|Error" | sed 's/^/    /'
done <<< "$TOPICS"
echo -e "${GREEN}✅ Retention restored${NC}"
echo ""

# Verify topics still exist
echo -e "${YELLOW}✅ Verifying topics...${NC}"
REMAINING=$(kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -c kafka -- \
    kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | \
    grep -v "^__consumer" | wc -l | tr -d ' ')
echo -e "${GREEN}✅ $REMAINING topics remain (messages flushed)${NC}"
echo ""

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Flush complete! Topics intact, messages cleared.${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
