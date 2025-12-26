#!/bin/bash
# Create all required Kafka topics in local Docker Kafka

topics=(
    "web-scrape-requests:12:1"
    "content-extracted:12:1"
    "citation-extraction:12:1"
    "chunk-embedding:12:1"
    "qa-generation:12:1"
    "qa-embedding:12:1"
    "qa-indexed:12:1"
    "session-complete:12:1"
    "summarise-document:12:1"
    "dead-letter-queue:12:1"
)

echo "Creating Kafka topics..."

for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    echo "Creating topic: $topic (partitions=$partitions, replication=$replication)"

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --if-not-exists
done

echo ""
echo "Topics created. Listing all topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
