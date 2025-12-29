# Kafka Version Compatibility

## Overview

This codebase supports two different Kafka deployments:

1. **Local Development**: Kafka 7.5.0 with ZooKeeper
2. **Cloud Production**: Kafka 8.0.0 with KRaft

## Key Differences

### Local (Kafka 7.5.0 + ZooKeeper)

- **Consumer Mode**: Consumer group subscription
- **Coordinator**: ZooKeeper-based consumer group coordinator
- **Configuration**: Standard Kafka consumer settings
- **Assignment**: Automatic partition assignment via consumer groups

### Cloud (Kafka 8.0.0 + KRaft)

- **Consumer Mode**: Manual partition assignment
- **Coordinator**: KRaft metadata (no ZooKeeper)
- **Configuration**: Manual partition assignment to avoid coordinator issues
- **Assignment**: Explicit partition assignment at startup

## Code Changes Made

### 1. Consumer Group vs Manual Assignment

**File**: `src/consumers/kafka_web_scraper.py`

The code now detects which mode to use via environment variable:

```python
use_manual_assignment = os.getenv('KAFKA_USE_MANUAL_ASSIGNMENT', 'false').lower() == 'true'

if use_manual_assignment:
    # KRaft mode - manual partition assignment
    from confluent_kafka import TopicPartition, OFFSET_BEGINNING
    partitions = [TopicPartition(topic, pid, OFFSET_BEGINNING) for pid in ...]
    consumer.assign(partitions)
else:
    # ZooKeeper mode - consumer group subscription
    consumer.subscribe([topic])
```

### 2. Synchronous Poll Loop

**Issue**: `confluent_kafka.Consumer.poll()` is a synchronous blocking call, not async-compatible.

**Fix**: Changed from async function to regular function:

```python
# Before (broken):
async def _consume_loop(self):
    msg = self.consumer.poll(timeout=1.0)  # Doesn't work in async

# After (working):
def _consume_loop(self):
    msg = self.consumer.poll(timeout=1.0)  # Works in sync function
```

Process async work by running event loop explicitly:
```python
result = asyncio.run(self._process_message(message_data))
```

### 3. Configuration Settings

**File**: `src/consumers/kafka_web_scraper.py`

Reverted KRaft-specific settings that broke ZooKeeper mode:

```python
# Standard config (works with both):
self.kafka_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'web-scraper-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'session.timeout.ms': 30000,
    'max.poll.interval.ms': 300000,
}
```

### 4. Helm Values for Cloud

**File**: `helm/research-indexing-consumers/values.yaml`

Added environment variable for cloud deployment:

```yaml
consumers:
  webScraper:
    env:
      - name: KAFKA_USE_MANUAL_ASSIGNMENT
        value: "true"  # Enable KRaft mode
```

## Running Locally

No changes needed! Default behavior uses consumer groups:

```bash
# Start local Kafka (ZooKeeper mode)
docker-compose up -d

# Run consumers (uses consumer groups by default)
./restart_pipeline.sh
```

## Running in Cloud

Set environment variable in Helm values (already configured):

```bash
# Deploy to AKS (uses manual partition assignment)
./deploy-consumers.sh
```

## Troubleshooting

### Local Issues

**Problem**: Consumer not joining group
- **Check**: Is ZooKeeper running? `docker-compose ps`
- **Check**: Are topics created? `docker exec kafka kafka-topics --list`
- **Fix**: Restart Kafka: `docker-compose restart kafka`

### Cloud Issues

**Problem**: Consumer group coordinator timeout
- **Solution**: Ensure `KAFKA_USE_MANUAL_ASSIGNMENT=true` is set
- **Verify**: Check pod logs for "Using manual partition assignment"

**Problem**: No partitions assigned
- **Check**: Are topics created? `kubectl exec kafka-direct-0 -- kafka-topics --list`
- **Check**: Pod logs show partition count: "Assigning X partitions from beginning"

## Testing Both Modes

### Test Local (ZooKeeper)
```bash
# Should see: "Using consumer group subscription (ZooKeeper mode)"
python3 src/consumers/run_kafka_consumer.py 2>&1 | grep "Using"
```

### Test Cloud (KRaft)
```bash
# Should see: "Using manual partition assignment (KRaft mode)"
export KAFKA_USE_MANUAL_ASSIGNMENT=true
python3 src/consumers/run_kafka_consumer.py 2>&1 | grep "Using"
```

## Migration Notes

When upgrading local Kafka to 8.0.0 KRaft:

1. Set `KAFKA_USE_MANUAL_ASSIGNMENT=true` in environment
2. Or keep using consumer groups if KRaft coordinator is working
3. Test thoroughly - KRaft coordinator behavior may vary

## Summary

- **Default**: Consumer groups (ZooKeeper compatible)
- **Cloud**: Manual assignment (KRaft workaround)
- **Toggle**: `KAFKA_USE_MANUAL_ASSIGNMENT` environment variable
- **Both versions work** without code changes
