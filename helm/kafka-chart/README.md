# Kafka Deployment on AKS (KRaft Mode)

Production-ready Apache Kafka deployment using **KRaft mode** (no ZooKeeper) on Azure Kubernetes Service (AKS). This deployment runs on a dedicated `kafkapool` node and is optimized for the research-indexing-consumers pipeline.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Deployment Pitfalls & Solutions](#deployment-pitfalls--solutions)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## 🏗 Architecture Overview

### KRaft Mode Architecture

```mermaid
graph TB
    subgraph AKS["AKS Cluster (embedding-aks-ne)"]
        subgraph KafkaPool["kafkapool Node (Standard_D2_v3)"]
            subgraph Pod["kafka-0 Pod"]
                Init["initContainer<br/>cleanup-lost-found<br/>(busybox)"]
                Kafka["Kafka Container<br/>confluent/cp-kafka:7.6.0<br/><b>KRaft Mode</b><br/>broker + controller"]
                PVC["PVC: kafka-data<br/>50Gi (managed-csi)"]

                Init -.->|"cleans lost+found"| PVC
                Kafka -->|"stores data"| PVC
            end

            Service["kafka-headless<br/>ClusterIP: None<br/>:9092 (client)<br/>:9093 (controller)"]
            Service -->|"stable DNS"| Pod
        end
    end

    Clients["Consumer Apps<br/>(research-indexing)"] -->|"bootstrap: kafka-0.kafka-headless:9092"| Service

    style Kafka fill:#4CAF50
    style Init fill:#FFC107
    style PVC fill:#2196F3
    style KafkaPool fill:#E3F2FD
```

### KRaft vs ZooKeeper Comparison

| Feature | KRaft Mode (Current) | ZooKeeper Mode (Old) |
|---------|---------------------|---------------------|
| Architecture | Single image, embedded metadata | Separate ZooKeeper ensemble |
| Complexity | Low (1 container) | High (2+ containers) |
| Resource Usage | ~2GB RAM | ~3GB+ RAM (Kafka + ZK) |
| Startup Time | ~30 seconds | ~60+ seconds |
| Kafka Version | 7.6+ (recommended for KRaft) | Any version |
| Metadata Storage | Internal Raft quorum | External ZooKeeper |
| Production Ready | ✅ Yes (as of Kafka 3.3+) | ✅ Yes (legacy) |

---

## 🚀 Quick Start

### Prerequisites

- AKS cluster with `kafkapool` node (Standard_D2_v3)
- `kubectl` configured for cluster access
- Helm 3.x installed
- Azure CLI (optional, for ACR)

### 1. Deploy Kafka

```bash
# Create namespace
kubectl create namespace kafka

# Deploy with Helm
helm install kafka ./helm/kafka-chart --namespace kafka

# Wait for pod to be ready
kubectl wait --for=condition=ready pod/kafka-0 -n kafka --timeout=120s

# Verify deployment
kubectl get pods -n kafka
```

Expected output:
```
NAME      READY   STATUS    RESTARTS   AGE
kafka-0   1/1     Running   0          2m
```

### 2. Create Topics

```bash
# Exec into pod
kubectl exec -it kafka-0 -n kafka -- bash

# Create topic for web scraping pipeline
kafka-topics --create \
  --topic web-scrape-requests \
  --bootstrap-server localhost:9092 \
  --partitions 12 \
  --replication-factor 1

# List topics
kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Test Producer/Consumer

```bash
# Produce messages
echo "test message" | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic web-scrape-requests

# Consume messages
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic web-scrape-requests \
  --from-beginning \
  --max-messages 1
```

### 4. Connect Application

Use this bootstrap server in your applications:

```python
# Python (confluent-kafka)
config = {
    'bootstrap.servers': 'kafka-0.kafka-headless.kafka.svc.cluster.local:9092'
}
```

```yaml
# Kubernetes environment variable
- name: KAFKA_BOOTSTRAP_SERVERS
  value: "kafka-0.kafka-headless.kafka.svc.cluster.local:9092"
```

---

## ⚙️ Configuration

### Key Environment Variables (KRaft Mode)

```yaml
env:
  # KRaft Process Roles (broker + controller in single node)
  - name: KAFKA_PROCESS_ROLES
    value: "broker,controller"

  # Unique node identifier
  - name: KAFKA_NODE_ID
    value: "1"

  # Cluster ID (REQUIRED for KRaft)
  - name: KAFKA_CLUSTER_ID
    value: "MkU3OEVBNTcwNTJENDM2Qk"

  # BOTH variables needed (see pitfalls below)
  - name: CLUSTER_ID
    value: "MkU3OEVBNTcwNTJENDM2Qk"

  # Controller quorum voters (self-voting for single node)
  - name: KAFKA_CONTROLLER_QUORUM_VOTERS
    value: "1@kafka-0.kafka-headless.kafka.svc.cluster.local:9093"

  # Listener configuration
  - name: KAFKA_LISTENERS
    value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"

  - name: KAFKA_ADVERTISED_LISTENERS
    value: "PLAINTEXT://kafka-0.kafka-headless.kafka.svc.cluster.local:9092"
```

### Image Configuration

**Current (Working):**
```yaml
image: confluentinc/cp-kafka:7.6.0
```

**ACR Alternative:**
```yaml
image: ragserviceneacr6881.azurecr.io/kafka:8.0.0
```

> ⚠️ **Note**: Ensure `--platform linux/amd64` when building images on Apple Silicon.

### Resource Limits (kafkapool: 2 cores, 8GB RAM)

```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "1000m"
```

### Storage

```yaml
volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      storageClassName: managed-csi  # Standard LRS (D2_v3 compatible)
      resources:
        requests:
          storage: 50Gi
```

---

## ⚠️ Deployment Pitfalls & Solutions

### Critical Issues Resolved

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant K8s as Kubernetes
    participant Kafka as Kafka Pod

    Note over Dev,Kafka: Issue 1: KAFKA_PORT Auto-Injection
    K8s->>Kafka: Inject KAFKA_PORT=tcp://10.0.x.x:9092
    Kafka->>Kafka: ❌ Error: "port is deprecated"
    Kafka->>Dev: CrashLoopBackOff
    Dev->>K8s: Set enableServiceLinks: false
    K8s->>Kafka: ✅ No auto-injection

    Note over Dev,Kafka: Issue 2: CLUSTER_ID Variable
    Kafka->>Kafka: Check CLUSTER_ID env var
    Kafka->>Kafka: ❌ Error: "CLUSTER_ID is required"
    Dev->>Kafka: Set both KAFKA_CLUSTER_ID + CLUSTER_ID
    Kafka->>Kafka: ✅ Startup successful

    Note over Dev,Kafka: Issue 3: lost+found Directory
    Kafka->>Kafka: Scan /var/lib/kafka/data
    Kafka->>Kafka: ❌ Error: "lost+found not valid topic"
    Dev->>K8s: Add initContainer to delete lost+found
    K8s->>Kafka: ✅ Clean data directory
```

### 1. Kubernetes Service Environment Variable Injection

**Problem:**
```
port is deprecated. Please use KAFKA_ADVERTISED_LISTENERS instead.
```

**Root Cause:**
Kubernetes automatically injects environment variables for Services in the same namespace:
```bash
KAFKA_PORT=tcp://10.0.213.97:9092  # Auto-injected by K8s
```

Confluent's startup script detects `KAFKA_PORT` and fails with "deprecated" error.

**Solution:**
```yaml
spec:
  template:
    spec:
      enableServiceLinks: false  # ← Critical fix
```

**Why it works:** Prevents Kubernetes from injecting service discovery environment variables.

---

### 2. Dual CLUSTER_ID Environment Variables

**Problem:**
```
CLUSTER_ID is required.
Command [/usr/local/bin/dub ensure CLUSTER_ID] FAILED !
```

**Root Cause:**
Confluent's Docker utilities (`dub`) translate `KAFKA_CLUSTER_ID` → `CLUSTER_ID`, but the translation wasn't working in KRaft mode for Kafka 7.6.0.

**Solution:**
```yaml
env:
  - name: KAFKA_CLUSTER_ID  # Standard Kafka variable
    value: "MkU3OEVBNTcwNTJENDM2Qk"
  - name: CLUSTER_ID         # Required by dub script
    value: "MkU3OEVBNTcwNTJENDM2Qk"
```

**How to generate Cluster ID:**
```bash
kubectl exec kafka-0 -n kafka -- kafka-storage random-uuid
```

---

### 3. PersistentVolume lost+found Directory

**Problem:**
```
Found directory /var/lib/kafka/data/lost+found
'lost+found' is not in the form of topic-partition
```

**Root Cause:**
Azure Managed Disks (ext4) create `lost+found` directory. Kafka scans `/var/lib/kafka/data` and treats every subdirectory as a topic partition.

**Solution:**
```yaml
initContainers:
  - name: cleanup-lost-found
    image: busybox:latest
    command:
      - sh
      - -c
      - |
        echo "Cleaning up lost+found directory..."
        rm -rf /var/lib/kafka/data/lost+found
        echo "Cleanup complete"
    volumeMounts:
      - name: kafka-data
        mountPath: /var/lib/kafka/data
    securityContext:
      runAsUser: 0  # Root required to delete lost+found
```

---

### 4. Docker Image Platform Mismatch

**Problem:**
```
no match for platform in manifest: not found
```

**Root Cause:**
Building images on Apple Silicon (ARM64) but AKS nodes are AMD64.

**Solution:**
```bash
# Build with explicit platform
docker build --platform linux/amd64 -t kafka:8.0.0 .
docker push ragserviceneacr6881.azurecr.io/kafka:8.0.0
```

---

### 5. Docker Layer Cache "Bitnami Ghost"

**Problem:**
Logs showed Bitnami startup scripts despite using `FROM confluentinc/cp-kafka:7.5.0`.

**Root Cause:**
Docker layer cache reusing old Bitnami image layers even after rebuilding.

**Solution:**
```bash
# Nuclear option: purge all Docker cache
docker system prune -a -f --volumes

# OR: Use specific version with new layers
docker pull confluentinc/cp-kafka:7.6.0  # Different base layers
```

**Why 7.6.0 worked:** Completely different image layers, no cache pollution.

---

## 🧪 Testing

### Health Check

```bash
# Check broker API
kubectl exec kafka-0 -n kafka -- \
  kafka-broker-api-versions --bootstrap-server localhost:9092 | head -5

# Expected output:
# kafka-0.kafka-headless.kafka.svc.cluster.local:9092 (id: 1 rack: null) -> (
#   Produce(0): 0 to 9 [usable: 9],
#   Fetch(1): 0 to 15 [usable: 15],
#   ...
```

### Performance Test

```bash
# Producer performance test (1M messages)
kubectl exec kafka-0 -n kafka -- \
  kafka-producer-perf-test \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092

# Consumer performance test
kubectl exec kafka-0 -n kafka -- \
  kafka-consumer-perf-test \
  --bootstrap-server localhost:9092 \
  --topic perf-test \
  --messages 1000000 \
  --threads 1
```

### Topic Management

```bash
# List all topics
kubectl exec kafka-0 -n kafka -- \
  kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
kubectl exec kafka-0 -n kafka -- \
  kafka-topics --describe --topic web-scrape-requests \
  --bootstrap-server localhost:9092

# Delete topic
kubectl exec kafka-0 -n kafka -- \
  kafka-topics --delete --topic test-topic \
  --bootstrap-server localhost:9092
```

---

## 🔧 Troubleshooting

### Pod Not Starting

```bash
# Check pod status
kubectl get pods -n kafka

# View logs
kubectl logs kafka-0 -n kafka --tail=100

# Check init container logs
kubectl logs kafka-0 -n kafka -c cleanup-lost-found

# Describe pod for events
kubectl describe pod kafka-0 -n kafka
```

### Common Error Patterns

| Error Message | Solution |
|--------------|----------|
| `CLUSTER_ID is required` | Add both `KAFKA_CLUSTER_ID` and `CLUSTER_ID` env vars |
| `port is deprecated` | Set `enableServiceLinks: false` |
| `lost+found is not valid` | Verify initContainer is running |
| `CrashLoopBackOff` | Check logs: `kubectl logs kafka-0 -n kafka --tail=50` |
| `Pending` state | Check PVC binding: `kubectl get pvc -n kafka` |

### Connectivity Issues

```bash
# Test from within cluster
kubectl run kafka-test --rm -i --tty --image=confluentinc/cp-kafka:7.6.0 \
  -- bash -c "kafka-broker-api-versions --bootstrap-server kafka-0.kafka-headless.kafka.svc.cluster.local:9092"

# Expected: List of API versions
```

### Performance Tuning

For high-throughput scenarios, adjust these values in `values.yaml`:

```yaml
kafka:
  config:
    numPartitions: 24              # Increase for parallelism
    logRetentionHours: 48          # Reduce for smaller storage
    logSegmentBytes: 536870912     # 512MB (smaller = faster cleanup)

  resources:
    requests:
      memory: "2Gi"
      cpu: "1000m"
    limits:
      memory: "4Gi"
      cpu: "2000m"
```

---

## 📚 References

- [Confluent KRaft Mode Documentation](https://docs.confluent.io/platform/current/kafka-metadata/kraft.html)
- [Kafka KIP-500 (ZooKeeper Removal)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- [Azure AKS Best Practices](https://learn.microsoft.com/en-us/azure/aks/best-practices)

---

## 📝 Architecture Decisions

### Why KRaft Mode?

1. **Simplified Operations**: Single-image deployment, no external dependencies
2. **Better Performance**: Lower latency for metadata operations (Raft vs ZooKeeper)
3. **Future-Proof**: ZooKeeper deprecated in Kafka 4.0+ (2024)
4. **Resource Efficiency**: ~40% less memory usage vs ZooKeeper mode
5. **Faster Startup**: 30s vs 60s+ with ZooKeeper

### Why Single Replica?

- **Cost Optimization**: kafkapool has 1 node (Standard_D2_v3)
- **Use Case**: Development/research pipeline (not production-critical)
- **Easy Scale-Out**: Can add replicas when upgrading to 3-node kafkapool

### Why Helm Chart?

- **GitOps Ready**: Track configuration changes
- **Repeatable Deployments**: Deploy to multiple clusters
- **Values Override**: Environment-specific configs
- **Rollback Support**: `helm rollback` for quick recovery

---

## 🎯 Next Steps

1. **Enable Monitoring**: Deploy Prometheus + Grafana for Kafka metrics
2. **Add Backup**: Schedule PVC snapshots for disaster recovery
3. **Scale Cluster**: Upgrade to 3-node kafkapool for HA
4. **TLS/SASL**: Enable authentication and encryption for production
5. **Consumer Groups**: Deploy research-indexing consumers

---

**Deployment Status:** ✅ Production-Ready
**Kafka Version:** Confluent Platform 7.6.0 (Apache Kafka 3.6.x)
**Deployment Mode:** KRaft (No ZooKeeper)
**Target Cluster:** embedding-aks-ne (North Europe)
**Last Updated:** December 15, 2025
