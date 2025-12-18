# Kafka Helm Chart

This Helm chart deploys Apache Kafka with ZooKeeper as a sidecar container for the Research Indexing Consumers pipeline.

## Architecture

This chart uses a **sidecar pattern** where each Kafka pod runs two containers:
- **Kafka** (main container) - Message broker for research indexing pipeline
- **ZooKeeper** (sidecar container) - Cluster coordination

This approach simplifies deployment and ensures Kafka and ZooKeeper are co-located on the same node.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.0+
- Azure Container Registry with Kafka and ZooKeeper images pushed
- Storage class for persistent volumes (default: `managed-premium`)

## Installing the Chart

```bash
# Add namespace
kubectl create namespace kafka

# Install chart
helm install kafka ./helm/kafka \
  --namespace kafka \
  --set image.registry=ragserviceneacr6881.azurecr.io

# Or with custom values
helm install kafka ./helm/kafka \
  --namespace kafka \
  --values custom-values.yaml
```

## Configuration

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `kafka.replicas` | Number of Kafka broker pods | `3` |
| `kafka.persistence.size` | Kafka data volume size | `100Gi` |
| `zookeeper.persistence.size` | ZooKeeper data volume size | `20Gi` |
| `image.registry` | Container registry URL | `ragserviceneacr6881.azurecr.io` |
| `kafka.config.defaultReplicationFactor` | Default replication factor | `3` |
| `kafka.config.numPartitions` | Default number of partitions | `12` |

### Resource Limits

**Kafka:**
- Requests: 2Gi RAM, 1 CPU
- Limits: 4Gi RAM, 2 CPU

**ZooKeeper:**
- Requests: 512Mi RAM, 250m CPU
- Limits: 1Gi RAM, 500m CPU

## Accessing Kafka

### From within the cluster:

```bash
# Bootstrap servers
kafka-0.kafka-headless.scrape-pipeline.svc.cluster.local:9092
kafka-1.kafka-headless.scrape-pipeline.svc.cluster.local:9092
kafka-2.kafka-headless.scrape-pipeline.svc.cluster.local:9092

# Or use the service
kafka.scrape-pipeline.svc.cluster.local:9092
```

### Create topics:

```bash
# Exec into a Kafka pod
kubectl exec -it kafka-0 -n scrape-pipeline -c kafka -- bash

# Create topic
kafka-topics --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 12 \
  --replication-factor 3

# List topics
kafka-topics --list --bootstrap-server localhost:9092
```

## Monitoring

JMX metrics are exposed on port 5556 for Prometheus scraping.

## Upgrading

```bash
helm upgrade kafka ./helm/kafka \
  --namespace kafka \
  --values values.yaml
```

## Uninstalling

```bash
helm uninstall kafka --namespace kafka

# Clean up PVCs (optional)
kubectl delete pvc -n scrape-pipeline -l app.kubernetes.io/name=kafka
```

## Production Recommendations

1. **Use separate StatefulSets** for Kafka and ZooKeeper in production (not sidecar pattern)
2. **Enable authentication** with SASL/SCRAM or mTLS
3. **Configure resource quotas** based on workload
4. **Set up monitoring** with Prometheus and Grafana
5. **Use node affinity** to spread brokers across availability zones
6. **Tune JVM settings** for better performance
7. **Regular backups** of ZooKeeper data

## Troubleshooting

### Check pod status:
```bash
kubectl get pods -n scrape-pipeline -l app.kubernetes.io/name=kafka
```

### Check logs:
```bash
# Kafka logs
kubectl logs -n scrape-pipeline kafka-0 -c kafka

# ZooKeeper logs
kubectl logs -n scrape-pipeline kafka-0 -c zookeeper
```

### Check PVCs:
```bash
kubectl get pvc -n scrape-pipeline
```

### Exec into pod:
```bash
kubectl exec -it kafka-0 -n scrape-pipeline -c kafka -- bash
```
