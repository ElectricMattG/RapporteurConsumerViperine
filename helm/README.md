# Monitoring Helm Charts

Helm charts for deploying Prometheus and Grafana to monitor Kafka consumers.

## Charts

### Prometheus
Metrics collection and storage for all 9 Kafka consumers.

### Grafana
Visualization and dashboards for monitoring data.

## Prerequisites

- Kubernetes cluster with monitoring node pool
- Helm 3.x installed
- kubectl configured

## Quick Start

### Deploy Prometheus

```bash
helm install prometheus ./helm/prometheus \
  --namespace monitoring \
  --create-namespace
```

### Deploy Grafana

```bash
helm install grafana ./helm/grafana \
  --namespace monitoring
```

### Deploy Both Together

```bash
helm install monitoring ./helm/grafana \
  --namespace monitoring \
  --create-namespace \
  --set prometheus.enabled=true
```

## Configuration

### Prometheus Values

```yaml
# values.yaml
persistence:
  size: 50Gi

retention:
  time: 30d

kafkaConsumers:
  - name: web_scraper
    target: 'host.docker.internal:8001'
```

### Grafana Values

```yaml
# values.yaml
adminPassword: "change-me"

service:
  type: LoadBalancer

datasources:
  prometheus:
    url: http://prometheus:9090
```

## Accessing Services

### Get Grafana LoadBalancer IP

```bash
kubectl get svc -n monitoring grafana
```

### Port Forward (Development)

```bash
# Prometheus
kubectl port-forward -n monitoring svc/prometheus 9090:9090

# Grafana
kubectl port-forward -n monitoring svc/grafana 3000:3000
```

## Monitoring Consumers

All 9 Kafka consumers are automatically scraped:
- web_scraper (port 8001)
- document_chunker (port 8002)
- chunk_embedder (port 8003)
- qa_generator (port 8004)
- qa_embedder (port 8005)
- summarizer (port 8006)
- citation_extractor (port 8007)
- index_aggregator (port 8008)
- dead_letter (port 8009)

## Upgrading

```bash
helm upgrade prometheus ./helm/prometheus -n monitoring
helm upgrade grafana ./helm/grafana -n monitoring
```

## Uninstalling

```bash
helm uninstall prometheus -n monitoring
helm uninstall grafana -n monitoring
```
