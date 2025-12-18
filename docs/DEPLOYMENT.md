# Research Indexing Consumers - Deployment Guide

## Overview

This guide covers the deployment of research indexing Kafka consumers to AKS (Azure Kubernetes Service). The deployment includes comprehensive health checks for Kafka infrastructure and automated topic creation.

## Prerequisites

### Local Development
- Docker Desktop installed and running
- Azure CLI (`az`) installed and logged in
- kubectl configured for your AKS cluster
- Helm 3.x installed

### Jenkins/CI Environment
- Jenkins with Docker support
- Azure credentials configured
- kubectl access to AKS cluster
- ACR credentials stored in Jenkins

## Deployment Scripts

### 1. `build-and-push.sh`
Builds and pushes the Docker image to Azure Container Registry.

**Usage:**
```bash
# Build and push with 'latest' tag
./build-and-push.sh

# Build and push with custom tag
IMAGE_TAG=v1.2.3 ./build-and-push.sh
```

**What it does:**
- Logs in to Azure Container Registry
- Builds Docker image for linux/amd64 platform
- Tags image with specified tag and 'latest'
- Pushes both tags to ACR

### 2. `deploy-consumers.sh`
Comprehensive deployment script with health checks and verification.

**Usage:**
```bash
# Deploy with latest image
./deploy-consumers.sh

# Deploy with specific image tag
IMAGE_TAG=v1.2.3 ./deploy-consumers.sh
```

**Deployment Steps:**

#### Step 1/8: Prerequisites Verification
- Checks kubectl and helm CLI availability
- Verifies Kafka namespace exists
- Confirms Kafka StatefulSet is deployed
- Validates Kafka broker pod is running
- Tests Kafka broker connectivity

#### Step 2/8: Namespace Setup
- Creates `consumers` namespace if not exists
- Labels namespace appropriately

#### Step 3/8: ACR Credentials
- Copies ACR credentials from default namespace
- Ensures ImagePullSecrets are configured

#### Step 4/8: Kafka Topics Creation
Creates and verifies all required topics:
- `web-scrape-requests` - Input for web scraping
- `document-chunking` - Document chunking pipeline
- `chunk-embedding` - Chunk embeddings
- `qa-generation` - Q&A generation
- `qa-embedding` - Q&A embedding
- `qa-indexed` - Indexed Q&A pairs
- `session-complete` - Session completion events
- `dead-letter` - Failed message handling

**Topic Configuration:**
- Partitions: 3 (for parallelism)
- Replication Factor: 1 (adjust for production)

#### Step 5/8: Helm Deployment
- Installs or upgrades Helm chart
- Sets image pull policy to "Always"
- Waits for deployment to complete (5 min timeout)

#### Step 6/8: Rollout Restart
- Triggers rollout restart of all deployments
- Forces pods to pull latest image
- Waits for rollout completion per deployment

#### Step 7/8: Deployment Verification
- Lists all pods with wide output
- Shows deployment status
- Checks logs for errors
- Displays recent namespace events

#### Step 8/8: Comprehensive Health Checks

**Kafka Health:**
- Broker status and phase
- Total topic count
- Topic configuration details

**Consumer Health:**
- Total vs running pod count
- Success rate percentage
- Kafka connectivity from consumer namespace

**Consumer Groups:**
- Lists active consumer groups
- Shows group membership

## Jenkins Pipeline

The `Jenkinsfile` defines a complete CI/CD pipeline:

### Pipeline Stages

1. **Checkout** - Clone repository
2. **Build Docker Image** - Build for linux/amd64
3. **Push to ACR** - Push with build number and latest tags
4. **Configure kubectl** - Setup K8s access
5. **Deploy to AKS** - Run deployment script
6. **Verify Deployment** - Check all pods running
7. **Health Check** - Validate Kafka and consumers

### Jenkins Configuration

**Required Credentials:**
- `kubeconfig-embedding-aks` - Kubeconfig file
- `acr-credentials` - ACR username/password

**Environment Variables:**
```groovy
ACR_NAME = 'ragserviceneacr6881'
IMAGE_NAME = 'scrape-consumer'
AKS_CLUSTER = 'embedding-aks-ne'
AKS_RESOURCE_GROUP = 'rg-embedding-aks-ne'
```

## Consumer Architecture

### Deployed Consumers

1. **web-scraper** - Scrapes web content from URLs
2. **document-chunker** - Chunks documents into manageable pieces
3. **chunk-embedder** - Generates embeddings for chunks
4. **qa-generator** - Generates Q&A pairs from documents
5. **qa-embedder** - Generates embeddings for Q&A pairs
6. **citation-extractor** - Extracts citations from documents
7. **summarizer** - Summarizes document content
8. **index-aggregator** - Aggregates and uploads to Azure Storage

### Resource Allocation
- CPU Request: 400m per consumer
- Memory Request: 2500Mi per consumer
- CPU Limit: 1000m per consumer
- Memory Limit: 6Gi per consumer

### Node Affinity
All consumers are deployed to the `ridxpool` node pool with:
- Node Selector: `agentpool: ridxpool`
- Toleration: `workload=research-indexing:NoSchedule`

## Manual Operations

### View Pod Status
```bash
kubectl get pods -n consumers -o wide
```

### Check Consumer Logs
```bash
# Follow logs for specific consumer
kubectl logs -n consumers <pod-name> -f

# View last 100 lines
kubectl logs -n consumers <pod-name> --tail=100
```

### List Kafka Topics
```bash
kubectl exec -n kafka kafka-direct-0 -- \
  kafka-topics --bootstrap-server localhost:9092 --list
```

### Check Topic Details
```bash
kubectl exec -n kafka kafka-direct-0 -- \
  kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic <topic-name>
```

### View Consumer Groups
```bash
kubectl exec -n kafka kafka-direct-0 -- \
  kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Check Consumer Group Lag
```bash
kubectl exec -n kafka kafka-direct-0 -- \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <consumer-group-name>
```

### Restart Single Consumer
```bash
kubectl rollout restart deployment/<consumer-name> -n consumers
```

### Delete Deployment
```bash
helm uninstall research-indexing-consumers -n consumers
```

## Troubleshooting

### Pod Stuck in ImagePullBackOff
**Cause:** ACR credentials not configured
**Fix:**
```bash
kubectl get secret acr-secret -n default -o yaml | \
  sed 's/namespace: default/namespace: consumers/' | \
  kubectl apply -f -
```

### Pod CrashLoopBackOff
**Check logs:**
```bash
kubectl logs -n consumers <pod-name> --previous
```

**Common issues:**
- Missing Kafka topics - run deployment script
- Kafka broker not reachable - check Kafka health
- Missing environment variables - check Helm values

### Topic Not Found Error
**Create topics manually:**
```bash
kubectl exec -n kafka kafka-direct-0 -- \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic <topic-name> \
  --partitions 3 --replication-factor 1
```

### Insufficient Resources
**Check node capacity:**
```bash
kubectl describe node <node-name> | grep -A 10 "Allocated resources"
```

**Scale down if needed:**
```bash
kubectl scale deployment <consumer-name> -n consumers --replicas=0
```

## Monitoring

### Key Metrics to Monitor
- Pod restart count
- Consumer group lag
- Memory/CPU usage
- Error rate in logs
- Kafka broker health

### Set Up Monitoring
```bash
# Watch pod status
kubectl get pods -n consumers -w

# Monitor resource usage
kubectl top pods -n consumers

# Check events
kubectl get events -n consumers --sort-by='.lastTimestamp'
```

## Production Considerations

### High Availability
- Increase topic replication factor to 3
- Scale consumers based on partition count
- Deploy Kafka across multiple zones

### Resource Optimization
- Adjust memory requests based on actual usage
- Use HPA (Horizontal Pod Autoscaler) for dynamic scaling
- Monitor and optimize based on consumer lag

### Security
- Use private ACR endpoints
- Enable Pod Security Standards
- Rotate ACR credentials regularly
- Use Azure Key Vault for secrets

### Backup and Recovery
- Regular backups of Kafka topics
- Document disaster recovery procedures
- Test recovery scenarios

## Support

For issues or questions:
1. Check logs: `kubectl logs -n consumers <pod-name>`
2. Review recent events: `kubectl get events -n consumers`
3. Verify Kafka health: `./deploy-consumers.sh` (health checks only)
4. Consult architecture documentation: `PIPELINE_ARCHITECTURE.md`
