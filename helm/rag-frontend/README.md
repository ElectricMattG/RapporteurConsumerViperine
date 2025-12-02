# RAG Service Helm Chart

Helm chart for deploying the RAG Service to Kubernetes.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- PV provisioner support in the underlying infrastructure
- Ingress controller (nginx recommended)

## Installing the Chart

### 1. Add values for your environment

Create a `values-prod.yaml` file with your configuration:

```yaml
image:
  tag: "0.0.1"

ingress:
  hosts:
    - host: rag.yourdomain.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: rag-service-tls
      hosts:
        - rag.yourdomain.com

config:
  embeddingServiceUrl: "http://embedding-service-head-svc.default.svc.cluster.local:8080"
  gcpLocation: "us-central1"

secrets:
  azureStorageConnectionString: "DefaultEndpointsProtocol=https;AccountName=..."
  gcpProjectId: "your-gcp-project-id"

resources:
  limits:
    cpu: 4000m
    memory: 8Gi
  requests:
    cpu: 2000m
    memory: 4Gi

autoscaling:
  minReplicas: 3
  maxReplicas: 20
```

### 2. Install the chart

```bash
# Install with release name 'rag-service'
helm install rag-service ./helm/rag-service -f values-prod.yaml

# Or install to specific namespace
helm install rag-service ./helm/rag-service \
  --namespace rag-service \
  --create-namespace \
  -f values-prod.yaml
```

### 3. Verify installation

```bash
# Check release status
helm status rag-service

# List installed charts
helm list

# Get deployment info
kubectl get all -n rag-service
```

## Upgrading the Chart

```bash
# Upgrade with new values
helm upgrade rag-service ./helm/rag-service -f values-prod.yaml

# Upgrade with new image tag
helm upgrade rag-service ./helm/rag-service \
  --set image.tag=0.0.2 \
  -f values-prod.yaml
```

## Uninstalling the Chart

```bash
helm uninstall rag-service
```

## Configuration

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `2` |
| `image.registry` | Image registry | `192.168.1.3:5000` |
| `image.repository` | Image repository | `rag-service` |
| `image.tag` | Image tag | `0.0.1` |
| `image.pullPolicy` | Image pull policy | `Always` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `8000` |
| `ingress.enabled` | Enable ingress | `true` |
| `ingress.className` | Ingress class | `nginx` |
| `persistence.cache.enabled` | Enable cache PVC | `true` |
| `persistence.cache.size` | Cache PVC size | `1Gi` |
| `persistence.logs.enabled` | Enable logs PVC | `true` |
| `persistence.logs.size` | Logs PVC size | `5Gi` |
| `resources.requests.cpu` | CPU request | `1000m` |
| `resources.requests.memory` | Memory request | `2Gi` |
| `resources.limits.cpu` | CPU limit | `2000m` |
| `resources.limits.memory` | Memory limit | `4Gi` |
| `autoscaling.enabled` | Enable HPA | `true` |
| `autoscaling.minReplicas` | Min replicas | `2` |
| `autoscaling.maxReplicas` | Max replicas | `10` |

### Configuration via values file

The recommended way is to create environment-specific values files:

```bash
# Development
helm install rag-service ./helm/rag-service -f values-dev.yaml

# Staging
helm install rag-service ./helm/rag-service -f values-staging.yaml

# Production
helm install rag-service ./helm/rag-service -f values-prod.yaml
```

### Configuration via command line

```bash
helm install rag-service ./helm/rag-service \
  --set image.tag=0.0.2 \
  --set replicaCount=3 \
  --set autoscaling.maxReplicas=20 \
  --set resources.limits.cpu=4000m
```

## Secrets Management

### Option 1: Via values file (Not recommended for production)

```yaml
secrets:
  azureStorageConnectionString: "your-connection-string"
  gcpProjectId: "your-project-id"
```

### Option 2: External Secrets (Recommended)

Use [external-secrets](https://external-secrets.io/) to pull from cloud secret managers:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: rag-service-secrets
  namespace: rag-service
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcpsm
    kind: SecretStore
  target:
    name: rag-service-secrets
  data:
  - secretKey: AZURE_STORAGE_CONNECTION_STRING
    remoteRef:
      key: azure-storage-connection-string
  - secretKey: PROJECT_ID
    remoteRef:
      key: gcp-project-id
```

### Option 3: Sealed Secrets

Use [sealed-secrets](https://github.com/bitnami-labs/sealed-secrets) for GitOps workflows.

## GCP Authentication

### Option 1: Service Account Key (Works everywhere)

```bash
# Create secret from SA key file
kubectl create secret generic gcp-credentials \
  --from-file=key.json=/path/to/service-account-key.json \
  --namespace=rag-service

# Enable in values
gcpServiceAccount:
  enabled: true
  secretName: gcp-credentials
```

### Option 2: Workload Identity (GKE only)

```yaml
gcpServiceAccount:
  workloadIdentity:
    enabled: true
    serviceAccount: "rag-service@your-project.iam.gserviceaccount.com"
```

Then bind the GCP SA to K8s SA:
```bash
gcloud iam service-accounts add-iam-policy-binding \
  rag-service@your-project.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:your-project.svc.id.goog[rag-service/rag-service]"
```

## Customization Examples

### High Performance Setup

```yaml
replicaCount: 4

resources:
  limits:
    cpu: 8000m
    memory: 16Gi
  requests:
    cpu: 4000m
    memory: 8Gi

autoscaling:
  minReplicas: 4
  maxReplicas: 50
  targetCPUUtilizationPercentage: 60
  targetMemoryUtilizationPercentage: 70

persistence:
  cache:
    size: 2Gi
  logs:
    size: 20Gi
```

### Development Setup

```yaml
replicaCount: 1

autoscaling:
  enabled: false

resources:
  limits:
    cpu: 1000m
    memory: 2Gi
  requests:
    cpu: 500m
    memory: 1Gi

ingress:
  hosts:
    - host: rag-dev.local

persistence:
  cache:
    size: 500Mi
  logs:
    size: 1Gi
```

## Monitoring

### Prometheus Metrics

If using Prometheus, add ServiceMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: rag-service
  namespace: rag-service
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: rag-service
  endpoints:
  - port: http
    path: /metrics
```

### Grafana Dashboard

Import dashboard ID: [Create custom dashboard based on FastAPI metrics]

## Troubleshooting

### Check Helm Release

```bash
# Get release status
helm status rag-service

# Get release values
helm get values rag-service

# Get full manifest
helm get manifest rag-service
```

### Debug Installation

```bash
# Dry run to see generated manifests
helm install rag-service ./helm/rag-service --dry-run --debug

# Template without installing
helm template rag-service ./helm/rag-service -f values-prod.yaml
```

### Rollback

```bash
# List revisions
helm history rag-service

# Rollback to previous version
helm rollback rag-service

# Rollback to specific revision
helm rollback rag-service 2
```

## Advanced Usage

### Using with ArgoCD

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: rag-service
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/ElectricMattG/RAGService
    targetRevision: main
    path: helm/rag-service
    helm:
      valueFiles:
        - values-prod.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: rag-service
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

### Using with Flux

```yaml
apiVersion: helm.toolkit.fluxcd.io/v2beta1
kind: HelmRelease
metadata:
  name: rag-service
  namespace: rag-service
spec:
  interval: 5m
  chart:
    spec:
      chart: ./helm/rag-service
      sourceRef:
        kind: GitRepository
        name: rag-service
        namespace: flux-system
  values:
    image:
      tag: 0.0.1
    # ... other values
```
