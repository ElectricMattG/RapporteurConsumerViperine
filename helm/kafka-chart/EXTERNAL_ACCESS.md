# Kafka Helm Chart - External Access Configuration

This Kafka deployment is configured to mirror the local Docker Compose setup with ZooKeeper and includes external access via Ingress.

## Architecture

The deployment consists of:
- **Kafka 7.5.0** (StatefulSet with 1 replica)
- **ZooKeeper 7.5.0** (sidecar container)
- **Internal Service** (ClusterIP for in-cluster access)
- **External Service** (LoadBalancer for external access)
- **Ingress** (NGINX with TLS and Basic Auth)

## Local vs Cloud Configuration

| Feature | Local (docker-compose) | Cloud (Kubernetes) |
|---------|----------------------|-------------------|
| Kafka Version | 7.5.0 | 7.5.0 |
| Coordination | ZooKeeper | ZooKeeper (sidecar) |
| Internal Listener | `kafka:29092` | `kafka-0.kafka-headless:9092` |
| External Listener | `localhost:9092` | `kafka.rappoteur.ai:9094` |
| Authentication | None | Basic Auth (Ingress) |
| TLS | No | Yes (Let's Encrypt) |

## Listeners Configuration

The Kafka broker exposes two listeners:

1. **PLAINTEXT** (port 9092)
   - Internal cluster communication
   - Used by consumers/producers within Kubernetes
   - Bootstrap servers: `kafka-0.kafka-headless.research-indexing-consumers.svc.cluster.local:9092`

2. **EXTERNAL** (port 9094)
   - External access via LoadBalancer and Ingress
   - Used by remote scripts and external clients
   - Bootstrap servers: `kafka.rappoteur.ai:9094`

## Deployment

### Prerequisites

1. **NGINX Ingress Controller**
   ```bash
   helm upgrade --install ingress-nginx ingress-nginx \
     --repo https://kubernetes.github.io/ingress-nginx \
     --namespace ingress-nginx --create-namespace
   ```

2. **cert-manager** (for TLS)
   ```bash
   helm install cert-manager jetstack/cert-manager \
     --namespace cert-manager \
     --create-namespace \
     --set installCRDs=true
   ```

3. **ClusterIssuer for Let's Encrypt**
   ```bash
   kubectl apply -f - <<EOF
   apiVersion: cert-manager.io/v1
   kind: ClusterIssuer
   metadata:
     name: letsencrypt-prod
   spec:
     acme:
       server: https://acme-v02.api.letsencrypt.org/directory
       email: your-email@example.com
       privateKeySecretRef:
         name: letsencrypt-prod
       solvers:
       - http01:
           ingress:
             class: nginx
   EOF
   ```

### Deploy Kafka

```bash
# From the repository root
helm upgrade --install kafka ./helm/kafka-chart \
  --namespace research-indexing-consumers \
  --create-namespace \
  --wait
```

### Verify Deployment

```bash
# Check pods
kubectl get pods -n research-indexing-consumers -l app.kubernetes.io/name=kafka

# Check services
kubectl get svc -n research-indexing-consumers

# Check ingress
kubectl get ingress -n research-indexing-consumers

# Check external LoadBalancer IP
kubectl get svc kafka-external -n research-indexing-consumers -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

## DNS Configuration

Point your DNS A record for `kafka.rappoteur.ai` to the LoadBalancer IP:

```bash
# Get the LoadBalancer IP
EXTERNAL_IP=$(kubectl get svc kafka-external -n research-indexing-consumers -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Point kafka.rappoteur.ai DNS A record to: $EXTERNAL_IP"
```

## Authentication

The Ingress uses HTTP Basic Authentication.

### Default Credentials (CHANGE IN PRODUCTION!)
- **Username**: `kafka`
- **Password**: `kafka123`

### Generate New Credentials

```bash
# Generate new htpasswd entry
./generate-kafka-auth.sh myuser mypassword

# Update values.yaml with the output
# Then upgrade the deployment
helm upgrade kafka ./helm/kafka-chart -n research-indexing-consumers
```

## Using Kafka Externally

### Python Client Example

```python
from confluent_kafka import Producer
import base64

# For authenticated access via Ingress
config = {
    'bootstrap.servers': 'kafka.rappoteur.ai:9094',
    'security.protocol': 'PLAINTEXT',  # Note: PLAINTEXT for Kafka protocol
    # Authentication is handled at the Ingress level (HTTP Basic Auth)
    # You'll need to configure your HTTP client accordingly
}

producer = Producer(config)

# Produce a message
producer.produce('web-scraping-requests', key='test', value='Hello from external!')
producer.flush()
```

### Using kafka-python with Ingress Auth

```python
from kafka import KafkaProducer
import requests
from requests.auth import HTTPBasicAuth

# Note: Direct Kafka protocol through NGINX Ingress with Basic Auth
# may require custom configuration or using a TCP load balancer instead

bootstrap_servers = 'kafka.rappoteur.ai:9094'
producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    # Add any additional SSL/auth config as needed
)

# Send message
producer.send('web-scraping-requests', b'test message')
producer.flush()
```

### cURL Example (for HTTP-based Kafka REST Proxy)

If you deploy Kafka REST Proxy:
```bash
curl -X POST \
  -u kafka:kafka123 \
  https://kafka.rappoteur.ai/topics/web-scraping-requests \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records":[{"value":{"url":"https://example.com"}}]}'
```

## Troubleshooting

### Check Kafka Logs
```bash
kubectl logs -n research-indexing-consumers kafka-0 -c kafka --tail=100 -f
```

### Check ZooKeeper Logs
```bash
kubectl logs -n research-indexing-consumers kafka-0 -c zookeeper --tail=100 -f
```

### Test Internal Connectivity
```bash
kubectl run -it --rm kafka-test --image=confluentinc/cp-kafka:7.5.0 \
  --namespace=research-indexing-consumers \
  --restart=Never -- \
  kafka-topics --bootstrap-server kafka-0.kafka-headless:9092 --list
```

### Test External Connectivity
```bash
# From your local machine (after DNS is configured)
kafka-topics --bootstrap-server kafka.rappoteur.ai:9094 --list
```

### Ingress Issues
```bash
# Check ingress controller logs
kubectl logs -n ingress-nginx -l app.kubernetes.io/name=ingress-nginx

# Describe ingress for events
kubectl describe ingress kafka-ingress -n research-indexing-consumers
```

## Migration from KRaft (8.0.0) to ZooKeeper (7.5.0)

If migrating from an existing KRaft deployment:

```bash
# Delete existing StatefulSet (preserves data)
kubectl delete statefulset kafka -n research-indexing-consumers

# Delete PVCs (WARNING: data loss!)
kubectl delete pvc -n research-indexing-consumers -l app.kubernetes.io/name=kafka

# Deploy new version
helm upgrade --install kafka ./helm/kafka-chart -n research-indexing-consumers

# Recreate topics (auto-created on first message)
```

## Security Considerations

1. **Change Default Credentials**: Update `ingress.auth.htpasswd` in values.yaml
2. **TLS is Required**: Ingress forces HTTPS redirect
3. **Network Policies**: Consider adding NetworkPolicies to restrict access
4. **SASL Authentication**: For production, consider enabling SASL/SCRAM or mTLS at Kafka level
5. **Firewall Rules**: Restrict LoadBalancer to specific IP ranges if possible

## Performance Tuning

For production workloads, adjust:

```yaml
kafka:
  replicas: 3  # High availability
  config:
    numPartitions: 24  # More parallelism
    defaultReplicationFactor: 3
  resources:
    limits:
      memory: "4Gi"
      cpu: "2000m"
  persistence:
    size: 200Gi
```

## Monitoring

Consider adding:
- **Kafka Exporter** for Prometheus metrics
- **Grafana dashboards** for visualization
- **Kafka UI** for topic management (deployed separately)

See [KAFKA_VERSION_COMPATIBILITY.md](../../KAFKA_VERSION_COMPATIBILITY.md) for version details.
