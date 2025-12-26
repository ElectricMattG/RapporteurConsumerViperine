#!/bin/bash
# Deploy Kafka with External Access to Kubernetes
#
# This script deploys Kafka 7.5.0 with ZooKeeper to mirror the local setup
# and configures external access via Ingress with authentication.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="kafka"
RELEASE_NAME="kafka"
CHART_PATH="$SCRIPT_DIR/helm/kafka-chart"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Kafka Deployment - External Access Configuration${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo -e "${YELLOW}📋 Checking prerequisites...${NC}"

if ! command_exists kubectl; then
    echo -e "${RED}❌ kubectl is not installed${NC}"
    exit 1
fi

if ! command_exists helm; then
    echo -e "${RED}❌ helm is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites satisfied${NC}"
echo ""

# Check if namespace exists
echo -e "${YELLOW}🔍 Checking namespace...${NC}"
if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Namespace '$NAMESPACE' exists${NC}"
else
    echo -e "${YELLOW}📦 Creating namespace '$NAMESPACE'...${NC}"
    kubectl create namespace "$NAMESPACE"
    echo -e "${GREEN}✅ Namespace created${NC}"
fi
echo ""

# Check for existing Kafka deployment
echo -e "${YELLOW}🔍 Checking for existing Kafka deployment...${NC}"
if helm list -n "$NAMESPACE" | grep -q "^$RELEASE_NAME"; then
    echo -e "${YELLOW}⚠️  Existing Kafka deployment found${NC}"
    echo -e "${YELLOW}   This will upgrade the existing deployment${NC}"
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}❌ Deployment cancelled${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ No existing deployment found${NC}"
fi
echo ""

# Check for NGINX Ingress Controller
echo -e "${YELLOW}🔍 Checking for NGINX Ingress Controller...${NC}"
if kubectl get pods -n ingress-nginx -l app.kubernetes.io/name=ingress-nginx | grep -q "Running"; then
    echo -e "${GREEN}✅ NGINX Ingress Controller is running${NC}"
else
    echo -e "${YELLOW}⚠️  NGINX Ingress Controller not found${NC}"
    echo -e "${YELLOW}   External access requires NGINX Ingress Controller${NC}"
    echo ""
    echo -e "${BLUE}To install NGINX Ingress Controller:${NC}"
    echo -e "${BLUE}helm upgrade --install ingress-nginx ingress-nginx \\${NC}"
    echo -e "${BLUE}  --repo https://kubernetes.github.io/ingress-nginx \\${NC}"
    echo -e "${BLUE}  --namespace ingress-nginx --create-namespace${NC}"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}❌ Deployment cancelled${NC}"
        exit 1
    fi
fi
echo ""

# Check for cert-manager
echo -e "${YELLOW}🔍 Checking for cert-manager...${NC}"
if kubectl get pods -n cert-manager -l app.kubernetes.io/name=cert-manager | grep -q "Running"; then
    echo -e "${GREEN}✅ cert-manager is running${NC}"
else
    echo -e "${YELLOW}⚠️  cert-manager not found${NC}"
    echo -e "${YELLOW}   TLS certificates require cert-manager${NC}"
    echo ""
    echo -e "${BLUE}To install cert-manager:${NC}"
    echo -e "${BLUE}helm install cert-manager jetstack/cert-manager \\${NC}"
    echo -e "${BLUE}  --namespace cert-manager --create-namespace \\${NC}"
    echo -e "${BLUE}  --set installCRDs=true${NC}"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}❌ Deployment cancelled${NC}"
        exit 1
    fi
fi
echo ""

# Deploy Kafka
echo -e "${GREEN}🚀 Deploying Kafka with external access...${NC}"
echo ""

helm upgrade --install "$RELEASE_NAME" "$CHART_PATH" \
    --namespace "$NAMESPACE" \
    --create-namespace \
    --wait \
    --timeout 10m \
    --values "$CHART_PATH/values.yaml"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Kafka deployed successfully!${NC}"
else
    echo ""
    echo -e "${RED}❌ Kafka deployment failed${NC}"
    exit 1
fi
echo ""

# Get deployment info
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Deployment Information${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Get pod status
echo -e "${YELLOW}📦 Pod Status:${NC}"
kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
echo ""

# Wait for Kafka to be fully ready
echo -e "${YELLOW}⏳ Waiting for Kafka to be ready...${NC}"
kubectl wait --for=condition=ready pod/"$RELEASE_NAME-0" -n "$NAMESPACE" --timeout=300s
echo -e "${GREEN}✅ Kafka is ready${NC}"
echo ""

# Create required Kafka topics
echo -e "${YELLOW}📝 Creating Kafka topics...${NC}"

# Define all required topics for the pipeline
# Format: topic_name:partitions:replication_factor
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

for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    echo -e "  Creating topic: ${BLUE}$topic${NC} (partitions=$partitions, replication=$replication)"

    kubectl exec -n "$NAMESPACE" "$RELEASE_NAME-0" -c kafka -- kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --if-not-exists 2>&1 | grep -E "Created|already exists" | sed 's/^/    /'
done

echo -e "${GREEN}✅ Topics created${NC}"
echo ""

# Verify topics
echo -e "${YELLOW}📋 Verifying topics...${NC}"
TOPIC_COUNT=$(kubectl exec -n "$NAMESPACE" "$RELEASE_NAME-0" -c kafka -- kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -v "^__" | wc -l | tr -d ' ')
echo -e "${GREEN}  Total topics: $TOPIC_COUNT${NC}"
echo ""

# Get services
echo -e "${YELLOW}🌐 Services:${NC}"
kubectl get svc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
echo ""

# Get ingress
echo -e "${YELLOW}🔗 Ingress:${NC}"
kubectl get ingress -n "$NAMESPACE"
echo ""

# Get NGINX Ingress Controller IP (used for external access)
echo -e "${YELLOW}📍 External Access (via NGINX TCP Proxy):${NC}"
NGINX_IP=$(kubectl get svc ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")

if [ "$NGINX_IP" != "pending" ] && [ -n "$NGINX_IP" ]; then
    echo -e "${GREEN}  NGINX Ingress IP: $NGINX_IP${NC}"
    echo -e "${YELLOW}  DNS Record: kafka.rapporteur.ai → $NGINX_IP${NC}"
    echo -e "${BLUE}  TCP Proxy: Port 9094 → kafka/kafka-external:9094${NC}"
else
    echo -e "${YELLOW}  NGINX Ingress IP: Pending${NC}"
    echo ""
    echo -e "${BLUE}  Check the IP with:${NC}"
    echo -e "${BLUE}  kubectl get svc ingress-nginx-controller -n ingress-nginx -w${NC}"
fi
echo ""

# Bootstrap servers
echo -e "${YELLOW}🔌 Bootstrap Servers:${NC}"
echo -e "${GREEN}  Internal (in-cluster):${NC}"
echo -e "    $RELEASE_NAME-0.$RELEASE_NAME-headless.$NAMESPACE.svc.cluster.local:9092"
echo ""
echo -e "${GREEN}  External (remote access via NGINX TCP proxy):${NC}"
echo -e "    kafka.rapporteur.ai:9094"
echo -e "    (or directly: $NGINX_IP:9094)"
echo ""

# Authentication info
echo -e "${YELLOW}🔐 Authentication:${NC}"
echo -e "  Kafka Protocol: PLAINTEXT (no authentication)"
echo -e "  HTTP Ingress: Basic Authentication (for HTTP endpoints only)"
echo -e "  Note: NGINX TCP proxy forwards Kafka protocol directly"
echo ""

# Next steps
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Next Steps${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "1. ${YELLOW}Configure NGINX TCP Proxy (if not done):${NC}"
echo -e "   kubectl apply -f - <<EOF"
echo -e "   apiVersion: v1"
echo -e "   kind: ConfigMap"
echo -e "   metadata:"
echo -e "     name: tcp-services"
echo -e "     namespace: ingress-nginx"
echo -e "   data:"
echo -e "     \"9094\": \"$NAMESPACE/$RELEASE_NAME-external:9094\""
echo -e "   EOF"
echo ""
echo -e "   # Ensure NGINX controller uses TCP ConfigMap:"
echo -e "   kubectl patch deployment ingress-nginx-controller -n ingress-nginx --type=json \\"
echo -e "     -p='[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--tcp-services-configmap=ingress-nginx/tcp-services\"}]'"
echo ""
echo -e "   # Expose port 9094 on NGINX service:"
echo -e "   kubectl patch svc ingress-nginx-controller -n ingress-nginx --type=json \\"
echo -e "     -p='[{\"op\":\"add\",\"path\":\"/spec/ports/-\",\"value\":{\"name\":\"kafka\",\"port\":9094,\"targetPort\":9094,\"protocol\":\"TCP\"}}]'"
echo ""
echo -e "2. ${YELLOW}Verify DNS:${NC}"
echo -e "   dig kafka.rapporteur.ai +short  # Should return $NGINX_IP"
echo ""
echo -e "3. ${YELLOW}Test internal access:${NC}"
echo -e "   kubectl run -it --rm kafka-test --image=confluentinc/cp-kafka:7.5.0 \\"
echo -e "     --namespace=$NAMESPACE --restart=Never -- \\"
echo -e "     kafka-topics --bootstrap-server $RELEASE_NAME-0.$RELEASE_NAME-headless:9092 --list"
echo ""
echo -e "4. ${YELLOW}Test external access:${NC}"
echo -e "   kafka-topics --bootstrap-server kafka.rapporteur.ai:9094 --list"
echo ""
echo -e "5. ${YELLOW}View logs:${NC}"
echo -e "   kubectl logs -n $NAMESPACE $RELEASE_NAME-0 -c kafka -f"
echo ""
echo -e "${GREEN}✅ Deployment complete!${NC}"
echo ""
echo -e "For more information, see:"
echo -e "  - helm/kafka-chart/EXTERNAL_ACCESS.md"
echo -e "  - KAFKA_VERSION_COMPATIBILITY.md"
