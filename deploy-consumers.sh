#!/bin/bash

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="consumers"
KAFKA_NAMESPACE="kafka"
KAFKA_BROKER="kafka-0"
KAFKA_HEADLESS_SERVICE="kafka-headless"
KAFKA_EXTERNAL="kafka.rapporteur.ai:9094"
ACR_NAME="ragserviceneacr6881"
IMAGE_NAME="scrape-consumer"
IMAGE_TAG="${IMAGE_TAG:-latest}"
HELM_RELEASE="research-indexing-consumers"
HELM_CHART="./helm/research-indexing-consumers"

echo -e "${BLUE}=================================================================================${NC}"
echo -e "${BLUE}🚀 Research Indexing Consumers - Continuous Deployment${NC}"
echo -e "${BLUE}=================================================================================${NC}"

# Function to print status messages
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Function to create Kafka topic if it doesn't exist
create_kafka_topic() {
    local topic_name=$1
    local partitions=${2:-3}
    local replication_factor=${3:-1}

    print_info "Creating Kafka topic: $topic_name (partitions=$partitions, replication=$replication_factor)"

    kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor "$replication_factor" \
        --if-not-exists 2>&1 | grep -q "Created topic" && print_status "Topic $topic_name created" || print_warning "Topic $topic_name already exists"
}

# Step 1: Verify prerequisites
print_info "Step 1/8: Verifying prerequisites..."

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl not found. Please install kubectl."
    exit 1
fi

# Check if helm is available
if ! command -v helm &> /dev/null; then
    print_error "helm not found. Please install helm."
    exit 1
fi

print_status "CLI tools verified"

# Step 1.1: Verify Kafka deployment
print_info "Verifying Kafka deployment..."

# Check if Kafka namespace exists
if ! kubectl get namespace $KAFKA_NAMESPACE &> /dev/null; then
    print_error "Kafka namespace '$KAFKA_NAMESPACE' does not exist"
    exit 1
fi

# Check if Kafka StatefulSet exists
if ! kubectl get statefulset -n $KAFKA_NAMESPACE kafka &> /dev/null; then
    print_error "Kafka StatefulSet 'kafka' not found in namespace $KAFKA_NAMESPACE"
    exit 1
fi

# Check if Kafka broker pod is running
if ! kubectl get pod -n $KAFKA_NAMESPACE $KAFKA_BROKER &> /dev/null; then
    print_error "Kafka broker pod $KAFKA_BROKER not found in namespace $KAFKA_NAMESPACE"
    exit 1
fi

KAFKA_STATUS=$(kubectl get pod -n $KAFKA_NAMESPACE $KAFKA_BROKER -o jsonpath='{.status.phase}')
if [ "$KAFKA_STATUS" != "Running" ]; then
    print_error "Kafka broker $KAFKA_BROKER is not running (status: $KAFKA_STATUS)"
    exit 1
fi

print_status "Kafka deployment verified"

# Step 1.2: Health check Kafka broker
print_info "Running Kafka health check..."

# Test Kafka broker connectivity by listing topics
if ! kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then
    print_error "Cannot connect to Kafka broker. Bootstrap server may not be ready."
    exit 1
fi

# Get broker metadata
BROKER_COUNT=$(kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null | grep -c "^localhost" || echo "0")
if [ "$BROKER_COUNT" -eq "0" ]; then
    print_warning "Could not retrieve broker information, but topic listing succeeded"
else
    print_status "Kafka broker is healthy ($BROKER_COUNT broker(s) available)"
fi

print_status "Prerequisites verified"

# Step 2: Create namespace if it doesn't exist
print_info "Step 2/8: Ensuring namespace exists..."

if ! kubectl get namespace $NAMESPACE &> /dev/null; then
    kubectl create namespace $NAMESPACE
    print_status "Namespace $NAMESPACE created"
else
    print_warning "Namespace $NAMESPACE already exists"
fi

# Step 3: Copy ACR credentials to consumers namespace
print_info "Step 3/8: Setting up ACR credentials..."

if kubectl get secret acr-secret -n default &> /dev/null; then
    if ! kubectl get secret acr-secret -n $NAMESPACE &> /dev/null; then
        kubectl get secret acr-secret -n default -o yaml | \
            sed "s/namespace: default/namespace: $NAMESPACE/" | \
            kubectl apply -f -
        print_status "ACR credentials copied to $NAMESPACE namespace"
    else
        print_warning "ACR credentials already exist in $NAMESPACE namespace"
    fi
else
    print_warning "ACR secret not found in default namespace. You may need to create it manually."
fi

# Step 4: Create required Kafka topics
print_info "Step 4/8: Creating and verifying Kafka topics..."

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
)

for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    create_kafka_topic "$topic" "$partitions" "$replication"
done

# Verify all topics were created
print_info "Verifying topic creation..."
EXISTING_TOPICS=$(kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)

missing_topics=()
for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    if ! echo "$EXISTING_TOPICS" | grep -q "^${topic}$"; then
        missing_topics+=("$topic")
    fi
done

if [ ${#missing_topics[@]} -gt 0 ]; then
    print_error "Failed to create topics: ${missing_topics[*]}"
    exit 1
fi

# Display topic configuration
print_info "Topic details:"
for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    TOPIC_INFO=$(kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-topics --bootstrap-server localhost:9092 --describe --topic "$topic" 2>/dev/null | head -2 | tail -1)
    echo "  ✓ $topic: $TOPIC_INFO"
done

print_status "Kafka topics created and verified"

# Step 5: Deploy/upgrade Helm chart
print_info "Step 5/8: Deploying consumers via Helm..."

if [ ! -d "$HELM_CHART" ]; then
    print_error "Helm chart not found at $HELM_CHART"
    exit 1
fi

# Check if release exists
if helm list -n $NAMESPACE | grep -q "^$HELM_RELEASE"; then
    print_info "Upgrading existing Helm release..."
    helm upgrade $HELM_RELEASE $HELM_CHART \
        -n $NAMESPACE \
        --set image.tag=$IMAGE_TAG \
        --set image.pullPolicy=Always \
        --wait \
        --timeout 5m
    print_status "Helm release upgraded successfully"
else
    print_info "Installing new Helm release..."
    helm install $HELM_RELEASE $HELM_CHART \
        -n $NAMESPACE \
        --set image.tag=$IMAGE_TAG \
        --set image.pullPolicy=Always \
        --wait \
        --timeout 5m
    print_status "Helm release installed successfully"
fi

# Step 6: Trigger rollout restart to pull latest image
print_info "Step 6/8: Triggering rollout restart..."

kubectl rollout restart deployment -n $NAMESPACE
print_status "Rollout restart triggered"

# Wait for rollout to complete
print_info "Waiting for rollout to complete (this may take a few minutes)..."
sleep 10

# Get list of deployments
deployments=$(kubectl get deployments -n $NAMESPACE -o jsonpath='{.items[*].metadata.name}')

for deployment in $deployments; do
    print_info "Waiting for deployment: $deployment"
    kubectl rollout status deployment/$deployment -n $NAMESPACE --timeout=5m || true
done

print_status "Rollout completed"

# Step 7: Verify deployment
print_info "Step 7/8: Verifying deployment..."

echo ""
print_info "Pod Status:"
kubectl get pods -n $NAMESPACE -o wide

echo ""
print_info "Deployment Status:"
kubectl get deployments -n $NAMESPACE

echo ""
print_info "Checking for errors in consumer logs..."

# Check each consumer for errors (last 10 lines)
consumers=$(kubectl get pods -n $NAMESPACE -o jsonpath='{.items[*].metadata.name}')
has_errors=false

for consumer in $consumers; do
    if kubectl get pod -n $NAMESPACE $consumer -o jsonpath='{.status.phase}' | grep -qE "Running|Succeeded"; then
        error_lines=$(kubectl logs -n $NAMESPACE $consumer --tail=20 2>/dev/null | grep -i "error\|exception\|traceback" | head -3 || true)
        if [ -n "$error_lines" ]; then
            print_warning "Errors found in $consumer:"
            echo "$error_lines"
            has_errors=true
        fi
    fi
done

if [ "$has_errors" = false ]; then
    print_status "No errors detected in consumer logs"
fi

echo ""
print_info "Recent events in namespace:"
kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -10

# Step 8: Comprehensive health check
print_info "Step 8/8: Running comprehensive health checks..."

echo ""
print_info "Kafka Health Summary:"
echo "  Kafka Namespace: $KAFKA_NAMESPACE"
echo "  Kafka Broker: $KAFKA_BROKER"
echo "  Broker Status: $(kubectl get pod -n $KAFKA_NAMESPACE $KAFKA_BROKER -o jsonpath='{.status.phase}')"
echo "  Internal Bootstrap: $KAFKA_BROKER.$KAFKA_HEADLESS_SERVICE.$KAFKA_NAMESPACE.svc.cluster.local:9092"
echo "  External Bootstrap: $KAFKA_EXTERNAL"
echo "  Kafka Topics: $(kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l | tr -d ' ')"

echo ""
print_info "Consumer Health Summary:"
TOTAL_PODS=$(kubectl get pods -n $NAMESPACE --no-headers | wc -l | tr -d ' ')
RUNNING_PODS=$(kubectl get pods -n $NAMESPACE --field-selector=status.phase=Running --no-headers | wc -l | tr -d ' ')
echo "  Total Consumers: $TOTAL_PODS"
echo "  Running Consumers: $RUNNING_PODS"
echo "  Success Rate: $(awk "BEGIN {printf \"%.1f%%\", ($RUNNING_PODS/$TOTAL_PODS)*100}")"

echo ""
print_info "Testing Kafka connectivity from consumer namespace..."
# Deploy a test pod to verify connectivity
TEST_POD="kafka-connectivity-test"
kubectl run $TEST_POD -n $NAMESPACE --image=confluentinc/cp-kafka:7.5.0 --restart=Never --rm -i --quiet --timeout=30s -- \
    kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER.$KAFKA_HEADLESS_SERVICE.$KAFKA_NAMESPACE.svc.cluster.local:9092 &> /dev/null && \
    print_status "Kafka connectivity from consumers namespace: OK" || \
    print_warning "Kafka connectivity test failed (consumers may still work)"

echo ""
print_info "Consumer Group Status:"
# Check consumer groups
CONSUMER_GROUPS=$(kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "embedder|aggregator|generator|scraper|chunker|extractor|summarizer" || echo "")
if [ -n "$CONSUMER_GROUPS" ]; then
    echo "$CONSUMER_GROUPS" | while read -r group; do
        echo "  - $group"
    done
else
    print_warning "No active consumer groups found yet (consumers may be starting up)"
fi

echo ""
echo -e "${BLUE}=================================================================================${NC}"
if [ "$has_errors" = false ]; then
    echo -e "${GREEN}✅ Deployment completed successfully!${NC}"
else
    echo -e "${YELLOW}⚠️  Deployment completed with warnings. Check logs above.${NC}"
fi
echo -e "${BLUE}=================================================================================${NC}"

echo ""
print_info "Useful commands:"
echo "  - View pods:           kubectl get pods -n $NAMESPACE"
echo "  - View logs:           kubectl logs -n $NAMESPACE <pod-name> -f"
echo "  - View topics:         kubectl exec -n $KAFKA_NAMESPACE $KAFKA_BROKER -- kafka-topics --bootstrap-server localhost:9092 --list"
echo "  - Delete deployment:   helm uninstall $HELM_RELEASE -n $NAMESPACE"
echo ""
