#!/bin/bash

set -e  # Exit on any error
set -o pipefail  # Catch errors in pipes

# Retry function for network operations
retry() {
    local max_attempts=3
    local delay=5
    local attempt=1
    local cmd="$@"

    while [ $attempt -le $max_attempts ]; do
        echo "  Attempt $attempt/$max_attempts..."
        if $cmd; then
            return 0
        fi

        if [ $attempt -lt $max_attempts ]; then
            echo "  Failed. Retrying in ${delay}s..."
            sleep $delay
            delay=$((delay * 2))  # Exponential backoff
        fi
        attempt=$((attempt + 1))
    done

    echo "  Command failed after $max_attempts attempts"
    return 1
}

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ACR_NAME="ragserviceneacr6881"
ACR_REGISTRY="${ACR_NAME}.azurecr.io"
IMAGE_NAME="scrape-consumer"
IMAGE_TAG="${IMAGE_TAG:-latest}"
DOCKERFILE="Dockerfile"

echo -e "${BLUE}=================================================================================${NC}"
echo -e "${BLUE}🔨 Building, Pushing, and Deploying Docker Image${NC}"
echo -e "${BLUE}=================================================================================${NC}"

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Step 1: Login to ACR
print_info "Step 1/4: Logging in to Azure Container Registry..."
az acr login --name $ACR_NAME
print_status "Logged in to ACR"

# Step 2: Build the image
print_info "Step 2/4: Building Docker image..."
print_info "Image: ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"

retry docker buildx build \
    --platform linux/amd64 \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:latest \
    -f $DOCKERFILE \
    .

print_status "Image built successfully"

# Step 3: Push to ACR
print_info "Step 3/4: Pushing image to ACR..."

# Push both tags (buildx automatically tags both during build, this just pushes them)
retry docker push ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}

# Only push latest tag separately if it's different from IMAGE_TAG
if [ "${IMAGE_TAG}" != "latest" ]; then
    retry docker push ${ACR_REGISTRY}/${IMAGE_NAME}:latest
fi

print_status "Image pushed successfully"

# Step 4: Deploy with Helm
print_info "Step 4/4: Deploying with Helm..."

# Force pods to restart by setting a timestamp annotation
# Note: This also deploys consumer-services.yaml for Prometheus scraping
TIMESTAMP=$(date +%s)
helm upgrade research-indexing-consumers ./helm/research-indexing-consumers -n consumers \
    --set-string podAnnotations.deployedAt="$TIMESTAMP" \
    --wait

print_status "Helm deployment updated successfully"

echo ""
echo -e "${BLUE}=================================================================================${NC}"
echo -e "${GREEN}✅ Build, push, and deploy completed successfully!${NC}"
echo -e "${BLUE}=================================================================================${NC}"
echo ""
echo -e "${BLUE}Image tags pushed:${NC}"
echo "  - ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
echo "  - ${ACR_REGISTRY}/${IMAGE_NAME}:latest"
echo ""
echo -e "${BLUE}Deployed:${NC}"
echo "  - Release: research-indexing-consumers"
echo "  - Namespace: consumers"
echo ""
