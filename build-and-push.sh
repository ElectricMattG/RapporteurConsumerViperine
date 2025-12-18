#!/bin/bash

set -e  # Exit on any error

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

docker buildx build \
    --platform linux/amd64 \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:latest \
    -f $DOCKERFILE \
    .

print_status "Image built successfully"

# Step 3: Push to ACR
print_info "Step 3/4: Pushing image to ACR..."

docker push ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
docker push ${ACR_REGISTRY}/${IMAGE_NAME}:latest

print_status "Image pushed successfully"

# Step 4: Deploy with Helm
print_info "Step 4/4: Deploying with Helm..."

helm upgrade research-indexing-consumers ./helm/research-indexing-consumers -n consumers

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
