#!/bin/bash
set -e

ACR_NAME="ragserviceneacr6881"
ACR_REGISTRY="${ACR_NAME}.azurecr.io"
IMAGE_NAME="time-series-indexer"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo "================================================"
echo "Building Time Series CronJob Image"
echo "================================================"

# Login to ACR
echo "Logging in to ACR..."
az acr login --name $ACR_NAME

# Build image
echo "Building Docker image..."
cd cronjobs/
docker buildx build \
    --platform linux/amd64 \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:latest \
    .
cd ..

# Push images
echo "Pushing images to ACR..."
docker push ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}

if [ "${IMAGE_TAG}" != "latest" ]; then
    docker push ${ACR_REGISTRY}/${IMAGE_NAME}:latest
fi

echo ""
echo "✅ Image built and pushed successfully"
echo "   ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
echo "   ${ACR_REGISTRY}/${IMAGE_NAME}:latest"
echo ""
