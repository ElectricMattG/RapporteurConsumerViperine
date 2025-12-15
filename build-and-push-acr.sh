#!/bin/bash
set -e

# Azure Container Registry configuration
ACR_NAME="ragserviceneacr6881"
REGION="northeurope"
RESOURCE_GROUP="rapporteur-ne-rg"
IMAGE_NAME="scrape-consumer"
VERSION="v1.0.0"

# Full registry URL
ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.io"
FULL_IMAGE="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${VERSION}"
LATEST_IMAGE="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest"

echo "🔐 Logging in to Azure..."
az login

echo "🔐 Logging in to ACR: ${ACR_LOGIN_SERVER}..."
az acr login --name ${ACR_NAME}

echo "🏗️  Building Docker image..."
docker build -t ${IMAGE_NAME}:${VERSION} .

echo "🏷️  Tagging images for ACR..."
docker tag ${IMAGE_NAME}:${VERSION} ${FULL_IMAGE}
docker tag ${IMAGE_NAME}:${VERSION} ${LATEST_IMAGE}

echo "📤 Pushing to ACR..."
docker push ${FULL_IMAGE}
docker push ${LATEST_IMAGE}

echo "✅ Successfully pushed to ACR!"
echo ""
echo "Image: ${FULL_IMAGE}"
echo "Latest: ${LATEST_IMAGE}"
echo ""
echo "Next steps:"
echo "1. Update k8s/05-consumer-deployments.yaml with image: ${LATEST_IMAGE}"
echo "2. Deploy to AKS: kubectl apply -f k8s/"
