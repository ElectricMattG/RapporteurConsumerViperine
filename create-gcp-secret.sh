#!/bin/bash

###############################################################################
# Create GCP Service Account Secret for Kubernetes
###############################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "🔐 Creating GCP Service Account Secret"
echo "================================================================================"

# Check if credentials file path is provided
if [ -z "$1" ]; then
    echo -e "${RED}❌ Error: GCP service account JSON file path not provided${NC}"
    echo ""
    echo "Usage:"
    echo "  ./create-gcp-secret.sh <path-to-service-account.json>"
    echo ""
    echo "Example:"
    echo "  ./create-gcp-secret.sh ~/Downloads/my-project-service-account.json"
    echo ""
    exit 1
fi

CREDS_FILE="$1"
NAMESPACE="consumers"
SECRET_NAME="gcp-service-account"

# Check if file exists
if [ ! -f "$CREDS_FILE" ]; then
    echo -e "${RED}❌ Error: File not found: $CREDS_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}ℹ️  Configuration:${NC}"
echo "   Namespace: $NAMESPACE"
echo "   Secret name: $SECRET_NAME"
echo "   Credentials file: $CREDS_FILE"
echo ""

# Validate JSON
if ! jq empty "$CREDS_FILE" 2>/dev/null; then
    echo -e "${RED}❌ Error: Invalid JSON file${NC}"
    exit 1
fi

# Check if secret already exists
if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &>/dev/null; then
    echo -e "${YELLOW}⚠️  Secret '$SECRET_NAME' already exists in namespace '$NAMESPACE'${NC}"
    read -p "Do you want to delete and recreate it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Deleting existing secret..."
        kubectl delete secret "$SECRET_NAME" -n "$NAMESPACE"
    else
        echo "❌ Aborted"
        exit 1
    fi
fi

# Create the secret
echo "📦 Creating secret..."
kubectl create secret generic "$SECRET_NAME" \
    --from-file=credentials.json="$CREDS_FILE" \
    -n "$NAMESPACE"

echo ""
echo -e "${GREEN}✅ Secret created successfully!${NC}"
echo ""
echo "Next steps:"
echo "  1. Deploy/upgrade consumers: helm upgrade research-indexing-consumers ./helm/research-indexing-consumers -n consumers"
echo "  2. Pods will automatically mount credentials at /var/secrets/google/credentials.json"
echo "  3. GOOGLE_APPLICATION_CREDENTIALS env var will be set automatically"
echo ""
echo "To verify:"
echo "  kubectl get secret $SECRET_NAME -n $NAMESPACE"
echo "  kubectl describe secret $SECRET_NAME -n $NAMESPACE"
echo ""
