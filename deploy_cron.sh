#!/bin/bash
set -e

NAMESPACE="consumers"
CHART_PATH="./cronjobs/helm"
RELEASE_NAME="time-series-indexer"

echo "================================================"
echo "Deploying Time Series CronJob"
echo "================================================"

# Load .env file if it exists (for local development)
if [ -f "cronjobs/.env" ]; then
    echo "Loading environment variables from cronjobs/.env"
    export $(cat cronjobs/.env | grep -v '^#' | xargs)
fi

# Check required environment variables
if [ -z "$AZURE_CONNECTION_STRING" ]; then
    echo "❌ Error: AZURE_CONNECTION_STRING not set"
    echo "   Set it in cronjobs/.env or export directly:"
    echo "   export AZURE_CONNECTION_STRING='...'"
    exit 1
fi

if [ -z "$RAG_API_KEY" ]; then
    echo "❌ Error: RAG_API_KEY not set"
    echo "   Set it in cronjobs/.env or export directly:"
    echo "   export RAG_API_KEY='...'"
    exit 1
fi

echo "Environment variables validated ✓"
echo ""

# Deploy via Helm
echo "Deploying with Helm..."
helm upgrade --install $RELEASE_NAME $CHART_PATH \
    --namespace $NAMESPACE \
    --create-namespace \
    --set azure.storageConnectionString="$AZURE_CONNECTION_STRING" \
    --set rag.apiKey="$RAG_API_KEY" \
    --wait

echo ""
echo "✅ CronJob deployed successfully"
echo ""

# Show CronJob status
echo "CronJob status:"
kubectl get cronjob -n $NAMESPACE $RELEASE_NAME

echo ""
echo "================================================"
echo "Useful Commands"
echo "================================================"
echo "View CronJob:"
echo "  kubectl get cronjob -n $NAMESPACE $RELEASE_NAME"
echo ""
echo "Trigger manual test run:"
echo "  kubectl create job test-run-\$(date +%s) --from=cronjob/$RELEASE_NAME -n $NAMESPACE"
echo ""
echo "View job history:"
echo "  kubectl get jobs -n $NAMESPACE -l app.kubernetes.io/name=$RELEASE_NAME"
echo ""
echo "View logs (replace <job-name>):"
echo "  kubectl logs -f -n $NAMESPACE job/<job-name>"
echo ""
echo "Delete CronJob:"
echo "  helm uninstall $RELEASE_NAME -n $NAMESPACE"
echo ""
