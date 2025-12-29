#!/bin/bash
set -e

echo "================================================================================"
echo "🔄 Migrating Cloud Kafka from 8.0.0 KRaft to 7.5.0 ZooKeeper"
echo "================================================================================"
echo ""
echo "⚠️  WARNING: This will DELETE existing Kafka data!"
echo "   - All messages in topics will be lost"
echo "   - Consumer group offsets will be reset"
echo "   - Kafka will be redeployed with ZooKeeper"
echo ""
read -p "Continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Migration cancelled"
    exit 1
fi

NAMESPACE="kafka"

echo ""
echo "Step 1/5: Deleting existing Kafka StatefulSet..."
kubectl delete statefulset kafka-direct -n $NAMESPACE --ignore-not-found=true
echo "✅ StatefulSet deleted"

echo ""
echo "Step 2/5: Deleting PVCs to remove old data..."
kubectl delete pvc -l app.kubernetes.io/name=kafka -n $NAMESPACE --ignore-not-found=true
echo "✅ PVCs deleted"

echo ""
echo "Step 3/5: Waiting for resources to be fully deleted..."
sleep 10
echo "✅ Resources cleaned up"

echo ""
echo "Step 4/5: Deploying Kafka 7.5.0 with ZooKeeper..."
helm upgrade --install kafka-direct ./helm/kafka-chart \
  --namespace $NAMESPACE \
  --create-namespace \
  --wait \
  --timeout 10m

echo "✅ Kafka 7.5.0 deployed"

echo ""
echo "Step 5/5: Verifying deployment..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n $NAMESPACE --timeout=300s

echo ""
echo "🔍 Checking Kafka version..."
kubectl exec -n $NAMESPACE kafka-direct-0 -c kafka -- kafka-broker-api-versions --bootstrap-server localhost:9092 | head -1

echo ""
echo "🔍 Checking ZooKeeper..."
kubectl exec -n $NAMESPACE kafka-direct-0 -c zookeeper -- sh -c 'echo ruok | nc localhost 2181'

echo ""
echo "================================================================================"
echo "✅ Migration Complete!"
echo "================================================================================"
echo ""
echo "Kafka is now running 7.5.0 with ZooKeeper (matching local environment)"
echo ""
echo "Next steps:"
echo "  1. Recreate topics: kubectl exec -n kafka kafka-direct-0 -c kafka -- kafka-topics --create ..."
echo "  2. Redeploy consumers: ./deploy-consumers.sh"
echo "  3. Test pipeline: ./send_urls.py"
echo ""
