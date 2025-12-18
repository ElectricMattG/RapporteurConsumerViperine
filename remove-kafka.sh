#!/bin/bash
# Remove existing Kafka deployment from Kubernetes cluster
#
# This script safely removes the Kafka deployment and optionally deletes
# persistent volumes and data.

set -e

NAMESPACE="kafka"
RELEASE_NAME="kafka"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Kafka Removal Script${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Check if kubectl is available
if ! command -v kubectl >/dev/null 2>&1; then
    echo -e "${RED}❌ kubectl is not installed${NC}"
    exit 1
fi

# Check if helm is available
if ! command -v helm >/dev/null 2>&1; then
    echo -e "${RED}❌ helm is not installed${NC}"
    exit 1
fi

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Namespace '$NAMESPACE' does not exist${NC}"
    exit 0
fi

# Check if Kafka release exists
echo -e "${YELLOW}🔍 Checking for Kafka deployment...${NC}"
if ! helm list -n "$NAMESPACE" | grep -q "^$RELEASE_NAME"; then
    echo -e "${YELLOW}⚠️  No Kafka Helm release found in namespace '$NAMESPACE'${NC}"
else
    echo -e "${GREEN}✅ Found Kafka release: $RELEASE_NAME${NC}"
fi
echo ""

# Show current resources
echo -e "${YELLOW}📦 Current Kafka resources:${NC}"
echo ""
echo -e "${BLUE}StatefulSets:${NC}"
kubectl get statefulset -n "$NAMESPACE" -l app.kubernetes.io/name=kafka 2>/dev/null || echo "  None"
echo ""
echo -e "${BLUE}Pods:${NC}"
kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kafka 2>/dev/null || echo "  None"
echo ""
echo -e "${BLUE}Services:${NC}"
kubectl get svc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka 2>/dev/null || echo "  None"
echo ""
echo -e "${BLUE}PersistentVolumeClaims:${NC}"
kubectl get pvc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka 2>/dev/null || echo "  None"
echo ""
echo -e "${BLUE}Ingress:${NC}"
kubectl get ingress -n "$NAMESPACE" 2>/dev/null | grep kafka || echo "  None"
echo ""

# Confirm deletion
echo -e "${RED}⚠️  WARNING: This will remove the Kafka deployment${NC}"
echo ""
read -p "Do you want to proceed? (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo -e "${YELLOW}❌ Operation cancelled${NC}"
    exit 0
fi

# Ask about data deletion
echo ""
echo -e "${YELLOW}🗑️  Data Deletion Options:${NC}"
echo "  1) Remove deployment only (keep PVCs and data)"
echo "  2) Remove deployment AND delete all data (PVCs)"
echo ""
read -p "Choose option (1 or 2): " -r DELETE_DATA
echo ""

# Uninstall Helm release
echo -e "${YELLOW}📤 Uninstalling Helm release '$RELEASE_NAME'...${NC}"
if helm list -n "$NAMESPACE" | grep -q "^$RELEASE_NAME"; then
    helm uninstall "$RELEASE_NAME" -n "$NAMESPACE"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Helm release uninstalled${NC}"
    else
        echo -e "${RED}❌ Failed to uninstall Helm release${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️  No Helm release found${NC}"
fi
echo ""

# Delete StatefulSet if it still exists
echo -e "${YELLOW}🗑️  Checking for remaining StatefulSet...${NC}"
if kubectl get statefulset -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting StatefulSet...${NC}"
    kubectl delete statefulset -n "$NAMESPACE" -l app.kubernetes.io/name=kafka --grace-period=30
    echo -e "${GREEN}✅ StatefulSet deleted${NC}"
else
    echo -e "${GREEN}✅ No StatefulSet found${NC}"
fi
echo ""

# Delete pods if they still exist
echo -e "${YELLOW}🗑️  Checking for remaining pods...${NC}"
if kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting pods...${NC}"
    kubectl delete pods -n "$NAMESPACE" -l app.kubernetes.io/name=kafka --grace-period=30
    echo -e "${GREEN}✅ Pods deleted${NC}"
else
    echo -e "${GREEN}✅ No pods found${NC}"
fi
echo ""

# Delete services
echo -e "${YELLOW}🗑️  Checking for services...${NC}"
if kubectl get svc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting services...${NC}"
    kubectl delete svc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
    echo -e "${GREEN}✅ Services deleted${NC}"
else
    echo -e "${GREEN}✅ No services found${NC}"
fi
echo ""

# Delete ingress
echo -e "${YELLOW}🗑️  Checking for ingress...${NC}"
if kubectl get ingress -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting ingress...${NC}"
    kubectl delete ingress -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
    echo -e "${GREEN}✅ Ingress deleted${NC}"
else
    echo -e "${GREEN}✅ No ingress found${NC}"
fi
echo ""

# Delete ConfigMaps
echo -e "${YELLOW}🗑️  Checking for ConfigMaps...${NC}"
if kubectl get configmap -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting ConfigMaps...${NC}"
    kubectl delete configmap -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
    echo -e "${GREEN}✅ ConfigMaps deleted${NC}"
else
    echo -e "${GREEN}✅ No ConfigMaps found${NC}"
fi
echo ""

# Delete Secrets (including auth secret)
echo -e "${YELLOW}🗑️  Checking for Secrets...${NC}"
if kubectl get secret -n "$NAMESPACE" kafka-basic-auth >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting kafka-basic-auth secret...${NC}"
    kubectl delete secret -n "$NAMESPACE" kafka-basic-auth
    echo -e "${GREEN}✅ Auth secret deleted${NC}"
fi
if kubectl get secret -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
    echo -e "${YELLOW}Deleting Kafka secrets...${NC}"
    kubectl delete secret -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
    echo -e "${GREEN}✅ Secrets deleted${NC}"
else
    echo -e "${GREEN}✅ No secrets found${NC}"
fi
echo ""

# Handle PVCs based on user choice
if [[ $DELETE_DATA == "2" ]]; then
    echo -e "${RED}🗑️  Deleting PersistentVolumeClaims (DATA WILL BE LOST!)...${NC}"
    echo ""
    read -p "Are you absolutely sure? Type 'DELETE' to confirm: " -r CONFIRM
    echo

    if [[ $CONFIRM == "DELETE" ]]; then
        if kubectl get pvc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka >/dev/null 2>&1; then
            # List PVCs before deletion
            echo -e "${YELLOW}PVCs to be deleted:${NC}"
            kubectl get pvc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
            echo ""

            kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka
            echo -e "${GREEN}✅ PVCs deleted${NC}"
        else
            echo -e "${GREEN}✅ No PVCs found${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️  PVC deletion cancelled (data preserved)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Keeping PVCs (data preserved)${NC}"
    echo ""
    echo -e "${BLUE}Existing PVCs:${NC}"
    kubectl get pvc -n "$NAMESPACE" -l app.kubernetes.io/name=kafka 2>/dev/null || echo "  None"
    echo ""
    echo -e "${YELLOW}💡 To delete PVCs later, run:${NC}"
    echo -e "${YELLOW}   kubectl delete pvc -n $NAMESPACE -l app.kubernetes.io/name=kafka${NC}"
fi
echo ""

# Final status
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Cleanup Complete${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}✅ Kafka deployment removed successfully${NC}"
echo ""

# Show remaining resources
echo -e "${YELLOW}📊 Remaining resources in namespace '$NAMESPACE':${NC}"
echo ""
kubectl get all -n "$NAMESPACE" 2>/dev/null || echo "  None"
echo ""

echo -e "${BLUE}To redeploy Kafka with external access, run:${NC}"
echo -e "${BLUE}  ./deploy-kafka-external.sh${NC}"
echo ""
