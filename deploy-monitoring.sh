#!/bin/bash

#############################################################################
# Monitoring & Telemetry Deployment Script
# Deploys Prometheus and Grafana with SSL/Auth for Kafka consumer monitoring
#############################################################################

set -e  # Exit on error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="monitoring"
PROMETHEUS_RELEASE="prometheus"
GRAFANA_RELEASE="grafana"
CLUSTER_NAME="embedding-aks-ne"
RESOURCE_GROUP="rapporteur-ne-rg"
INGRESS_NAMESPACE="ingress-nginx"

# Helm chart paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROMETHEUS_CHART="${SCRIPT_DIR}/helm/prometheus"
GRAFANA_CHART="${SCRIPT_DIR}/helm/grafana"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Monitoring & Telemetry Deployment for Kafka Pipeline      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

#############################################################################
# Pre-flight Checks
#############################################################################

preflight_checks() {
    echo -e "${YELLOW}[1/10] Running pre-flight checks...${NC}"

    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        echo -e "${RED}✗ kubectl not found. Please install kubectl.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ kubectl installed${NC}"

    # Check helm
    if ! command -v helm &> /dev/null; then
        echo -e "${RED}✗ helm not found. Please install Helm 3.x.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Helm installed ($(helm version --short))${NC}"

    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        echo -e "${RED}✗ Cannot connect to Kubernetes cluster${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Connected to cluster: $(kubectl config current-context)${NC}"

    # Check monitoring node pool
    if ! kubectl get nodes -l workload=monitoring &> /dev/null; then
        echo -e "${RED}✗ Monitoring node pool not found (label: workload=monitoring)${NC}"
        echo -e "${YELLOW}  Run: az aks nodepool add --name monitorpool --node-vm-size Standard_B2s ...${NC}"
        exit 1
    fi

    local node_count=$(kubectl get nodes -l workload=monitoring --no-headers | wc -l | tr -d ' ')
    echo -e "${GREEN}✓ Monitoring node pool ready ($node_count node(s))${NC}"

    # Check cert-manager
    if ! kubectl get clusterissuer letsencrypt-prod &> /dev/null; then
        echo -e "${YELLOW}⚠ cert-manager ClusterIssuer 'letsencrypt-prod' not found${NC}"
        echo -e "${YELLOW}  SSL certificates will not be issued automatically${NC}"
    else
        echo -e "${GREEN}✓ cert-manager ClusterIssuer ready${NC}"
    fi

    # Check ingress controller
    if ! kubectl get svc -n ${INGRESS_NAMESPACE} ingress-nginx-controller &> /dev/null; then
        echo -e "${YELLOW}⚠ NGINX Ingress Controller not found in ${INGRESS_NAMESPACE}${NC}"
        echo -e "${YELLOW}  Ingress resources will not work${NC}"
    else
        local ingress_ip=$(kubectl get svc -n ${INGRESS_NAMESPACE} ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
        echo -e "${GREEN}✓ NGINX Ingress Controller ready (IP: ${ingress_ip})${NC}"
    fi

    echo ""
}

#############################################################################
# DNS Verification
#############################################################################

verify_dns() {
    echo -e "${YELLOW}[2/10] Verifying DNS records...${NC}"

    local ingress_ip=$(kubectl get svc -n ${INGRESS_NAMESPACE} ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)

    if [ -z "$ingress_ip" ]; then
        echo -e "${YELLOW}⚠ Cannot determine ingress IP${NC}"
        return
    fi

    # Check grafana DNS
    local grafana_ip=$(nslookup grafana.rapporteur.ai 8.8.8.8 2>/dev/null | grep -A1 "Name:" | tail -1 | awk '{print $2}')
    if [ "$grafana_ip" == "$ingress_ip" ]; then
        echo -e "${GREEN}✓ grafana.rapporteur.ai → ${ingress_ip}${NC}"
    else
        echo -e "${YELLOW}⚠ grafana.rapporteur.ai DNS mismatch (expected: ${ingress_ip}, got: ${grafana_ip})${NC}"
    fi

    # Check prometheus DNS
    local prometheus_ip=$(nslookup prometheus.rapporteur.ai 8.8.8.8 2>/dev/null | grep -A1 "Name:" | tail -1 | awk '{print $2}')
    if [ "$prometheus_ip" == "$ingress_ip" ]; then
        echo -e "${GREEN}✓ prometheus.rapporteur.ai → ${ingress_ip}${NC}"
    else
        echo -e "${YELLOW}⚠ prometheus.rapporteur.ai DNS mismatch (expected: ${ingress_ip}, got: ${prometheus_ip})${NC}"
    fi

    echo ""
}

#############################################################################
# Namespace Check
#############################################################################

check_namespace() {
    echo -e "${YELLOW}[3/10] Checking namespace...${NC}"

    if kubectl get namespace ${NAMESPACE} &> /dev/null; then
        # Check if namespace is managed by Helm
        local managed_by=$(kubectl get namespace ${NAMESPACE} -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}' 2>/dev/null)
        if [ "$managed_by" != "Helm" ]; then
            echo -e "${YELLOW}⚠ Namespace '${NAMESPACE}' exists but not managed by Helm${NC}"
            echo -e "${YELLOW}  Helm will create it during installation${NC}"
        else
            echo -e "${GREEN}✓ Namespace '${NAMESPACE}' already managed by Helm${NC}"
        fi
    else
        echo -e "${BLUE}ℹ Namespace '${NAMESPACE}' will be created by Helm${NC}"
    fi

    echo ""
}

#############################################################################
# Check Consumer Services
#############################################################################

check_consumer_services() {
    echo -e "${YELLOW}[4/10] Checking Kafka consumer services...${NC}"

    local consumers=("web-scraper" "document-chunker" "chunk-embedder" "qa-generator" "qa-embedder" "summarizer" "citation-extractor" "index-aggregator" "dead-letter")
    local found=0

    for consumer in "${consumers[@]}"; do
        if kubectl get svc -n consumers "${consumer}" &> /dev/null; then
            echo -e "${GREEN}✓ Service: ${consumer}${NC}"
            ((found++))
        fi
    done

    if [ $found -eq 0 ]; then
        echo -e "${RED}✗ No consumer services found in 'consumers' namespace${NC}"
        echo -e "${YELLOW}  Run: helm upgrade research-indexing-consumers ./helm/research-indexing-consumers -n consumers${NC}"
        exit 1
    elif [ $found -lt 9 ]; then
        echo -e "${YELLOW}⚠ Found ${found}/9 consumer services${NC}"
        echo -e "${YELLOW}  Some consumers may not be monitored${NC}"
    else
        echo -e "${GREEN}✓ All ${found}/9 consumer services ready${NC}"
    fi

    echo ""
}

#############################################################################
# Deploy Prometheus
#############################################################################

deploy_prometheus() {
    echo -e "${YELLOW}[5/10] Deploying Prometheus...${NC}"

    if helm status ${PROMETHEUS_RELEASE} -n ${NAMESPACE} &> /dev/null; then
        echo -e "${BLUE}⟳ Prometheus already installed, upgrading...${NC}"
        helm upgrade ${PROMETHEUS_RELEASE} ${PROMETHEUS_CHART} \
            --namespace ${NAMESPACE} \
            --wait \
            --timeout 10m
        echo -e "${GREEN}✓ Prometheus upgraded${NC}"
    else
        helm install ${PROMETHEUS_RELEASE} ${PROMETHEUS_CHART} \
            --namespace ${NAMESPACE} \
            --create-namespace \
            --wait \
            --timeout 10m
        echo -e "${GREEN}✓ Prometheus installed${NC}"
    fi

    # Check if pod is running
    local max_wait=60
    local waited=0
    while [ $waited -lt $max_wait ]; do
        local status=$(kubectl get pods -n ${NAMESPACE} -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
        if [ "$status" == "Running" ]; then
            echo -e "${GREEN}✓ Prometheus pod is running${NC}"
            break
        fi
        sleep 2
        ((waited+=2))
    done

    echo ""
}

#############################################################################
# Deploy Grafana
#############################################################################

deploy_grafana() {
    echo -e "${YELLOW}[6/10] Deploying Grafana...${NC}"

    if helm status ${GRAFANA_RELEASE} -n ${NAMESPACE} &> /dev/null; then
        echo -e "${BLUE}⟳ Grafana already installed, upgrading...${NC}"
        helm upgrade ${GRAFANA_RELEASE} ${GRAFANA_CHART} \
            --namespace ${NAMESPACE} \
            --wait \
            --timeout 10m
        echo -e "${GREEN}✓ Grafana upgraded${NC}"
    else
        helm install ${GRAFANA_RELEASE} ${GRAFANA_CHART} \
            --namespace ${NAMESPACE} \
            --wait \
            --timeout 10m
        echo -e "${GREEN}✓ Grafana installed${NC}"
    fi

    # Check if pod is running
    local max_wait=60
    local waited=0
    while [ $waited -lt $max_wait ]; do
        local status=$(kubectl get pods -n ${NAMESPACE} -l app.kubernetes.io/name=grafana -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
        if [ "$status" == "Running" ]; then
            echo -e "${GREEN}✓ Grafana pod is running${NC}"
            break
        fi
        sleep 2
        ((waited+=2))
    done

    echo ""
}

#############################################################################
# Verify Deployments
#############################################################################

verify_deployments() {
    echo -e "${YELLOW}[7/10] Verifying pod status...${NC}"

    # Check for failed pods
    local failed_pods=$(kubectl get pods -n ${NAMESPACE} --field-selector=status.phase=Failed --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [ "$failed_pods" -gt 0 ]; then
        echo -e "${RED}⚠ Found $failed_pods failed pod(s)${NC}"
        kubectl get pods -n ${NAMESPACE} --field-selector=status.phase=Failed
    fi

    # Check pod status
    echo ""
    kubectl get pods -n ${NAMESPACE} -o wide

    # Show any pod issues
    local not_ready=$(kubectl get pods -n ${NAMESPACE} --field-selector=status.phase!=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [ "$not_ready" -gt 0 ]; then
        echo ""
        echo -e "${YELLOW}⚠ Some pods are not running. Showing recent events:${NC}"
        kubectl get events -n ${NAMESPACE} --sort-by='.lastTimestamp' | tail -10
    else
        echo -e "${GREEN}✓ All pods are running${NC}"
    fi

    echo ""
}

#############################################################################
# Verify Services
#############################################################################
# Verify Services
#############################################################################

verify_services() {
    echo -e "${YELLOW}[8/10] Verifying services...${NC}"

    kubectl get svc -n ${NAMESPACE}
    echo ""
}

#############################################################################
# Check SSL Certificates
#############################################################################

check_certificates() {
    echo -e "${YELLOW}[9/10] Checking SSL certificates...${NC}"

    sleep 10  # Give cert-manager time to create certificates

    if kubectl get certificate -n ${NAMESPACE} &> /dev/null; then
        echo ""
        kubectl get certificate -n ${NAMESPACE}
        echo ""

        # Check certificate status
        local certs=$(kubectl get certificate -n ${NAMESPACE} -o jsonpath='{.items[*].metadata.name}')
        for cert in $certs; do
            local ready=$(kubectl get certificate -n ${NAMESPACE} $cert -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
            local reason=$(kubectl get certificate -n ${NAMESPACE} $cert -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}')
            if [ "$ready" == "True" ]; then
                echo -e "${GREEN}✓ Certificate '$cert' is ready${NC}"
            else
                echo -e "${YELLOW}⚠ Certificate '$cert' is not ready yet${NC}"
                if [ -n "$reason" ]; then
                    echo -e "${YELLOW}  Reason: $reason${NC}"
                fi
                echo -e "${BLUE}  Certificates may take 1-2 minutes to be issued${NC}"
            fi
        done
    else
        echo -e "${YELLOW}⚠ No certificates found (cert-manager may not be configured)${NC}"
    fi

    echo ""
}

#############################################################################
# Display Access Information
#############################################################################

display_access_info() {
    echo -e "${YELLOW}[10/10] Deployment Summary${NC}"
    echet ingress IPs
    local prometheus_ip=$(kubectl get ingress -n ${NAMESPACE} prometheus-ingress -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
    local grafana_ip=$(kubectl get ingress -n ${NAMESPACE} grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)

    # Grafana
    echo -e "${GREEN}Grafana:${NC}"
    echo -e "  URL: https://grafana.rapporteur.ai"
    if [ -n "$grafana_ip" ]; then
        echo -e "  Ingress IP: $grafana_ip"
    fi
    echo -e "  Basic Auth: admin / changeme123"
    echo -e "  App Login:  admin / admin123"
    echo ""

    # Prometheus
    echo -e "${GREEN}Prometheus:${NC}"
    echo -e "  URL: https://prometheus.rapporteur.ai"
    if [ -n "$prometheus_ip" ]; then
        echo -e "  Ingress IP: $prometheus_ip"
    fi
    echo -e "  Basic Auth: admin / changeme123"
    echo ""

    # Image Registry
    echo -e "${GREEN}Container Registry:${NC}"
    echo -e "  Prometheus: ragserviceneacr6881.azurecr.io/prometheus:v2.48.0"
    echo -e "  Grafana:    ragserviceneacr6881.azurecr.io/grafana:10.2.2"
    echo ""

    # Consumer Services
    echo -e "${GREEN}Monitored Consumers:${NC}"
    echo -e "  9 services in 'consumers' namespace"
    echo -e "  Metrics exposed on ports 8001-8009"
    echo ""

    # Credentials warning
    echo -e "${RED}⚠ IMPORTANT: Change default passwords in production!${NC}"
    echo -e "  Edit: helm/grafana/values.yaml and helm/prometheus/values.yaml"
    echo ""

    # Port forwarding option
    echo -e "${BLUE}Local Access (if DNS/Ingress not ready):${NC}"
    echo -e "  Prometheus: kubectl port-forward -n ${NAMESPACE} svc/prometheus 9090:9090"
    echo -e "  Grafana:    kubectl port-forward -n ${NAMESPACE} svc/grafana 3000:3000"
    echo ""

    # Monitoring commands
    echo -e "${BLUE}Useful Commands:${NC}"
    echo -e "  Watch pods:        kubectl get pods -n ${NAMESPACE} -w"
    echo -e "  View logs:         kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=prometheus"
    echo -e "  Check ingress:     kubectl get ingress -n ${NAMESPACE}"
    echo -e "  Check certs:       kubectl get certificate -n ${NAMESPACE}"
    echo -e "  Test scraping:     curl -u admin:changeme123 https://prometheus.rapporteur.ai/api/v1/targets"
    echo -e "  Uninstall:         helm uninstall ${PROMETHEUS_RELEASE} ${GRAFANA_RELEASE} -n ${NAMESPACE}"
    echo ""
}

#############################################################################
# Main Execution
#############################################################################

main() {
    preflight_checks
    verify_dns
    check_namespace
    check_consumer_services
    deploy_prometheus
    deploy_grafana
    verify_deployments
    verify_services
    check_certificates
    display_access_info

    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║          Monitoring Deployment Completed Successfully!         ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Run main function
main
