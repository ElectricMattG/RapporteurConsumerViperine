#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# deploy_cluster_monitoring.sh
#
# Idempotent script to deploy Viperine monitoring config to the local k3s cluster.
#
# What it does:
#   1. Patches prometheus-config ConfigMap with Viperine scrape jobs + alert rules
#   2. Provisions the Grafana dashboard JSON into grafana-dashboards ConfigMap
#   3. Reloads Prometheus via POST /-/reload (NEVER restart — RWO PVC lock)
#   4. Verifies all Viperine targets are UP
#   5. Prints dashboard URL
#
# Usage:
#   bash scripts/deploy_cluster_monitoring.sh
#   make monitoring_deploy
#
# Requirements:
#   - kubectl configured for the k3s cluster (192.168.1.3)
#   - Prometheus accessible at PROMETHEUS_URL
#   - Grafana accessible at GRAFANA_URL
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
PROMETHEUS_URL="${PROMETHEUS_URL:-http://192.168.1.3:30090}"
GRAFANA_URL="${GRAFANA_URL:-http://192.168.1.3:30030}"
PROM_NS="${PROM_NS:-monitoring}"
PROM_CM="${PROM_CM:-prometheus-config}"
ALERTS_CM="${ALERTS_CM:-viperine-alerts}"
GRAFANA_CM="${GRAFANA_CM:-grafana-dashboards}"
GRAFANA_NS="${GRAFANA_NS:-monitoring}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ALERTS_FILE="$REPO_ROOT/monitoring/prometheus/alerts.yml"
PROMETHEUS_YML="$REPO_ROOT/prometheus.yml"
DASHBOARD_FILE="$REPO_ROOT/grafana/dashboards/viperine-pipeline-overview.json"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${YELLOW}▶${NC} $*"; }
ok()    { echo -e "${GREEN}✓${NC} $*"; }
err()   { echo -e "${RED}✗${NC} $*" >&2; }

# ── Step 1: Update Prometheus ConfigMap with scrape config + alerts ────────────
info "Updating Prometheus ConfigMap (${PROM_NS}/${PROM_CM})..."

# Check the ConfigMap exists
if ! kubectl get configmap "$PROM_CM" -n "$PROM_NS" &>/dev/null; then
    err "ConfigMap ${PROM_NS}/${PROM_CM} not found. Is Prometheus deployed?"
    exit 1
fi

# Patch prometheus.yml into the ConfigMap
kubectl create configmap "$PROM_CM" \
    --from-file=prometheus.yml="$PROMETHEUS_YML" \
    -n "$PROM_NS" \
    --dry-run=client -o yaml | kubectl apply -f -

ok "Prometheus ConfigMap updated with scrape config"

# Patch alert rules into the separate viperine-alerts ConfigMap
# (mounted at /etc/prometheus/rules/ in the Prometheus deployment)
info "Updating alert rules ConfigMap (${PROM_NS}/${ALERTS_CM})..."

kubectl create configmap "$ALERTS_CM" \
    --from-file=viperine-alerts.yml="$ALERTS_FILE" \
    -n "$PROM_NS" \
    --dry-run=client -o yaml | kubectl apply -f -

ok "Alert rules ConfigMap updated"

# ── Step 2: Update Grafana dashboard ConfigMap ────────────────────────────────
if [[ -f "$DASHBOARD_FILE" ]]; then
    info "Updating Grafana dashboard ConfigMap (${GRAFANA_NS}/${GRAFANA_CM})..."

    kubectl create configmap "$GRAFANA_CM" \
        --from-file=viperine-pipeline-overview.json="$DASHBOARD_FILE" \
        -n "$GRAFANA_NS" \
        --dry-run=client -o yaml | kubectl apply -f -

    ok "Grafana dashboard ConfigMap updated"
else
    info "No dashboard file at $DASHBOARD_FILE — skipping Grafana update"
fi

# ── Step 3: Reload Prometheus (never restart!) ────────────────────────────────
info "Reloading Prometheus configuration..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${PROMETHEUS_URL}/-/reload" 2>/dev/null || echo "000")

if [[ "$HTTP_CODE" == "200" ]]; then
    ok "Prometheus reloaded successfully"
elif [[ "$HTTP_CODE" == "000" ]]; then
    err "Cannot reach Prometheus at ${PROMETHEUS_URL} — is it running?"
    echo "   You may need to reload manually: curl -X POST ${PROMETHEUS_URL}/-/reload"
else
    err "Prometheus reload returned HTTP ${HTTP_CODE}"
    echo "   Check Prometheus logs: kubectl -n ${PROM_NS} logs -l app=prometheus --tail=50"
fi

# ── Step 4: Verify targets ───────────────────────────────────────────────────
info "Waiting 10s for scrape cycle..."
sleep 10

info "Checking Viperine targets..."
TARGETS_JSON=$(curl -s "${PROMETHEUS_URL}/api/v1/targets" 2>/dev/null || echo '{}')

if [[ "$TARGETS_JSON" == "{}" ]]; then
    err "Cannot query Prometheus targets API"
else
    # Count viperine targets that are UP
    UP_COUNT=$(echo "$TARGETS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
targets = data.get('data', {}).get('activeTargets', [])
viperine_up = [t for t in targets if 'viperine' in t.get('labels', {}).get('job', '') and t.get('health') == 'up']
viperine_all = [t for t in targets if 'viperine' in t.get('labels', {}).get('job', '')]
print(f'{len(viperine_up)}/{len(viperine_all)}')
for t in viperine_all:
    status = '✓' if t['health'] == 'up' else '✗'
    print(f\"  {status} {t['labels'].get('job', '?'):30s} {t['health']}\")
" 2>/dev/null || echo "?/?")

    echo "   Viperine targets UP: $UP_COUNT"
fi

# ── Step 5: Print dashboard URL ──────────────────────────────────────────────
echo ""
ok "Deployment complete!"
echo ""
echo "   Prometheus: ${PROMETHEUS_URL}"
echo "   Grafana:    ${GRAFANA_URL}"
echo "   Dashboard:  ${GRAFANA_URL}/d/viperine-pipeline/viperine-pipeline-overview"
echo ""
echo "   Verify targets: ${PROMETHEUS_URL}/targets"
echo "   Verify alerts:  ${PROMETHEUS_URL}/alerts"
