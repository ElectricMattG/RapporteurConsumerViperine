#!/usr/bin/env bash
# scripts/smoke_test_e2e.sh
#
# End-to-end smoke test for the Viperine consumer group.
# Verifies that a RAG query triggers the full Kafka pipeline:
#
#   RAG → web-scrape-requests → web-scraper → summarise-document
#       → summarizer → summary-completed → summary-indexer
#                                        ↘ dead-letter-queue (should stay silent)
#
# Usage:
#   ./scripts/smoke_test_e2e.sh
#
# Overridable env vars:
#   RELEASE_NAME   (default: viperine)
#   K8S_NS         (default: consumers)
#   RAG_HOST       (default: 192.168.1.3)
#   RAG_VHOST      (default: rag.rappoteur.ai)
#   KAFKA_NS       (default: kafka)
#   POLL_TIMEOUT   (default: 90s — per pipeline stage)
#   POLL_INTERVAL  (default: 5s)
#   ENRICH         (default: true — set false for search-only test)

set -euo pipefail

RELEASE_NAME=${RELEASE_NAME:-viperine}
K8S_NS=${K8S_NS:-consumers}
RAG_HOST=${RAG_HOST:-192.168.1.3}
RAG_VHOST=${RAG_VHOST:-rag.rapporteur.ai}
KAFKA_NS=${KAFKA_NS:-kafka}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-kafka:9092}
POLL_TIMEOUT=${POLL_TIMEOUT:-90}
POLL_INTERVAL=${POLL_INTERVAL:-5}
ENRICH=${ENRICH:-true}

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

pass()   { echo -e "${GREEN}  ✅ $*${RESET}"; }
fail()   { echo -e "${RED}  ❌ $*${RESET}"; FAILURES=$((FAILURES+1)); }
info()   { echo -e "${CYAN}  ·  $*${RESET}"; }
warn()   { echo -e "${YELLOW}  ⚠  $*${RESET}"; }
header() { echo -e "\n${BOLD}${YELLOW}══ $* ══${RESET}"; }

FAILURES=0

# ── Helpers ───────────────────────────────────────────────────────────────────
kafka_exec() {
  kubectl -n "$KAFKA_NS" exec kafka-0 -c kafka -- "$@" 2>/dev/null
}

# Sum log-end-offsets across all partitions for a topic.
sum_log_end_offset() {
  local topic=$1
  kafka_exec kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP" --topic "$topic" --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}'
}

# Sum current consumer group LAG across all partitions.
group_total_lag() {
  local group=$1
  kafka_exec kafka-consumer-groups \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --describe --group "$group" 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}'
}

# Poll until a topic's log-end-offset sum exceeds a baseline.
poll_for_new_messages() {
  local topic=$1 baseline=$2 label=$3
  local elapsed=0
  while [[ $elapsed -lt $POLL_TIMEOUT ]]; do
    new=$(sum_log_end_offset "$topic")
    if [[ "$new" -gt "$baseline" ]]; then
      delta=$((new - baseline))
      pass "${label}: +${delta} messages (${baseline} → ${new})"
      return 0
    fi
    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
    info "${label}: waiting... (${elapsed}s / ${POLL_TIMEOUT}s, current offset=${new})"
  done
  fail "${label}: no new messages after ${POLL_TIMEOUT}s (offset stayed at ${baseline})"
  return 1
}

# ── 1. Consumer pod health ────────────────────────────────────────────────────
header "1 · Consumer pod health  (${RELEASE_NAME} in ${K8S_NS})"

for role in web-scraper summarizer summary-indexer dead-letter-consumer; do
  deploy="${RELEASE_NAME}-${role}"
  pod=$(kubectl -n "$K8S_NS" get pod -l "app=${deploy}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "$pod" ]]; then
    fail "${deploy}: pod not found"
    continue
  fi
  pod_ip=$(kubectl -n "$K8S_NS" get pod "$pod" -o jsonpath='{.status.podIP}')
  port=$(kubectl -n "$K8S_NS" get svc "$deploy" \
         -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo "0")

  for endpoint in health ready; do
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
      "http://${pod_ip}:${port}/${endpoint}" --max-time 5 2>/dev/null || echo "000")
    if [[ "$http_code" == "200" ]]; then
      pass "${deploy}: /${endpoint} → 200"
    else
      fail "${deploy}: /${endpoint} → ${http_code}  (${pod_ip}:${port})"
    fi
  done
done

# ── 2. Baseline Kafka offsets ─────────────────────────────────────────────────
header "2 · Baseline Kafka offsets"

b_scrape=$(sum_log_end_offset "web-scrape-requests")
b_summarise=$(sum_log_end_offset "summarise-document")
b_summary_completed=$(sum_log_end_offset "summary-completed")
b_dlq=$(sum_log_end_offset "dead-letter-queue")

info "web-scrape-requests  end-offset: ${b_scrape}"
info "summarise-document   end-offset: ${b_summarise}"
info "summary-completed    end-offset: ${b_summary_completed}"
info "dead-letter-queue    end-offset: ${b_dlq}"

# ── 3. Fire RAG query ─────────────────────────────────────────────────────────
header "3 · Fire RAG query  (enrich=${ENRICH})"

PAYLOAD=$(printf \
  '{"query":"Explain Middle East and Conflict with Iran Air and Naval Forces?","mode":"search","top_k":10,"start_date":"2026-01-01","end_date":"2026-03-01","enrich":%s,"use_qa":true}' \
  "$ENRICH")

info "POST http://${RAG_HOST}/query_llm  Host: ${RAG_VHOST}"
http_code=$(curl -s -o /tmp/rag_smoke_response.json -w "%{http_code}" \
  -H "Host: ${RAG_VHOST}" \
  -H "Content-Type: application/json" \
  "http://${RAG_HOST}/query_llm" \
  -d "$PAYLOAD" \
  --max-time 60 2>/dev/null || echo "000")

if [[ "$http_code" =~ ^2 ]]; then
  pass "RAG responded HTTP ${http_code}"
  preview=$(python3 -c "
import json, sys
try:
    d = json.load(open('/tmp/rag_smoke_response.json'))
    ans = d.get('answer') or d.get('response') or d.get('result') or str(d)
    print(str(ans)[:200])
except Exception as e:
    print(open('/tmp/rag_smoke_response.json').read()[:200])
" 2>/dev/null || head -c 200 /tmp/rag_smoke_response.json)
  info "Response preview: ${preview}"
elif [[ "$http_code" == "000" ]]; then
  fail "RAG query timed out or unreachable  (host=${RAG_HOST}, vhost=${RAG_VHOST})"
else
  fail "RAG responded HTTP ${http_code}"
  cat /tmp/rag_smoke_response.json 2>/dev/null || true
fi

if [[ "$ENRICH" != "true" ]]; then
  warn "ENRICH=false — skipping pipeline publish checks (search-only mode)"
  echo ""
  header "Smoke test summary  (search-only)"
  if [[ $FAILURES -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}ALL CHECKS PASSED${RESET}"
  else
    echo -e "${RED}${BOLD}${FAILURES} CHECK(S) FAILED${RESET}"
    exit 1
  fi
  exit 0
fi

# ── 4. Verify: RAG → web-scrape-requests ─────────────────────────────────────
header "4 · Verify: RAG published to web-scrape-requests"
poll_for_new_messages "web-scrape-requests" "$b_scrape" "web-scrape-requests" || true

# ── 5. Verify: web-scraper consumed + published to summarise-document ─────────
header "5 · Verify: web-scraper → summarise-document"
poll_for_new_messages "summarise-document" "$b_summarise" "summarise-document" || true

# Report scraper LAG regardless
lag=$(group_total_lag "web-scraper-consumer")
if [[ "$lag" -eq 0 ]]; then
  pass "web-scraper-consumer LAG = 0 (fully caught up)"
else
  info "web-scraper-consumer LAG = ${lag} (still processing)"
fi

# ── 6. Verify: summarizer → summary-completed ────────────────────────────────
header "6 · Verify: summarizer → summary-completed"
poll_for_new_messages "summary-completed" "$b_summary_completed" "summary-completed" || true

# ── 7. Dead-letter queue silent ───────────────────────────────────────────────
header "7 · Dead-letter queue integrity"
dlq_end=$(sum_log_end_offset "dead-letter-queue")
if [[ "$dlq_end" -le "$b_dlq" ]]; then
  pass "dead-letter-queue: no new failures (offset=${dlq_end})"
else
  delta=$((dlq_end - b_dlq))
  fail "dead-letter-queue: +${delta} new failure messages — check consumer logs"
  info "kubectl -n ${K8S_NS} logs deploy/${RELEASE_NAME}-dead-letter-consumer --tail=30"
fi

# ── 8. Consumer group lag summary ────────────────────────────────────────────
header "8 · Consumer group lag snapshot"
for group in web-scraper-consumer summarizer-consumer; do
  lag=$(group_total_lag "$group")
  if [[ "$lag" -eq 0 ]]; then
    pass "${group}: LAG = 0"
  else
    info "${group}: LAG = ${lag} (may still be processing)"
  fi
done

# ── Final summary ─────────────────────────────────────────────────────────────
header "Smoke test summary"
echo ""
if [[ $FAILURES -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}ALL CHECKS PASSED${RESET}"
else
  echo -e "${RED}${BOLD}${FAILURES} CHECK(S) FAILED${RESET}"
  exit 1
fi
