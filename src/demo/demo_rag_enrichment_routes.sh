#!/usr/bin/env bash
# =============================================================================
# demo_rag_enrichment_routes.sh
#
# Start the RAG service locally and exercise the enrichment-wired routes:
#   /health  /ready  /search  /grounding_preview  /query_llm
#
# The service is started in a background uvicorn process and killed on exit.
# No Docker, Kafka or live LLM credentials are required — the script passes
# stub env vars so the service boots and the enrichment index loads.
# Embedding calls ARE needed for vector search; set EMBEDDING_SERVICE_URL to a
# real embedding service or the search/query routes will 503.
#
# Usage:
#   cd /home/electric/workspace/RapporteurApp
#   bash src/demo/demo_rag_enrichment_routes.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT=${RAG_PORT:-8765}              # Non-standard port to avoid collision
HOST=127.0.0.1
BASE_URL="http://${HOST}:${PORT}"
STARTUP_TIMEOUT=30                  # seconds to wait for /health
LOG_FILE="/tmp/rag_demo_$$.log"
PIDFILE="/tmp/rag_demo_$$.pid"

# Enrichment query — a URL that exists in the linked index
DEMO_URL="https://www.war.gov/News/Transcripts/Transcript/Article/4024947/deputy-pentagon-press-secretary-sabrina-singh-holds-press-briefing/"
DEMO_QUERY="Who is the Deputy Pentagon press secretary?"

# Colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo -e "${CYAN}[demo]${RESET} $*"; }
ok()   { echo -e "${GREEN}[✓]${RESET} $*"; }
warn() { echo -e "${YELLOW}[!]${RESET} $*"; }
die()  { echo -e "${RED}[✗]${RESET} $*" >&2; exit 1; }

section() {
    echo
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${BOLD}  $*${RESET}"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
}

pretty_json() {
    # Print JSON response, truncating very long strings for readability
    python3 -c "
import json, sys, textwrap
data = json.load(sys.stdin)

def _trim(v, maxlen=200):
    if isinstance(v, str) and len(v) > maxlen:
        return v[:maxlen] + '…'
    if isinstance(v, list):
        return [_trim(i, maxlen) for i in v[:3]] + (['…'] if len(v) > 3 else [])
    if isinstance(v, dict):
        return {k: _trim(vv, maxlen) for k, vv in v.items()}
    return v

print(json.dumps(_trim(data), indent=2))
"
}

check_jq() {
    if command -v jq &>/dev/null; then echo "jq"; else echo "python3"; fi
}

extract() {
    # extract a top-level key from JSON; fall back gracefully
    local key="$1"
    python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('$key','<not present>'))"
}

# ---------------------------------------------------------------------------
# Locate project root and Python environment
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${PROJECT_ROOT}"

PYTHON=$(which python || which python3)
log "Project root : ${PROJECT_ROOT}"
log "Python       : ${PYTHON}"
log "Service port : ${PORT}"

# ---------------------------------------------------------------------------
# Cleanup trap — always kill the uvicorn process on exit
# ---------------------------------------------------------------------------
_cleanup() {
    if [[ -f "${PIDFILE}" ]]; then
        PID=$(cat "${PIDFILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            log "Stopping RAG service (PID ${PID})…"
            kill "${PID}" 2>/dev/null || true
        fi
        rm -f "${PIDFILE}"
    fi
    rm -f "${LOG_FILE}"
}
trap _cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Start service
# ---------------------------------------------------------------------------
section "Starting RAG service on port ${PORT}"

# Minimal env: suppress Kafka, point embedding client at a no-op stub
export PYTHONPATH="${PROJECT_ROOT}"
export KAFKA_BOOTSTRAP_SERVERS="localhost:19092"   # unreachable — Kafka errors are non-fatal
# GCP creds: use whatever is available in the shell; LLM calls may fail but
# enrichment routes don't need them.
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-rapporteurclients}"
export PROJECT_ID="${GCP_PROJECT_ID}"
export LOCATION="${LOCATION:-us-central1}"
export GCP_LOCATION="${LOCATION}"

# Point the service at the filtered (smaller) index files used in Docker
FILTERED_SUMMARIES="${PROJECT_ROOT}/research_summaries_filtered.jsonl"
FILTERED_QA="${PROJECT_ROOT}/research_qa_pairs_filtered.jsonl"
if [[ -f "${FILTERED_SUMMARIES}" ]]; then
    export INDEX_PATH="${FILTERED_SUMMARIES}"
    log "Summaries index: ${FILTERED_SUMMARIES}"
else
    warn "research_summaries_filtered.jsonl not found; falling back to combined index"
fi
if [[ -f "${FILTERED_QA}" ]]; then
    export QA_INDEX_PATH="${FILTERED_QA}"
    log "QA index       : ${FILTERED_QA}"
else
    warn "research_qa_pairs_filtered.jsonl not found; QA index will be skipped"
fi

# Point the service at the real enrichment index files in project root
LINKED=$(ls -t "${PROJECT_ROOT}"/linked_media_bboa_full_*.jsonl 2>/dev/null | head -1 || true)
UNLINKED=$(ls -t "${PROJECT_ROOT}"/unlinked_media_bboa_full_*.jsonl 2>/dev/null | head -1 || true)
if [[ -n "${LINKED}" ]];   then export LINKED_INDEX_PATH="${LINKED}";   log "Linked index  : ${LINKED}"; fi
if [[ -n "${UNLINKED}" ]]; then export UNLINKED_INDEX_PATH="${UNLINKED}"; log "Unlinked index: ${UNLINKED}"; fi

# Launch uvicorn in background (module path, so src.* imports resolve)
"${PYTHON}" -m uvicorn src.services.rag.main:app \
    --host "${HOST}" --port "${PORT}" \
    --log-level warning \
    >"${LOG_FILE}" 2>&1 &
SVC_PID=$!
echo "${SVC_PID}" >"${PIDFILE}"
log "Service PID  : ${SVC_PID} (log → ${LOG_FILE})"

# ---------------------------------------------------------------------------
# Wait for /health
# ---------------------------------------------------------------------------
log "Waiting for service to become healthy (timeout ${STARTUP_TIMEOUT}s)…"
ELAPSED=0
until curl -sf "${BASE_URL}/health" >/dev/null 2>&1; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [[ ${ELAPSED} -ge ${STARTUP_TIMEOUT} ]]; then
        warn "Service failed to start within ${STARTUP_TIMEOUT}s. Tail of log:"
        tail -30 "${LOG_FILE}" >&2
        die "Aborting demo."
    fi
    printf "."
done
echo
ok "Service healthy after ${ELAPSED}s"

# ---------------------------------------------------------------------------
# Route: /health
# ---------------------------------------------------------------------------
section "GET /health"
curl -sf "${BASE_URL}/health" | pretty_json
echo

# ---------------------------------------------------------------------------
# Route: /ready
# ---------------------------------------------------------------------------
section "GET /ready  (shows enrichment index counts)"
READY=$(curl -sf "${BASE_URL}/ready")
echo "${READY}" | pretty_json
# Show enrichment summary
echo "${READY}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
e = d.get('enrichment', {})
print(f\"  linked_urls  : {e.get('linked_urls',  'N/A')}\")
print(f\"  unlinked_urls: {e.get('unlinked_urls', 'N/A')}\")
"

# ---------------------------------------------------------------------------
# Route: /search  — no enrichment (default)
# ---------------------------------------------------------------------------
section "POST /search  (enrich=false, default)"
SEARCH_PAYLOAD=$(python3 -c "
import json
print(json.dumps({'query': '${DEMO_QUERY}', 'top_k': 3}))
")
curl -sf -X POST "${BASE_URL}/search" \
    -H 'Content-Type: application/json' \
    -d "${SEARCH_PAYLOAD}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f\"  total_retrieved   : {d['total_retrieved']}\")
print(f\"  enrichment_results: {d.get('enrichment_results')}\")
docs = d.get('retrieved_docs', [])
for i, doc in enumerate(docs, 1):
    print(f\"  [{i}] {doc['source_url'][:80]}  score={doc['similarity_score']:.4f}\")
"
echo

# ---------------------------------------------------------------------------
# Route: /search  — with enrichment (unlinked only, default)
# ---------------------------------------------------------------------------
section "POST /search  (enrich=true, get_linked=false)"
SEARCH_ENRICH_PAYLOAD=$(python3 -c "
import json
print(json.dumps({'query': '${DEMO_QUERY}', 'top_k': 3, 'enrich': True}))
")
curl -sf -X POST "${BASE_URL}/search" \
    -H 'Content-Type: application/json' \
    -d "${SEARCH_ENRICH_PAYLOAD}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
results = d.get('enrichment_results') or []
print(f\"  enrichment_results: {len(results)} record(s)\")
for r in results[:3]:
    linked = bool(r.get('event_descriptor'))
    label = 'event-linked' if linked else 'unlinked'
    print(f\"  [{label}] {r.get('link','?')[:80]}\")
    print(f\"           publisher={r.get('publisher','?')}  salience={r.get('consumer_salience','?')}\")
"
echo

# ---------------------------------------------------------------------------
# Route: /search  — with enrichment + get_linked
# ---------------------------------------------------------------------------
section "POST /search  (enrich=true, get_linked=true, type=events)"
SEARCH_LINKED_PAYLOAD=$(python3 -c "
import json
print(json.dumps({
    'query': '${DEMO_QUERY}',
    'top_k': 3,
    'enrich': True,
    'get_linked': True,
    'enrich_types': ['events']
}))
")
curl -sf -X POST "${BASE_URL}/search" \
    -H 'Content-Type: application/json' \
    -d "${SEARCH_LINKED_PAYLOAD}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
results = d.get('enrichment_results') or []
print(f\"  enrichment_results: {len(results)} event-linked record(s)\")
for r in results[:5]:
    ev = r.get('event_descriptor','?')[:60]
    print(f\"  event: {ev}\")
    print(f\"  link:  {r.get('link','?')[:80]}\")
    mf = r.get('media_framing') or {}
    risk = (mf.get('risk') or {}).get('rating', 'none')
    print(f\"  risk:  {risk}\")
    print()
"
echo

# ---------------------------------------------------------------------------
# Route: /grounding_preview  — with enrichment
# ---------------------------------------------------------------------------
section "POST /grounding_preview  (enrich=true, get_linked=true)"
PREVIEW_PAYLOAD=$(python3 -c "
import json
print(json.dumps({
    'query': '${DEMO_QUERY}',
    'top_k': 3,
    'enrich': True,
    'get_linked': True,
}))
")
curl -sf -X POST "${BASE_URL}/grounding_preview" \
    -H 'Content-Type: application/json' \
    -d "${PREVIEW_PAYLOAD}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
results = d.get('enrichment_results') or []
preview = d.get('enrichment_preview') or ''
print(f\"  enrichment_results : {len(results)} record(s)\")
print(f\"  enrichment_preview : {len(preview)} chars\")
if preview:
    # Print first 600 chars of the formatted grounding block
    print()
    print(preview[:600])
    if len(preview) > 600:
        print('  … (truncated)')
"
echo

# ---------------------------------------------------------------------------
# Route: /search — invalid enrich_types (expect 422)
# ---------------------------------------------------------------------------
section "POST /search  (invalid enrich_types → expect 422)"
HTTP_CODE=$(curl -s -o /tmp/rag_422_$$.json -w "%{http_code}" -X POST "${BASE_URL}/search" \
    -H 'Content-Type: application/json' \
    -d '{"query":"test","enrich":true,"enrich_types":["not_a_real_type"]}')
if [[ "${HTTP_CODE}" == "422" ]]; then
    ok "Correctly rejected with 422"
    python3 -c "
import json
d = json.load(open('/tmp/rag_422_$$.json'))
errs = d.get('detail', [])
for e in (errs if isinstance(errs, list) else [errs]):
    msg = e.get('msg','') if isinstance(e, dict) else str(e)
    print(f'  {msg[:120]}')
"
else
    warn "Expected 422, got ${HTTP_CODE}"
fi
rm -f /tmp/rag_422_$$.json
echo

# ---------------------------------------------------------------------------
# Route: /query_llm  — note: requires live embedding + LLM credentials
# ---------------------------------------------------------------------------
section "POST /query_llm  (enrich=true, get_linked=true) — may fail without GCP creds"
QL_PAYLOAD=$(python3 -c "
import json
print(json.dumps({
    'query': '${DEMO_QUERY}',
    'top_k': 3,
    'mode': 'ask',
    'max_tokens': 512,
    'enrich': True,
    'get_linked': True,
    'enrich_types': ['events', 'risk'],
}))
")
HTTP_CODE=$(curl -s -o /tmp/rag_ql_$$.json -w "%{http_code}" -X POST "${BASE_URL}/query_llm" \
    -H 'Content-Type: application/json' \
    --max-time 60 \
    -d "${QL_PAYLOAD}")
if [[ "${HTTP_CODE}" == "200" ]]; then
    ok "200 OK"
    python3 -c "
import json, sys
d = json.load(open('/tmp/rag_ql_$$.json'))
results = d.get('enrichment_results') or []
prompt  = d.get('prompt', '')
resp    = d.get('llm_response', '')[:300]
print(f'  enrichment_results : {len(results)} record(s)')
print(f'  prompt contains enrichment block: {\"Enrichment Context\" in prompt}')
print(f'  llm_response (first 300 chars):')
print(f'  {resp}')
"
else
    warn "/query_llm returned HTTP ${HTTP_CODE} (embedding/LLM service may not be reachable)"
    python3 -c "
import json
try:
    d = json.load(open('/tmp/rag_ql_$$.json'))
    print(f'  detail: {str(d.get(\"detail\",d))[:200]}')
except Exception:
    pass
"
fi
rm -f /tmp/rag_ql_$$.json
echo

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
section "Demo complete"
ok "All enrichment routes exercised."
ok "Service will now be stopped."
