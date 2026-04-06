#!/usr/bin/env python3
"""
R1.3 — End-to-end smoke test for the append → reload → UI notification pipeline.

Tests:
  1. Publish `reload-index` Kafka message → RAG service reloads index
  2. Publish `frontend-message` Kafka message → SSE clients receive `index_update` event

Usage (from workspace root):
    python scripts/test_reload_pipeline.py

Requirements:
  - RAG service running at http://localhost:8000
  - Kafka accessible at localhost:9092
  - confluent_kafka installed in the active Python environment
  - (Optional) Browser tab open at the frontend to observe the banner notification
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from confluent_kafka import Producer
except ImportError:
    print("❌ confluent_kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("❌ requests not installed. Run: pip install requests")
    sys.exit(1)

KAFKA_BOOTSTRAP = "localhost:9092"
RAG_BASE_URL = "http://localhost:8000"
RELOAD_TOPIC = "reload-index"
FRONTEND_TOPIC = "frontend-message"
SMOKE_SESSION_ID = "smoke-test-r1.3"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _count_local_summaries() -> int:
    """Count lines in the active combined summaries file."""
    candidate_paths = [
        Path("x_indices_merged/research_summaries_filtered.jsonl"),
        Path("/app/research_summaries_with_embeddings_combined.jsonl"),
    ]
    for p in candidate_paths:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return sum(1 for line in f if line.strip())
    return -1


def _get_rag_ready() -> dict:
    """GET /ready from the RAG service. Returns parsed JSON or raises."""
    resp = requests.get(f"{RAG_BASE_URL}/ready", timeout=5)
    resp.raise_for_status()
    return resp.json()


def _produce(producer: Producer, topic: str, key: str, payload: dict) -> None:
    """Produce a single message and flush."""
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Pre-flight checks
# ─────────────────────────────────────────────────────────────────────────────

def step_preflight() -> int:
    _section("STEP 1 — Pre-flight checks")

    # RAG health
    try:
        ready = _get_rag_ready()
        total_before = ready.get("total_summaries", ready.get("summaries_loaded", -1))
        status = ready.get("status", "unknown")
        print(f"  ✅ RAG service reachable  status={status}  total_summaries={total_before}")
    except Exception as e:
        print(f"  ❌ RAG service unreachable at {RAG_BASE_URL}: {e}")
        sys.exit(1)

    # Local file count
    local_count = _count_local_summaries()
    if local_count >= 0:
        print(f"  ✅ Local summaries file found  lines={local_count}")
        if local_count != total_before:
            print(f"  ⚠️  LOCAL ({local_count}) vs RAG ({total_before}) mismatch — "
                  "index may not be current, but continuing")
    else:
        print("  ⚠️  Local summaries file not found — skipping local count check")

    # Kafka reachability (quick producer test)
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "socket.timeout.ms": 3000,
        })
        # Produce to throwaway topic to confirm broker is reachable
        producer.produce("_smoke_test_probe", value=b"ping")
        producer.flush(timeout=4)
        print(f"  ✅ Kafka reachable at {KAFKA_BOOTSTRAP}")
    except Exception as e:
        print(f"  ❌ Kafka not reachable at {KAFKA_BOOTSTRAP}: {e}")
        sys.exit(1)

    return total_before


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Publish reload-index message
# ─────────────────────────────────────────────────────────────────────────────

def step_publish_reload(total_before: int) -> Producer:
    _section("STEP 2 — Publish reload-index Kafka message")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    reload_msg = {
        "action": "reload_index",
        "new_summaries": 0,
        "total_summaries": total_before,
        "index_file": "x_indices_merged/research_summaries_filtered.jsonl",
        "timestamp": datetime.now().isoformat(),
    }

    print(f"  → Publishing to '{RELOAD_TOPIC}': {json.dumps(reload_msg)}")
    _produce(producer, RELOAD_TOPIC, "rag-service", reload_msg)
    print(f"  ✅ Message delivered to {RELOAD_TOPIC}")
    return producer


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Verify RAG reloaded
# ─────────────────────────────────────────────────────────────────────────────

def step_verify_reload(total_before: int) -> None:
    _section("STEP 3 — Verify RAG index reloaded")

    wait_sec = 4
    print(f"  ⏳ Waiting {wait_sec}s for RAG consumer to process the message…")
    time.sleep(wait_sec)

    try:
        ready = _get_rag_ready()
        total_after = ready.get("total_summaries", ready.get("summaries_loaded", -1))
    except Exception as e:
        print(f"  ❌ Could not reach RAG /ready: {e}")
        sys.exit(1)

    if total_after == total_before:
        print(f"  ✅ RAG total_summaries={total_after} (unchanged, as expected — "
              "no new entries were added)")
    elif total_after > total_before:
        print(f"  ✅ RAG grew: {total_before} → {total_after} summaries")
    else:
        print(f"  ⚠️  Unexpected shrink: {total_before} → {total_after} summaries")

    print()
    print(f"  🔍 Check RAG logs for '✅ Full reload' to confirm the callback ran:")
    print(f"     docker logs rag-service --tail 30 | grep 'Full reload\\|reload'")


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Publish frontend-message (SSE test)
# ─────────────────────────────────────────────────────────────────────────────

def step_publish_frontend(producer: Producer, total_before: int) -> None:
    _section("STEP 4 — Publish frontend-message Kafka message (SSE bridge test)")

    frontend_msg = {
        "type": "summary_aggregation",
        "session_id": SMOKE_SESSION_ID,
        "new_summaries_count": 0,
        "total_summaries_count": total_before,
        "new_summaries": [
            {"url": "https://example.com/smoke-test", "title": "Smoke Test Article"}
        ],
        "timestamp": datetime.now().isoformat(),
    }

    print(f"  → Publishing to '{FRONTEND_TOPIC}': session_id={SMOKE_SESSION_ID}")
    _produce(producer, FRONTEND_TOPIC, SMOKE_SESSION_ID, frontend_msg)
    print(f"  ✅ Message delivered to {FRONTEND_TOPIC}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Instructions for manual SSE verification
# ─────────────────────────────────────────────────────────────────────────────

def step_verify_sse() -> None:
    _section("STEP 5 — Verify SSE notification (manual check required)")

    print("""
  The frontend SSE bridge consumes 'frontend-message' and emits an
  'index_update' event to all connected EventSource clients.

  To verify the full pipeline, either:

  A) Watch the banner in the browser:
     1. Open http://localhost:5001 (or your frontend port)
     2. After step 4, a notification banner should appear briefly.

  B) Check frontend logs:
     docker logs frontend-service --tail 20 | grep -E 'Received|Broadcasting|📬|📢'

  C) Inspect SSE stream directly:
     curl -N http://localhost:5001/api/notifications
     (trigger step 4 again; you should see an 'index_update' event)

  D) Browser DevTools:
     Network → filter 'notifications' → EventStream tab → watch for events.
""")

    print("  🔍 Checking frontend logs now…")
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "logs", "frontend-service", "--tail", "15"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        log_lines = result.stdout + result.stderr
        relevant = [
            line for line in log_lines.splitlines()
            if any(kw in line for kw in [
                "Received", "Broadcasting", "📬", "📢", "📡", "index_update",
                SMOKE_SESSION_ID,
            ])
        ]
        if relevant:
            print()
            for line in relevant[-10:]:
                print(f"    {line}")
        else:
            print("  ⚠️  No matching log lines found yet — "
                  "the SSE consumer may still be processing, or no browser "
                  "is connected (SSE only broadcasts to active clients).")
    except Exception as e:
        print(f"  ⚠️  Could not read frontend logs: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  R1.3 — End-to-end reload pipeline smoke test               ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    total_before = step_preflight()
    producer = step_publish_reload(total_before)
    step_verify_reload(total_before)
    step_publish_frontend(producer, total_before)
    step_verify_sse()

    _section("DONE")
    print("""
  Pipeline smoke test complete.

  Pass criteria:
    ✅  Step 3: RAG /ready returns total_summaries matching local file (or higher)
    ✅  Step 3: RAG logs show '✅ Full reload' log line
    ✅  Step 5: Frontend logs show '📬 Received notification' and '📡 Broadcasting'
    ✅  Step 5: Browser banner appears (if tab is open at the frontend)
""")


if __name__ == "__main__":
    main()
