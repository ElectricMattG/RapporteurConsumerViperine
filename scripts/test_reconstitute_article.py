import json
import time

import requests

RAG_URL = "http://localhost:8000"
INDEX = "x_indices_merged"
URL = "https://www.rickscott.senate.gov/2025/1/why-the-pentagon-needs-pete-hegseth"

def test_reconstitution():
    print(f"--- Switching to index {INDEX} ---")
    resp = requests.post(f"{RAG_URL}/switch-index", json={"folder": INDEX})
    print(f"Switch status: {resp.status_code}")

    print(f"--- Querying metadata for {URL} ---")
    payload = {
        "source_url": URL,
        "include_summaries": True,
        "include_qa": True,
        "max_results": 100
    }

    resp = requests.post(f"{RAG_URL}/query_metadata", json=payload)
    if resp.status_code != 200:
        print(f"Error: {resp.text}")
        return

    data = resp.json()

    docs = data.get("retrieved_docs", [])
    qas = data.get("retrieved_qa", [])

    print(f"\nRetrieved {len(docs)} summary blocks and {len(qas)} QA pairs.")

    print("\n--- RECONSTITUTED DOCUMENT ---")

    full_text = []

    # Add Title/URL
    full_text.append(f"# {data.get('doc_metadata', {}).get('title', 'Unknown Title')}")
    full_text.append(f"Source: {URL}\n")

    # Add Summaries
    full_text.append("## Executive Summary\n")
    for doc in docs:
        full_text.append(doc.get("text", ""))

    # Add Q&A
    full_text.append("\n## Detailed Content (Q&A)\n")
    for qa in qas:
        full_text.append(f"Q: {qa.get('question', '')}")
        full_text.append(f"A: {qa.get('answer', '')}\n")

    reconstituted = "\n".join(full_text)
    print(reconstituted)

if __name__ == "__main__":
    test_reconstitution()
