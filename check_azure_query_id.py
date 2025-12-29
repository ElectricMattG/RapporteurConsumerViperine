#!/usr/bin/env python3
"""Check if query_id exists in document_metadata of Azure blobs."""

import json
import os
from azure.storage.blob import BlobServiceClient

def check_blob_for_query_id(blob_service_client, container_name, blob_name, blob_type):
    """Download and check first line of blob for query_id."""
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        print(f"\n{'='*80}")
        print(f"Checking {blob_type}: {blob_name}")
        print(f"{'='*80}")
        
        # Download blob
        blob_data = blob_client.download_blob().readall()
        lines = blob_data.decode('utf-8').split('\n')
        
        if not lines[0].strip():
            print("❌ First line is empty")
            return
        
        # Parse first line
        first_item = json.loads(lines[0].strip())
        
        # Check for document_metadata
        if 'document_metadata' not in first_item:
            print("❌ No 'document_metadata' field found")
            print(f"Available top-level keys: {list(first_item.keys())}")
            return
        
        doc_metadata = first_item['document_metadata']
        print(f"✅ document_metadata found with keys: {list(doc_metadata.keys())}")
        
        # Check for query_id
        if 'query_id' in doc_metadata:
            print(f"✅ query_id found: '{doc_metadata['query_id']}'")
        else:
            print(f"❌ query_id NOT found in document_metadata")
            print(f"Document metadata contents:")
            for key, value in doc_metadata.items():
                if isinstance(value, str) and len(value) > 100:
                    print(f"  {key}: {value[:100]}...")
                else:
                    print(f"  {key}: {value}")
        
    except Exception as e:
        print(f"❌ Error checking {blob_name}: {e}")

def main():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("❌ AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Check chunks with embeddings
    check_blob_for_query_id(
        blob_service_client,
        "archive",
        "sess_e0fc67c87f0lc/req_20251225_231034_000/chunked_document_enriched_embedded.jsonl",
        "Embedded Chunks"
    )
    
    # Check QA pairs with embeddings
    check_blob_for_query_id(
        blob_service_client,
        "archive",
        "sess_e0fc67c87f0lc/req_20251225_231034_000/req_20251225_231034_000_qa_pairs_enriched_embedded.jsonl",
        "Embedded QA Pairs"
    )
    
    print(f"\n{'='*80}")
    print("Analysis complete")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
