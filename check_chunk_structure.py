#!/usr/bin/env python3
"""Check the structure of chunked_document_enriched_embedded.jsonl"""

import json
import os
from azure.storage.blob import BlobServiceClient

def main():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("❌ AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(
        container="archive",
        blob="sess_e0fc67c87f0lc/req_20251225_231034_000/chunked_document_enriched_embedded.jsonl"
    )
    
    print("Downloading chunked_document_enriched_embedded.jsonl...")
    blob_data = blob_client.download_blob().readall()
    lines = blob_data.decode('utf-8').split('\n')
    
    print(f"\nTotal lines: {len([l for l in lines if l.strip()])}")
    
    # Parse first chunk
    first_chunk = json.loads(lines[0].strip())
    
    print("\n" + "="*80)
    print("FIRST CHUNK STRUCTURE")
    print("="*80)
    
    print("\nTop-level keys:")
    for key in first_chunk.keys():
        print(f"  - {key}")
    
    print("\n" + "="*80)
    print("HAS document_metadata?", "document_metadata" in first_chunk)
    print("="*80)
    
    if "document_metadata" in first_chunk:
        print("\ndocument_metadata keys:")
        for key in first_chunk['document_metadata'].keys():
            print(f"  - {key}")
        
        if 'query_id' in first_chunk['document_metadata']:
            print(f"\n✅ query_id in document_metadata: '{first_chunk['document_metadata']['query_id']}'")
        else:
            print("\n❌ query_id NOT in document_metadata")
    
    if 'query_id' in first_chunk:
        print(f"\n✅ query_id at top level: '{first_chunk['query_id']}'")
    else:
        print("\n❌ query_id NOT at top level")
    
    print("\n" + "="*80)
    print("SAMPLE CHUNK (first 1000 chars):")
    print("="*80)
    print(json.dumps(first_chunk, indent=2)[:1000])

if __name__ == "__main__":
    main()
