#!/usr/bin/env python3
"""
Check the status of a session in Azure Storage
"""
import os
from azure.storage.blob import BlobServiceClient

def check_session_status(session_id):
    """Check if session files exist in Azure"""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("❌ AZURE_STORAGE_CONNECTION_STRING not set")
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "session-backups"
    
    print(f"\n🔍 Checking session: {session_id}\n")
    
    files_to_check = [
        f"{session_id}/session_metadata.json",
        f"{session_id}/chunks_aggregated.jsonl",
        f"{session_id}/qa_pairs_aggregated.jsonl"
    ]
    
    for file_path in files_to_check:
        try:
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=file_path
            )
            properties = blob_client.get_blob_properties()
            size_kb = properties.size / 1024
            last_modified = properties.last_modified
            print(f"✅ {file_path}")
            print(f"   Size: {size_kb:.2f} KB")
            print(f"   Modified: {last_modified}")
        except Exception as e:
            print(f"❌ {file_path}")
            print(f"   Error: {str(e)[:100]}")
        print()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python check_session_status.py <session_id>")
        sys.exit(1)
    
    session_id = sys.argv[1]
    check_session_status(session_id)
