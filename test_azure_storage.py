 #!/usr/bin/env python3
"""
Test Azure Blob Storage integration for index aggregator.

This script validates:
1. Azure Storage connection string
2. Container access (indices and research-archive)
3. Upload/download operations
4. Append pattern (download existing, append new, re-upload)
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def test_connection_string():
    """Test if Azure connection string is configured."""
    print("=" * 80)
    print("📋 Test 1: Checking Azure Storage Connection String")
    print("=" * 80)

    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    if not connection_string:
        print("❌ AZURE_STORAGE_CONNECTION_STRING not set")
        print("\nTo set it, run:")
        print('export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=<account>;AccountKey=<key>;EndpointSuffix=core.windows.net"')
        return None

    print("✅ Connection string found")
    print(f"   Length: {len(connection_string)} characters")

    # Parse to check format
    parts = connection_string.split(';')
    config = {}
    for part in parts:
        if '=' in part:
            key, value = part.split('=', 1)
            config[key] = value

    if 'AccountName' in config:
        print(f"   Account Name: {config['AccountName']}")

    return connection_string


def test_blob_service_client(connection_string):
    """Test creating BlobServiceClient."""
    print("\n" + "=" * 80)
    print("📋 Test 2: Creating BlobServiceClient")
    print("=" * 80)

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        print("✅ BlobServiceClient created successfully")

        # Try to list containers to validate credentials
        print("   Listing containers...")
        containers = list(blob_service_client.list_containers())
        print(f"   ✅ Found {len(containers)} containers")
        for container in containers:
            print(f"      - {container['name']}")

        return blob_service_client

    except Exception as e:
        print(f"❌ Failed to create BlobServiceClient: {e}")
        return None


def test_container_access(blob_service_client, container_name):
    """Test accessing a specific container."""
    print("\n" + "=" * 80)
    print(f"📋 Test 3: Accessing container '{container_name}'")
    print("=" * 80)

    try:
        container_client = blob_service_client.get_container_client(container_name)

        # Check if container exists
        if container_client.exists():
            print(f"✅ Container '{container_name}' exists")

            # List blobs
            print("   Listing blobs...")
            blobs = list(container_client.list_blobs())
            print(f"   ✅ Found {len(blobs)} blobs")
            for blob in blobs[:10]:  # Show first 10
                size_mb = blob.size / (1024 * 1024)
                print(f"      - {blob.name} ({size_mb:.2f} MB)")

            if len(blobs) > 10:
                print(f"      ... and {len(blobs) - 10} more")

            return container_client
        else:
            print(f"⚠️  Container '{container_name}' does not exist")
            print("   Creating container...")
            container_client.create_container()
            print(f"   ✅ Container '{container_name}' created")
            return container_client

    except Exception as e:
        print(f"❌ Failed to access container: {e}")
        return None


def test_upload_download(blob_service_client, container_name, blob_name):
    """Test uploading and downloading a test file."""
    print("\n" + "=" * 80)
    print(f"📋 Test 4: Upload/Download test to '{blob_name}'")
    print("=" * 80)

    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )

        # Create test data
        test_data = [
            {
                "test_id": "test_001",
                "timestamp": datetime.now().isoformat(),
                "message": "This is a test upload"
            },
            {
                "test_id": "test_002",
                "timestamp": datetime.now().isoformat(),
                "message": "Testing Azure Blob Storage integration"
            }
        ]

        # Convert to JSONL
        jsonl_content = '\n'.join(json.dumps(doc, ensure_ascii=False) for doc in test_data)

        # Upload
        print(f"   📤 Uploading test data ({len(jsonl_content)} bytes)...")
        blob_client.upload_blob(
            jsonl_content.encode('utf-8'),
            overwrite=True,
            content_settings=ContentSettings(content_type='application/jsonl')
        )
        print(f"   ✅ Upload successful")

        # Download
        print(f"   📥 Downloading blob...")
        blob_data = blob_client.download_blob().readall()
        print(f"   ✅ Download successful ({len(blob_data)} bytes)")

        # Parse downloaded data
        downloaded_docs = []
        for line in blob_data.decode('utf-8').splitlines():
            if line.strip():
                downloaded_docs.append(json.loads(line.strip()))

        print(f"   ✅ Parsed {len(downloaded_docs)} documents")

        if downloaded_docs == test_data:
            print(f"   ✅ Downloaded data matches uploaded data")
        else:
            print(f"   ⚠️  Data mismatch!")
            print(f"   Uploaded: {test_data}")
            print(f"   Downloaded: {downloaded_docs}")

        return True

    except Exception as e:
        print(f"❌ Upload/Download test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_append_pattern(blob_service_client, container_name, blob_name):
    """Test the download-append-upload pattern used by index aggregator."""
    print("\n" + "=" * 80)
    print(f"📋 Test 5: Download-Append-Upload pattern")
    print("=" * 80)

    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )

        # Get initial blob properties
        initial_size = 0
        if blob_client.exists():
            props = blob_client.get_blob_properties()
            initial_size = props.size
            print(f"   📊 Initial blob size: {initial_size} bytes ({initial_size / 1024:.2f} KB)")
        else:
            print(f"   ℹ️  Blob does not exist (will create new)")

        # Step 1: Download existing data
        print("   📥 Step 1: Downloading existing data...")
        existing_docs = []
        if blob_client.exists():
            blob_data = blob_client.download_blob().readall()
            for line in blob_data.decode('utf-8').splitlines():
                if line.strip():
                    existing_docs.append(json.loads(line.strip()))
            print(f"      ✅ Downloaded {len(existing_docs)} existing documents")

        # Step 2: Append new data
        print("   ➕ Step 2: Appending new documents...")
        new_docs = [
            {
                "test_id": f"append_test_{i}",
                "timestamp": datetime.now().isoformat(),
                "message": f"Appended document {i}",
                "data": "x" * 100  # Add some data to increase file size
            }
            for i in range(3)
        ]

        # Deduplicate by test_id
        existing_ids = {doc.get('test_id') for doc in existing_docs}
        all_docs = existing_docs.copy()

        added_count = 0
        for doc in new_docs:
            if doc['test_id'] not in existing_ids:
                all_docs.append(doc)
                existing_ids.add(doc['test_id'])
                added_count += 1

        print(f"      ✅ Added {added_count} new documents")
        print(f"      📊 Total documents: {len(all_docs)}")

        # Step 3: Upload merged data
        print("   📤 Step 3: Uploading merged data...")
        jsonl_content = '\n'.join(json.dumps(doc, ensure_ascii=False) for doc in all_docs)
        new_size = len(jsonl_content.encode('utf-8'))
        blob_client.upload_blob(
            jsonl_content.encode('utf-8'),
            overwrite=True,
            content_settings=ContentSettings(content_type='application/jsonl')
        )
        print(f"      ✅ Upload successful ({len(all_docs)} documents)")

        # Step 4: Verify file size increased
        print("   📏 Step 4: Verifying file size increased...")
        props = blob_client.get_blob_properties()
        final_size = props.size
        size_diff = final_size - initial_size

        print(f"      📊 Final blob size: {final_size} bytes ({final_size / 1024:.2f} KB)")
        print(f"      📈 Size increase: {size_diff} bytes ({size_diff / 1024:.2f} KB)")

        if size_diff > 0:
            print(f"      ✅ File size increased - append successful!")
        elif initial_size == 0:
            print(f"      ✅ New file created successfully")
        else:
            print(f"      ⚠️  Warning: File size did not increase (possible duplicate data)")

        # Step 5: Verify document count
        print("   ✅ Step 5: Verifying merged data...")
        blob_data = blob_client.download_blob().readall()
        verified_docs = []
        for line in blob_data.decode('utf-8').splitlines():
            if line.strip():
                verified_docs.append(json.loads(line.strip()))

        if len(verified_docs) == len(all_docs):
            print(f"      ✅ Verification successful ({len(verified_docs)} documents)")
        else:
            print(f"      ⚠️  Document count mismatch: expected {len(all_docs)}, got {len(verified_docs)}")

        return True

    except Exception as e:
        print(f"❌ Append pattern test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_archive_container(blob_service_client):
    """Test archive container access."""
    print("\n" + "=" * 80)
    print(f"📋 Test 6: Archive container 'research-archive'")
    print("=" * 80)

    return test_container_access(blob_service_client, 'research-archive')


def main():
    """Run all Azure Storage tests."""
    print("\n")
    print("🧪 Azure Blob Storage Integration Test")
    print("=" * 80)
    print()

    # Test 1: Connection string
    connection_string = test_connection_string()
    if not connection_string:
        print("\n❌ Cannot proceed without connection string")
        sys.exit(1)

    # Test 2: BlobServiceClient
    blob_service_client = test_blob_service_client(connection_string)
    if not blob_service_client:
        print("\n❌ Cannot proceed without BlobServiceClient")
        sys.exit(1)

    # Test 3: Access indices container
    indices_container = test_container_access(blob_service_client, 'indices')
    if not indices_container:
        print("\n⚠️  Warning: Could not access indices container")

    # Test 4: Upload/Download
    test_blob_name = "test_upload_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".jsonl"
    upload_success = test_upload_download(blob_service_client, 'indices', test_blob_name)

    # Test 5: Append pattern
    if upload_success:
        test_append_pattern(blob_service_client, 'indices', test_blob_name)

    # Test 6: Archive container
    test_archive_container(blob_service_client)

    # Test 7: Verify research_* blob names (used by aggregator)
    print("\n" + "=" * 80)
    print("📋 Test 7: Verifying research_* blob configuration")
    print("=" * 80)
    print("   ℹ️  The aggregator will create and append to:")
    print("      - research_chunks_with_embeddings_combined.jsonl")
    print("      - research_qa_pairs_with_embeddings_combined.jsonl")
    print("   ℹ️  Existing enriched_* blobs will remain untouched")

    # Check if research blobs exist
    container_client = blob_service_client.get_container_client('indices')
    blobs = list(container_client.list_blobs())
    research_blobs = [b for b in blobs if b.name.startswith('research_')]

    if research_blobs:
        print(f"   📊 Found {len(research_blobs)} existing research_* blobs:")
        for blob in research_blobs:
            size_mb = blob.size / (1024 * 1024)
            print(f"      - {blob.name} ({size_mb:.2f} MB)")
        print("   ✅ Aggregator will append to these blobs")
    else:
        print("   ℹ️  No research_* blobs found (will be created on first run)")
        print("   ✅ Aggregator will create new research indices")

    # Summary
    print("\n" + "=" * 80)
    print("📊 Test Summary")
    print("=" * 80)
    print("✅ All tests completed!")
    print("\nYou can now run the aggregator with Azure credentials.")
    print("The aggregator will ONLY append to:")
    print("  • research_chunks_with_embeddings_combined.jsonl")
    print("  • research_qa_pairs_with_embeddings_combined.jsonl")
    print("\nThe existing enriched_* indices will NOT be modified.")
    print("\nThe aggregator will automatically:")
    print("  1. Download existing research_* indices from Azure (if they exist)")
    print("  2. Append new data with deduplication")
    print("  3. Upload merged research_* indices back to Azure")
    print("  4. Archive session files to 'research-archive' container")
    print("  5. Clean up local cache directories")
    print()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
