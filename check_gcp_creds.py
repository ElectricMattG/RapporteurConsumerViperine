#!/usr/bin/env python3
"""Check for GCP credentials and test authentication."""

import os
from pathlib import Path

print("=" * 80)
print("🔍 Checking for GCP Credentials")
print("=" * 80)

# Check environment variable
env_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
print(f"\n1. GOOGLE_APPLICATION_CREDENTIALS env var: {env_creds}")
if env_creds and os.path.exists(env_creds):
    print(f"   ✅ File exists: {env_creds}")
elif env_creds:
    print(f"   ❌ File does not exist: {env_creds}")

# Check standard locations
print("\n2. Checking standard credential file locations:")
possible_paths = [
    '/home/electric/.config/gcp-keys/rapporteur-sa.json',
    os.path.expanduser('~/.config/gcp-keys/rapporteur-sa.json'),
    os.path.expanduser('~/.config/gcloud/application_default_credentials.json'),
    os.path.expanduser('~/.gemini/oauth_creds.json'),
]

found_any = False
for path in possible_paths:
    exists = os.path.exists(path)
    status = "✅" if exists else "❌"
    print(f"   {status} {path}")
    if exists:
        found_any = True
        # Try to read and validate it's JSON
        try:
            with open(path) as f:
                import json
                data = json.load(f)
                # Check what type of credentials
                if 'type' in data:
                    print(f"      Type: {data.get('type')}")
                if 'client_id' in data:
                    print(f"      Has client_id (OAuth)")
                if 'project_id' in data:
                    print(f"      Project: {data.get('project_id')}")
        except Exception as e:
            print(f"      ⚠️  Error reading file: {e}")

if not found_any:
    print("\n❌ No credential files found!")
    print("\nTo fix this, you need to either:")
    print("1. Run: gcloud auth application-default login")
    print("2. Or create a service account key and set GOOGLE_APPLICATION_CREDENTIALS")
else:
    print("\n✅ Found at least one credential file")

# Try to authenticate with google.auth
print("\n3. Testing authentication with google.auth.default():")
try:
    import google.auth
    credentials, project = google.auth.default()
    print(f"   ✅ Authentication successful!")
    print(f"   Project: {project}")
    print(f"   Credentials type: {type(credentials).__name__}")
except Exception as e:
    print(f"   ❌ Authentication failed: {e}")
    print("\n   You need to set up credentials. Run:")
    print("   gcloud auth application-default login --project rapporteurclients")

print("\n" + "=" * 80)
