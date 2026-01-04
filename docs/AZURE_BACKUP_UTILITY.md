# Azure Storage Backup Utility

A comprehensive Python utility for backing up Azure Storage containers to long-term storage with automatic cleanup capabilities.

## Overview

The `backup_azure_storage.py` script provides a complete solution for:
- 📦 Downloading blobs from Azure Storage containers
- 🗜️ Creating compressed archives (ZIP or TAR.GZ)
- 📤 Uploading to a dedicated `long-term-storage` container
- 📋 Tracking metadata (session ID, size, timestamp, source)
- 🧹 Cleaning up source containers after successful backup
- 🔍 Pattern-based filtering for selective backups

## Features

### Backup Capabilities
- **Session-based backups**: Backup specific research sessions with all associated data
- **Container backups**: Backup entire containers or filtered subsets
- **Flexible formats**: Choose between ZIP (default) or TAR.GZ compression
- **Meaningful naming**: Automatic or custom backup names with timestamps
- **Metadata tracking**: Each backup includes detailed metadata in JSON format

### Cleanup Capabilities
- **Automatic cleanup**: Delete source blobs after successful backup
- **Dry-run mode**: Preview deletions without making changes
- **Safety confirmations**: Requires explicit "yes" confirmation before deletion
- **Pattern filtering**: Clean up only specific blobs matching regex patterns
- **Standalone cleanup**: Clean up independently from backup operations

### Safety Features
- ✅ Confirmation prompts before any deletion
- ✅ Dry-run mode for risk-free testing
- ✅ Metadata preservation for backup verification
- ✅ Pattern-based filtering to prevent accidental deletion
- ✅ Backup-first approach (only cleans after successful upload)

## Prerequisites

### Required Environment Variables
```bash
AZURE_STORAGE_CONNECTION_STRING=<your_connection_string>
```

Set this in your `.env` file or export it in your shell.

### Required Python Packages
```bash
pip install azure-storage-blob python-dotenv
```

## Installation

1. Ensure the script is executable:
```bash
chmod +x backup_azure_storage.py
```

2. Verify environment setup:
```bash
python backup_azure_storage.py list-backups
```

## Usage

### Command Structure
```bash
python backup_azure_storage.py <command> [options]
```

## Commands

### 1. Session Backup

Backup a specific research session (e.g., `sess_20260102_220418`) with all its associated data.

#### Syntax
```bash
python backup_azure_storage.py session --session <session_id> [options]
```

#### Options
- `--session` (required): Session ID to backup
- `--container` (optional): Source container (default: `archive`)
- `--name` (optional): Custom backup name
- `--format` (optional): Archive format - `zip` or `tar.gz` (default: `zip`)
- `--cleanup` (optional): Delete source blobs after successful backup
- `--dry-run` (optional): Preview cleanup without actual deletion

#### Examples

**Basic session backup:**
```bash
python backup_azure_storage.py session --session sess_20260102_220418
```
Output: `backup_sess_20260102_220418_20260102_220530.zip`

**Custom name:**
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --name jan2_research_backup
```
Output: `jan2_research_backup.zip`

**Backup with cleanup:**
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --cleanup
```
Downloads → Archives → Uploads → Confirms → Deletes source

**Backup with dry-run cleanup:**
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --cleanup --dry-run
```
Shows what would be deleted without actually deleting

**TAR.GZ format:**
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --format tar.gz
```
Output: `backup_sess_20260102_220418_20260102_220530.tar.gz`

---

### 2. Container Backup

Backup an entire container or filtered subset of blobs.

#### Syntax
```bash
python backup_azure_storage.py container --container <container_name> [options]
```

#### Options
- `--container` (required): Container name to backup
- `--pattern` (optional): Regex pattern to filter blobs
- `--name` (optional): Custom backup name
- `--format` (optional): Archive format - `zip` or `tar.gz` (default: `zip`)
- `--cleanup` (optional): Delete source blobs after successful backup
- `--dry-run` (optional): Preview cleanup without actual deletion

#### Examples

**Backup entire container:**
```bash
python backup_azure_storage.py container --container archive
```
Output: `backup_archive_20260102_220530.zip`

**Backup with filter pattern (January 2026 sessions):**
```bash
python backup_azure_storage.py container \
  --container archive \
  --pattern "sess_202601.*"
```
Output: `backup_archive_filtered_20260102_220530.zip`

**Backup specific session pattern with custom name:**
```bash
python backup_azure_storage.py container \
  --container archive \
  --pattern "sess_20260102.*" \
  --name jan2_all_sessions
```
Output: `jan2_all_sessions.zip`

**Backup and cleanup filtered data:**
```bash
python backup_azure_storage.py container \
  --container archive \
  --pattern "sess_202601.*" \
  --cleanup
```
Backs up and removes all January 2026 sessions

---

### 3. List Sessions

List all available session IDs in a container.

#### Syntax
```bash
python backup_azure_storage.py list-sessions [--container <name>]
```

#### Options
- `--container` (optional): Container to list from (default: `archive`)

#### Example
```bash
python backup_azure_storage.py list-sessions
```

**Output:**
```
📋 Sessions in archive:

  • sess_20260101_120000
  • sess_20260102_220418
  • sess_20260103_150000

✅ Total sessions: 3
```

---

### 4. List Backups

List all backups in the long-term storage container.

#### Syntax
```bash
python backup_azure_storage.py list-backups
```

#### Example
```bash
python backup_azure_storage.py list-backups
```

**Output:**
```
📦 Backups in long-term-storage:

  📁 backup_sess_20260102_220418_20260102_220530.zip
     Size: 45.32 MB | Modified: 2026-01-02 22:05:30

  📁 jan2_research_backup.zip
     Size: 45.32 MB | Modified: 2026-01-02 22:10:15

✅ Total backups: 2
```

---

### 5. Cleanup Session

Clean up a specific session independently (use after manual backup verification).

#### Syntax
```bash
python backup_azure_storage.py cleanup-session --session <session_id> [options]
```

#### Options
- `--session` (required): Session ID to clean up
- `--container` (optional): Container to clean from (default: `archive`)
- `--dry-run` (optional): Preview without actual deletion

#### Examples

**Dry-run (preview only):**
```bash
python backup_azure_storage.py cleanup-session \
  --session sess_20260102_220418 \
  --dry-run
```

**Actual cleanup:**
```bash
python backup_azure_storage.py cleanup-session \
  --session sess_20260102_220418
```

**Output:**
```
⚠️  Are you sure you want to delete session sess_20260102_220418? (yes/no): yes

🧹 Cleaning up session: sess_20260102_220418
  🗑️  Deleting: sess_20260102_220418/req_001/chunked_document.jsonl
  🗑️  Deleting: sess_20260102_220418/req_001/summary.jsonl
  ...
✅ Deleted 45 blobs for session sess_20260102_220418
```

---

### 6. Cleanup Container

Clean up blobs from a container with optional pattern filtering.

#### Syntax
```bash
python backup_azure_storage.py cleanup-container --container <name> [options]
```

#### Options
- `--container` (required): Container to clean up
- `--pattern` (optional): Regex pattern to filter blobs
- `--dry-run` (optional): Preview without actual deletion

#### Examples

**Cleanup all January 2026 sessions (dry-run):**
```bash
python backup_azure_storage.py cleanup-container \
  --container archive \
  --pattern "sess_202601.*" \
  --dry-run
```

**Actual cleanup:**
```bash
python backup_azure_storage.py cleanup-container \
  --container archive \
  --pattern "sess_202601.*"
```

---

## Workflow Examples

### Complete Backup and Cleanup Workflow

#### 1. List available sessions
```bash
python backup_azure_storage.py list-sessions
```

#### 2. Preview backup with cleanup (dry-run)
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --cleanup --dry-run
```

#### 3. Execute backup with cleanup
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --name jan2_important_research \
  --cleanup
```

#### 4. Verify backup
```bash
python backup_azure_storage.py list-backups
```

---

### Monthly Archival Workflow

Archive all sessions from a specific month and clean up storage:

#### 1. Backup all December 2025 sessions
```bash
python backup_azure_storage.py container \
  --container archive \
  --pattern "sess_202512.*" \
  --name december_2025_archive \
  --format tar.gz
```

#### 2. Verify backup in long-term storage
```bash
python backup_azure_storage.py list-backups
```

#### 3. Clean up December sessions (after verification)
```bash
python backup_azure_storage.py cleanup-container \
  --container archive \
  --pattern "sess_202512.*"
```

---

### Cautious Backup Workflow (Maximum Safety)

For critical data, use a two-step process:

#### Step 1: Backup without cleanup
```bash
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --name critical_research_backup
```

#### Step 2: Manually verify backup
Download and inspect the backup from long-term storage

#### Step 3: Cleanup after verification
```bash
python backup_azure_storage.py cleanup-session \
  --session sess_20260102_220418
```

---

## Backup Structure

### Archive Contents
Each backup archive contains the original folder structure:
```
backup_sess_20260102_220418_20260102_220530.zip
├── sess_20260102_220418/
│   ├── req_20260102_220418_000/
│   │   ├── chunked_document.jsonl
│   │   ├── chunked_document_enriched.jsonl
│   │   ├── chunked_document_enriched_embedded.jsonl
│   │   ├── req_20260102_220418_000_qa_pairs_enriched_embedded.jsonl
│   │   ├── summary.jsonl
│   │   └── ...
│   ├── req_20260102_220418_001/
│   │   └── ...
│   └── ...
```

### Metadata Structure
Each backup includes a metadata file:
```json
{
  "backup_name": "backup_sess_20260102_220418_20260102_220530",
  "session_id": "sess_20260102_220418",
  "source_container": "archive",
  "blob_count": 45,
  "total_size_bytes": 47543296,
  "total_size_mb": 45.32,
  "backup_timestamp": "2026-01-02T22:05:30.123456",
  "created_by": "backup_azure_storage.py"
}
```

---

## Storage Containers

### Source Containers
- `archive`: Primary storage for processed research sessions
- `research-archive`: Alternative storage location
- `indices`: Index files
- Custom containers as specified

### Destination Container
- `long-term-storage`: Automatically created if it doesn't exist
  - Stores compressed backup archives
  - Stores metadata JSON files
  - Organized by backup name

---

## Best Practices

### 1. Always Use Dry-Run First
```bash
# Preview before executing
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --cleanup --dry-run
```

### 2. Use Meaningful Backup Names
```bash
# Instead of auto-generated names
python backup_azure_storage.py session \
  --session sess_20260102_220418 \
  --name q1_2026_climate_research
```

### 3. Verify Backups Before Cleanup
```bash
# Step 1: Backup
python backup_azure_storage.py session --session sess_20260102_220418

# Step 2: List and verify
python backup_azure_storage.py list-backups

# Step 3: Cleanup after verification
python backup_azure_storage.py cleanup-session --session sess_20260102_220418
```

### 4. Use Pattern Filtering for Bulk Operations
```bash
# Archive by time period
python backup_azure_storage.py container \
  --container archive \
  --pattern "sess_2026Q1.*" \
  --name q1_2026_full_archive
```

### 5. Regular Archival Schedule
- Daily: Backup completed sessions
- Weekly: Cleanup verified backups from working storage
- Monthly: Create consolidated monthly archives
- Quarterly: Review and consolidate long-term storage

---

## Error Handling

### Common Issues

**1. Connection String Not Set**
```
❌ AZURE_STORAGE_CONNECTION_STRING not set
```
**Solution:** Set the environment variable in `.env` or export it

**2. Container Not Found**
```
❌ Topic archive not found
```
**Solution:** Verify container name and access permissions

**3. No Blobs Found**
```
⚠️  No blobs found for session: sess_20260102_220418
```
**Solution:** Check session ID spelling and container location

**4. Upload Failed**
```
❌ Error uploading archive: [error details]
```
**Solution:** Check Azure credentials and network connectivity

---

## Performance Considerations

### Large Backups
- **ZIP format**: Faster compression, larger files
- **TAR.GZ format**: Slower compression, smaller files

### Recommendations
- Use ZIP for backups < 1 GB
- Use TAR.GZ for backups > 1 GB
- Run large backups during off-peak hours
- Monitor Azure bandwidth costs

### Typical Performance
- 100 MB session: ~30 seconds (download + compress + upload)
- 1 GB session: ~5 minutes
- 10 GB container: ~30-45 minutes

---

## Security Considerations

1. **Connection String**: Store in `.env`, never commit to git
2. **Confirmation Prompts**: Always enabled for deletion operations
3. **Dry-Run**: Use for testing before production runs
4. **Access Control**: Ensure appropriate Azure RBAC permissions
5. **Backup Verification**: Always verify before cleanup

---

## Integration Examples

### Shell Script Integration
```bash
#!/bin/bash
# daily_backup.sh

SESSION_DATE=$(date +%Y%m%d)
BACKUP_NAME="daily_backup_${SESSION_DATE}"

python backup_azure_storage.py container \
  --container archive \
  --pattern "sess_${SESSION_DATE}.*" \
  --name "${BACKUP_NAME}" \
  --cleanup

echo "Backup complete: ${BACKUP_NAME}"
```

### Cron Job Setup
```bash
# Backup daily at 2 AM
0 2 * * * cd /path/to/project && python backup_azure_storage.py container --container archive --pattern "sess_$(date -d yesterday +\%Y\%m\%d).*" --cleanup
```

### Python Integration
```python
import subprocess

session_id = "sess_20260102_220418"
result = subprocess.run([
    "python", "backup_azure_storage.py",
    "session",
    "--session", session_id,
    "--cleanup"
])

if result.returncode == 0:
    print(f"✅ Backup successful: {session_id}")
else:
    print(f"❌ Backup failed: {session_id}")
```

---

## Troubleshooting

### Enable Verbose Logging
Modify the script to add debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Check Azure Connection
```bash
python -c "from azure.storage.blob import BlobServiceClient; import os; BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING')).list_containers()"
```

### Verify Pattern Matching
Use dry-run to test patterns:
```bash
python backup_azure_storage.py cleanup-container \
  --container archive \
  --pattern "your_pattern.*" \
  --dry-run
```

---

## Appendix

### Regex Pattern Examples

**All sessions from January 2026:**
```
sess_202601.*
```

**Specific date range:**
```
sess_2026010[1-5].*
```

**Multiple patterns (OR logic):**
```
(sess_202601.*|sess_202602.*)
```

**Exclude pattern (NOT containing):**
```
^(?!.*_test).*
```

### Backup Naming Conventions

**Recommended formats:**
- `YYYY-MM-DD_description`: `2026-01-02_climate_research`
- `project_YYYYMMDD`: `climate_20260102`
- `category_timeframe`: `research_q1_2026`
- `session_purpose`: `sess_20260102_220418_final`

---

## Support

For issues or questions:
1. Check this documentation
2. Review error messages
3. Test with `--dry-run` flag
4. Verify Azure credentials and permissions
5. Check network connectivity

---

## Version History

- **v1.0** (2026-01-02): Initial release
  - Session and container backup
  - ZIP and TAR.GZ formats
  - Cleanup with dry-run
  - Metadata tracking
  - Pattern-based filtering
