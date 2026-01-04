#!/usr/bin/env python3
"""
Backup Azure Storage containers to long-term storage.

Downloads blobs from specified containers, creates a compressed archive,
and uploads to long_term_storage container with meaningful naming.

Usage:
    python backup_azure_storage.py --session sess_20260102_220418
    python backup_azure_storage.py --container archive --name my_backup_20260102
    python backup_azure_storage.py --session-pattern "sess_202601*" --name jan_2026_sessions
"""

import argparse
import io
import json
import os
import re
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()


class AzureStorageBackup:
    """Backup Azure Storage containers to long-term storage."""

    def __init__(self):
        """Initialize Azure Storage client."""
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("❌ AZURE_STORAGE_CONNECTION_STRING not set")

        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        self.long_term_container = "long-term-storage"

    def ensure_long_term_container(self):
        """Ensure long-term storage container exists."""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.long_term_container
            )
            if not container_client.exists():
                print(f"📦 Creating container: {self.long_term_container}")
                container_client.create_container()
            else:
                print(f"✅ Container exists: {self.long_term_container}")
        except Exception as e:
            print(f"❌ Error ensuring container: {e}")
            raise

    def list_sessions(self, container_name: str = "archive") -> List[str]:
        """List all session IDs in a container."""
        try:
            container_client = self.blob_service_client.get_container_client(
                container_name
            )
            blobs = container_client.list_blobs()

            sessions = set()
            for blob in blobs:
                # Extract session ID from blob path (e.g., sess_20260102_220418/...)
                match = re.match(r'^([^/]+)/', blob.name)
                if match:
                    sessions.add(match.group(1))

            return sorted(list(sessions))
        except Exception as e:
            print(f"❌ Error listing sessions: {e}")
            return []

    def download_session_blobs(
        self,
        session_id: str,
        container_name: str = "archive"
    ) -> Dict[str, bytes]:
        """
        Download all blobs for a session.

        Returns:
            Dict mapping blob paths to their content
        """
        print(f"\n📥 Downloading blobs for session: {session_id}")
        container_client = self.blob_service_client.get_container_client(container_name)
        blobs = {}

        try:
            blob_list = container_client.list_blobs(name_starts_with=f"{session_id}/")
            blob_count = 0

            for blob in blob_list:
                blob_count += 1
                print(f"  📄 Downloading: {blob.name} ({blob.size / 1024:.2f} KB)")

                blob_client = container_client.get_blob_client(blob.name)
                content = blob_client.download_blob().readall()
                blobs[blob.name] = content

            print(f"✅ Downloaded {blob_count} blobs ({sum(len(c) for c in blobs.values()) / (1024*1024):.2f} MB)")

        except Exception as e:
            print(f"❌ Error downloading blobs: {e}")

        return blobs

    def download_container_blobs(
        self,
        container_name: str,
        pattern: Optional[str] = None
    ) -> Dict[str, bytes]:
        """
        Download all or filtered blobs from a container.

        Args:
            container_name: Name of the container
            pattern: Optional regex pattern to filter blob names

        Returns:
            Dict mapping blob paths to their content
        """
        print(f"\n📥 Downloading blobs from container: {container_name}")
        if pattern:
            print(f"   🔍 Filter pattern: {pattern}")

        container_client = self.blob_service_client.get_container_client(container_name)
        blobs = {}

        try:
            blob_list = container_client.list_blobs()
            blob_count = 0
            pattern_re = re.compile(pattern) if pattern else None

            for blob in blob_list:
                # Apply pattern filter if specified
                if pattern_re and not pattern_re.search(blob.name):
                    continue

                blob_count += 1
                print(f"  📄 Downloading: {blob.name} ({blob.size / 1024:.2f} KB)")

                blob_client = container_client.get_blob_client(blob.name)
                content = blob_client.download_blob().readall()
                blobs[blob.name] = content

            print(f"✅ Downloaded {blob_count} blobs ({sum(len(c) for c in blobs.values()) / (1024*1024):.2f} MB)")

        except Exception as e:
            print(f"❌ Error downloading blobs: {e}")

        return blobs

    def create_archive(
        self,
        blobs: Dict[str, bytes],
        archive_name: str,
        format: str = "zip"
    ) -> bytes:
        """
        Create a compressed archive from blobs.

        Args:
            blobs: Dict mapping paths to content
            archive_name: Base name for the archive
            format: 'zip' or 'tar.gz'

        Returns:
            Archive content as bytes
        """
        print(f"\n📦 Creating {format} archive: {archive_name}")

        archive_buffer = io.BytesIO()

        if format == "zip":
            with zipfile.ZipFile(archive_buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
                for blob_path, content in blobs.items():
                    archive.writestr(blob_path, content)
                    print(f"  ✅ Added: {blob_path}")

        elif format == "tar.gz":
            with tarfile.open(fileobj=archive_buffer, mode='w:gz') as archive:
                for blob_path, content in blobs.items():
                    info = tarfile.TarInfo(name=blob_path)
                    info.size = len(content)
                    info.mtime = int(datetime.now().timestamp())
                    archive.addfile(info, io.BytesIO(content))
                    print(f"  ✅ Added: {blob_path}")

        else:
            raise ValueError(f"Unsupported format: {format}")

        archive_content = archive_buffer.getvalue()
        print(f"✅ Archive created: {len(archive_content) / (1024*1024):.2f} MB")

        return archive_content

    def create_metadata(
        self,
        session_id: Optional[str],
        container_name: str,
        blob_count: int,
        total_size: int,
        backup_name: str
    ) -> Dict:
        """Create metadata for the backup."""
        return {
            "backup_name": backup_name,
            "session_id": session_id or "",
            "source_container": container_name,
            "blob_count": str(blob_count),
            "total_size_bytes": str(total_size),
            "total_size_mb": f"{total_size / (1024 * 1024):.2f}",
            "backup_timestamp": datetime.now().isoformat(),
            "created_by": "backup_azure_storage.py"
        }

    def upload_archive(
        self,
        archive_content: bytes,
        archive_name: str,
        metadata: Dict
    ):
        """Upload archive to long-term storage container."""
        print(f"\n📤 Uploading archive to {self.long_term_container}...")

        try:
            container_client = self.blob_service_client.get_container_client(
                self.long_term_container
            )

            # Upload archive
            blob_client = container_client.get_blob_client(archive_name)
            blob_client.upload_blob(
                archive_content,
                overwrite=True,
                metadata=metadata
            )

            print(f"✅ Archive uploaded: {archive_name}")

            # Upload metadata as JSON
            metadata_name = f"{archive_name}.metadata.json"
            metadata_blob = container_client.get_blob_client(metadata_name)
            metadata_blob.upload_blob(
                json.dumps(metadata, indent=2).encode(),
                overwrite=True
            )

            print(f"✅ Metadata uploaded: {metadata_name}")

        except Exception as e:
            print(f"❌ Error uploading archive: {e}")
            raise

    def cleanup_session(
        self,
        session_id: str,
        container_name: str = "archive",
        dry_run: bool = False
    ) -> int:
        """
        Delete all blobs for a session after backup.

        Args:
            session_id: Session ID to clean up
            container_name: Source container
            dry_run: If True, only show what would be deleted

        Returns:
            Number of blobs deleted (or would be deleted in dry-run)
        """
        print(f"\n🧹 Cleaning up session: {session_id}")
        if dry_run:
            print("   (DRY RUN - no actual deletions)")

        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs(name_starts_with=f"{session_id}/")

            deleted_count = 0
            for blob in blob_list:
                deleted_count += 1
                if dry_run:
                    print(f"  🗑️  Would delete: {blob.name}")
                else:
                    print(f"  🗑️  Deleting: {blob.name}")
                    blob_client = container_client.get_blob_client(blob.name)
                    blob_client.delete_blob()

            if not dry_run:
                print(f"✅ Deleted {deleted_count} blobs for session {session_id}")
            else:
                print(f"✅ Would delete {deleted_count} blobs for session {session_id}")

            return deleted_count

        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            raise

    def cleanup_blobs(
        self,
        container_name: str,
        pattern: Optional[str] = None,
        dry_run: bool = False
    ) -> int:
        """
        Delete blobs matching pattern from container.

        Args:
            container_name: Container to clean up
            pattern: Optional regex pattern to filter blobs
            dry_run: If True, only show what would be deleted

        Returns:
            Number of blobs deleted (or would be deleted in dry-run)
        """
        print(f"\n🧹 Cleaning up container: {container_name}")
        if pattern:
            print(f"   🔍 Filter pattern: {pattern}")
        if dry_run:
            print("   (DRY RUN - no actual deletions)")

        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            pattern_re = re.compile(pattern) if pattern else None

            deleted_count = 0
            for blob in blob_list:
                # Apply pattern filter if specified
                if pattern_re and not pattern_re.search(blob.name):
                    continue

                deleted_count += 1
                if dry_run:
                    print(f"  🗑️  Would delete: {blob.name}")
                else:
                    print(f"  🗑️  Deleting: {blob.name}")
                    blob_client = container_client.get_blob_client(blob.name)
                    blob_client.delete_blob()

            if not dry_run:
                print(f"✅ Deleted {deleted_count} blobs")
            else:
                print(f"✅ Would delete {deleted_count} blobs")

            return deleted_count

        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            raise

    def backup_session(
        self,
        session_id: str,
        container_name: str = "archive",
        backup_name: Optional[str] = None,
        format: str = "zip",
        cleanup: bool = False,
        dry_run: bool = False
    ):
        """
        Backup a specific session to long-term storage.

        Args:
            session_id: Session ID to backup
            container_name: Source container
            backup_name: Custom backup name
            format: Archive format (zip or tar.gz)
            cleanup: If True, delete source blobs after successful backup
            dry_run: If True, show what would be cleaned up without deleting
        """
        self.ensure_long_term_container()

        # Download blobs
        blobs = self.download_session_blobs(session_id, container_name)

        if not blobs:
            print(f"⚠️  No blobs found for session: {session_id}")
            return

        # Generate backup name
        if not backup_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"backup_{session_id}_{timestamp}"

        archive_name = f"{backup_name}.{format}"

        # Create archive
        archive_content = self.create_archive(blobs, archive_name, format)

        # Create metadata
        metadata = self.create_metadata(
            session_id,
            container_name,
            len(blobs),
            sum(len(c) for c in blobs.values()),
            backup_name
        )

        # Upload
        self.upload_archive(archive_content, archive_name, metadata)

        print(f"\n🎉 Backup complete: {archive_name}")

        # Cleanup if requested
        if cleanup:
            if not dry_run:
                response = input(f"\n⚠️  Delete {len(blobs)} blobs from {container_name}/{session_id}? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Cleanup cancelled")
                    return

            self.cleanup_session(session_id, container_name, dry_run)

    def backup_container(
        self,
        container_name: str,
        backup_name: Optional[str] = None,
        pattern: Optional[str] = None,
        format: str = "zip",
        cleanup: bool = False,
        dry_run: bool = False
    ):
        """
        Backup entire container or filtered blobs to long-term storage.

        Args:
            container_name: Container to backup
            backup_name: Custom backup name
            pattern: Optional regex pattern to filter blobs
            format: Archive format (zip or tar.gz)
            cleanup: If True, delete source blobs after successful backup
            dry_run: If True, show what would be cleaned up without deleting
        """
        self.ensure_long_term_container()

        # Download blobs
        blobs = self.download_container_blobs(container_name, pattern)

        if not blobs:
            print(f"⚠️  No blobs found in container: {container_name}")
            return

        # Generate backup name
        if not backup_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pattern_suffix = f"_filtered" if pattern else ""
            backup_name = f"backup_{container_name}{pattern_suffix}_{timestamp}"

        archive_name = f"{backup_name}.{format}"

        # Create archive
        archive_content = self.create_archive(blobs, archive_name, format)

        # Create metadata
        metadata = self.create_metadata(
            None,
            container_name,
            len(blobs),
            sum(len(c) for c in blobs.values()),
            backup_name
        )
        if pattern:
            metadata["filter_pattern"] = pattern

        # Upload
        self.upload_archive(archive_content, archive_name, metadata)

        print(f"\n🎉 Backup complete: {archive_name}")

        # Cleanup if requested
        if cleanup:
            if not dry_run:
                response = input(f"\n⚠️  Delete {len(blobs)} blobs from {container_name}? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Cleanup cancelled")
                    return

            self.cleanup_blobs(container_name, pattern, dry_run)

    def backup_local_folder(
        self,
        folder_path: str,
        backup_name: Optional[str] = None,
        format: str = "tar.gz",
        cleanup: bool = False,
        dry_run: bool = False
    ):
        """
        Backup a local folder to Azure long-term storage.

        Args:
            folder_path: Path to local folder to backup
            backup_name: Custom backup name
            format: Archive format (zip or tar.gz)
            cleanup: If True, delete local folder after successful backup
            dry_run: If True, show what would be done without actually doing it
        """
        self.ensure_long_term_container()

        folder = Path(folder_path)
        if not folder.exists():
            print(f"❌ Folder not found: {folder_path}")
            return

        if not folder.is_dir():
            print(f"❌ Not a directory: {folder_path}")
            return

        # Generate backup name
        if not backup_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            folder_name = folder.name
            backup_name = f"{folder_name}_{timestamp}"

        archive_name = f"{backup_name}.{format}"

        print(f"\n📦 Backing up local folder: {folder_path}")
        print(f"   📝 Archive name: {archive_name}")

        if dry_run:
            print("   (DRY RUN - no actual backup)")

        # Create archive
        try:
            archive_content = io.BytesIO()
            file_count = 0
            total_size = 0

            if format == "tar.gz":
                with tarfile.open(fileobj=archive_content, mode='w:gz') as tar:
                    for file_path in folder.rglob('*'):
                        if file_path.is_file():
                            file_count += 1
                            file_size = file_path.stat().st_size
                            total_size += file_size
                            arcname = file_path.relative_to(folder.parent)
                            print(f"  📄 Adding: {arcname} ({file_size / 1024:.2f} KB)")
                            tar.add(file_path, arcname=str(arcname))
            else:  # zip
                with zipfile.ZipFile(archive_content, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for file_path in folder.rglob('*'):
                        if file_path.is_file():
                            file_count += 1
                            file_size = file_path.stat().st_size
                            total_size += file_size
                            arcname = file_path.relative_to(folder.parent)
                            print(f"  📄 Adding: {arcname} ({file_size / 1024:.2f} KB)")
                            zip_file.write(file_path, arcname=str(arcname))

            archive_bytes = archive_content.getvalue()
            print(f"✅ Archive created: {len(archive_bytes) / (1024*1024):.2f} MB")

            if dry_run:
                print(f"\n✅ Would create backup with {file_count} files ({total_size / (1024*1024):.2f} MB)")
                if cleanup:
                    print(f"⚠️  Would delete local folder: {folder_path}")
                return

            # Create metadata
            metadata = {
                "backup_name": backup_name,
                "source": f"local:{folder_path}",
                "source_folder": str(folder.absolute()),
                "file_count": str(file_count),
                "total_size_bytes": str(total_size),
                "total_size_mb": f"{total_size / (1024 * 1024):.2f}",
                "backup_timestamp": datetime.now().isoformat(),
                "created_by": "backup_azure_storage.py"
            }

            # Upload archive
            self.upload_archive(archive_bytes, archive_name, metadata)

            # Cleanup if requested
            if cleanup:
                print(f"\n🧹 Cleaning up local folder: {folder_path}")
                import shutil
                shutil.rmtree(folder_path)
                print(f"✅ Deleted local folder: {folder_path}")

            print(f"\n✅ Backup complete: {archive_name}")

        except Exception as e:
            print(f"❌ Error creating archive: {e}")
            raise

    def list_backups(self):
        """List all backups in long-term storage."""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.long_term_container
            )

            print(f"\n📦 Backups in {self.long_term_container}:\n")

            blobs = container_client.list_blobs()
            backup_count = 0

            for blob in blobs:
                if blob.name.endswith('.metadata.json'):
                    continue

                backup_count += 1
                size_mb = blob.size / (1024 * 1024)
                last_modified = blob.last_modified.strftime('%Y-%m-%d %H:%M:%S')

                print(f"  📁 {blob.name}")
                print(f"     Size: {size_mb:.2f} MB | Modified: {last_modified}")

            print(f"\n✅ Total backups: {backup_count}")

        except Exception as e:
            print(f"❌ Error listing backups: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backup Azure Storage containers to long-term storage"
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Backup session command
    session_parser = subparsers.add_parser(
        'session',
        help='Backup a specific session'
    )
    session_parser.add_argument(
        '--session',
        required=True,
        help='Session ID to backup (e.g., sess_20260102_220418)'
    )
    session_parser.add_argument(
        '--container',
        default='archive',
        help='Source container (default: archive)'
    )
    session_parser.add_argument(
        '--name',
        help='Custom backup name (optional)'
    )
    session_parser.add_argument(
        '--format',
        choices=['zip', 'tar.gz'],
        default='zip',
        help='Archive format (default: zip)'
    )
    session_parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Delete source blobs after successful backup'
    )
    session_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    # Backup container command
    container_parser = subparsers.add_parser(
        'container',
        help='Backup entire container or filtered blobs'
    )
    container_parser.add_argument(
        '--container',
        required=True,
        help='Container name to backup'
    )
    container_parser.add_argument(
        '--pattern',
        help='Regex pattern to filter blobs (optional)'
    )
    container_parser.add_argument(
        '--name',
        help='Custom backup name (optional)'
    )
    container_parser.add_argument(
        '--format',
        choices=['zip', 'tar.gz'],
        default='zip',
        help='Archive format (default: zip)'
    )
    container_parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Delete source blobs after successful backup'
    )
    container_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    # List sessions command
    list_sessions_parser = subparsers.add_parser(
        'list-sessions',
        help='List all available sessions in a container'
    )
    list_sessions_parser.add_argument(
        '--container',
        default='archive',
        help='Container to list sessions from (default: archive)'
    )

    # Local folder backup command
    local_parser = subparsers.add_parser(
        'local-folder',
        help='Backup a local folder to long-term storage'
    )
    local_parser.add_argument(
        '--folder',
        required=True,
        help='Path to local folder to backup'
    )
    local_parser.add_argument(
        '--name',
        help='Custom backup name (default: foldername_timestamp)'
    )
    local_parser.add_argument(
        '--format',
        choices=['zip', 'tar.gz'],
        default='tar.gz',
        help='Archive format (default: tar.gz)'
    )
    local_parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Delete local folder after successful backup'
    )
    local_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be backed up without actually doing it'
    )

    # List backups command
    subparsers.add_parser(
        'list-backups',
        help='List all backups in long-term storage'
    )

    # Cleanup session command
    cleanup_session_parser = subparsers.add_parser(
        'cleanup-session',
        help='Clean up a specific session (use after manual verification of backup)'
    )
    cleanup_session_parser.add_argument(
        '--session',
        required=True,
        help='Session ID to clean up'
    )
    cleanup_session_parser.add_argument(
        '--container',
        default='archive',
        help='Container to clean up from (default: archive)'
    )
    cleanup_session_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    # Cleanup container command
    cleanup_container_parser = subparsers.add_parser(
        'cleanup-container',
        help='Clean up blobs from a container (use after manual verification of backup)'
    )
    cleanup_container_parser.add_argument(
        '--container',
        required=True,
        help='Container to clean up'
    )
    cleanup_container_parser.add_argument(
        '--pattern',
        help='Regex pattern to filter blobs to delete (optional)'
    )
    cleanup_container_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    backup = AzureStorageBackup()

    try:
        if args.command == 'session':
            backup.backup_session(
                args.session,
                args.container,
                args.name,
                args.format,
                args.cleanup,
                args.dry_run
            )

        elif args.command == 'container':
            backup.backup_container(
                args.container,
                args.name,
                args.pattern,
                args.format,
                args.cleanup,
                args.dry_run
            )

        elif args.command == 'cleanup-session':
            if not args.dry_run:
                response = input(f"\n⚠️  Are you sure you want to delete session {args.session}? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Cleanup cancelled")
                    return

            backup.cleanup_session(args.session, args.container, args.dry_run)

        elif args.command == 'cleanup-container':
            if not args.dry_run:
                pattern_msg = f" matching pattern '{args.pattern}'" if args.pattern else ""
                response = input(f"\n⚠️  Are you sure you want to delete blobs{pattern_msg} from {args.container}? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Cleanup cancelled")
                    return

            backup.cleanup_blobs(args.container, args.pattern, args.dry_run)

        elif args.command == 'local-folder':
            backup.backup_local_folder(
                args.folder,
                args.name,
                args.format,
                args.cleanup,
                args.dry_run
            )

        elif args.command == 'list-sessions':
            sessions = backup.list_sessions(args.container)
            print(f"\n📋 Sessions in {args.container}:\n")
            for session in sessions:
                print(f"  • {session}")
            print(f"\n✅ Total sessions: {len(sessions)}")

        elif args.command == 'list-backups':
            backup.list_backups()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
