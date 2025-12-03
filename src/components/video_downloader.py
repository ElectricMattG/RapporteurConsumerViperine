"""
Video Downloader Component

A centralized video downloader component that inherits from Base,
providing configurable rate limiting and comprehensive logging.
"""

import json
import os
import random
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm import tqdm

from config_handler import Base


class VideoDownloader(Base):
    """
    Video downloader component with channel organization and rate limiting.

    Inherits from Base to get:
    - Centralized configuration via self._app_config
    - Structured logging via self._logger
    - Consistent component architecture
    """

    def __init__(self, output_dir: Optional[str] = None, prefer_mp4: bool = False, show_progress: bool = True):
        """
        Initialize the VideoDownloader component.

        Args:
            output_dir: Directory for downloaded videos (defaults to config)
            prefer_mp4: Whether to prefer MP4 format over WebM
            show_progress: Whether to show progress bars (False for legacy logging mode)
        """
        # Initialize Base class (sets up self._logger and self._app_config)
        super().__init__()

        # Set output directory from config or parameter
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            # Get from config with sensible default
            default_dir = self._app_config.get('downloader.output_dir')
            self.output_dir = Path(default_dir)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.prefer_mp4 = prefer_mp4
        self.show_progress = show_progress

        # Track failed downloads for retry reports
        self.failed_downloads = []

        # Load rate limiting configuration from central config
        self.rate_limit_config = self._load_rate_limit_config()

        # Simple progress tracking
        self.progress_file = self.output_dir / "download_progress.txt"
        self.completed_videos = self.load_completed()

        self._logger.info(f"VideoDownloader initialized")
        self._logger.info(f"Output directory: {self.output_dir}")
        self._logger.info(f"Rate limiting: {self.rate_limit_config['min_delay']}-{self.rate_limit_config['max_delay']}s between videos")
        self._logger.info(f"Speed limit: {self.rate_limit_config['speed_limit']}")

    def _load_rate_limit_config(self) -> Dict[str, Any]:
        """Load rate limiting configuration from central config."""
        return {
            # Video-to-video delays (script level)
            'min_delay': self._app_config.get('downloader.min_delay'),
            'max_delay': self._app_config.get('downloader.max_delay'),

            # yt-dlp internal rate limiting
            'speed_limit': self._app_config.get('downloader.speed_limit'),
            'sleep_requests': self._app_config.get('downloader.sleep_requests'),
            'sleep_interval': self._app_config.get('downloader.sleep_interval'),
            'max_sleep_interval': self._app_config.get('downloader.max_sleep_interval'),
            'sleep_subtitles': self._app_config.get('downloader.sleep_subtitles'),

            # Timeouts and retries
            'socket_timeout': self._app_config.get('downloader.socket_timeout'),
            'retries': self._app_config.get('downloader.retries'),
            'file_access_retries': self._app_config.get('downloader.file_access_retries'),

            # Metadata options
            'write_info_json': self._app_config.get('downloader.write_info_json'),
            'write_description': self._app_config.get('downloader.write_description'),
            'write_thumbnail': self._app_config.get('downloader.write_thumbnail'),
            'write_subs': self._app_config.get('downloader.write_subs'),
            'write_auto_subs': self._app_config.get('downloader.write_auto_subs'),
            'skip_download': self._app_config.get('downloader.skip_download'),
            'force_download': self._app_config.get('downloader.force_download'),

            # Subtitle language preferences
            'subtitle_languages': self._app_config.get('downloader.subtitle_languages'),
        }

    def load_completed(self) -> set:
        """Load list of completed video IDs."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    completed = set(line.strip() for line in f if line.strip())
                self._logger.info(f"Loaded {len(completed)} completed video IDs")
                return completed
            except Exception as e:
                self._logger.error(f"Failed to load progress file: {e}")
                return set()
        return set()

    def mark_completed(self, video_id: str) -> None:
        """Mark video as completed."""
        self.completed_videos.add(video_id)
        try:
            with open(self.progress_file, 'a') as f:
                f.write(f"{video_id}\n")
            self._logger.debug(f"Marked video {video_id} as completed")
        except Exception as e:
            self._logger.error(f"Failed to mark video {video_id} as completed: {e}")

    def get_safe_filename(self, text: str) -> str:
        """Create safe filename from text."""
        safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_.()[]"
        safe_text = ''.join(c if c in safe_chars else '_' for c in text)
        return safe_text[:100]  # Limit length

    def build_yt_dlp_command(self, video_id: str, title: str, channel_dir: Path) -> List[str]:
        """
        Build yt-dlp command with all rate limiting and metadata options.

        Args:
            video_id: YouTube video ID
            title: Video title for filename
            channel_dir: Directory for the channel

        Returns:
            Complete yt-dlp command as list
        """
        safe_title = self.get_safe_filename(title)
        config = self.rate_limit_config

        cmd = [
            'yt-dlp',
            f'https://www.youtube.com/watch?v={video_id}',
            '--output', str(channel_dir / f'{safe_title}_{video_id}.%(ext)s'),
            '--ignore-errors',
            '--impersonate', 'chrome',  # Bypass YouTube 403 blocks via browser impersonation
        ]

        # Skip video download if only subtitles are needed
        if config.get('skip_download', False):
            cmd.append('--skip-download')

        # Add metadata options
        if config['write_info_json']:
            cmd.append('--write-info-json')
        if config['write_description']:
            cmd.append('--write-description')
        if config['write_thumbnail']:
            cmd.append('--write-thumbnail')

        # Add subtitle options based on configuration
        if config['write_subs']:
            cmd.append('--write-subs')
            # Request subtitles in multiple languages from centralized config
            subtitle_langs = ','.join(config['subtitle_languages'])
            cmd.extend(['--sub-langs', subtitle_langs])
        if config['write_auto_subs']:
            cmd.append('--write-auto-subs')
            # For auto-generated subs, use the same language preferences
            subtitle_langs = ','.join(config['subtitle_languages'])
            cmd.extend(['--sub-langs', subtitle_langs])

        # Add rate limiting options
        cmd.extend([
            '--limit-rate', config['speed_limit'],
            '--sleep-requests', str(config['sleep_requests']),
            '--sleep-interval', str(config['sleep_interval']),
            '--max-sleep-interval', str(config['max_sleep_interval']),
            '--sleep-subtitles', str(config['sleep_subtitles']),
            '--socket-timeout', str(config['socket_timeout']),
            '--retries', str(config['retries']),
            '--file-access-retries', str(config['file_access_retries']),
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ])

        # Add format preference
        if self.prefer_mp4:
            cmd.extend(['--format', 'best[ext=mp4]/best[height<=720][ext=mp4]/best'])

        return cmd

    def download_single_video(self, video: Dict[str, Any]) -> tuple[bool, str]:
        """
        Download a single video with full logging and error handling.

        Args:
            video: Video metadata dictionary

        Returns:
            Tuple of (success: bool, failure_reason: str)
        """
        video_id = video['video_id']
        title = video.get('title', 'Unknown Title')
        channel = video.get('channel_title', 'Unknown Channel')

        self._logger.info(f"Starting download: {title[:60]} (ID: {video_id})")
        self._logger.info(f"Channel: {channel}")

        # Skip if already completed (unless force_download is enabled)
        if video_id in self.completed_videos and not self.rate_limit_config.get('force_download', False):
            self._logger.info(f"Already downloaded: {video_id}")
            return True, "Already completed"

        # Create channel directory
        safe_channel = self.get_safe_filename(channel)
        channel_dir = self.output_dir / safe_channel
        channel_dir.mkdir(exist_ok=True)

        # Check if complete video file already exists
        video_files = []
        for ext in ['mp4', 'webm', 'mkv', 'avi', 'm4a', 'flv']:
            video_files.extend(channel_dir.glob(f"*{video_id}*.{ext}"))

        # Filter out .part files
        complete_video_files = [f for f in video_files if not f.name.endswith('.part')]

        if complete_video_files:
            self._logger.info(f"Video file already exists: {complete_video_files[0].name}")
            self.mark_completed(video_id)
            return True, "File already exists"

        # Check for incomplete downloads and clean up
        metadata_files = list(channel_dir.glob(f"*{video_id}*.info.json"))
        if metadata_files and not complete_video_files:
            self._logger.warning(f"Found metadata but no video file - cleaning up incomplete download")
            for metadata_file in channel_dir.glob(f"*{video_id}*"):
                if metadata_file.suffix in ['.json', '.webp', '.vtt', '.part']:
                    try:
                        metadata_file.unlink()
                        self._logger.debug(f"Removed: {metadata_file.name}")
                    except Exception as e:
                        self._logger.error(f"Failed to remove {metadata_file.name}: {e}")

        # Build command
        cmd = self.build_yt_dlp_command(video_id, title, channel_dir)

        format_info = "MP4 (H.264+AAC)" if self.prefer_mp4 else "WebM (VP9+Opus)"
        self._logger.info(f"Format preference: {format_info}")
        self._logger.debug(f"Command: {' '.join(cmd)}")
        self._logger.info(f"Output directory: {channel_dir}")

        try:
            if self.show_progress:
                # Progress bar mode with sub-component tracking
                return self._download_with_progress(cmd, video_id, channel_dir)
            else:
                # Legacy mode - simple execution with live output
                return self._download_legacy(cmd, video_id)

        except KeyboardInterrupt:
            self._logger.warning("Download interrupted by user")
            return False, "User interrupted"
        except Exception as e:
            self._logger.error(f"Exception downloading {video_id}: {e}")
            return False, "Exception occurred"

    def _download_with_progress(self, cmd: List[str], video_id: str, channel_dir: Path) -> tuple[bool, str]:
        """Download with progress bar and component tracking."""
        # Create a sub-progress bar for individual file components
        config = self.rate_limit_config
        include_subtitles = config['write_subs'] or config['write_auto_subs']

        if include_subtitles:
            file_components = ["metadata", "subtitles", "thumbnail", "video"]
            desc_text = "📦 Components"
        else:
            file_components = ["metadata", "thumbnail", "video"]
            desc_text = "📦 Components (no subs)"

        with tqdm(
            total=len(file_components),
            desc=desc_text,
            unit="file",
            leave=False,
            position=1,
            ncols=80,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n}/{total} {postfix}"
        ) as sub_pbar:

            # Run yt-dlp and capture output for progress tracking
            import threading
            import time

            # Start the subprocess
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True)

            # Track components as they complete
            components_completed = set()

            def monitor_output():
                while True:
                    if process.poll() is not None:
                        break

                    # Check for completed components by looking at filesystem
                    if "metadata" not in components_completed:
                        if list(channel_dir.glob(f"*{video_id}*.info.json")):
                            components_completed.add("metadata")
                            sub_pbar.set_postfix_str("JSON metadata")
                            sub_pbar.update(1)

                    if "subtitles" not in components_completed and include_subtitles:
                        if list(channel_dir.glob(f"*{video_id}*.vtt")):
                            components_completed.add("subtitles")
                            sub_pbar.set_postfix_str("Subtitles")
                            sub_pbar.update(1)

                    if "thumbnail" not in components_completed:
                        if list(channel_dir.glob(f"*{video_id}*.webp")):
                            components_completed.add("thumbnail")
                            sub_pbar.set_postfix_str("Thumbnail")
                            sub_pbar.update(1)

                    if "video" not in components_completed:
                        video_files = []
                        for ext in ['mp4', 'webm', 'mkv']:
                            video_files.extend(channel_dir.glob(f"*{video_id}*.{ext}"))
                        if video_files:
                            components_completed.add("video")
                            sub_pbar.set_postfix_str("Video file")
                            sub_pbar.update(1)

                    time.sleep(0.5)  # Check every 500ms

            # Start monitoring in a separate thread
            monitor_thread = threading.Thread(target=monitor_output, daemon=True)
            monitor_thread.start()

            # Wait for process to complete
            stdout, stderr = process.communicate()

            # Ensure all updates are complete
            monitor_thread.join(timeout=1)

            # Fill remaining progress if needed
            remaining = len(file_components) - len(components_completed)
            if remaining > 0:
                sub_pbar.update(remaining)

            result_code = process.returncode

        return self._process_download_result(result_code, video_id, stdout, stderr)

    def _download_legacy(self, cmd: List[str], video_id: str) -> tuple[bool, str]:
        """Legacy download mode with live output to terminal."""
        self._logger.info(f"Executing: {' '.join(cmd)}")

        # Run yt-dlp with live output to terminal
        process = subprocess.Popen(cmd, text=True)

        # Wait for process to complete
        result_code = process.wait()

        # For legacy mode, we get basic result info
        if result_code == 0:
            self._logger.info(f"Successfully downloaded: {video_id}")
            self.mark_completed(video_id)
            return True, "Success"
        else:
            self._logger.error(f"Failed to download {video_id}: exit code {result_code}")
            return False, "Download failed"

    def _process_download_result(self, result_code: int, video_id: str, stdout: str, stderr: str) -> tuple[bool, str]:
        """Process download result and categorize failures."""
        if result_code == 0:
            self._logger.info(f"Successfully downloaded: {video_id}")
            self.mark_completed(video_id)
            return True, "Success"
        else:
            # Categorize different types of failures for better analysis
            stderr_output = stderr.strip() if stderr else ""

            failure_reason = "Unknown error"
            if "Video unavailable" in stderr_output:
                if "account associated with this video has been terminated" in stderr_output:
                    failure_reason = "Account terminated"
                elif "private video" in stderr_output:
                    failure_reason = "Private video"
                else:
                    failure_reason = "Video unavailable"
            elif "Copyright" in stderr_output or "copyright" in stderr_output:
                failure_reason = "Copyright claim"
            elif "not available in your country" in stderr_output:
                failure_reason = "Geographic restriction"
            elif "Requested format is not available" in stderr_output:
                failure_reason = "Format unavailable"
            elif "Sign in to confirm your age" in stderr_output:
                failure_reason = "Age restriction"
            elif "This video is not available" in stderr_output:
                failure_reason = "Video not available"

            # Log with categorized reason
            self._logger.error(f"Failed to download {video_id}: {failure_reason}")
            if stderr_output and failure_reason == "Unknown error":
                self._logger.debug(f"Full error for {video_id}: {stderr_output}")
            return False, failure_reason

    def download_videos(self, videos: List[Dict[str, Any]], max_videos: Optional[int] = None) -> Dict[str, Any]:
        """
        Download multiple videos with progress tracking and statistics.

        Args:
            videos: List of video metadata dictionaries
            max_videos: Maximum number of videos to download (for testing)

        Returns:
            Statistics dictionary with download results
        """
        if max_videos:
            videos = videos[:max_videos]

        total_videos = len(videos)
        completed_count = len(self.completed_videos)

        # Filter remaining videos based on force_download setting
        if self.rate_limit_config.get('force_download', False):
            remaining_videos = videos  # Process all videos when force_download is enabled
            self._logger.info("Force download enabled - processing all videos regardless of completion status")
        else:
            remaining_videos = [v for v in videos if v['video_id'] not in self.completed_videos]

        self._logger.info("Starting video download session")
        self._logger.info(f"Total videos: {total_videos}")
        self._logger.info(f"Already completed: {completed_count}")
        self._logger.info(f"Remaining: {len(remaining_videos)}")

        if max_videos:
            self._logger.info(f"Limited to: {max_videos} videos (test mode)")

        estimated_time = len(remaining_videos) * ((self.rate_limit_config['min_delay'] + self.rate_limit_config['max_delay']) / 2) / 60
        self._logger.info(f"Estimated time: {estimated_time:.1f} minutes")

        if not remaining_videos:
            self._logger.info("All videos already downloaded!")
            return {
                'total': total_videos,
                'completed': completed_count,
                'success': 0,
                'failed': 0,
                'skipped': len(remaining_videos)
            }

        success_count = 0
        failed_count = 0
        failure_categories = {
            "Account terminated": 0,
            "Video unavailable": 0,
            "Copyright claim": 0,
            "Geographic restriction": 0,
            "Format unavailable": 0,
            "Age restriction": 0,
            "Private video": 0,
            "Unknown error": 0
        }

        if self.show_progress:
            # Progress bar mode
            success_count, failed_count = self._download_with_main_progress(remaining_videos, failure_categories)
        else:
            # Legacy mode - simple iteration with full logging
            success_count, failed_count = self._download_legacy_mode(remaining_videos, failure_categories)

        # Final statistics
        stats = {
            'total': total_videos,
            'completed': len(self.completed_videos),
            'success': success_count,
            'failed': failed_count,
            'skipped': total_videos - len(remaining_videos)
        }

        self._logger.info("Download session complete! ")
        self._logger.info(f"Successfully downloaded: {success_count}")
        self._logger.info(f"Failed downloads: {failed_count}")

        # Log failure breakdown if there were failures
        if failed_count > 0:
            self._logger.info("Failure breakdown:")
            for category, count in failure_categories.items():
                if count > 0:
                    self._logger.info(f"  {category}: {count}")

        self._logger.info(f"Total completed: {len(self.completed_videos)}")

        # Generate failure report if there were failures
        if failed_count > 0:
            self._generate_failure_report(stats, failure_categories)

        return stats

    def _generate_failure_report(self, stats: Dict[str, Any], failure_categories: Dict[str, int]) -> None:
        """Generate detailed failure report for retry purposes."""
        import json
        from datetime import datetime

        # Create failure report
        report = {
            'generated_at': datetime.now().isoformat(),
            'generation_method': 'Real-time download tracking',
            'summary': {
                'total_videos': stats['total'],
                'successful_downloads': stats['success'],
                'failed_downloads': stats['failed'],
                'success_rate': f"{(stats['success'] / stats['total'] * 100):.1f}%" if stats['total'] > 0 else "0.0%",
                'failure_rate': f"{(stats['failed'] / stats['total'] * 100):.1f}%" if stats['total'] > 0 else "0.0%",
                'failure_categories': {k: v for k, v in failure_categories.items() if v > 0}
            },
            'failed_downloads': self.failed_downloads,
            'retry_candidates': [
                video for video in self.failed_downloads
                if video['retry_recommended']
            ],
            'permanent_failures': [
                video for video in self.failed_downloads
                if not video['retry_recommended']
            ],
            'output_directory': str(self.output_dir),
            'retry_instructions': {
                'command_template': 'python3 research_cli.py download {failure_report_file} --output-dir {output_dir} --no-progress',
                'recommended_settings': {
                    'rate_limiting': 'Increase delays to 60-120 seconds between videos',
                    'time_of_day': 'Try during off-peak hours (late night/early morning)',
                    'network': 'Consider using different network connection or proxy',
                    'format': 'Try different format preferences (--mp4 vs default WebM)'
                }
            }
        }

        # Save failure report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        failure_file = self.output_dir / f"failed_downloads_{timestamp}.json"

        try:
            with open(failure_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            self._logger.info(f"📋 Failure report generated: {failure_file.name}")
            self._logger.info(f"   • Retry candidates: {len(report['retry_candidates'])}")
            self._logger.info(f"   • Permanent failures: {len(report['permanent_failures'])}")

            # Also create a simple retry input file with just the failed video metadata
            retry_input_file = self.output_dir / f"retry_videos_{timestamp}.json"
            with open(retry_input_file, 'w', encoding='utf-8') as f:
                json.dump(report['retry_candidates'], f, indent=2, ensure_ascii=False)

            self._logger.info(f"📋 Retry input file generated: {retry_input_file.name}")
            self._logger.info(f"   Use: python3 research_cli.py download {retry_input_file.name} --output-dir {self.output_dir} --no-progress")

        except Exception as e:
            self._logger.error(f"Failed to generate failure report: {e}")

    def _download_with_main_progress(self, remaining_videos: List[Dict[str, Any]], failure_categories: Dict[str, int]) -> tuple[int, int]:
        """Download videos with main progress bar."""
        success_count = 0
        failed_count = 0

        # Create progress bar with better formatting
        pbar = tqdm(
            remaining_videos,
            desc="🎬 Downloading videos",
            unit="video",
            ncols=120,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
            position=0,
            leave=True,
            colour='green'
        )

        for i, video in enumerate(pbar, 1):
            # Update progress bar description with current video
            title = video.get('title', 'Unknown')[:30]
            channel = video.get('channel_title', 'Unknown')[:15]
            pbar.set_description(f"🎬 {channel}: {title}")

            # Temporarily reduce logging verbosity during download
            original_level = self._logger.level
            self._logger.setLevel(40)  # ERROR level only

            # Download video
            success, reason = self.download_single_video(video)

            # Restore logging level
            self._logger.setLevel(original_level)

            if success:
                success_count += 1
                pbar.set_postfix({"✅ Success": success_count, "❌ Failed": failed_count})
            else:
                failed_count += 1
                # Track failure category
                if reason in failure_categories:
                    failure_categories[reason] += 1
                else:
                    failure_categories["Unknown error"] += 1

                # Store failed video details for retry report
                self.failed_downloads.append({
                    'video_id': video['video_id'],
                    'title': video.get('title', 'Unknown Title'),
                    'channel_title': video.get('channel_title', 'Unknown Channel'),
                    'published_at': video.get('published_at', 'Unknown Date'),
                    'view_count': video.get('view_count', 'Unknown'),
                    'duration': video.get('duration', 'Unknown'),
                    'description': video.get('description', 'No description'),
                    'youtube_url': f"https://www.youtube.com/watch?v={video['video_id']}",
                    'failure_reason': reason,
                    'retry_recommended': reason not in ['Account terminated', 'Private video', 'Copyright claim']
                })
                pbar.set_postfix({"✅ Success": success_count, "❌ Failed": failed_count})

            # Rate limiting delay between downloads
            if i < len(remaining_videos):
                delay = random.uniform(
                    self.rate_limit_config['min_delay'],
                    self.rate_limit_config['max_delay']
                )
                # Show delay countdown in progress bar
                pbar.set_description(f"⏳ Rate limiting: {delay:.1f}s")
                time.sleep(delay)

        pbar.close()
        return success_count, failed_count

    def _download_legacy_mode(self, remaining_videos: List[Dict[str, Any]], failure_categories: Dict[str, int]) -> tuple[int, int]:
        """Download videos in legacy mode with full logging."""
        success_count = 0
        failed_count = 0
        total_videos = len(remaining_videos)

        for i, video in enumerate(remaining_videos, 1):
            title = video.get('title', 'Unknown')
            channel = video.get('channel_title', 'Unknown')
            video_id = video.get('video_id', 'Unknown')

            self._logger.info(f"=== Processing video {i}/{total_videos} ===")
            self._logger.info(f"Title: {title}")
            self._logger.info(f"Channel: {channel}")
            self._logger.info(f"Video ID: {video_id}")

            # Download video with full logging
            success, reason = self.download_single_video(video)

            if success:
                success_count += 1
                self._logger.info(f"✅ Success! Total: {success_count}/{i}")
            else:
                failed_count += 1
                # Track failure category
                if reason in failure_categories:
                    failure_categories[reason] += 1
                else:
                    failure_categories["Unknown error"] += 1

                # Store failed video details for retry report
                self.failed_downloads.append({
                    'video_id': video['video_id'],
                    'title': video.get('title', 'Unknown Title'),
                    'channel_title': video.get('channel_title', 'Unknown Channel'),
                    'published_at': video.get('published_at', 'Unknown Date'),
                    'view_count': video.get('view_count', 'Unknown'),
                    'duration': video.get('duration', 'Unknown'),
                    'description': video.get('description', 'No description'),
                    'youtube_url': f"https://www.youtube.com/watch?v={video['video_id']}",
                    'failure_reason': reason,
                    'retry_recommended': reason not in ['Account terminated', 'Private video', 'Copyright claim']
                })

                self._logger.info(f"❌ Failed! Reason: {reason}. Total failed: {failed_count}/{i}")

            # Rate limiting delay between downloads
            if i < total_videos:
                delay = random.uniform(
                    self.rate_limit_config['min_delay'],
                    self.rate_limit_config['max_delay']
                )
                self._logger.info(f"⏳ Rate limiting delay: {delay:.1f}s")
                time.sleep(delay)

        return success_count, failed_count

        # Final statistics
        stats = {
            'total': total_videos,
            'completed': len(self.completed_videos),
            'success': success_count,
            'failed': failed_count,
            'skipped': 0,
            'failure_breakdown': failure_categories
        }

        self._logger.info("Download session complete!")
        self._logger.info(f"Successfully downloaded: {success_count}")
        self._logger.info(f"Failed downloads: {failed_count}")

        # Log failure breakdown
        if failed_count > 0:
            self._logger.info("Failure breakdown:")
            for reason, count in failure_categories.items():
                if count > 0:
                    self._logger.info(f"  {reason}: {count}")

        self._logger.info(f"Total completed: {len(self.completed_videos)}")

        return stats

    def download_from_json(
        self,
        json_file: str,
        max_videos: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Download videos from a JSON file.

        Args:
            json_file: Path to JSON file containing video metadata
            max_videos: Maximum number of videos to download

        Returns:
            Statistics dictionary with download results
        """
        if not os.path.exists(json_file):
            self._logger.error(f"JSON file not found: {json_file}")
            raise FileNotFoundError(f"Videos file not found: {json_file}")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                videos = json.load(f)

            self._logger.info(f"Loaded {len(videos)} videos from {json_file}")
            return self.download_videos(videos, max_videos)

        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to parse JSON file {json_file}: {e}")
            raise
        except Exception as e:
            self._logger.error(f"Error loading videos from {json_file}: {e}")
            raise

    def get_download_statistics(self) -> Dict[str, Any]:
        """Get comprehensive download statistics."""
        total_completed = len(self.completed_videos)

        # Count files by type in output directory
        video_files = 0
        metadata_files = 0
        subtitle_files = 0
        thumbnail_files = 0

        for channel_dir in self.output_dir.iterdir():
            if channel_dir.is_dir():
                for file in channel_dir.iterdir():
                    if file.suffix in ['.mp4', '.webm', '.mkv', '.avi', '.m4a', '.flv']:
                        video_files += 1
                    elif file.suffix == '.json':
                        metadata_files += 1
                    elif file.suffix == '.vtt':
                        subtitle_files += 1
                    elif file.suffix == '.webp':
                        thumbnail_files += 1

        return {
            'completed_videos': total_completed,
            'video_files': video_files,
            'metadata_files': metadata_files,
            'subtitle_files': subtitle_files,
            'thumbnail_files': thumbnail_files,
            'output_directory': str(self.output_dir),
            'rate_limit_config': self.rate_limit_config
        }
