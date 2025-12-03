#!/usr/bin/env python3
"""
Simple script to fetch YouTube transcripts directly from the API.
Usage: python test_youtube_transcript.py <video_url_or_id>
"""

import json
import sys

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter


def extract_video_id(url_or_id):
    """Extract video ID from URL or return as-is if already an ID."""
    if 'youtube.com' in url_or_id or 'youtu.be' in url_or_id:
        if 'v=' in url_or_id:
            return url_or_id.split('v=')[1].split('&')[0]
        elif 'youtu.be/' in url_or_id:
            return url_or_id.split('youtu.be/')[1].split('?')[0]
    return url_or_id

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_youtube_transcript.py <video_url_or_id>")
        print("Example: python test_youtube_transcript.py uHL7MEd2iyw")
        print("Example: python test_youtube_transcript.py 'https://www.youtube.com/watch?v=uHL7MEd2iyw'")
        sys.exit(1)

    video_url_or_id = sys.argv[1]
    video_id = extract_video_id(video_url_or_id)

    print(f"📹 Fetching transcript for video ID: {video_id}")
    print("=" * 80)

    try:
        # Fetch the transcript using fetch method
        from youtube_transcript_api._api import YouTubeTranscriptApi as API
        transcript_data = API().fetch(video_id)

        print(f"\n✅ Found transcript")

        print(f"\n📊 Transcript stats:")
        print(transcript_data)
        print(f"   Total segments: {len(transcript_data)}")

        print(f"\n📝 Raw transcript segments (first 5):")
        print("=" * 80)
        for i, segment in enumerate(transcript_data[:5]):
            print(f"\nSegment {i}:")
            print(f"   Text: {repr(segment['text'])}")
            print(f"   Start: {segment['start']:.2f}s")
            print(f"   Duration: {segment['duration']:.2f}s")

        # Format as plain text
        formatter = TextFormatter()
        formatted_text = formatter.format_transcript(transcript_data)

        print(f"\n📄 Formatted transcript (first 500 chars):")
        print("=" * 80)
        print(formatted_text[:500])

        # Show how text would look when joined with spaces
        text_segments = [seg['text'] for seg in transcript_data]
        joined_text = ' '.join(text_segments)

        print(f"\n🔗 Joined text (first 500 chars):")
        print("=" * 80)
        print(joined_text[:500])

        # Save full transcript to file
        output_file = f"transcript_{video_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'video_id': video_id,
                'segment_count': len(transcript_data),
                'segments': transcript_data,
                'formatted_text': formatted_text,
                'joined_text': joined_text
            }, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Full transcript saved to: {output_file}")

    except Exception as e:
        print(f"\n❌ Error fetching transcript: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
