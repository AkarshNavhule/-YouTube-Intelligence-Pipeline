"""
Stage 2: Transcript extraction for top videos.

Reads youtube_raw.json, takes the top 15 videos by view count,
and fetches their transcripts using youtube-transcript-api (no API key needed).

Output: .tmp/transcripts.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

PROJECT_ROOT = Path(__file__).parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_FILE = TMP_DIR / "youtube_raw.json"
OUTPUT_FILE = TMP_DIR / "transcripts.json"

TOP_N = 15          # Number of videos to fetch transcripts for
MAX_WORDS = 8000    # Truncate transcripts to this word count to manage Claude context size


def fetch_transcript(video_id: str) -> str | None:
    """
    Fetch transcript for a video. Returns joined text or None if unavailable.
    Tries English first, then auto-generated, then any available language.
    """
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Prefer manually created English transcript
        try:
            transcript = transcript_list.find_manually_created_transcript(["en"])
        except NoTranscriptFound:
            # Fall back to auto-generated English
            try:
                transcript = transcript_list.find_generated_transcript(["en"])
            except NoTranscriptFound:
                # Take whatever is available and translate
                transcript = transcript_list.find_generated_transcript(
                    [t.language_code for t in transcript_list]
                )

        segments = transcript.fetch()
        text = " ".join(seg["text"] for seg in segments)
        return text

    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        return None
    except Exception:
        return None


def truncate_to_words(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + " [truncated]"


def main():
    print("[Stage 2/6] Fetching transcripts...")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing {INPUT_FILE} — run fetch_youtube_data.py first")

    data = json.loads(INPUT_FILE.read_text(encoding="utf-8"))
    videos = data.get("videos", [])

    # Take top N by view count (already sorted descending from Stage 1)
    top_videos = videos[:TOP_N]
    print(f"  Fetching transcripts for top {len(top_videos)} videos by view count...")

    transcripts = []
    failed = []

    for i, video in enumerate(top_videos, 1):
        vid_id = video["video_id"]
        title = video["title"]
        print(f"  [{i}/{len(top_videos)}] {title[:60]}...")

        text = fetch_transcript(vid_id)

        if text:
            truncated = truncate_to_words(text, MAX_WORDS)
            word_count = len(truncated.split())
            transcripts.append({
                "video_id": vid_id,
                "title": title,
                "channel_name": video["channel_name"],
                "view_count": video["view_count"],
                "transcript_text": truncated,
                "word_count": word_count,
                "transcript_available": True,
            })
            print(f"    [OK] {word_count} words")
        else:
            failed.append(vid_id)
            transcripts.append({
                "video_id": vid_id,
                "title": title,
                "channel_name": video["channel_name"],
                "view_count": video["view_count"],
                "transcript_text": "",
                "word_count": 0,
                "transcript_available": False,
            })
            print(f"    [X] Transcript unavailable")

    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total_attempted": len(top_videos),
        "successful": len(top_videos) - len(failed),
        "failed_count": len(failed),
        "failed_video_ids": failed,
        "transcripts": transcripts,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Transcripts: {output['successful']}/{len(top_videos)} successful -> {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
