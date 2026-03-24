"""
Stage 1: YouTube Data API v3 scraper.

Searches YouTube for AI/automation niche videos across configurable keywords,
then fetches detailed video stats and channel stats in batched API calls.

Output: .tmp/youtube_raw.json

Quota usage: ~570 units per run (out of 10,000 free daily units)
  - 100 units per search.list call (1 per keyword)
  - 1 unit per video in videos.list (batched by 50)
  - 1 unit per channel in channels.list (batched by 50)
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TMP_DIR = PROJECT_ROOT / ".tmp"
TMP_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = TMP_DIR / "youtube_raw.json"

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SEARCH_KEYWORDS = [k.strip() for k in os.getenv("SEARCH_KEYWORDS", "AI automation,AI agents").split(",")]
RESULTS_PER_KEYWORD = int(os.getenv("SEARCH_RESULTS_PER_KEYWORD", "10"))
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))


def get_youtube_client():
    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "your_youtube_api_key_here":
        raise ValueError(
            "YOUTUBE_API_KEY not set in .env\n"
            "Get a free API key at console.cloud.google.com > APIs & Services > Credentials > Create API Key"
        )
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def search_videos(youtube, keyword: str, published_after: str) -> list[dict]:
    """Search YouTube for videos matching keyword, published after the given date."""
    print(f"  Searching: '{keyword}'...")
    try:
        response = youtube.search().list(
            q=keyword,
            type="video",
            part="id,snippet",
            maxResults=RESULTS_PER_KEYWORD,
            publishedAfter=published_after,
            order="viewCount",
            relevanceLanguage="en",
            safeSearch="none",
        ).execute()
    except HttpError as e:
        if e.resp.status == 403:
            raise RuntimeError(f"YouTube API quota exceeded or key invalid: {e}")
        raise

    results = []
    for item in response.get("items", []):
        results.append({
            "video_id": item["id"]["videoId"],
            "title": item["snippet"]["title"],
            "channel_id": item["snippet"]["channelId"],
            "channel_name": item["snippet"]["channelTitle"],
            "published_at": item["snippet"]["publishedAt"],
            "thumbnail_url": item["snippet"]["thumbnails"].get("high", {}).get("url", ""),
            "description_snippet": item["snippet"]["description"][:300],
            "search_keyword": keyword,
        })
    return results


def get_video_details(youtube, video_ids: list[str]) -> dict[str, dict]:
    """Fetch detailed stats for up to 50 video IDs per call. Returns dict keyed by video_id."""
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        response = youtube.videos().list(
            id=",".join(batch),
            part="statistics,contentDetails,snippet",
        ).execute()
        for item in response.get("items", []):
            vid_id = item["id"]
            stats = item.get("statistics", {})
            tags = item.get("snippet", {}).get("tags", [])
            duration_raw = item.get("contentDetails", {}).get("duration", "PT0S")
            details[vid_id] = {
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "duration_seconds": _parse_duration(duration_raw),
                "tags": tags[:10],  # cap at 10 tags
            }
    return details


def get_channel_details(youtube, channel_ids: list[str]) -> dict[str, dict]:
    """Fetch channel stats for up to 50 channel IDs per call. Returns dict keyed by channel_id."""
    details = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        response = youtube.channels().list(
            id=",".join(batch),
            part="statistics,snippet",
        ).execute()
        for item in response.get("items", []):
            ch_id = item["id"]
            stats = item.get("statistics", {})
            details[ch_id] = {
                "channel_name": item["snippet"]["title"],
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "video_count": int(stats.get("videoCount", 0)),
                "total_views": int(stats.get("viewCount", 0)),
                "country": item["snippet"].get("country", "Unknown"),
            }
    return details


def _parse_duration(duration: str) -> int:
    """Convert ISO 8601 duration (PT4M33S) to total seconds."""
    import re
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def main():
    print("[Stage 1/6] Fetching YouTube data...")

    youtube = get_youtube_client()

    published_after = (
        datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- Search across all keywords ---
    all_videos_raw = []
    seen_video_ids = set()

    for keyword in SEARCH_KEYWORDS:
        results = search_videos(youtube, keyword, published_after)
        for r in results:
            if r["video_id"] not in seen_video_ids:
                seen_video_ids.add(r["video_id"])
                all_videos_raw.append(r)

    print(f"  Found {len(all_videos_raw)} unique videos across {len(SEARCH_KEYWORDS)} keywords")

    # --- Batch fetch video details ---
    video_ids = [v["video_id"] for v in all_videos_raw]
    print(f"  Fetching stats for {len(video_ids)} videos...")
    video_details = get_video_details(youtube, video_ids)

    # --- Merge video details ---
    videos = []
    for v in all_videos_raw:
        vid_id = v["video_id"]
        detail = video_details.get(vid_id, {})
        videos.append({
            "video_id": vid_id,
            "title": v["title"],
            "channel_id": v["channel_id"],
            "channel_name": v["channel_name"],
            "published_at": v["published_at"],
            "thumbnail_url": v["thumbnail_url"],
            "description_snippet": v["description_snippet"],
            "search_keyword": v["search_keyword"],
            "view_count": detail.get("view_count", 0),
            "like_count": detail.get("like_count", 0),
            "comment_count": detail.get("comment_count", 0),
            "duration_seconds": detail.get("duration_seconds", 0),
            "tags": detail.get("tags", []),
        })

    # --- Batch fetch channel details ---
    unique_channel_ids = list({v["channel_id"] for v in videos})
    print(f"  Fetching stats for {len(unique_channel_ids)} channels...")
    channel_details = get_channel_details(youtube, unique_channel_ids)

    channels = []
    for ch_id, detail in channel_details.items():
        channels.append({
            "channel_id": ch_id,
            "channel_name": detail["channel_name"],
            "subscriber_count": detail["subscriber_count"],
            "video_count": detail["video_count"],
            "total_views": detail["total_views"],
            "country": detail["country"],
        })

    # Estimate quota: 100 per keyword search + 1 per video + 1 per channel
    quota_used = len(SEARCH_KEYWORDS) * 100 + len(videos) + len(channels)

    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "keywords_searched": SEARCH_KEYWORDS,
        "days_back": DAYS_BACK,
        "videos": sorted(videos, key=lambda x: x["view_count"], reverse=True),
        "channels": sorted(channels, key=lambda x: x["total_views"], reverse=True),
        "quota_used_estimate": quota_used,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Saved {len(videos)} videos, {len(channels)} channels -> {OUTPUT_FILE}")
    print(f"  Estimated quota used: {quota_used} / 10,000 units")
    return output


if __name__ == "__main__":
    main()
