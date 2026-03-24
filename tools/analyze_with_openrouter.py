"""
Stage 3: Claude intelligence engine via OpenRouter.

Sends YouTube video stats, channel data, and transcript text to
anthropic/claude-sonnet-4-6 through OpenRouter's OpenAI-compatible API.

Output: .tmp/analysis.json
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_VIDEOS = TMP_DIR / "youtube_raw.json"
INPUT_TRANSCRIPTS = TMP_DIR / "transcripts.json"
OUTPUT_FILE = TMP_DIR / "analysis.json"

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "anthropic/claude-sonnet-4-5"  # OpenRouter model slug (4-6 not yet on OR; 4-5 is latest available)

OUTPUT_SCHEMA = {
    "executive_summary": "string - 2-3 paragraph narrative overview of the AI/automation YouTube space right now",
    "trending_themes": [
        {
            "theme": "string - theme name",
            "frequency_score": "integer 1-10 (how often it appeared across videos)",
            "sentiment": "string - 'optimistic' | 'cautious' | 'mixed' | 'concerned'",
            "evidence": "string - brief explanation of why this theme scored this way"
        }
    ],
    "top_videos": [
        {
            "rank": "integer",
            "video_id": "string",
            "title": "string",
            "channel_name": "string",
            "view_count": "integer",
            "engagement_rate": "float - (likes + comments) / views",
            "why_its_working": "string - Claude's analysis of why this video performed well"
        }
    ],
    "channel_rankings": [
        {
            "rank": "integer",
            "channel_name": "string",
            "subscriber_count": "integer",
            "avg_views_per_video": "integer",
            "content_focus": "string - brief description of their content angle",
            "growth_signal": "string - 'high' | 'medium' | 'low'"
        }
    ],
    "overall_sentiment": {
        "label": "string - e.g. 'cautiously optimistic'",
        "score": "float 0-1 (0=very negative, 0.5=neutral, 1=very positive)",
        "key_concerns": ["string - list of 3-5 concerns discussed in the space"],
        "key_excitement_drivers": ["string - list of 3-5 things creators are excited about"]
    },
    "content_gaps": ["string - list of 5-8 topics/angles NOT being well covered that would be valuable"],
    "strategic_recommendations": [
        {
            "recommendation": "string - actionable recommendation for a content creator",
            "rationale": "string - why this would work based on the data",
            "priority": "string - 'high' | 'medium' | 'low'"
        }
    ],
    "key_stats": {
        "total_videos_analyzed": "integer",
        "total_channels_analyzed": "integer",
        "avg_view_count": "integer",
        "avg_engagement_rate": "float",
        "date_range": "string - e.g. 'Last 30 days'"
    }
}


def build_prompt(videos: list, channels: list, transcripts: list, days_back: int) -> str:
    """Build the user message with all YouTube data for Claude to analyze."""

    # Pre-compute engagement rates
    for v in videos:
        views = v.get("view_count", 1) or 1
        v["engagement_rate"] = round((v.get("like_count", 0) + v.get("comment_count", 0)) / views, 4)

    # Top 30 videos table
    top_videos = sorted(videos, key=lambda x: x["view_count"], reverse=True)[:30]
    video_table = "RANK | TITLE | CHANNEL | VIEWS | LIKES | COMMENTS | ENGAGEMENT RATE\n"
    video_table += "-" * 100 + "\n"
    for i, v in enumerate(top_videos, 1):
        title = v["title"][:55]
        channel = v["channel_name"][:25]
        video_table += f"{i:2} | {title:<55} | {channel:<25} | {v['view_count']:>8,} | {v.get('like_count',0):>7,} | {v.get('comment_count',0):>8,} | {v['engagement_rate']:.3f}\n"

    # Channel table
    channel_table = "CHANNEL | SUBSCRIBERS | TOTAL VIEWS | VIDEO COUNT\n"
    channel_table += "-" * 80 + "\n"
    for ch in channels[:20]:
        channel_table += f"{ch['channel_name']:<35} | {ch['subscriber_count']:>12,} | {ch['total_views']:>12,} | {ch['video_count']:>6,}\n"

    # Transcript summaries
    transcript_section = ""
    available = [t for t in transcripts if t.get("transcript_available") and t.get("transcript_text")]
    if available:
        transcript_section = "\n\n## VIDEO TRANSCRIPTS (top videos by view count)\n\n"
        for t in available[:10]:
            words = t["transcript_text"].split()[:1500]
            excerpt = " ".join(words)
            transcript_section += f"### {t['title']} ({t['channel_name']}, {t['view_count']:,} views)\n{excerpt}\n\n---\n\n"
    else:
        transcript_section = "\n\n## VIDEO TRANSCRIPTS\nNo transcripts were available. Analyze based on titles, descriptions, and engagement data only.\n"

    avg_views = int(sum(v["view_count"] for v in videos) / len(videos)) if videos else 0
    avg_er = round(sum(v["engagement_rate"] for v in videos) / len(videos), 4) if videos else 0

    return f"""You are analyzing YouTube content intelligence data for the AI and AI automation niche.

## DATA OVERVIEW
- Videos analyzed: {len(videos)}
- Channels tracked: {len(channels)}
- Date range: Last {days_back} days
- Search keywords: AI automation, AI agents, LLM tools, Claude AI, ChatGPT automation, AI tools

## TOP VIDEOS BY VIEW COUNT
{video_table}

## CHANNEL STATISTICS
{channel_table}
{transcript_section}

## ANALYSIS INSTRUCTIONS

Analyze this data and respond with ONLY valid JSON matching the exact schema below.
No markdown formatting, no code blocks, no explanatory text — just the raw JSON object.

Produce genuine insights:
- Identify real patterns from the data, not generic advice
- For top_videos, explain specifically WHY each video performed well based on title, engagement, and transcript content
- For channel_rankings, base growth_signal on subscriber-to-view ratios and channel trajectory
- For content_gaps, identify specific topics that are underrepresented given current audience interest
- For strategic_recommendations, make them specific and actionable for a content creator in this niche
- The executive_summary should read like a professional analyst's briefing

Pre-computed stats for key_stats:
- total_videos_analyzed: {len(videos)}
- total_channels_analyzed: {len(channels)}
- avg_view_count: {avg_views}
- avg_engagement_rate: {avg_er}
- date_range: "Last {days_back} days"

## REQUIRED JSON SCHEMA
{json.dumps(OUTPUT_SCHEMA, indent=2)}

Respond with ONLY the JSON, no other text."""


def call_openrouter(system: str, user: str) -> str:
    """Make a chat completion request to OpenRouter. Returns the assistant's text."""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/wat-framework",
        "X-Title": "WAT YouTube Intelligence",
    }
    payload = {
        "model": MODEL,
        "max_tokens": 4096,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    response = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120)

    if not response.ok:
        print(f"  OpenRouter HTTP {response.status_code}: {response.text[:500]}")
        response.raise_for_status()

    data = response.json()

    # Surface any API-level error returned in the body
    if "error" in data:
        raise RuntimeError(f"OpenRouter API error: {data['error']}")

    content = data["choices"][0]["message"]["content"]

    if not content or not content.strip():
        raise RuntimeError(f"OpenRouter returned empty content. Full response: {data}")

    return content


def main():
    print("[Stage 3/6] Analyzing with Claude via OpenRouter...")

    if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "your_openrouter_api_key_here":
        raise ValueError("OPENROUTER_API_KEY not set in .env")

    for f in [INPUT_VIDEOS, INPUT_TRANSCRIPTS]:
        if not f.exists():
            raise FileNotFoundError(f"Missing {f} — run earlier stages first")

    raw_data = json.loads(INPUT_VIDEOS.read_text(encoding="utf-8"))
    transcript_data = json.loads(INPUT_TRANSCRIPTS.read_text(encoding="utf-8"))

    videos = raw_data.get("videos", [])
    channels = raw_data.get("channels", [])
    transcripts = transcript_data.get("transcripts", [])
    days_back = raw_data.get("days_back", 30)

    available_transcripts = len([t for t in transcripts if t.get("transcript_available")])
    print(f"  Sending {len(videos)} videos, {len(channels)} channels, {available_transcripts} transcripts to {MODEL}...")

    system_msg = (
        "You are a YouTube content intelligence analyst specializing in the AI and automation niche. "
        "You produce data-driven insights to help content creators understand what's working and what opportunities exist."
    )
    user_msg = build_prompt(videos, channels, transcripts, days_back)

    raw_text = call_openrouter(system_msg, user_msg)

    # Strip markdown code fences if the model wrapped the JSON
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        cleaned = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

    try:
        analysis = json.loads(cleaned)
    except json.JSONDecodeError:
        print("  JSON parse failed, retrying with stricter prompt...")
        strict_system = system_msg + " CRITICAL: Respond with ONLY valid JSON. No markdown, no code blocks. Start with { and end with }."
        raw_text2 = call_openrouter(strict_system, user_msg)
        cleaned2 = raw_text2.strip()
        if cleaned2.startswith("```"):
            lines = cleaned2.split("\n")
            cleaned2 = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
        analysis = json.loads(cleaned2)

    analysis["generated_at"] = datetime.now(timezone.utc).isoformat()

    OUTPUT_FILE.write_text(json.dumps(analysis, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Analysis complete -> {OUTPUT_FILE}")
    print(f"  Themes found: {len(analysis.get('trending_themes', []))}")
    print(f"  Recommendations: {len(analysis.get('strategic_recommendations', []))}")
    return analysis


if __name__ == "__main__":
    main()
