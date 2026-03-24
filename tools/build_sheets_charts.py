"""
Stage 4: Google Sheets + Chart builder.

Creates a Google Spreadsheet with 4 data sheets and native Google Charts.
Charts are embedded in Slides in Stage 5 — this intermediate step is required
because the Slides API cannot create charts natively.

Output: .tmp/sheets_metadata.json (contains spreadsheet_id and chart_ids)
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tools.google_auth import get_google_service

TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_FILE = TMP_DIR / "analysis.json"
OUTPUT_FILE = TMP_DIR / "sheets_metadata.json"


def create_spreadsheet(service, title: str) -> str:
    """Create a new blank spreadsheet and return its ID."""
    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"sheetId": 0, "title": "Top Videos", "index": 0}},
            {"properties": {"sheetId": 1, "title": "Channel Rankings", "index": 1}},
            {"properties": {"sheetId": 2, "title": "Trending Themes", "index": 2}},
            {"properties": {"sheetId": 3, "title": "Sentiment", "index": 3}},
        ],
    }
    result = service.spreadsheets().create(body=body).execute()
    return result["spreadsheetId"]


def write_all_data(service, spreadsheet_id: str, analysis: dict):
    """Write data to all four sheets in a single batchUpdate call."""
    top_videos = analysis.get("top_videos", [])[:20]
    channel_rankings = analysis.get("channel_rankings", [])
    trending_themes = analysis.get("trending_themes", [])
    sentiment = analysis.get("overall_sentiment", {})

    # Sheet 1: Top Videos
    videos_data = [["Rank", "Title", "Channel", "Views", "Likes (est)", "Comments (est)", "Engagement Rate"]]
    for v in top_videos:
        videos_data.append([
            v.get("rank", ""),
            v.get("title", "")[:80],
            v.get("channel_name", ""),
            v.get("view_count", 0),
            int(v.get("view_count", 0) * v.get("engagement_rate", 0) * 0.8),
            int(v.get("view_count", 0) * v.get("engagement_rate", 0) * 0.2),
            round(v.get("engagement_rate", 0), 4),
        ])

    # Sheet 2: Channel Rankings
    channels_data = [["Rank", "Channel", "Subscribers", "Avg Views/Video", "Growth Signal", "Content Focus"]]
    for ch in channel_rankings:
        channels_data.append([
            ch.get("rank", ""),
            ch.get("channel_name", ""),
            ch.get("subscriber_count", 0),
            ch.get("avg_views_per_video", 0),
            ch.get("growth_signal", ""),
            ch.get("content_focus", "")[:80],
        ])

    # Sheet 3: Trending Themes
    themes_data = [["Theme", "Frequency Score (1-10)", "Sentiment"]]
    for t in trending_themes:
        themes_data.append([
            t.get("theme", ""),
            t.get("frequency_score", 0),
            t.get("sentiment", ""),
        ])

    # Sheet 4: Sentiment
    concerns = sentiment.get("key_concerns", [])
    drivers = sentiment.get("key_excitement_drivers", [])
    sentiment_data = [["Category", "Item", "Score"]]
    sentiment_data.append(["Overall Sentiment", sentiment.get("label", ""), sentiment.get("score", 0.5)])
    for c in concerns:
        sentiment_data.append(["Concern", c, ""])
    for d in drivers:
        sentiment_data.append(["Excitement Driver", d, ""])

    data = [
        {"range": "Top Videos!A1", "values": videos_data},
        {"range": "Channel Rankings!A1", "values": channels_data},
        {"range": "Trending Themes!A1", "values": themes_data},
        {"range": "Sentiment!A1", "values": sentiment_data},
    ]

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def add_charts(service, spreadsheet_id: str, analysis: dict) -> dict:
    """
    Add charts to each sheet. Returns dict of chart_ids keyed by sheet name.
    Chart IDs are explicitly set so we can reference them in Slides.
    """
    top_videos = analysis.get("top_videos", [])
    channel_rankings = analysis.get("channel_rankings", [])
    trending_themes = analysis.get("trending_themes", [])

    chart_ids = {
        "top_videos": 1001,
        "channel_rankings": 1002,
        "trending_themes": 1003,
        "sentiment": 1004,
    }

    num_videos = min(len(top_videos), 10)
    num_channels = min(len(channel_rankings), 10)
    num_themes = min(len(trending_themes), 8)

    requests = []

    # Chart 1: Top Videos - horizontal bar chart (Views by title)
    if num_videos > 0:
        requests.append({
            "addChart": {
                "chart": {
                    "chartId": chart_ids["top_videos"],
                    "spec": {
                        "title": "Top Videos by View Count",
                        "basicChart": {
                            "chartType": "BAR",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Views"},
                                {"position": "LEFT_AXIS", "title": "Video"},
                            ],
                            "domains": [{
                                "domain": {
                                    "sourceRange": {
                                        "sources": [{"sheetId": 0, "startRowIndex": 1, "endRowIndex": 1 + num_videos, "startColumnIndex": 1, "endColumnIndex": 2}]
                                    }
                                }
                            }],
                            "series": [{
                                "series": {
                                    "sourceRange": {
                                        "sources": [{"sheetId": 0, "startRowIndex": 1, "endRowIndex": 1 + num_videos, "startColumnIndex": 3, "endColumnIndex": 4}]
                                    }
                                },
                                "targetAxis": "BOTTOM_AXIS",
                            }],
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": 0, "rowIndex": 1, "columnIndex": 8},
                            "widthPixels": 600,
                            "heightPixels": 371,
                        }
                    }
                }
            }
        })

    # Chart 2: Channel Rankings - vertical bar chart (Subscribers by channel)
    if num_channels > 0:
        requests.append({
            "addChart": {
                "chart": {
                    "chartId": chart_ids["channel_rankings"],
                    "spec": {
                        "title": "Top Channels by Subscribers",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Channel"},
                                {"position": "LEFT_AXIS", "title": "Subscribers"},
                            ],
                            "domains": [{
                                "domain": {
                                    "sourceRange": {
                                        "sources": [{"sheetId": 1, "startRowIndex": 1, "endRowIndex": 1 + num_channels, "startColumnIndex": 1, "endColumnIndex": 2}]
                                    }
                                }
                            }],
                            "series": [{
                                "series": {
                                    "sourceRange": {
                                        "sources": [{"sheetId": 1, "startRowIndex": 1, "endRowIndex": 1 + num_channels, "startColumnIndex": 2, "endColumnIndex": 3}]
                                    }
                                },
                                "targetAxis": "LEFT_AXIS",
                            }],
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": 1, "rowIndex": 1, "columnIndex": 7},
                            "widthPixels": 600,
                            "heightPixels": 371,
                        }
                    }
                }
            }
        })

    # Chart 3: Trending Themes - horizontal bar chart
    if num_themes > 0:
        requests.append({
            "addChart": {
                "chart": {
                    "chartId": chart_ids["trending_themes"],
                    "spec": {
                        "title": "Trending Themes - Frequency Score",
                        "basicChart": {
                            "chartType": "BAR",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Frequency Score (1-10)"},
                                {"position": "LEFT_AXIS", "title": "Theme"},
                            ],
                            "domains": [{
                                "domain": {
                                    "sourceRange": {
                                        "sources": [{"sheetId": 2, "startRowIndex": 1, "endRowIndex": 1 + num_themes, "startColumnIndex": 0, "endColumnIndex": 1}]
                                    }
                                }
                            }],
                            "series": [{
                                "series": {
                                    "sourceRange": {
                                        "sources": [{"sheetId": 2, "startRowIndex": 1, "endRowIndex": 1 + num_themes, "startColumnIndex": 1, "endColumnIndex": 2}]
                                    }
                                },
                                "targetAxis": "BOTTOM_AXIS",
                            }],
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": 2, "rowIndex": 1, "columnIndex": 4},
                            "widthPixels": 600,
                            "heightPixels": 371,
                        }
                    }
                }
            }
        })

    # Chart 4: Sentiment - simple column chart of score
    requests.append({
        "addChart": {
            "chart": {
                "chartId": chart_ids["sentiment"],
                "spec": {
                    "title": f"Overall Sentiment Score: {analysis.get('overall_sentiment', {}).get('label', 'N/A')}",
                    "basicChart": {
                        "chartType": "COLUMN",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "Category"},
                            {"position": "LEFT_AXIS", "title": "Score"},
                        ],
                        "domains": [{
                            "domain": {
                                "sourceRange": {
                                    "sources": [{"sheetId": 3, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 1}]
                                }
                            }
                        }],
                        "series": [{
                            "series": {
                                "sourceRange": {
                                    "sources": [{"sheetId": 3, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 2, "endColumnIndex": 3}]
                                }
                            },
                            "targetAxis": "LEFT_AXIS",
                        }],
                    }
                },
                "position": {
                    "overlayPosition": {
                        "anchorCell": {"sheetId": 3, "rowIndex": 2, "columnIndex": 4},
                        "widthPixels": 400,
                        "heightPixels": 300,
                    }
                }
            }
        }
    })

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    return chart_ids


def main():
    print("[Stage 4/6] Building Google Sheets + charts...")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing {INPUT_FILE} — run analyze_with_claude.py first")

    analysis = json.loads(INPUT_FILE.read_text(encoding="utf-8"))

    service = get_google_service("sheets", "v4")

    date_str = datetime.now().strftime("%Y-%m-%d")
    title = f"YT Intelligence {date_str}"

    print(f"  Creating spreadsheet: '{title}'...")
    spreadsheet_id = create_spreadsheet(service, title)
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    print(f"  Created: {spreadsheet_url}")

    print("  Writing data to sheets...")
    write_all_data(service, spreadsheet_id, analysis)

    print("  Adding charts...")
    chart_ids = add_charts(service, spreadsheet_id, analysis)

    output = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "chart_ids": chart_ids,
        "sheets": {
            "top_videos": {"sheet_id": 0, "chart_id": chart_ids["top_videos"]},
            "channel_rankings": {"sheet_id": 1, "chart_id": chart_ids["channel_rankings"]},
            "trending_themes": {"sheet_id": 2, "chart_id": chart_ids["trending_themes"]},
            "sentiment": {"sheet_id": 3, "chart_id": chart_ids["sentiment"]},
        }
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"  Spreadsheet ready -> {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
