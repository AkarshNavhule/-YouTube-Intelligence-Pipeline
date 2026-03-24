"""
Stage 5: Google Slides deck assembler.

Creates a professional 9-slide presentation using Claude's analysis and
embedded Google Sheets charts. All operations are batched into a single
batchUpdate call for efficiency.

Output: Google Slides URL + .tmp/slides_metadata.json
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tools.google_auth import get_google_service

TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_ANALYSIS = TMP_DIR / "analysis.json"
INPUT_SHEETS = TMP_DIR / "sheets_metadata.json"
OUTPUT_FILE = TMP_DIR / "slides_metadata.json"

# Slide dimensions (16:9 widescreen in EMUs — 1 inch = 914400 EMUs)
SLIDE_WIDTH = 9144000
SLIDE_HEIGHT = 5143500

# Color palette
DARK_BG = {"red": 0.102, "green": 0.102, "blue": 0.180}        # #1a1a2e
BLUE_ACCENT = {"red": 0.0, "green": 0.40, "blue": 1.0}          # #0066ff
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
LIGHT_GRAY = {"red": 0.92, "green": 0.92, "blue": 0.95}
DARK_TEXT = {"red": 0.15, "green": 0.15, "blue": 0.20}
GREEN = {"red": 0.13, "green": 0.77, "blue": 0.37}
ORANGE = {"red": 1.0, "green": 0.60, "blue": 0.0}
RED_SOFT = {"red": 0.95, "green": 0.26, "blue": 0.21}


# ─── Layout helpers ──────────────────────────────────────────────────────────

def emu(inches: float) -> int:
    return int(inches * 914400)


def pt(points: float) -> int:
    """Points to EMU (for font sizes, use magnitude directly in Slides API)."""
    return int(points)


def rgb(r, g, b):
    return {"red": r / 255, "green": g / 255, "blue": b / 255}


def text_box(object_id: str, text: str, x: float, y: float, w: float, h: float,
             font_size: int = 14, bold: bool = False, color: dict = None,
             bg_color: dict = None, h_align: str = "LEFT", v_align: str = "TOP",
             italic: bool = False, page_id: str = None) -> list:
    """Returns a list of requests to create a text box."""
    if color is None:
        color = DARK_TEXT

    requests = [
        {
            "createShape": {
                "objectId": object_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": page_id if page_id else object_id.rsplit("_", 1)[0],
                    "size": {"width": {"magnitude": emu(w), "unit": "EMU"},
                              "height": {"magnitude": emu(h), "unit": "EMU"}},
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": emu(x), "translateY": emu(y),
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "insertText": {
                "objectId": object_id,
                "text": text,
            }
        },
        {
            "updateTextStyle": {
                "objectId": object_id,
                "style": {
                    "fontSize": {"magnitude": font_size, "unit": "PT"},
                    "bold": bold,
                    "italic": italic,
                    "foregroundColor": {"opaqueColor": {"rgbColor": color}},
                    "fontFamily": "Montserrat" if bold else "Open Sans",
                },
                "fields": "fontSize,bold,italic,foregroundColor,fontFamily",
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": object_id,
                "style": {"alignment": {"LEFT": "START", "RIGHT": "END"}.get(h_align, h_align)},
                "fields": "alignment",
            }
        },
    ]

    if bg_color:
        requests.append({
            "updateShapeProperties": {
                "objectId": object_id,
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "solidFill": {"color": {"rgbColor": bg_color}}
                    }
                },
                "fields": "shapeBackgroundFill",
            }
        })

    return requests


def slide_bg(slide_id: str, color: dict) -> dict:
    return {
        "updatePageProperties": {
            "objectId": slide_id,
            "pageProperties": {
                "pageBackgroundFill": {
                    "solidFill": {"color": {"rgbColor": color}}
                }
            },
            "fields": "pageBackgroundFill",
        }
    }


def embed_chart(object_id: str, slide_id: str, spreadsheet_id: str, chart_id: int,
                x: float, y: float, w: float, h: float) -> dict:
    return {
        "createSheetsChart": {
            "objectId": object_id,
            "spreadsheetId": spreadsheet_id,
            "chartId": chart_id,
            "linkingMode": "LINKED",
            "elementProperties": {
                "pageObjectId": slide_id,
                "size": {
                    "width": {"magnitude": emu(w), "unit": "EMU"},
                    "height": {"magnitude": emu(h), "unit": "EMU"},
                },
                "transform": {
                    "scaleX": 1, "scaleY": 1,
                    "translateX": emu(x), "translateY": emu(y),
                    "unit": "EMU",
                },
            },
        }
    }


def divider_line(object_id: str, slide_id: str, x: float, y: float, w: float) -> list:
    """Thin accent-colored horizontal line."""
    return [
        {
            "createLine": {
                "objectId": object_id,
                "lineCategory": "STRAIGHT",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {
                        "width": {"magnitude": emu(w), "unit": "EMU"},
                        "height": {"magnitude": emu(0.01), "unit": "EMU"},
                    },
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": emu(x), "translateY": emu(y),
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "updateLineProperties": {
                "objectId": object_id,
                "lineProperties": {
                    "lineFill": {
                        "solidFill": {"color": {"rgbColor": BLUE_ACCENT}}
                    },
                    "weight": {"magnitude": 3, "unit": "PT"},
                },
                "fields": "lineFill,weight",
            }
        },
    ]


def truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 3] + "..."


# ─── Slide builders ──────────────────────────────────────────────────────────

def build_slide1_title(slide_id: str, analysis: dict, date_str: str) -> list:
    """Slide 1: Dark title slide."""
    key_stats = analysis.get("key_stats", {})
    subtitle = f"Week of {date_str}  |  {key_stats.get('total_videos_analyzed', 0)} videos  |  {key_stats.get('total_channels_analyzed', 0)} channels"

    requests = [slide_bg(slide_id, DARK_BG)]
    requests += text_box(f"{slide_id}_title", "AI & Automation\nYouTube Intelligence Report",
                         0.4, 1.4, 9.2, 1.8, font_size=40, bold=True, color=WHITE, h_align="CENTER")
    requests += text_box(f"{slide_id}_subtitle", subtitle,
                         0.4, 3.3, 9.2, 0.6, font_size=18, color=BLUE_ACCENT, h_align="CENTER")
    requests += text_box(f"{slide_id}_tag", f"Generated {date_str}  |  Powered by Claude AI",
                         0.4, 4.5, 9.2, 0.4, font_size=12, color=LIGHT_GRAY, h_align="CENTER", italic=True)
    requests += divider_line(f"{slide_id}_line", slide_id, 1.5, 3.1, 7.0)
    return requests


def build_slide2_exec_summary(slide_id: str, analysis: dict) -> list:
    """Slide 2: Executive summary."""
    summary = analysis.get("executive_summary", "No summary available.")
    truncated = truncate(summary, 900)

    requests = [slide_bg(slide_id, WHITE)]
    requests += text_box(f"{slide_id}_label", "EXECUTIVE SUMMARY",
                         0.4, 0.25, 3.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "What's Happening in AI Right Now",
                         0.4, 0.55, 9.2, 0.8, font_size=28, bold=True, color=DARK_TEXT)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.35, 9.2)
    requests += text_box(f"{slide_id}_body", truncated,
                         0.4, 1.55, 9.2, 3.0, font_size=13, color=DARK_TEXT)
    return requests


def build_slide3_stats(slide_id: str, analysis: dict) -> list:
    """Slide 3: Key stats at a glance (4 cards)."""
    stats = analysis.get("key_stats", {})
    sentiment = analysis.get("overall_sentiment", {})

    cards = [
        ("Videos Analyzed", str(stats.get("total_videos_analyzed", 0)), BLUE_ACCENT),
        ("Channels Tracked", str(stats.get("total_channels_analyzed", 0)), GREEN),
        ("Avg View Count", f"{stats.get('avg_view_count', 0):,}", ORANGE),
        ("Avg Engagement", f"{stats.get('avg_engagement_rate', 0):.2%}", rgb(156, 39, 176)),
    ]

    requests = [slide_bg(slide_id, LIGHT_GRAY)]
    requests += text_box(f"{slide_id}_title", "Key Stats at a Glance",
                         0.4, 0.25, 9.2, 0.7, font_size=28, bold=True, color=DARK_TEXT)
    requests += text_box(f"{slide_id}_range", stats.get("date_range", ""),
                         0.4, 0.85, 9.2, 0.4, font_size=13, color=BLUE_ACCENT)

    # 4 stat cards in 2x2 grid
    positions = [(0.4, 1.5), (5.0, 1.5), (0.4, 3.3), (5.0, 3.3)]
    for i, ((label, value, accent), (x, y)) in enumerate(zip(cards, positions)):
        cid = f"{slide_id}_card{i}"
        requests.append({
            "createShape": {
                "objectId": cid,
                "shapeType": "RECTANGLE",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {"width": {"magnitude": emu(4.2), "unit": "EMU"},
                              "height": {"magnitude": emu(1.5), "unit": "EMU"}},
                    "transform": {"scaleX": 1, "scaleY": 1,
                                   "translateX": emu(x), "translateY": emu(y), "unit": "EMU"},
                },
            }
        })
        requests.append({
            "updateShapeProperties": {
                "objectId": cid,
                "shapeProperties": {
                    "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": WHITE}}}
                },
                "fields": "shapeBackgroundFill",
            }
        })
        requests += text_box(f"{cid}_val", value,
                             x + 0.2, y + 0.15, 3.8, 0.7, font_size=32, bold=True, color=accent, h_align="CENTER", page_id=slide_id)
        requests += text_box(f"{cid}_lbl", label,
                             x + 0.2, y + 0.95, 3.8, 0.4, font_size=12, color=DARK_TEXT, h_align="CENTER", page_id=slide_id)

    return requests


def build_slide4_top_videos(slide_id: str, analysis: dict, spreadsheet_id: str, chart_id: int) -> list:
    """Slide 4: Top trending videos with embedded chart."""
    top = analysis.get("top_videos", [])[:3]

    requests = [slide_bg(slide_id, WHITE)]
    requests += text_box(f"{slide_id}_label", "PERFORMANCE ANALYSIS",
                         0.4, 0.25, 4.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "Top Trending Videos",
                         0.4, 0.55, 9.2, 0.7, font_size=28, bold=True, color=DARK_TEXT)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.2, 9.2)

    # Chart on left
    requests.append(embed_chart(f"{slide_id}_chart", slide_id, spreadsheet_id, chart_id,
                                 0.4, 1.35, 5.5, 3.4))

    # Top 3 bullets on right
    y = 1.4
    for i, v in enumerate(top):
        title = truncate(v.get("title", ""), 50)
        channel = v.get("channel_name", "")
        why = truncate(v.get("why_its_working", ""), 120)
        requests += text_box(f"{slide_id}_v{i}_title", f"#{v.get('rank', i+1)} {title}",
                             6.1, y, 3.7, 0.5, font_size=11, bold=True, color=DARK_TEXT, page_id=slide_id)
        requests += text_box(f"{slide_id}_v{i}_ch", channel,
                             6.1, y + 0.45, 3.7, 0.3, font_size=10, color=BLUE_ACCENT, italic=True, page_id=slide_id)
        requests += text_box(f"{slide_id}_v{i}_why", why,
                             6.1, y + 0.72, 3.7, 0.65, font_size=10, color=DARK_TEXT, page_id=slide_id)
        y += 1.15

    return requests


def build_slide5_channels(slide_id: str, analysis: dict, spreadsheet_id: str, chart_id: int) -> list:
    """Slide 5: Channel rankings."""
    channels = analysis.get("channel_rankings", [])[:8]

    requests = [slide_bg(slide_id, LIGHT_GRAY)]
    requests += text_box(f"{slide_id}_label", "COMPETITIVE LANDSCAPE",
                         0.4, 0.25, 4.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "Top Channels in the Space",
                         0.4, 0.55, 9.2, 0.7, font_size=28, bold=True, color=DARK_TEXT)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.2, 9.2)

    # Chart
    requests.append(embed_chart(f"{slide_id}_chart", slide_id, spreadsheet_id, chart_id,
                                 0.4, 1.35, 5.0, 3.4))

    # Table on right
    requests += text_box(f"{slide_id}_hdr", "Channel  |  Subs  |  Growth",
                         5.6, 1.35, 4.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    y = 1.85
    for ch in channels:
        signal = ch.get("growth_signal", "medium")
        signal_color = GREEN if signal == "high" else (ORANGE if signal == "medium" else RED_SOFT)
        name = truncate(ch.get("channel_name", ""), 22)
        subs = f"{ch.get('subscriber_count', 0):,}"
        requests += text_box(f"{slide_id}_ch{ch.get('rank', 0)}_n", f"{ch.get('rank','')}. {name}",
                             5.6, y, 2.2, 0.38, font_size=10, color=DARK_TEXT, page_id=slide_id)
        requests += text_box(f"{slide_id}_ch{ch.get('rank', 0)}_s", subs,
                             7.85, y, 1.0, 0.38, font_size=10, color=DARK_TEXT, page_id=slide_id)
        requests += text_box(f"{slide_id}_ch{ch.get('rank', 0)}_g", signal.upper(),
                             8.9, y, 0.7, 0.38, font_size=9, bold=True, color=signal_color, page_id=slide_id)
        y += 0.41

    return requests


def build_slide6_themes(slide_id: str, analysis: dict, spreadsheet_id: str, chart_id: int) -> list:
    """Slide 6: Trending themes."""
    themes = analysis.get("trending_themes", [])[:6]

    requests = [slide_bg(slide_id, WHITE)]
    requests += text_box(f"{slide_id}_label", "CONTENT TRENDS",
                         0.4, 0.25, 4.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "What Creators Are Talking About",
                         0.4, 0.55, 9.2, 0.7, font_size=28, bold=True, color=DARK_TEXT)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.2, 9.2)

    # Chart on left
    requests.append(embed_chart(f"{slide_id}_chart", slide_id, spreadsheet_id, chart_id,
                                 0.4, 1.35, 5.3, 3.4))

    # Theme bullets on right
    y = 1.4
    sentiment_colors = {"optimistic": GREEN, "cautious": ORANGE, "mixed": BLUE_ACCENT, "concerned": RED_SOFT}
    for i, t in enumerate(themes):
        theme_name = truncate(t.get("theme", ""), 45)
        sentiment = t.get("sentiment", "mixed").lower()
        score = t.get("frequency_score", 0)
        s_color = sentiment_colors.get(sentiment, BLUE_ACCENT)
        requests += text_box(f"{slide_id}_t{i}_n", f"• {theme_name}",
                             5.9, y, 3.7, 0.4, font_size=11, bold=True, color=DARK_TEXT, page_id=slide_id)
        requests += text_box(f"{slide_id}_t{i}_s", f"Score: {score}/10  |  {sentiment.capitalize()}",
                             5.9, y + 0.38, 3.7, 0.3, font_size=10, color=s_color, italic=True, page_id=slide_id)
        y += 0.75

    return requests


def build_slide7_sentiment(slide_id: str, analysis: dict) -> list:
    """Slide 7: Overall sentiment."""
    sentiment = analysis.get("overall_sentiment", {})
    label = sentiment.get("label", "Neutral").title()
    concerns = sentiment.get("key_concerns", [])[:5]
    drivers = sentiment.get("key_excitement_drivers", [])[:5]

    score = sentiment.get("score", 0.5)
    score_color = GREEN if score >= 0.65 else (ORANGE if score >= 0.45 else RED_SOFT)

    requests = [slide_bg(slide_id, DARK_BG)]
    requests += text_box(f"{slide_id}_label", "MARKET SENTIMENT",
                         0.4, 0.25, 4.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "Sentiment in the AI Space",
                         0.4, 0.55, 9.2, 0.7, font_size=28, bold=True, color=WHITE)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.2, 9.2)

    # Big sentiment label
    requests += text_box(f"{slide_id}_big", label,
                         2.5, 1.4, 4.6, 0.9, font_size=36, bold=True, color=score_color, h_align="CENTER")
    requests += text_box(f"{slide_id}_score", f"Sentiment Score: {score:.0%}",
                         3.0, 2.2, 3.6, 0.45, font_size=14, color=WHITE, h_align="CENTER")

    # Concerns (left column)
    requests += text_box(f"{slide_id}_ch", "Key Concerns",
                         0.4, 2.85, 4.3, 0.45, font_size=13, bold=True, color=RED_SOFT)
    y = 3.35
    for c in concerns:
        requests += text_box(f"{slide_id}_c{concerns.index(c)}", f"• {truncate(c, 55)}",
                             0.4, y, 4.3, 0.45, font_size=11, color=LIGHT_GRAY)
        y += 0.40

    # Excitement drivers (right column)
    requests += text_box(f"{slide_id}_eh", "Excitement Drivers",
                         5.0, 2.85, 4.3, 0.45, font_size=13, bold=True, color=GREEN)
    y = 3.35
    for d in drivers:
        requests += text_box(f"{slide_id}_d{drivers.index(d)}", f"• {truncate(d, 55)}",
                             5.0, y, 4.3, 0.45, font_size=11, color=LIGHT_GRAY)
        y += 0.40

    return requests


def build_slide8_gaps(slide_id: str, analysis: dict) -> list:
    """Slide 8: Content gaps and opportunities."""
    gaps = analysis.get("content_gaps", [])[:8]

    requests = [slide_bg(slide_id, WHITE)]
    requests += text_box(f"{slide_id}_label", "CONTENT OPPORTUNITIES",
                         0.4, 0.25, 5.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "Underserved Topics — Your Opportunity",
                         0.4, 0.55, 9.2, 0.7, font_size=28, bold=True, color=DARK_TEXT)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.2, 9.2)
    requests += text_box(f"{slide_id}_sub", "Topics with audience demand but limited quality coverage in the AI niche:",
                         0.4, 1.35, 9.2, 0.45, font_size=13, color=DARK_TEXT, italic=True)

    # Two columns of gaps
    left_gaps = gaps[:4]
    right_gaps = gaps[4:]

    y = 2.0
    for i, gap in enumerate(left_gaps):
        requests += text_box(f"{slide_id}_lg{i}", f"→  {truncate(gap, 60)}",
                             0.4, y, 4.4, 0.55, font_size=12, color=DARK_TEXT)
        y += 0.6

    y = 2.0
    for i, gap in enumerate(right_gaps):
        requests += text_box(f"{slide_id}_rg{i}", f"→  {truncate(gap, 60)}",
                             5.0, y, 4.4, 0.55, font_size=12, color=DARK_TEXT)
        y += 0.6

    return requests


def build_slide9_recommendations(slide_id: str, analysis: dict) -> list:
    """Slide 9: Strategic recommendations."""
    recs = analysis.get("strategic_recommendations", [])[:3]

    priority_colors = {"high": RED_SOFT, "medium": ORANGE, "low": GREEN}

    requests = [slide_bg(slide_id, LIGHT_GRAY)]
    requests += text_box(f"{slide_id}_label", "ACTION PLAN",
                         0.4, 0.25, 4.0, 0.4, font_size=10, bold=True, color=BLUE_ACCENT)
    requests += text_box(f"{slide_id}_title", "Strategic Recommendations",
                         0.4, 0.55, 9.2, 0.7, font_size=28, bold=True, color=DARK_TEXT)
    requests += divider_line(f"{slide_id}_line", slide_id, 0.4, 1.2, 9.2)

    card_x = [0.4, 3.4, 6.4]
    for i, rec in enumerate(recs):
        x = card_x[i]
        priority = rec.get("priority", "medium").lower()
        p_color = priority_colors.get(priority, ORANGE)
        rec_text = truncate(rec.get("recommendation", ""), 80)
        rationale = truncate(rec.get("rationale", ""), 160)

        # Card background
        cid = f"{slide_id}_card{i}"
        requests.append({
            "createShape": {
                "objectId": cid,
                "shapeType": "RECTANGLE",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {"width": {"magnitude": emu(2.8), "unit": "EMU"},
                              "height": {"magnitude": emu(3.5), "unit": "EMU"}},
                    "transform": {"scaleX": 1, "scaleY": 1,
                                   "translateX": emu(x), "translateY": emu(1.4), "unit": "EMU"},
                },
            }
        })
        requests.append({
            "updateShapeProperties": {
                "objectId": cid,
                "shapeProperties": {
                    "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": WHITE}}}
                },
                "fields": "shapeBackgroundFill",
            }
        })

        requests += text_box(f"{cid}_priority", f"● {priority.upper()} PRIORITY",
                             x + 0.15, 1.5, 2.5, 0.4, font_size=10, bold=True, color=p_color, page_id=slide_id)
        requests += text_box(f"{cid}_rec", rec_text,
                             x + 0.15, 1.95, 2.5, 1.1, font_size=12, bold=True, color=DARK_TEXT, page_id=slide_id)
        requests += text_box(f"{cid}_rat", rationale,
                             x + 0.15, 3.1, 2.5, 1.6, font_size=10, color=DARK_TEXT, italic=True, page_id=slide_id)

    return requests


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("[Stage 5/6] Building Google Slides deck...")

    for f in [INPUT_ANALYSIS, INPUT_SHEETS]:
        if not f.exists():
            raise FileNotFoundError(f"Missing {f}")

    analysis = json.loads(INPUT_ANALYSIS.read_text(encoding="utf-8"))
    sheets_meta = json.loads(INPUT_SHEETS.read_text(encoding="utf-8"))

    spreadsheet_id = sheets_meta["spreadsheet_id"]
    chart_ids = sheets_meta["chart_ids"]

    service = get_google_service("slides", "v1")
    date_str = datetime.now().strftime("%Y-%m-%d")

    print("  Creating presentation...")
    pres = service.presentations().create(body={
        "title": f"AI YouTube Intelligence Report {date_str}"
    }).execute()
    presentation_id = pres["presentationId"]
    presentation_url = f"https://docs.google.com/presentation/d/{presentation_id}"

    # The presentation starts with one default slide — we'll delete it after adding ours
    default_slide_id = pres["slides"][0]["objectId"]

    # Generate unique slide IDs
    slide_ids = [f"slide{i:02d}" for i in range(1, 10)]

    # Build all requests
    all_requests = []

    # Create 9 slides
    for i, sid in enumerate(slide_ids):
        all_requests.append({
            "createSlide": {
                "objectId": sid,
                "insertionIndex": i,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        })

    # Delete the default slide
    all_requests.append({"deleteObject": {"objectId": default_slide_id}})

    # Build slide content
    all_requests += build_slide1_title(slide_ids[0], analysis, date_str)
    all_requests += build_slide2_exec_summary(slide_ids[1], analysis)
    all_requests += build_slide3_stats(slide_ids[2], analysis)
    all_requests += build_slide4_top_videos(slide_ids[3], analysis, spreadsheet_id, chart_ids["top_videos"])
    all_requests += build_slide5_channels(slide_ids[4], analysis, spreadsheet_id, chart_ids["channel_rankings"])
    all_requests += build_slide6_themes(slide_ids[5], analysis, spreadsheet_id, chart_ids["trending_themes"])
    all_requests += build_slide7_sentiment(slide_ids[6], analysis)
    all_requests += build_slide8_gaps(slide_ids[7], analysis)
    all_requests += build_slide9_recommendations(slide_ids[8], analysis)

    print(f"  Executing {len(all_requests)} slide operations...")
    service.presentations().batchUpdate(
        presentationId=presentation_id,
        body={"requests": all_requests},
    ).execute()

    output = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "presentation_id": presentation_id,
        "presentation_url": presentation_url,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"  Slides deck created: {presentation_url}")
    return output


if __name__ == "__main__":
    main()
