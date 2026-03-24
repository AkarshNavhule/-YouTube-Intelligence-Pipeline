"""
Stage 6: Gmail delivery.

Sends a professional HTML email with the Google Slides report link
to the configured recipient using the Gmail API (OAuth, no SMTP needed).

Output: Console confirmation with Gmail message ID.
"""

import base64
import json
import os
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from tools.google_auth import get_google_service

TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_SLIDES = TMP_DIR / "slides_metadata.json"
INPUT_ANALYSIS = TMP_DIR / "analysis.json"

RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "")


def build_html_email(slides_url: str, analysis: dict, date_str: str) -> str:
    key_stats = analysis.get("key_stats", {})
    sentiment = analysis.get("overall_sentiment", {})
    recs = analysis.get("strategic_recommendations", [])[:3]
    summary_short = analysis.get("executive_summary", "")[:600]

    recs_html = ""
    priority_colors = {"high": "#e53935", "medium": "#fb8c00", "low": "#43a047"}
    for r in recs:
        p = r.get("priority", "medium").lower()
        color = priority_colors.get(p, "#fb8c00")
        recs_html += f"""
        <div style="background:#f8f9fa;border-left:4px solid {color};padding:12px 16px;margin:8px 0;border-radius:4px;">
            <span style="color:{color};font-size:11px;font-weight:bold;text-transform:uppercase;">{p} priority</span>
            <p style="margin:4px 0 0;font-size:14px;color:#1a1a2e;font-weight:600;">{r.get('recommendation', '')}</p>
        </div>"""

    sentiment_score = sentiment.get("score", 0.5)
    sentiment_color = "#43a047" if sentiment_score >= 0.65 else ("#fb8c00" if sentiment_score >= 0.45 else "#e53935")

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5;">
<tr><td align="center" style="padding:32px 16px;">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">

  <!-- Header -->
  <tr><td style="background:#1a1a2e;padding:40px 40px 32px;text-align:center;">
    <p style="margin:0 0 8px;color:#0066ff;font-size:12px;font-weight:700;letter-spacing:2px;text-transform:uppercase;">AI Content Intelligence</p>
    <h1 style="margin:0;color:#ffffff;font-size:28px;font-weight:700;line-height:1.3;">YouTube Intelligence Report</h1>
    <p style="margin:12px 0 0;color:#a0aec0;font-size:14px;">{date_str}</p>
  </td></tr>

  <!-- Stats bar -->
  <tr><td style="background:#0066ff;padding:16px 40px;">
    <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
      <td align="center" style="color:#ffffff;">
        <strong style="font-size:22px;">{key_stats.get('total_videos_analyzed', 0)}</strong><br>
        <span style="font-size:11px;opacity:0.85;">Videos Analyzed</span>
      </td>
      <td align="center" style="color:#ffffff;border-left:1px solid rgba(255,255,255,0.3);">
        <strong style="font-size:22px;">{key_stats.get('total_channels_analyzed', 0)}</strong><br>
        <span style="font-size:11px;opacity:0.85;">Channels</span>
      </td>
      <td align="center" style="color:#ffffff;border-left:1px solid rgba(255,255,255,0.3);">
        <strong style="font-size:22px;">{key_stats.get('avg_view_count', 0):,}</strong><br>
        <span style="font-size:11px;opacity:0.85;">Avg Views</span>
      </td>
      <td align="center" style="color:#ffffff;border-left:1px solid rgba(255,255,255,0.3);">
        <strong style="font-size:22px;color:{sentiment_color};">{sentiment.get('label', 'Neutral').title()}</strong><br>
        <span style="font-size:11px;opacity:0.85;">Sentiment</span>
      </td>
    </tr>
    </table>
  </td></tr>

  <!-- CTA Button -->
  <tr><td style="padding:40px 40px 24px;text-align:center;">
    <p style="margin:0 0 24px;color:#4a5568;font-size:15px;">Your weekly AI YouTube intelligence report is ready. Click below to view the full slide deck with charts and analysis.</p>
    <a href="{slides_url}" style="display:inline-block;background:#0066ff;color:#ffffff;text-decoration:none;padding:16px 40px;border-radius:8px;font-size:16px;font-weight:700;letter-spacing:0.5px;">View Full Report →</a>
    <p style="margin:16px 0 0;font-size:12px;color:#a0aec0;">Or copy this link: <a href="{slides_url}" style="color:#0066ff;">{slides_url[:60]}...</a></p>
  </td></tr>

  <!-- Summary -->
  <tr><td style="padding:0 40px 32px;">
    <h2 style="margin:0 0 12px;color:#1a1a2e;font-size:18px;border-bottom:2px solid #0066ff;padding-bottom:8px;">Executive Summary</h2>
    <p style="margin:0;color:#4a5568;font-size:14px;line-height:1.7;">{summary_short}...</p>
  </td></tr>

  <!-- Recommendations -->
  <tr><td style="padding:0 40px 40px;">
    <h2 style="margin:0 0 12px;color:#1a1a2e;font-size:18px;border-bottom:2px solid #0066ff;padding-bottom:8px;">Top Recommendations</h2>
    {recs_html}
  </td></tr>

  <!-- Footer -->
  <tr><td style="background:#f7f8fa;padding:24px 40px;text-align:center;border-top:1px solid #e2e8f0;">
    <p style="margin:0;color:#a0aec0;font-size:12px;">Generated by WAT Framework · Powered by Claude AI</p>
    <p style="margin:4px 0 0;color:#a0aec0;font-size:12px;">{key_stats.get('date_range', 'Last 30 days')}</p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def build_plain_text(slides_url: str, analysis: dict, date_str: str) -> str:
    key_stats = analysis.get("key_stats", {})
    sentiment = analysis.get("overall_sentiment", {})
    recs = analysis.get("strategic_recommendations", [])[:3]

    rec_lines = "\n".join(f"  [{r.get('priority','').upper()}] {r.get('recommendation', '')}" for r in recs)

    return f"""AI YouTube Intelligence Report — {date_str}
==============================================

View the full report: {slides_url}

KEY STATS
---------
Videos Analyzed:  {key_stats.get('total_videos_analyzed', 0)}
Channels Tracked: {key_stats.get('total_channels_analyzed', 0)}
Avg View Count:   {key_stats.get('avg_view_count', 0):,}
Sentiment:        {sentiment.get('label', 'Neutral').title()}

EXECUTIVE SUMMARY
-----------------
{analysis.get('executive_summary', '')[:500]}...

TOP RECOMMENDATIONS
-------------------
{rec_lines}

---
Generated by WAT Framework · Powered by Claude AI
"""


def main():
    print("[Stage 6/6] Sending email via Gmail...")

    if not RECIPIENT_EMAIL or RECIPIENT_EMAIL == "your@email.com":
        raise ValueError("RECIPIENT_EMAIL not set in .env")

    for f in [INPUT_SLIDES, INPUT_ANALYSIS]:
        if not f.exists():
            raise FileNotFoundError(f"Missing {f}")

    slides_meta = json.loads(INPUT_SLIDES.read_text(encoding="utf-8"))
    analysis = json.loads(INPUT_ANALYSIS.read_text(encoding="utf-8"))

    slides_url = slides_meta["presentation_url"]
    date_str = datetime.now().strftime("%B %d, %Y")
    subject = f"AI YouTube Intelligence Report — {datetime.now().strftime('%Y-%m-%d')}"

    # Build MIME email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"] = RECIPIENT_EMAIL

    plain = build_plain_text(slides_url, analysis, date_str)
    html = build_html_email(slides_url, analysis, date_str)

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    # Encode for Gmail API (base64url)
    raw_bytes = msg.as_bytes()
    raw_b64 = base64.urlsafe_b64encode(raw_bytes).decode("utf-8")

    gmail = get_google_service("gmail", "v1")
    result = gmail.users().messages().send(
        userId="me",
        body={"raw": raw_b64},
    ).execute()

    print(f"  Email sent to {RECIPIENT_EMAIL}")
    print(f"  Gmail message ID: {result.get('id')}")
    return result


if __name__ == "__main__":
    main()
