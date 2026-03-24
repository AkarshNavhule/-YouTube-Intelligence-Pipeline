"""
Master orchestrator for the YouTube AI Intelligence Pipeline.

Runs all 6 stages in sequence, validates outputs between stages,
and reports progress with timing.

Usage:
    python run_pipeline.py                  # Full run
    python run_pipeline.py --skip-fetch     # Skip stages 1-2 (reuse cached YouTube data)
    python run_pipeline.py --skip-to-slides # Skip stages 1-3 (reuse cached analysis)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

TMP_DIR = PROJECT_ROOT / ".tmp"

# Expected output files per stage
STAGE_OUTPUTS = {
    1: TMP_DIR / "youtube_raw.json",
    2: TMP_DIR / "transcripts.json",
    3: TMP_DIR / "analysis.json",
    4: TMP_DIR / "sheets_metadata.json",
    5: TMP_DIR / "slides_metadata.json",
}


def validate_env():
    """Check required environment variables and files before starting."""
    errors = []

    if not os.getenv("YOUTUBE_API_KEY") or os.getenv("YOUTUBE_API_KEY") == "your_youtube_api_key_here":
        errors.append("YOUTUBE_API_KEY not set in .env")

    if not os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") == "your_openai_api_key_here":
        errors.append("OPENAI_API_KEY not set in .env")

    if not os.getenv("RECIPIENT_EMAIL") or os.getenv("RECIPIENT_EMAIL") == "your@email.com":
        errors.append("RECIPIENT_EMAIL not set in .env")

    if not (PROJECT_ROOT / "credentials.json").exists():
        errors.append(
            "credentials.json not found in project root.\n"
            "  -> Go to console.cloud.google.com\n"
            "  -> Enable: YouTube Data API v3, Sheets, Slides, Gmail APIs\n"
            "  -> Create OAuth 2.0 Client ID (Desktop app)\n"
            "  -> Download as credentials.json"
        )

    if errors:
        print("\n[ERROR] Setup incomplete. Please fix the following:\n")
        for e in errors:
            print(f"  [X] {e}")
        print()
        sys.exit(1)

    print("  Environment validated OK")


def run_stage(name: str, stage_num: int, func, output_file: Path | None = None):
    """Run a single stage with timing and output validation."""
    print(f"\n[Stage {stage_num}/6] {name}")
    t0 = time.time()

    try:
        func()
    except Exception as e:
        elapsed = time.time() - t0
        print(f"\n[FAILED] Stage {stage_num} ({name}) failed after {elapsed:.1f}s")
        print(f"  Error: {e}")
        print(f"\n  Debug tip: Check .tmp/ for partial output files.")
        print(f"  Once fixed, re-run with appropriate --skip flag to resume from this stage.")
        sys.exit(1)

    elapsed = time.time() - t0

    if output_file and not output_file.exists():
        print(f"\n[FAILED] Stage {stage_num} completed but expected output not found: {output_file}")
        sys.exit(1)

    print(f"  Done in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="YouTube AI Intelligence Pipeline")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip stages 1-2 (use cached youtube_raw.json and transcripts.json)")
    parser.add_argument("--skip-to-slides", action="store_true",
                        help="Skip stages 1-3 (use cached analysis.json)")
    args = parser.parse_args()

    pipeline_start = time.time()

    print("=" * 50)
    print("YouTube AI Intelligence Pipeline")
    print("=" * 50)

    # Validate environment
    print("\n[Setup] Validating environment...")
    validate_env()

    TMP_DIR.mkdir(exist_ok=True)

    # Determine which stages to run
    skip_stages = set()
    if args.skip_fetch or args.skip_to_slides:
        skip_stages.update([1, 2])
        for f, stage in [(STAGE_OUTPUTS[1], 1), (STAGE_OUTPUTS[2], 2)]:
            if not f.exists():
                print(f"\n[ERROR] --skip-fetch requires {f} to exist. Run without skip flags first.")
                sys.exit(1)
        print("  Skipping stages 1-2 (using cached data)")

    if args.skip_to_slides:
        skip_stages.add(3)
        if not STAGE_OUTPUTS[3].exists():
            print(f"\n[ERROR] --skip-to-slides requires {STAGE_OUTPUTS[3]} to exist.")
            sys.exit(1)
        print("  Skipping stage 3 (using cached analysis)")

    # Import tools (lazy, to avoid import errors before validation)
    from tools.fetch_youtube_data import main as fetch_yt
    from tools.fetch_transcripts import main as fetch_transcripts
    from tools.analyze_with_openai import main as analyze
    from tools.build_sheets_charts import main as build_sheets
    from tools.build_slides_deck import main as build_slides
    from tools.send_email import main as send_email

    # Stage 1
    if 1 not in skip_stages:
        run_stage("Fetching YouTube data", 1, fetch_yt, STAGE_OUTPUTS[1])

    # Stage 2
    if 2 not in skip_stages:
        run_stage("Fetching transcripts", 2, fetch_transcripts, STAGE_OUTPUTS[2])

    # Stage 3
    if 3 not in skip_stages:
        run_stage("Analyzing with GPT-4o via OpenAI", 3, analyze, STAGE_OUTPUTS[3])

    # Stage 4
    run_stage("Building Sheets + charts", 4, build_sheets, STAGE_OUTPUTS[4])

    # Stage 5
    run_stage("Building Slides deck", 5, build_slides, STAGE_OUTPUTS[5])

    # Stage 6
    run_stage("Sending email", 6, send_email, None)

    # Final summary
    total_elapsed = time.time() - pipeline_start
    minutes = int(total_elapsed // 60)
    seconds = int(total_elapsed % 60)

    slides_meta = json.loads(STAGE_OUTPUTS[5].read_text(encoding="utf-8"))
    raw_data = json.loads(STAGE_OUTPUTS[1].read_text(encoding="utf-8"))
    analysis = json.loads(STAGE_OUTPUTS[3].read_text(encoding="utf-8"))

    stats = analysis.get("key_stats", {})

    print("\n" + "=" * 50)
    print("PIPELINE COMPLETE")
    print("=" * 50)
    print(f"  Videos analyzed:  {stats.get('total_videos_analyzed', len(raw_data.get('videos', [])))}")
    print(f"  Channels tracked: {stats.get('total_channels_analyzed', len(raw_data.get('channels', [])))}")
    print(f"  Slides deck:      {slides_meta['presentation_url']}")
    print(f"  Email sent to:    {os.getenv('RECIPIENT_EMAIL')}")
    print(f"  Total runtime:    {minutes}m {seconds}s")
    print("=" * 50)


if __name__ == "__main__":
    main()
