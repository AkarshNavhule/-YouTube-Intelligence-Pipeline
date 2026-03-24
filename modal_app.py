"""
Modal deployment for the YouTube AI Intelligence Pipeline.

Runs all 6 stages in Modal's cloud — on-demand via `modal run`.

Setup (one-time):
    pip install modal
    python3 -m modal setup
    python security_check.py          # must pass before deploying
    modal secret create yt-pipeline-secrets YOUTUBE_API_KEY="..." ...
    modal deploy modal_app.py

Usage:
    modal run modal_app.py                    # full pipeline
    modal run modal_app.py --skip-fetch       # reuse cached YouTube data
    modal run modal_app.py --skip-to-slides   # reuse cached analysis
"""

import modal
from pathlib import Path

# ── Image ─────────────────────────────────────────────────────────────────────
# Build a Debian container with all pipeline dependencies.
# Playwright is NOT included (PDF stage removed; Slides API used instead).

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install([
        "google-api-python-client==2.147.0",
        "google-auth==2.35.0",
        "google-auth-oauthlib==1.2.1",
        "google-auth-httplib2==0.2.0",
        "youtube-transcript-api==0.6.2",
        "openai>=1.30.0",
        "anthropic>=0.34.0",
        "python-dotenv==1.0.1",
        "requests==2.32.3",
    ])
    # Add project source into the image
    .add_local_dir("tools", "/root/tools")
    .add_local_file("run_pipeline.py", "/root/run_pipeline.py")
)

# ── Persistent volume for .tmp/ intermediates ─────────────────────────────────
# TMP_DIR in all tools resolves to PROJECT_ROOT/.tmp = /root/.tmp inside Modal.
# Mounting here means stage outputs persist and skip-flags work correctly.
volume = modal.Volume.from_name("yt-pipeline-volume", create_if_missing=True)

# ── App ───────────────────────────────────────────────────────────────────────
app = modal.App("youtube-intelligence-pipeline")


# ── Pipeline function ─────────────────────────────────────────────────────────

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("yt-pipeline-secrets")],
    volumes={"/root/.tmp": volume},
    timeout=600,    # 10 minutes — typical pipeline runtime is 5–8 min
    memory=1024,
)
def run_pipeline(skip_fetch: bool = False, skip_to_slides: bool = False):
    """
    Executes the full 6-stage YouTube AI Intelligence Pipeline in Modal.

    Stage 1: Fetch YouTube data        → /root/.tmp/youtube_raw.json
    Stage 2: Fetch transcripts         → /root/.tmp/transcripts.json
    Stage 3: Analyze with OpenAI GPT   → /root/.tmp/analysis.json
    Stage 4: Build Google Sheets       → /root/.tmp/sheets_metadata.json
    Stage 5: Build Google Slides deck  → /root/.tmp/slides_metadata.json
    Stage 6: Send email report

    Args:
        skip_fetch:     Skip stages 1–2 (reuse cached YouTube data + transcripts)
        skip_to_slides: Skip stages 1–3 (reuse cached analysis)
    """
    import sys
    import os
    import time
    import json

    # Add project root to path so tool imports work
    sys.path.insert(0, "/root")

    TMP_DIR = "/root/.tmp"
    os.makedirs(TMP_DIR, exist_ok=True)

    STAGE_OUTPUTS = {
        1: f"{TMP_DIR}/youtube_raw.json",
        2: f"{TMP_DIR}/transcripts.json",
        3: f"{TMP_DIR}/analysis.json",
        4: f"{TMP_DIR}/sheets_metadata.json",
        5: f"{TMP_DIR}/slides_metadata.json",
    }

    def run_stage(name, stage_num, func, output_file=None):
        print(f"\n[Stage {stage_num}/6] {name}")
        t0 = time.time()
        try:
            func()
        except Exception as e:
            elapsed = time.time() - t0
            print(f"\n[FAILED] Stage {stage_num} ({name}) after {elapsed:.1f}s: {e}")
            raise
        elapsed = time.time() - t0
        if output_file and not Path(output_file).exists():
            raise RuntimeError(f"Stage {stage_num} completed but output not found: {output_file}")
        print(f"  Done in {elapsed:.1f}s")

    # Validate skip preconditions
    skip_stages = set()
    if skip_fetch or skip_to_slides:
        for stage in [1, 2]:
            if not Path(STAGE_OUTPUTS[stage]).exists():
                raise RuntimeError(
                    f"--skip-fetch requires {STAGE_OUTPUTS[stage]} to exist. "
                    "Run without skip flags first."
                )
        skip_stages.update([1, 2])
        print("  Skipping stages 1–2 (using cached data)")

    if skip_to_slides:
        if not Path(STAGE_OUTPUTS[3]).exists():
            raise RuntimeError(
                f"--skip-to-slides requires {STAGE_OUTPUTS[3]} to exist."
            )
        skip_stages.add(3)
        print("  Skipping stage 3 (using cached analysis)")

    # Lazy imports (after sys.path is set)
    from tools.fetch_youtube_data import main as fetch_yt
    from tools.fetch_transcripts import main as fetch_transcripts
    from tools.analyze_with_openai import main as analyze
    from tools.build_sheets_charts import main as build_sheets
    from tools.build_slides_deck import main as build_slides
    from tools.send_email import main as send_email

    pipeline_start = time.time()

    if 1 not in skip_stages:
        run_stage("Fetching YouTube data", 1, fetch_yt, STAGE_OUTPUTS[1])
    if 2 not in skip_stages:
        run_stage("Fetching transcripts", 2, fetch_transcripts, STAGE_OUTPUTS[2])
    if 3 not in skip_stages:
        run_stage("Analyzing with OpenAI", 3, analyze, STAGE_OUTPUTS[3])

    run_stage("Building Sheets + charts", 4, build_sheets, STAGE_OUTPUTS[4])
    run_stage("Building Slides deck", 5, build_slides, STAGE_OUTPUTS[5])
    run_stage("Sending email", 6, send_email, None)

    # Commit volume writes so they persist after function exits
    volume.commit()

    # Final summary
    total = time.time() - pipeline_start
    slides_meta = json.loads(Path(STAGE_OUTPUTS[5]).read_text())
    analysis = json.loads(Path(STAGE_OUTPUTS[3]).read_text())
    stats = analysis.get("key_stats", {})

    print("\n" + "=" * 50)
    print("PIPELINE COMPLETE (Modal)")
    print("=" * 50)
    print(f"  Videos analyzed:  {stats.get('total_videos_analyzed', '?')}")
    print(f"  Channels tracked: {stats.get('total_channels_analyzed', '?')}")
    print(f"  Slides deck:      {slides_meta['presentation_url']}")
    print(f"  Email sent to:    {os.getenv('RECIPIENT_EMAIL')}")
    print(f"  Total runtime:    {int(total // 60)}m {int(total % 60)}s")
    print("=" * 50)

    return slides_meta["presentation_url"]


# ── Local entrypoint ──────────────────────────────────────────────────────────

@app.local_entrypoint()
def main(skip_fetch: bool = False, skip_to_slides: bool = False):
    """
    Triggers the pipeline on Modal from your local machine.

    Examples:
        modal run modal_app.py
        modal run modal_app.py --skip-fetch
        modal run modal_app.py --skip-to-slides
    """
    url = run_pipeline.remote(skip_fetch=skip_fetch, skip_to_slides=skip_to_slides)
    if url:
        print(f"\nSlides deck: {url}")
