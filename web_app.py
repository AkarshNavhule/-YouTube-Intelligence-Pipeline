"""
Web interface for the YouTube AI Intelligence Pipeline.

Usage:
    python web_app.py
    Then open http://localhost:5000
"""

import json
import os
import queue
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values, set_key
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

# Google auth imports (optional — only used for /auth/google route)
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    _GOOGLE_LIBS = True
except ImportError:
    _GOOGLE_LIBS = False

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/gmail.send",
]

BASE_DIR = Path(__file__).parent
ENV_FILE = BASE_DIR / ".env"
TMP_DIR = BASE_DIR / ".tmp"

app = Flask(__name__)

# ── Pipeline state ─────────────────────────────────────────────────────────────
_lock = threading.Lock()
_running = False
_subscribers: list[queue.Queue] = []


def _broadcast(msg: dict) -> None:
    data = json.dumps(msg)
    for q in list(_subscribers):
        try:
            q.put_nowait(data)
        except queue.Full:
            pass


def _run_pipeline(skip_fetch: bool, skip_to_slides: bool) -> None:
    global _running
    cmd = [sys.executable, "-u", str(BASE_DIR / "run_pipeline.py")]
    if skip_fetch:
        cmd.append("--skip-fetch")
    if skip_to_slides:
        cmd.append("--skip-to-slides")

    _broadcast({"type": "start"})
    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(BASE_DIR),
            bufsize=1,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        for line in proc.stdout:
            _broadcast({"type": "log", "data": line.rstrip()})
        proc.wait()
        _broadcast({"type": "done", "returncode": proc.returncode})
    except Exception as exc:
        _broadcast({"type": "error", "data": str(exc)})
        _broadcast({"type": "done", "returncode": 1})
    finally:
        with _lock:
            _running = False


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    # Pre-fill form with current .env values (non-sensitive only)
    vals = dotenv_values(ENV_FILE) if ENV_FILE.exists() else {}
    prefill = {
        "RECIPIENT_EMAIL":            vals.get("RECIPIENT_EMAIL", ""),
        "SEARCH_KEYWORDS":            vals.get("SEARCH_KEYWORDS", ""),
        "SEARCH_RESULTS_PER_KEYWORD": vals.get("SEARCH_RESULTS_PER_KEYWORD", "10"),
        "DAYS_BACK":                  vals.get("DAYS_BACK", "30"),
    }
    return render_template("index.html", prefill=prefill)


@app.route("/api/run", methods=["POST"])
def api_run():
    global _running
    with _lock:
        if _running:
            return jsonify({"error": "Pipeline already running"}), 409
        _running = True

    body = request.json or {}

    # Save user-provided fields to .env before running
    user_fields = {
        "RECIPIENT_EMAIL":            body.get("email", ""),
        "SEARCH_KEYWORDS":            body.get("keywords", ""),
        "SEARCH_RESULTS_PER_KEYWORD": str(body.get("results_per_keyword", "10")),
        "DAYS_BACK":                  str(body.get("days_back", "30")),
    }
    for k, v in user_fields.items():
        if v:
            set_key(str(ENV_FILE), k, v)

    t = threading.Thread(
        target=_run_pipeline,
        args=(body.get("skip_fetch", False), body.get("skip_to_slides", False)),
        daemon=True,
    )
    t.start()
    return jsonify({"ok": True})


@app.route("/api/stream")
def api_stream():
    q: queue.Queue = queue.Queue(maxsize=500)
    _subscribers.append(q)

    def generate():
        try:
            while True:
                try:
                    data = q.get(timeout=25)
                    yield f"data: {data}\n\n"
                    if json.loads(data).get("type") == "done":
                        break
                except queue.Empty:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            if q in _subscribers:
                _subscribers.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/status")
def api_status():
    return jsonify({"running": _running})


@app.route("/api/google-status")
def api_google_status():
    """Return whether Google credentials are present and valid."""
    token_file = BASE_DIR / "token.json"
    creds_file = BASE_DIR / "credentials.json"

    if not creds_file.exists():
        return jsonify({"status": "no_credentials",
                        "message": "credentials.json not found"})
    if not token_file.exists():
        return jsonify({"status": "not_authenticated",
                        "message": "Not authenticated — click Connect Google"})
    if not _GOOGLE_LIBS:
        return jsonify({"status": "unknown", "message": "google-auth not installed"})

    try:
        creds = Credentials.from_authorized_user_file(str(token_file), GOOGLE_SCOPES)
        if creds.valid:
            return jsonify({"status": "ok", "message": "Google authenticated"})
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_file.write_text(creds.to_json())
            return jsonify({"status": "ok", "message": "Google authenticated (refreshed)"})
        return jsonify({"status": "expired",
                        "message": "Token expired — click Connect Google"})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)})


@app.route("/api/google-auth", methods=["POST"])
def api_google_auth():
    """Delete stale token and run the OAuth browser flow to get a fresh one."""
    if not _GOOGLE_LIBS:
        return jsonify({"error": "google-auth-oauthlib not installed"}), 500

    creds_file = BASE_DIR / "credentials.json"
    token_file = BASE_DIR / "token.json"

    if not creds_file.exists():
        return jsonify({"error": "credentials.json not found in project root"}), 400

    # Remove stale token so flow starts fresh
    if token_file.exists():
        token_file.unlink()

    try:
        flow = InstalledAppFlow.from_client_secrets_file(str(creds_file), GOOGLE_SCOPES)
        # Opens the system browser; blocks until the user completes auth
        creds = flow.run_local_server(port=0)
        token_file.write_text(creds.to_json())
        return jsonify({"ok": True, "message": "Google authenticated successfully!"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/results")
def api_results():
    out: dict = {}
    for key, fname in [
        ("analysis", "analysis.json"),
        ("slides", "slides_metadata.json"),
        ("sheets", "sheets_metadata.json"),
    ]:
        f = TMP_DIR / fname
        if f.exists():
            out[key] = json.loads(f.read_text(encoding="utf-8"))
    if not out:
        return jsonify({"error": "No results yet."}), 404
    return jsonify(out)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TMP_DIR.mkdir(exist_ok=True)
    print("\n  YouTube AI Intelligence Pipeline — Web Interface")
    print("  Open  http://localhost:5000  in your browser\n")
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)
