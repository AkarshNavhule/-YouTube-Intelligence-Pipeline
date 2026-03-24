"""
Pre-deployment security audit for the YouTube AI Intelligence Pipeline.

Runs a series of checks before `modal deploy`. Exits with code 1 if any
check fails so it can be used as a gate in deployment scripts.

Usage:
    python security_check.py
"""

import os
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent

# ── Terminal colors ───────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def _pass(msg): print(f"  {GREEN}[PASS]{RESET} {msg}")
def _warn(msg): print(f"  {YELLOW}[WARN]{RESET} {msg}")
def _fail(msg): print(f"  {RED}[FAIL]{RESET} {msg}")


# ── Check A: Hardcoded secret scan in Python source files ────────────────────

SECRET_PATTERNS = [
    (r"AIza[0-9A-Za-z\-_]{35}",            "Google API key"),
    (r"sk-proj-[A-Za-z0-9\-_]{20,}",       "OpenAI project key"),
    (r"sk-[A-Za-z0-9]{20,}",               "OpenAI API key"),
    (r"ya29\.[0-9A-Za-z\-_]{20,}",         "Google OAuth bearer token"),
    (r"[A-Za-z_]+_KEY\s*=\s*[\"'][A-Za-z0-9\-_]{20,}[\"']", "hardcoded key assignment"),
]

SCAN_EXTENSIONS = {".py"}
SKIP_DIRS = {".tmp", "__pycache__", ".git", ".claude", "node_modules"}


def check_secrets_in_source() -> bool:
    print(f"\n{BOLD}A. Hardcoded secret scan (Python source files){RESET}")
    findings = []

    for path in PROJECT_ROOT.rglob("*"):
        if path.suffix not in SCAN_EXTENSIONS:
            continue
        if any(skip in path.parts for skip in SKIP_DIRS):
            continue
        if path.name == "security_check.py":
            continue  # skip ourselves

        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        for pattern, label in SECRET_PATTERNS:
            for match in re.finditer(pattern, text):
                line_num = text[: match.start()].count("\n") + 1
                findings.append((path.relative_to(PROJECT_ROOT), line_num, label, match.group()[:24] + "…"))

    if findings:
        for fpath, lineno, label, snippet in findings:
            _fail(f"{fpath}:{lineno} — {label} detected: {snippet}")
        return False
    else:
        _pass("No hardcoded secrets found in Python source files")
        return True


# ── Check B: .gitignore coverage ─────────────────────────────────────────────

GITIGNORE_REQUIRED = [".env", "credentials.json", "token.json", ".tmp/"]


def check_gitignore() -> bool:
    print(f"\n{BOLD}B. .gitignore coverage{RESET}")
    gitignore_path = PROJECT_ROOT / ".gitignore"

    if not gitignore_path.exists():
        _fail(".gitignore not found — sensitive files may be committed")
        return False

    content = gitignore_path.read_text(encoding="utf-8")
    ok = True
    for entry in GITIGNORE_REQUIRED:
        if entry in content:
            _pass(f"{entry!r} is gitignored")
        else:
            _fail(f"{entry!r} is NOT in .gitignore — add it immediately")
            ok = False
    return ok


# ── Check C: Required local credential files exist ───────────────────────────

def check_credential_files() -> bool:
    print(f"\n{BOLD}C. Local credential files{RESET}")
    ok = True

    creds = PROJECT_ROOT / "credentials.json"
    token = PROJECT_ROOT / "token.json"

    if creds.exists():
        _pass("credentials.json exists")
    else:
        _fail(
            "credentials.json not found.\n"
            "         Go to console.cloud.google.com → OAuth 2.0 Client IDs\n"
            "         → Download as credentials.json and place in project root"
        )
        ok = False

    if token.exists():
        _pass("token.json exists")
    else:
        _fail(
            "token.json not found.\n"
            "         Run this locally first to complete OAuth flow:\n"
            "           python tools/google_auth.py"
        )
        ok = False

    return ok


# ── Check D: .env variable completeness ──────────────────────────────────────

REQUIRED_ENV_VARS = [
    "YOUTUBE_API_KEY",
    "OPENAI_API_KEY",
    "RECIPIENT_EMAIL",
    "SEARCH_KEYWORDS",
    "SEARCH_RESULTS_PER_KEYWORD",
    "DAYS_BACK",
]
PLACEHOLDER_PATTERNS = ["your_", "_here", "xxx", "changeme", "todo"]


def check_env_file() -> bool:
    print(f"\n{BOLD}D. .env variable completeness{RESET}")
    env_path = PROJECT_ROOT / ".env"

    if not env_path.exists():
        _fail(".env file not found")
        return False

    # Parse .env manually (avoid loading into os.environ)
    env_vars: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env_vars[key.strip()] = val.strip().strip('"').strip("'")

    ok = True
    for var in REQUIRED_ENV_VARS:
        val = env_vars.get(var, "")
        if not val:
            _fail(f"{var} is missing from .env")
            ok = False
        elif any(p in val.lower() for p in PLACEHOLDER_PATTERNS):
            _fail(f"{var} still has a placeholder value: {val[:30]!r}")
            ok = False
        else:
            _pass(f"{var} is set")

    return ok


# ── Check E: Modal CLI reachability and secret existence ─────────────────────

MODAL_SECRET_NAME = "yt-pipeline-secrets"


def check_modal() -> bool:
    print(f"\n{BOLD}E. Modal CLI and secrets{RESET}")
    ok = True

    # Is modal installed?
    try:
        import modal  # noqa: F401
        _pass("modal package is installed")
    except ImportError:
        _fail("modal not installed. Run: pip install modal")
        return False

    # Is modal authenticated?
    try:
        result = subprocess.run(
            ["modal", "token", "list"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            _pass("Modal CLI is authenticated")
        else:
            _fail(
                "Modal CLI is not authenticated.\n"
                "         Run: python3 -m modal setup"
            )
            ok = False
    except FileNotFoundError:
        _fail("modal CLI not found in PATH. Run: pip install modal")
        return False
    except subprocess.TimeoutExpired:
        _warn("Modal CLI check timed out — skipping auth check")

    # Does the secret exist?
    try:
        result = subprocess.run(
            ["modal", "secret", "list"],
            capture_output=True, text=True, timeout=15
        )
        if MODAL_SECRET_NAME in result.stdout:
            _pass(f"Modal secret '{MODAL_SECRET_NAME}' exists")
        else:
            _warn(
                f"Modal secret '{MODAL_SECRET_NAME}' not found.\n"
                "         Create it with:\n\n"
                f"           modal secret create {MODAL_SECRET_NAME} \\\n"
                "             YOUTUBE_API_KEY=\"...\" \\\n"
                "             OPENAI_API_KEY=\"...\" \\\n"
                "             RECIPIENT_EMAIL=\"...\" \\\n"
                "             SEARCH_KEYWORDS=\"...\" \\\n"
                "             SEARCH_RESULTS_PER_KEYWORD=\"10\" \\\n"
                "             DAYS_BACK=\"30\" \\\n"
                "             GOOGLE_CREDENTIALS_JSON=\"$(python -c 'import base64,pathlib; "
                "print(base64.b64encode(pathlib.Path(\\\"credentials.json\\\").read_bytes()).decode())')\" \\\n"
                "             GOOGLE_TOKEN_JSON=\"$(python -c 'import base64,pathlib; "
                "print(base64.b64encode(pathlib.Path(\\\"token.json\\\").read_bytes()).decode())')\"\n"
            )
            # Secret missing is a WARN (not FAIL) — user might be creating it next
    except subprocess.TimeoutExpired:
        _warn("Modal secret list check timed out")

    return ok


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{BOLD}{'=' * 52}{RESET}")
    print(f"{BOLD}  Security Pre-Deployment Check{RESET}")
    print(f"{BOLD}{'=' * 52}{RESET}")

    results = {
        "A: Secret scan":       check_secrets_in_source(),
        "B: Gitignore":         check_gitignore(),
        "C: Credential files":  check_credential_files(),
        "D: .env completeness": check_env_file(),
        "E: Modal CLI":         check_modal(),
    }

    print(f"\n{BOLD}{'─' * 52}{RESET}")
    print(f"{BOLD}  Summary{RESET}")
    print(f"{BOLD}{'─' * 52}{RESET}")

    all_passed = True
    for name, passed in results.items():
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  {name:<30} {status}")
        if not passed:
            all_passed = False

    print(f"{BOLD}{'─' * 52}{RESET}")

    if all_passed:
        print(f"\n{GREEN}{BOLD}All checks passed. Safe to deploy:{RESET}")
        print("  modal deploy modal_app.py\n")
        sys.exit(0)
    else:
        print(f"\n{RED}{BOLD}One or more checks failed. Fix the issues above before deploying.{RESET}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
