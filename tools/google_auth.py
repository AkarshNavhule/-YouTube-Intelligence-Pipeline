"""
Shared Google OAuth 2.0 helper for all Google API tools.
Handles token creation, refresh, and service instantiation.

Usage:
    from tools.google_auth import get_google_service
    service = get_google_service('sheets', 'v4')
"""

import base64
import os
import tempfile
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# All scopes needed across all tools - requested together so one token covers everything
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/gmail.send",
]

PROJECT_ROOT = Path(__file__).parent.parent
CREDENTIALS_FILE = PROJECT_ROOT / "credentials.json"
TOKEN_FILE = PROJECT_ROOT / "token.json"


def _resolve_credential_paths() -> tuple[Path, Path]:
    """
    Returns (credentials_path, token_path) for the current environment.

    In Modal: decodes GOOGLE_CREDENTIALS_JSON and GOOGLE_TOKEN_JSON env vars
    (base64-encoded file contents) into temp files — no browser required.
    Locally: uses credentials.json and token.json from the project root.
    """
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    token_b64 = os.environ.get("GOOGLE_TOKEN_JSON")

    if creds_b64 and token_b64:
        # Running in Modal (or any environment with pre-encoded credentials)
        tmp_dir = Path(tempfile.mkdtemp())
        creds_path = tmp_dir / "credentials.json"
        token_path = tmp_dir / "token.json"
        creds_path.write_bytes(base64.b64decode(creds_b64))
        token_path.write_bytes(base64.b64decode(token_b64))
        return creds_path, token_path

    # Local development: use project-root files
    return CREDENTIALS_FILE, TOKEN_FILE


def get_google_service(api_name: str, version: str):
    """
    Returns an authenticated Google API service object.

    Local: opens browser for OAuth consent on first run, saves token.json.
    Modal: loads pre-generated credentials from GOOGLE_CREDENTIALS_JSON /
           GOOGLE_TOKEN_JSON environment variables (base64-encoded).

    Args:
        api_name: e.g. 'sheets', 'slides', 'gmail'
        version:  e.g. 'v4', 'v1'

    Returns:
        Authenticated googleapiclient service object
    """
    credentials_file, token_file = _resolve_credential_paths()

    if not credentials_file.exists():
        raise FileNotFoundError(
            f"credentials.json not found at {credentials_file}\n"
            "Please follow the Google Cloud setup steps:\n"
            "1. Go to console.cloud.google.com\n"
            "2. Enable: YouTube Data API v3, Google Sheets, Google Slides, Gmail APIs\n"
            "3. Create OAuth 2.0 Client ID (Desktop app type)\n"
            "4. Download as credentials.json and place in project root"
        )

    creds = None

    if token_file.exists():
        creds = Credentials.from_authorized_user_file(str(token_file), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_file, "w") as token:
            token.write(creds.to_json())

    return build(api_name, version, credentials=creds)
