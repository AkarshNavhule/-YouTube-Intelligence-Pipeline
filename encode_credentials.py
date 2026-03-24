"""
Prints base64-encoded Google credential files for use in Modal secrets.
Run this once, then copy the output into the modal secret create command.
"""
import base64
from pathlib import Path

root = Path(__file__).parent

for fname in ["credentials.json", "token.json"]:
    path = root / fname
    if not path.exists():
        print(f"ERROR: {fname} not found")
        continue
    encoded = base64.b64encode(path.read_bytes()).decode()
    key = fname.replace(".", "_").replace("-", "_").upper()
    if fname == "credentials.json":
        key = "GOOGLE_CREDENTIALS_JSON"
    elif fname == "token.json":
        key = "GOOGLE_TOKEN_JSON"
    print(f"\n{key}=")
    print(encoded)
