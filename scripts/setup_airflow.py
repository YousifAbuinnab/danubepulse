"""Append missing local Airflow secrets without printing or overwriting existing values."""

import base64
import os
import secrets
from pathlib import Path

path = Path(".env")
existing = path.read_text() if path.exists() else ""
keys = {
    line.split("=", 1)[0].strip()
    for line in existing.splitlines()
    if "=" in line and not line.lstrip().startswith("#")
}
values = {
    "AIRFLOW_DB_PASSWORD": secrets.token_urlsafe(32),
    "AIRFLOW_JWT_SECRET": secrets.token_urlsafe(48),
    "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
}
missing = {key: value for key, value in values.items() if key not in keys}
if missing:
    with path.open("a", encoding="utf-8") as stream:
        stream.write("\n# Local Airflow secrets; never commit this file.\n")
        for key, value in missing.items():
            stream.write(f"{key}={value}\n")
    path.chmod(0o600)
print("Local Airflow secrets are configured. Existing settings were preserved.")
