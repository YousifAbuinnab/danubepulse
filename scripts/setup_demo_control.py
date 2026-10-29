"""Generate a private local token for guarded synthetic-demo actions."""

import secrets
from pathlib import Path

path = Path(".env")
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
for index, line in enumerate(lines):
    if line.startswith("DEMO_CONTROL_TOKEN="):
        existing_token = line.partition("=")[2].strip()
        if existing_token and len(existing_token) >= 32:
            path.chmod(0o600)
            print("A local demo control token is already configured.")
            break
        if existing_token:
            raise SystemExit(
                "Existing DEMO_CONTROL_TOKEN is too short; review it locally before changing it."
            )
        lines[index] = f"DEMO_CONTROL_TOKEN={secrets.token_urlsafe(32)}"
        break
else:
    lines.extend(["", "# Private local demo-action token; never commit this file."])
    lines.append(f"DEMO_CONTROL_TOKEN={secrets.token_urlsafe(32)}")

path.write_text("\n".join(lines) + "\n", encoding="utf-8")
path.chmod(0o600)
print("A private demo control token is configured; its value was not displayed.")
