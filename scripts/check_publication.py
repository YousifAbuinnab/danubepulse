"""Fail closed on private/generated file paths without printing any file contents."""

import argparse
import re
import subprocess
from pathlib import PurePosixPath

PRIVATE_SUFFIXES = (
    ".dump",
    ".backup",
    ".sql.gz",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".parquet",
    ".log",
)
PRIVATE_DIRECTORIES = {
    "backups",
    ".aws",
    ".kube",
    ".terraform",
    ".venv",
    "__pycache__",
    "reports",
}


def forbidden(path: str) -> bool:
    parts = PurePosixPath(path).parts
    name = parts[-1].lower()
    if path == ".env.example":
        return False
    return (
        name == ".env"
        or name.startswith(".env.")
        or name.endswith(PRIVATE_SUFFIXES)
        or ".db-" in name
        or ".tfstate" in name
        or any(part.lower() in PRIVATE_DIRECTORIES for part in parts)
        or (
            path.startswith(("data/bronze/", "data/lakehouse/", "data/energy-imports/"))
            and name != ".gitkeep"
        )
    )


def git(*arguments: str) -> bytes:
    return subprocess.run(
        ["git", *arguments],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def check(history: bool = False) -> list[str]:
    if history:
        paths = []
        for line in git("rev-list", "--objects", "--all").decode().splitlines():
            _object, separator, path = line.partition(" ")
            if separator:
                paths.append(path)
    else:
        paths = git("ls-files", "-z").decode().rstrip("\0").split("\0")
    return sorted({path for path in paths if path and forbidden(path)})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", action="store_true", help="Check all locally fetched refs.")
    args = parser.parse_args()
    if args.history:
        shallow = git("rev-parse", "--is-shallow-repository").decode().strip()
        if shallow != "false":
            raise SystemExit("Full history required. Fetch complete refs before the release gate.")
    blocked = check(args.history)
    scope = "reachable history" if args.history else "tracked tree"
    if blocked:
        print(f"BLOCKED: {len(blocked)} private/generated paths in {scope}:")
        for path in blocked:
            # Escape terminal control characters; never output object contents.
            print("  " + re.sub(r"[\x00-\x1f\x7f]", "?", path))
        raise SystemExit(1)
    print(f"PASS: no prohibited file paths in {scope}. This is not a content/secret scan.")


if __name__ == "__main__":
    main()
