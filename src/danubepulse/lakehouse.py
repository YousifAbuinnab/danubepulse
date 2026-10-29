"""Read verified lakehouse manifests without importing Spark or mutating tables."""

import json
from pathlib import Path
from typing import Any


class InvalidManifest(ValueError):
    pass


def _read(path: Path) -> dict[str, Any] | None:
    try:
        content = path.read_text()
    except FileNotFoundError:
        return None
    try:
        result = json.loads(content)
    except json.JSONDecodeError as error:
        raise InvalidManifest(f"Invalid lakehouse JSON in {path.name}.") from error
    if not isinstance(result, dict) or result.get("status") not in {"running", "success", "failed"}:
        raise InvalidManifest(f"Invalid lakehouse status in {path.name}.")
    return result


def _validate_success(report: dict[str, Any], scope: str) -> None:
    required = ("id", "finished_at", "provenance")
    if report["status"] != "success" or any(
        not isinstance(report.get(key), str) for key in required
    ):
        raise InvalidManifest("Published lakehouse generation is incomplete or unverified.")
    if scope == "synthetic-scale":
        if not isinstance(report.get("results"), list) or not isinstance(report.get("note"), str):
            raise InvalidManifest("Synthetic scale results are incomplete.")
    else:
        tables = report.get("tables")
        if (
            not isinstance(report.get("silver_active_rows"), int)
            or not isinstance(report.get("gold_preview"), list)
            or not isinstance(report.get("sources"), list)
            or not isinstance(tables, dict)
            or set(tables) != {"bronze", "silver", "gold"}
        ):
            raise InvalidManifest("Lakehouse table summary is incomplete.")
        for detail in tables.values():
            if (
                not isinstance(detail, dict)
                or any(
                    not isinstance(detail.get(key), int)
                    for key in ("rows", "files", "size_bytes", "version")
                )
                or not isinstance(detail.get("history"), list)
            ):
                raise InvalidManifest("Lakehouse table statistics are incomplete.")
    expected = {"real": "real", "demo": "synthetic", "synthetic-scale": "synthetic-scale-test"}
    if report["provenance"] != expected[scope]:
        raise InvalidManifest("Lakehouse manifest provenance mismatch.")


def lakehouse_status(root: Path) -> dict[str, Any]:
    scopes = {}
    for scope in ("real", "demo", "synthetic-scale"):
        success = _read(root / scope / "status.json")
        latest = _read(root / scope / "latest_attempt.json")
        if success is not None:
            _validate_success(success, scope)
        if latest and latest["status"] == "failed" and not isinstance(latest.get("error"), str):
            raise InvalidManifest("Failed lakehouse attempt has no error details.")
        scopes[scope] = {
            "status": latest["status"] if latest else "not_processed",
            "last_success": success,
            "latest_attempt": latest,
        }
    return {
        "mode": "optional-manual-local[2]",
        "scopes": scopes,
        "note": "Version-pinned last verified manifests, not live Delta reads. "
        "PostgreSQL remains authoritative; no ingestion is triggered here.",
    }
