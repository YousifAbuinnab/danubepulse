"""Read-only, repeatable PostgreSQL export of trusted data, not a provider connector."""

import hashlib
import json
import math
from datetime import UTC, datetime
from typing import Any

METRICS = {
    "river": {"water_level_m": "m", "discharge_m3_s": "m3/s"},
    "weather": {
        "temperature_c": "degC",
        "precipitation_mm": "mm",
        "relative_humidity_percent": "%",
        "wind_speed_m_s": "m/s",
    },
    "energy": {
        "generation_mw": "MW",
        "price_eur_mwh": "EUR/MWh",
        "load_forecast_mw": "MW",
    },
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def timestamp(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if parsed.tzinfo is None:
        raise ValueError("Lakehouse export requires timezone-aware timestamps.")
    return parsed.astimezone(UTC).isoformat()


def normalize(domain: str, row: dict[str, Any], provenance: str) -> list[dict[str, Any]]:
    if row["provenance"] != provenance or provenance not in {"real", "synthetic"}:
        raise ValueError("Snapshot provenance mismatch.")
    points = []
    for metric, unit in METRICS[domain].items():
        value = row.get(metric)
        if value is None:
            continue
        if not math.isfinite(float(value)):
            raise ValueError("Trusted snapshot contains a non-finite measurement.")
        if (
            domain == "energy"
            and row.get("metric")
            != {
                "generation_mw": "generation",
                "price_eur_mwh": "price",
                "load_forecast_mw": "load_forecast",
            }[metric]
        ):
            raise ValueError("Energy metric/value mismatch.")
        point = {
            "provenance": provenance,
            "source_id": row["source_id"],
            "domain": domain,
            "location": row["region"] if domain == "energy" else row["station_id"],
            "metric": metric,
            "production_type": row.get("production_type") or "",
            "event_time": timestamp(row["event_time"]),
            "interval_end": timestamp(row.get("interval_end")),
            "duration_hours": row.get("duration_hours") if domain == "energy" else None,
            "value": float(value),
            "unit": unit,
            "source_record_id": row["source_record_id"],
            "run_id": row["run_id"],
            "source_document_id": row.get("document_id"),
            "revision": row.get("revision"),
            "source_document_time": timestamp(row.get("source_published_at")),
            "ingested_at": timestamp(row["ingested_at"]),
            "processed_at": timestamp(row["processed_at"]),
            "bronze_path": row["bronze_path"],
            "bronze_checksum": row["checksum"],
            "attribution": row["source_name"],
            "license": row.get("license"),
            "reference_url": row.get("reference_url"),
        }
        if domain == "energy":
            if not point["interval_end"] or point["duration_hours"] not in {0.25, 0.5, 1.0}:
                raise ValueError("Energy export requires supported interval duration.")
            actual = (
                datetime.fromisoformat(point["interval_end"])
                - datetime.fromisoformat(point["event_time"])
            ).total_seconds() / 3600
            if actual != point["duration_hours"]:
                raise ValueError("Energy interval boundaries and duration disagree.")
        point["key"] = digest(
            {
                name: point[name]
                for name in (
                    "provenance",
                    "source_id",
                    "domain",
                    "location",
                    "metric",
                    "production_type",
                    "event_time",
                    "interval_end",
                )
            }
        )
        point["content_hash"] = digest(point)
        points.append(point)
    return points


def export_snapshot(database_url: str, provenance: str) -> list[dict[str, Any]]:
    import psycopg
    from psycopg.rows import dict_row

    if provenance not in {"real", "synthetic"}:
        raise ValueError("Export requires explicit real or synthetic provenance.")
    records = []
    # This transaction cannot modify application rows, even with owner credentials.
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        for domain in METRICS:
            rows = connection.execute(
                f"""
                SELECT o.*, s.provenance, s.name AS source_name, s.license,
                       s.reference_url, r.bronze_path, r.checksum
                  FROM {domain}_observations o
                  JOIN data_sources s ON s.id=o.source_id
                  JOIN ingestion_runs r ON r.id=o.run_id
                 WHERE o.quality_status='valid' AND s.provenance=%s
                 ORDER BY o.id
                """,
                (provenance,),
            )
            for row in rows:
                records.extend(normalize(domain, row, provenance))
    return sorted(records, key=lambda record: record["key"])
