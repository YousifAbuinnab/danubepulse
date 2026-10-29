"""Small deterministic quality checks shared by ingestion and replay."""

from datetime import datetime, timedelta
from itertools import pairwise
from typing import Any

from danubepulse.time_utils import parse_utc, utc_now


def inspect_records(records: list[dict[str, Any]], domain: str) -> list[dict[str, Any]]:
    required = {
        "river": {"record_id", "station_id", "event_time"},
        "weather": {"record_id", "station_id", "event_time"},
        "energy": {"record_id", "region", "event_time"},
    }[domain]
    expected = {
        "river": required | {"water_level_m", "discharge_m3_s"},
        "weather": required | {"temperature_c", "precipitation_mm"},
        "energy": required | {"generation_mw", "price_eur_mwh"},
    }[domain]
    results: list[dict[str, Any]] = []
    invalid = 0
    missing = 0
    impossible = 0
    valid_times: list[datetime] = []
    for row in records:
        if not required.issubset(row):
            invalid += 1
            continue
        measurements = {
            "river": ("water_level_m", "discharge_m3_s"),
            "weather": ("temperature_c", "precipitation_mm"),
            "energy": ("generation_mw", "price_eur_mwh"),
        }[domain]
        missing += sum(row.get(key) is None for key in measurements)
        ranges = {
            "river": {"water_level_m": (0, 20), "discharge_m3_s": (0, 100_000)},
            "weather": {"temperature_c": (-90, 60), "precipitation_mm": (0, 1000)},
            "energy": {"generation_mw": (0, 2_000_000), "price_eur_mwh": (-500, 5000)},
        }[domain]
        for field, bounds in ranges.items():
            value = row.get(field)
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    invalid += 1
                elif not bounds[0] <= value <= bounds[1]:
                    impossible += 1
        try:
            valid_times.append(parse_utc(row["event_time"]))
        except (TypeError, ValueError):
            invalid += 1
    unexpected_fields = sorted(
        {key for row in records for key in row} - expected - {"source_published_at"}
    )
    missing_fields = sorted({key for row in records for key in expected.difference(row)})
    schema_issues = []
    if unexpected_fields:
        schema_issues.append(f"unexpected field(s): {', '.join(unexpected_fields)}")
    if missing_fields:
        schema_issues.append(f"missing field(s): {', '.join(missing_fields)}")
    results.append(
        {
            "check_name": "schema_change",
            "status": "failed" if schema_issues else "passed",
            "details": (
                "; ".join(schema_issues)
                if schema_issues
                else "No unexpected source fields were detected."
            ),
            "affected_count": len(records) if schema_issues else 0,
        }
    )
    results.append(
        {
            "check_name": "mandatory_fields_and_timestamp",
            "status": "failed" if invalid else "passed",
            "details": f"{invalid} record(s) have missing required fields or invalid timestamps.",
            "affected_count": invalid,
        }
    )
    results.append(
        {
            "check_name": "null_measurements",
            "status": "warning" if missing else "passed",
            "details": f"{missing} required measurement value(s) are null.",
            "affected_count": missing,
        }
    )
    results.append(
        {
            "check_name": "types_units_and_ranges",
            "status": "failed" if invalid or impossible else "passed",
            "details": (
                f"{invalid} invalid type/timestamp value(s); "
                f"{impossible} value(s) outside supported bounds."
            ),
            "affected_count": invalid + impossible,
        }
    )
    identity_fields = (
        ("region", "event_time") if domain == "energy" else ("station_id", "event_time")
    )
    keys = [
        tuple(row.get(field) for field in identity_fields)
        for row in records
        if all(row.get(field) for field in identity_fields)
    ]
    duplicates = len(keys) - len(set(keys))
    results.append(
        {
            "check_name": "duplicate_events",
            "status": "warning" if duplicates else "passed",
            "details": f"{duplicates} duplicate event(s) in the source batch.",
            "affected_count": duplicates,
        }
    )
    out_of_order = sum(right < left for left, right in pairwise(valid_times))
    results.append(
        {
            "check_name": "timestamp_ordering",
            "status": "warning" if out_of_order else "passed",
            "details": f"{out_of_order} event(s) are out of source timestamp order.",
            "affected_count": out_of_order,
        }
    )
    unique_times = sorted(set(valid_times))
    gaps = sum((right - left).total_seconds() > 3600 for left, right in pairwise(unique_times))
    results.append(
        {
            "check_name": "hourly_completeness",
            "status": "warning" if gaps else "passed",
            "details": f"{gaps} gap(s) exceed the expected hourly cadence.",
            "affected_count": gaps,
        }
    )
    now = utc_now()
    late = sum(event_time < now - timedelta(hours=72) for event_time in valid_times)
    results.append(
        {
            "check_name": "late_arrival",
            "status": "warning" if late else "passed",
            "details": f"{late} event(s) are older than the 72-hour freshness threshold.",
            "affected_count": late,
        }
    )
    return results


def row_is_valid(row: dict[str, Any], domain: str) -> bool:
    """Keep malformed measurements in Silver for audit, but exclude them from Gold."""
    measurement_fields = {
        "river": {
            "water_level_m": (0, 20),
            "discharge_m3_s": (0, 100_000),
        },
        "weather": {"temperature_c": (-90, 60), "precipitation_mm": (0, 1000)},
        "energy": {"generation_mw": (0, 2_000_000), "price_eur_mwh": (-500, 5000)},
    }[domain]
    required = {"record_id", "event_time"} | ({"station_id"} if domain != "energy" else {"region"})
    if not required.issubset(row):
        return False
    try:
        parse_utc(row["event_time"])
    except (TypeError, ValueError):
        return False
    for field, (minimum, maximum) in measurement_fields.items():
        value = row.get(field)
        if value is None:
            return False
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        if not minimum <= value <= maximum:
            return False
    return True
