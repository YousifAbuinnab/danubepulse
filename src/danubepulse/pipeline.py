"""Bronze ingestion, validation, standardized loading, and incident replay."""

import hashlib
import json
import logging
import math
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from math import isclose
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from danubepulse.ingestion_lock import serialized_ingestion
from danubepulse.models import (
    DataLineage,
    DataSource,
    Incident,
    IncidentEvent,
    IngestionRun,
    QualityResult,
    QuarantinedRecord,
    ReplayHistory,
    RiverObservation,
    Station,
    WeatherObservation,
)
from danubepulse.quality import inspect_records, row_is_valid
from danubepulse.sources import (
    DEFAULT_DANUBE_STATIONS,
    OPEN_METEO_VARIABLES,
    AustrianHydrographyConnector,
    OpenMeteoArchiveConnector,
    SourceBatch,
    SourceUnavailable,
)
from danubepulse.time_utils import parse_utc, utc_now

logger = logging.getLogger(__name__)
FIXTURES = Path(
    os.environ.get("FIXTURE_DIR", Path(__file__).resolve().parents[2] / "data" / "fixtures")
)
SOURCE_DETAILS = {
    "synthetic-river": ("Synthetic demo river", "synthetic", "CC0-1.0"),
    "synthetic-weather": ("Synthetic demo weather", "synthetic", "CC0-1.0"),
    "synthetic-schema": ("Synthetic schema-drift scenario", "synthetic", "CC0-1.0"),
    "synthetic-duplicate": ("Synthetic duplicate incident", "synthetic", "CC0-1.0"),
    "synthetic-missing-late": ("Synthetic missing and late incident", "synthetic", "CC0-1.0"),
}
STATIONS = {
    "AT-DANUBE-KREMS": ("Synthetic Danube reference station", "river"),
    "AT-WEATHER-KREMS": ("Synthetic Krems weather grid", "weather"),
    "AT-DANUBE-DUPLICATE": ("Synthetic duplicate scenario station", "river"),
}


def _canonical_bytes(records: list[dict[str, Any]]) -> bytes:
    return json.dumps(records, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _ensure_source(session: Session, source_id: str) -> DataSource:
    name, provenance, license_name = SOURCE_DETAILS[source_id]
    source = session.get(DataSource, source_id)
    if source is None:
        source = DataSource(
            id=source_id,
            name=name,
            provenance=provenance,
            license=license_name,
            reference_url="data/fixtures/README.md",
        )
        session.add(source)
        session.flush()
    return source


def _ensure_station(session: Session, station_id: str, source_id: str) -> Station:
    station = session.get(Station, station_id)
    if station is None:
        name, kind = STATIONS[station_id]
        station = Station(
            id=station_id,
            source_id=source_id,
            name=name,
            kind=kind,
            latitude=48.41,
            longitude=15.60,
        )
        session.add(station)
        session.flush()
    return station


def _daily_metrics(session: Session, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Read the Gold-equivalent Silver projection for affected station-days."""
    slices: list[dict[str, Any]] = []
    for key in sorted(
        {
            (row["station_id"], parse_utc(row["event_time"]).date().isoformat())
            for row in records
            if row.get("station_id") and row.get("event_time")
        }
    ):
        station_id, day = key
        start = datetime.fromisoformat(day).replace(tzinfo=UTC)
        end = start + timedelta(days=1)
        row = session.execute(
            select(
                func.count(RiverObservation.id),
                func.avg(RiverObservation.water_level_m),
                func.avg(RiverObservation.discharge_m3_s),
            ).where(
                RiverObservation.station_id == station_id,
                RiverObservation.event_time >= start,
                RiverObservation.event_time < end,
                RiverObservation.quality_status == "valid",
            )
        ).one()
        slices.append(
            {
                "station_id": station_id,
                "date_utc": day,
                "observation_count": row[0],
                "average_water_level_m": row[1],
                "average_discharge_m3_s": row[2],
            }
        )
    return {"daily": slices}


def _verify_persisted_recovery(
    session: Session,
    run: IngestionRun,
    source_records: list[dict[str, Any]],
    expected_records: list[dict[str, Any]],
    after_metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    """Verify SQL rows and the real Gold view, not the repaired in-memory payload."""
    expected = {row["record_id"]: row for row in expected_records}
    record_ids = {row["record_id"] for row in source_records} | expected.keys()
    persisted = {
        row["source_record_id"]: row
        for row in session.execute(
            select(
                RiverObservation.source_record_id,
                RiverObservation.station_id,
                RiverObservation.event_time,
                RiverObservation.quality_status,
                RiverObservation.water_level_m,
                RiverObservation.discharge_m3_s,
            ).where(
                RiverObservation.source_id == run.source_id,
                RiverObservation.source_record_id.in_(record_ids),
            )
        ).mappings()
    }
    if set(persisted) != record_ids:
        raise ValueError("Persisted recovery verification failed: source records are missing.")
    for record_id, row in persisted.items():
        corrected = expected.get(record_id)
        if corrected is None:
            if row["quality_status"] != "duplicate":
                raise ValueError(
                    "Persisted recovery verification failed: duplicate is still valid."
                )
            continue
        if (
            row["quality_status"] != "valid"
            or row["station_id"] != corrected["station_id"]
            or parse_utc(row["event_time"]) != parse_utc(corrected["event_time"])
            or any(
                row[field] is None
                or not isclose(row[field], corrected[field], rel_tol=1e-9, abs_tol=1e-9)
                for field in ("water_level_m", "discharge_m3_s")
            )
        ):
            raise ValueError(f"Persisted recovery verification failed for {record_id}.")

    for metrics in after_metrics["daily"]:
        gold = (
            session.execute(
                text(
                    "SELECT observation_count, average_primary, average_secondary "
                    "FROM gold_daily_observations "
                    "WHERE domain = 'river' AND location = :station_id "
                    "AND observation_date = :day"
                ),
                {"station_id": metrics["station_id"], "day": metrics["date_utc"]},
            )
            .mappings()
            .one_or_none()
        )
        if (
            gold is None
            or gold["observation_count"] != metrics["observation_count"]
            or any(
                gold[column] is None
                or metrics[field] is None
                or not isclose(gold[column], metrics[field], rel_tol=1e-9, abs_tol=1e-9)
                for column, field in (
                    ("average_primary", "average_water_level_m"),
                    ("average_secondary", "average_discharge_m3_s"),
                )
            )
        ):
            raise ValueError("Downstream Gold verification failed: aggregate differs from Silver.")
    return [
        {
            "check_name": "persisted_recovery",
            "status": "passed",
            "details": "SQL rows match the corrected records; redundant records are excluded.",
            "affected_count": 0,
        },
        {
            "check_name": "downstream_gold",
            "status": "passed",
            "details": "The Gold SQL view matches the corrected persisted Silver aggregates.",
            "affected_count": 0,
        },
    ]


def _add_event(
    session: Session,
    incident: Incident,
    event_type: str,
    from_status: str | None,
    to_status: str | None,
    details: dict[str, Any],
) -> None:
    session.add(
        IncidentEvent(
            incident_id=incident.id,
            event_type=event_type,
            from_status=from_status,
            to_status=to_status,
            details=details,
            created_at=utc_now(),
        )
    )


def _fixture(filename: str) -> tuple[dict[str, Any], bytes]:
    payload = json.loads((FIXTURES / filename).read_text(encoding="utf-8"))
    return payload, _canonical_bytes(payload["records"])


def ingest_records(
    session: Session,
    bronze_dir: Path,
    *,
    source_id: str,
    domain: str,
    records: list[dict[str, Any]],
    raw_payload: dict[str, Any] | None = None,
    scenario: str | None = None,
    incident_category: str | None = None,
) -> IngestionRun:
    """Persist an immutable raw snapshot and idempotently load valid observations."""
    _ensure_source(session, source_id)
    now = utc_now()
    run_id = str(uuid.uuid4())
    checksum = hashlib.sha256(_canonical_bytes(records)).hexdigest()
    source_dir = bronze_dir / source_id
    source_dir.mkdir(parents=True, exist_ok=True)
    bronze_path = source_dir / f"{run_id}.json"
    envelope = {
        "source_id": source_id,
        "provenance": "synthetic" if source_id.startswith("synthetic-") else "live",
        "retrieved_at": now.isoformat(),
        "source_published_at": (raw_payload or {}).get("source_published_at"),
        "run_id": run_id,
        "checksum_sha256": checksum,
        "records": records,
    }
    if raw_payload and raw_payload.get("recovery_records"):
        envelope["recovery_records"] = raw_payload["recovery_records"]
    bronze_path.write_text(json.dumps(envelope, indent=2, sort_keys=True), encoding="utf-8")
    run = IngestionRun(
        id=run_id,
        source_id=source_id,
        started_at=now,
        status="processing",
        record_count=len(records),
        checksum=checksum,
        bronze_path=str(bronze_path),
        source_published_at=parse_utc(raw_payload["source_published_at"])
        if raw_payload and raw_payload.get("source_published_at")
        else None,
    )
    session.add(run)
    session.flush()
    checks = inspect_records(records, domain)
    invalid_schema = any(
        item["check_name"] == "schema_change" and item["status"] == "failed" for item in checks
    )
    category = "schema_change" if invalid_schema else incident_category
    incident: Incident | None = None
    if category:
        affected_records = [
            {
                "source_record_id": row.get("record_id"),
                "event_time": row.get("event_time"),
                "station_id": row.get("station_id"),
            }
            for row in records
        ]
        incident = Incident(
            id=str(uuid.uuid4()),
            run_id=run_id,
            category=category,
            scenario=scenario or category,
            severity="high" if category == "schema_change" else "medium",
            status="detected",
            title={
                "schema_change": "Unexpected river source schema",
                "duplicate_records": ("Duplicate observations may inflate daily analytics"),
                "missing_late": "Hourly coverage is incomplete and records arrived late",
            }.get(category, f"Detected {category} incident"),
            details={
                "schema_change": (
                    "Unexpected fields are retained in Bronze and quarantined from Silver."
                ),
                "duplicate_records": ("Multiple source records share a station and event time."),
                "missing_late": "Hourly measurement missing; delivered records are late.",
            }.get(category, f"Incident detected for {category}."),
            downstream_impact={
                "datasets": ["river_observations", "gold_daily_observations"],
                "reason": (
                    "Affected station-day analytics require verification before trust is restored."
                ),
            },
            detected_at=now,
            affected_records=affected_records,
        )
        session.add(incident)
        session.flush()
        _add_event(
            session,
            incident,
            "detected",
            None,
            "detected",
            {"run_id": run_id, "source_record_ids": [row.get("record_id") for row in records]},
        )
    for check in checks:
        session.add(
            QualityResult(
                run_id=run_id,
                check_name=check["check_name"],
                status=check["status"],
                details=check["details"],
                affected_count=check["affected_count"],
                checked_at=now,
                phase="detection",
                incident_id=incident.id if incident else None,
            )
        )
    if invalid_schema:
        assert incident is not None
        incident.before_metrics = _daily_metrics(session, records)
        reason = next(item["details"] for item in checks if item["check_name"] == "schema_change")
        for row in records:
            session.add(
                QuarantinedRecord(
                    incident_id=incident.id,
                    source_record_id=str(row.get("record_id", "unknown")),
                    raw_record=row,
                    reason=reason,
                    status="quarantined",
                    quarantined_at=now,
                )
            )
        run.status = "failed"
    else:
        inserted = 0
        seen: set[tuple[str, str]] = set()
        unique_records: list[dict[str, Any]] = []
        for row in records:
            identity = (str(row.get("record_id")), str(row.get("event_time")))
            if identity not in seen:
                seen.add(identity)
                unique_records.append(row)
        for row in unique_records:
            if not {"record_id", "event_time"}.issubset(row):
                continue
            event_time = parse_utc(row["event_time"])
            station_id = row.get("station_id")
            if domain == "river":
                if not station_id:
                    continue
                _ensure_station(session, station_id, source_id)
                existing = session.scalar(
                    select(RiverObservation).where(
                        RiverObservation.source_id == source_id,
                        RiverObservation.source_record_id == row["record_id"],
                    )
                )
                if existing is not None:
                    continue
                observation = RiverObservation(
                    source_id=source_id,
                    run_id=run_id,
                    station_id=station_id,
                    source_record_id=row["record_id"],
                    event_time=event_time,
                    source_published_at=parse_utc(row["source_published_at"])
                    if row.get("source_published_at")
                    else None,
                    ingested_at=now,
                    processed_at=utc_now(),
                    water_level_m=row.get("water_level_m"),
                    discharge_m3_s=row.get("discharge_m3_s"),
                    quality_status="valid" if row_is_valid(row, domain) else "invalid",
                )
                session.add(observation)
                session.flush()
                session.add(
                    DataLineage(
                        run_id=run_id,
                        source_record_id=row["record_id"],
                        target_table="river_observations",
                        target_record_id=str(observation.id),
                    )
                )
                inserted += 1
            elif domain == "weather":
                if not station_id:
                    continue
                _ensure_station(session, station_id, source_id)
                existing = session.scalar(
                    select(WeatherObservation).where(
                        WeatherObservation.source_id == source_id,
                        WeatherObservation.source_record_id == row["record_id"],
                    )
                )
                if existing is not None:
                    continue
                observation = WeatherObservation(
                    source_id=source_id,
                    run_id=run_id,
                    station_id=station_id,
                    source_record_id=row["record_id"],
                    event_time=event_time,
                    source_published_at=parse_utc(row["source_published_at"])
                    if row.get("source_published_at")
                    else None,
                    ingested_at=now,
                    processed_at=utc_now(),
                    temperature_c=row.get("temperature_c"),
                    precipitation_mm=row.get("precipitation_mm"),
                    quality_status="valid" if row_is_valid(row, domain) else "invalid",
                )
                session.add(observation)
                session.flush()
                session.add(
                    DataLineage(
                        run_id=run_id,
                        source_record_id=row["record_id"],
                        target_table="weather_observations",
                        target_record_id=str(observation.id),
                    )
                )
                inserted += 1
        run.record_count = inserted
        run.status = "degraded" if incident else "completed"
        if incident:
            incident.before_metrics = _daily_metrics(session, records)
    run.completed_at = utc_now()
    logger.info(
        "ingestion_finished",
        extra={
            "run_id": run_id,
            "source_id": source_id,
            "status": run.status,
            "record_count": run.record_count,
            "checksum": checksum,
        },
    )
    session.flush()
    return run


def _ensure_real_source(session: Session, source_id: str) -> DataSource:
    details = {
        "ehyd-ogc-api": (
            "Hydrographie Österreich — Pegel Aktuell",
            "real",
            (
                "No redistribution license is declared in the collection metadata; "
                "verify provider terms."
            ),
            "https://ehyd.gv.at/",
        ),
        "open-meteo-archive": (
            "Open-Meteo Historical Weather API",
            "real",
            "CC BY 4.0; free API use is non-commercial only",
            "https://open-meteo.com/en/docs/historical-weather-api",
        ),
    }
    if source_id not in details:
        raise ValueError(f"Unknown real-data source: {source_id}.")
    name, provenance, license_name, reference_url = details[source_id]
    source = session.get(DataSource, source_id)
    if source is None:
        source = DataSource(
            id=source_id,
            name=name,
            provenance=provenance,
            license=license_name,
            reference_url=reference_url,
            enabled=True,
        )
        session.add(source)
    else:
        source.name = name
        source.provenance = provenance
        source.license = license_name
        source.reference_url = reference_url
    session.flush()
    return source


def _cache_hit(
    session: Session,
    source_id: str,
    request_key: str,
    cache_for: timedelta | None,
    force: bool,
) -> IngestionRun | None:
    if force:
        return None
    run = session.scalar(
        select(IngestionRun)
        .where(
            IngestionRun.source_id == source_id,
            IngestionRun.request_key == request_key,
            IngestionRun.status.in_(("completed", "degraded")),
        )
        .order_by(IngestionRun.started_at.desc())
    )
    if run is None or cache_for is None:
        if (
            run is not None
            and run.request_parameters
            and run.request_parameters.get("source_errors")
        ):
            return None
        return run
    if parse_utc(run.started_at) >= utc_now() - cache_for:
        return run
    return None


def _upsert_real_station(session: Session, metadata: dict[str, Any]) -> Station:
    station_id = str(metadata["id"])
    station = session.get(Station, station_id)
    if station is None:
        station = Station(
            id=station_id,
            **{
                key: metadata[key] for key in ("source_id", "name", "kind", "latitude", "longitude")
            },
        )
        session.add(station)
    else:
        station.name = metadata["name"]
        station.source_id = metadata["source_id"]
        station.kind = metadata["kind"]
        station.latitude = metadata["latitude"]
        station.longitude = metadata["longitude"]
    session.flush()
    return station


def _real_row_is_valid(row: dict[str, Any], domain: str) -> bool:
    if not row.get("record_id") or not row.get("station_id"):
        return False
    try:
        parse_utc(row["event_time"])
    except (KeyError, TypeError, ValueError):
        return False
    if domain == "river":
        measurement_type = row.get("measurement_type")
        value = row.get("source_value")
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            return False
        if measurement_type == "water_level":
            level = row.get("water_level_m")
            source_unit = str(row.get("source_unit", "")).casefold()
            return (
                source_unit in {"cm", "m"}
                and isinstance(level, (int, float))
                and not isinstance(level, bool)
                and -20 <= level <= 50
                and row.get("discharge_m3_s") is None
            )
        if measurement_type == "discharge":
            discharge = row.get("discharge_m3_s")
            source_unit = str(row.get("source_unit", "")).casefold().replace("³", "3")
            return (
                source_unit in {"m3/s", "m^3/s"}
                and isinstance(discharge, (int, float))
                and not isinstance(discharge, bool)
                and 0 <= discharge <= 100_000
                and row.get("water_level_m") is None
            )
        return False
    ranges = {
        "temperature_c": (-90, 60),
        "precipitation_mm": (0, 1000),
        "relative_humidity_percent": (0, 100),
        "wind_speed_m_s": (0, 150),
    }
    present = False
    for field_name, bounds in ranges.items():
        value = row.get(field_name)
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not bounds[0] <= value <= bounds[1]
        ):
            return False
        present = True
    return present


def _record_source_failure(
    session: Session,
    bronze_dir: Path,
    *,
    source_id: str,
    error: str,
    request_parameters: dict[str, Any],
) -> IngestionRun:
    _ensure_real_source(session, source_id)
    now = utc_now()
    run_id = str(uuid.uuid4())
    raw = {
        "source_id": source_id,
        "provenance": "real",
        "retrieved_at": now.isoformat(),
        "request_parameters": request_parameters,
        "error": error,
    }
    checksum = hashlib.sha256(_canonical_bytes(raw)).hexdigest()
    source_dir = bronze_dir / source_id
    source_dir.mkdir(parents=True, exist_ok=True)
    bronze_path = source_dir / f"{run_id}.json"
    bronze_path.write_text(json.dumps(raw, indent=2, sort_keys=True), encoding="utf-8")
    run = IngestionRun(
        id=run_id,
        source_id=source_id,
        started_at=now,
        completed_at=now,
        status="failed",
        record_count=0,
        checksum=checksum,
        bronze_path=str(bronze_path),
        request_parameters=request_parameters,
        accepted_count=0,
        rejected_count=0,
    )
    session.add(run)
    session.flush()
    session.add(
        QualityResult(
            run_id=run_id,
            check_name="source_availability",
            status="failed",
            details=error,
            affected_count=0,
            checked_at=now,
            phase="ingestion",
        )
    )
    session.commit()
    logger.error(
        "real_source_ingestion_failed",
        extra={"run_id": run_id, "source_id": source_id, "status": "failed"},
    )
    return run


def _persist_real_batch(
    session: Session,
    bronze_dir: Path,
    batch: SourceBatch,
    *,
    domain: str,
    request_key: str,
    expected_hours: int | None = None,
    source_errors: list[str] | None = None,
) -> IngestionRun:
    _ensure_real_source(session, batch.source_id)
    now = utc_now()
    run_id = str(uuid.uuid4())
    rejected_rows = list(batch.rejected_records)
    request_parameters = dict(batch.request_parameters)
    raw = {
        "source_id": batch.source_id,
        "provenance": "real",
        "retrieved_at": batch.retrieved_at.isoformat(),
        "source_published_at": batch.published_at.isoformat() if batch.published_at else None,
        "request_parameters": request_parameters,
        "checksum_sha256": batch.checksum,
        "raw_response": batch.raw_response,
        "records": batch.records,
        "rejected_records": rejected_rows,
        "schema_warnings": batch.warnings,
        "source_errors": source_errors or [],
    }
    checksum = hashlib.sha256(_canonical_bytes(batch.raw_response)).hexdigest()
    source_dir = bronze_dir / batch.source_id
    source_dir.mkdir(parents=True, exist_ok=True)
    bronze_path = source_dir / f"{run_id}.json"
    bronze_path.write_text(json.dumps(raw, indent=2, sort_keys=True), encoding="utf-8")
    run = IngestionRun(
        id=run_id,
        source_id=batch.source_id,
        started_at=now,
        status="processing",
        record_count=len(batch.records) + len(rejected_rows),
        checksum=checksum,
        bronze_path=str(bronze_path),
        request_key=request_key,
        request_parameters=request_parameters,
        accepted_count=0,
        rejected_count=len(rejected_rows),
    )
    session.add(run)
    session.flush()

    for station_metadata in batch.stations:
        _upsert_real_station(session, station_metadata)

    valid_count = 0
    invalid_count = 0
    duplicate_count = 0
    null_measurement_count = 0
    seen_records: set[str] = set()
    seen_events: set[tuple[str, str, str]] = set()
    latest_event: datetime | None = None
    for row in batch.records:
        record_id = str(row.get("record_id", ""))
        try:
            event_time = parse_utc(row["event_time"])
        except (KeyError, TypeError, ValueError):
            rejected_rows.append(
                {"source_record_id": record_id or "unknown", "reason": "Invalid event timestamp."}
            )
            invalid_count += 1
            continue
        latest_event = max(latest_event, event_time) if latest_event else event_time
        event_key = (
            str(row.get("station_id")),
            str(row.get("measurement_type", "weather")),
            event_time.isoformat(),
        )
        if record_id in seen_records or event_key in seen_events:
            duplicate_count += 1
            continue
        seen_records.add(record_id)
        seen_events.add(event_key)
        existing = (
            session.scalar(
                select(RiverObservation).where(
                    RiverObservation.source_id == batch.source_id,
                    RiverObservation.source_record_id == record_id,
                )
            )
            if domain == "river"
            else session.scalar(
                select(WeatherObservation).where(
                    WeatherObservation.source_id == batch.source_id,
                    WeatherObservation.source_record_id == record_id,
                )
            )
        )
        row_valid = _real_row_is_valid(row, domain)
        if row_valid:
            valid_count += 1
        else:
            invalid_count += 1
            rejected_rows.append(
                {
                    "source_record_id": record_id,
                    "reason": "Silver type, unit, or range validation failed.",
                }
            )
        if domain == "river":
            null_measurement_count += int(row.get("source_value") is None)
        else:
            null_measurement_count += sum(
                row.get(name) is None
                for name in (
                    "temperature_c",
                    "precipitation_mm",
                    "relative_humidity_percent",
                    "wind_speed_m_s",
                )
            )
        if existing is not None:
            duplicate_count += 1
            continue
        observation_status = "valid" if row_valid else "invalid"
        if domain == "river":
            observation = RiverObservation(
                source_id=batch.source_id,
                run_id=run_id,
                station_id=str(row["station_id"]),
                source_record_id=record_id,
                event_time=event_time,
                source_published_at=None,
                ingested_at=batch.retrieved_at,
                processed_at=now,
                water_level_m=row.get("water_level_m"),
                discharge_m3_s=row.get("discharge_m3_s"),
                measurement_type=row.get("measurement_type"),
                source_value=row.get("source_value"),
                source_unit=row.get("source_unit"),
                measurement_reference=row.get("measurement_reference"),
                quality_status=observation_status,
            )
            target_table = "river_observations"
        else:
            observation = WeatherObservation(
                source_id=batch.source_id,
                run_id=run_id,
                station_id=str(row["station_id"]),
                source_record_id=record_id,
                event_time=event_time,
                source_published_at=None,
                ingested_at=batch.retrieved_at,
                processed_at=now,
                temperature_c=row.get("temperature_c"),
                precipitation_mm=row.get("precipitation_mm"),
                relative_humidity_percent=row.get("relative_humidity_percent"),
                wind_speed_m_s=row.get("wind_speed_m_s"),
                source_model=row.get("source_model"),
                source_units=row.get("source_units"),
                quality_status=observation_status,
            )
            target_table = "weather_observations"
        session.add(observation)
        session.flush()
        session.add(
            DataLineage(
                run_id=run_id,
                source_record_id=record_id,
                target_table=target_table,
                target_record_id=str(observation.id),
            )
        )

    run.accepted_count = valid_count
    run.rejected_count = len(rejected_rows)
    missing_stations = batch.request_parameters.get("missing_hzbnr", [])
    source_status = (
        "passed" if batch.records and not source_errors and not missing_stations else "warning"
    )
    schema_status = "warning" if rejected_rows or batch.warnings else "passed"
    unit_status = (
        "warning"
        if any("unit" in str(row.get("reason", "")).casefold() for row in rejected_rows)
        else "passed"
    )
    checks: list[tuple[str, str, str, int]] = [
        (
            "source_availability",
            source_status,
            (
                f"Received {len(batch.records)} normalized record(s); "
                f"{len(source_errors or [])} location/source request(s) failed; "
                f"{len(missing_stations)} requested gauge(s) were absent."
            ),
            len(source_errors or []) + len(missing_stations),
        ),
        (
            "schema_validation",
            schema_status,
            f"{len(rejected_rows)} record(s) rejected; "
            f"{len(batch.warnings)} schema warning(s) retained in Bronze.",
            len(rejected_rows) + len(batch.warnings),
        ),
        (
            "unit_validation",
            unit_status,
            f"{len(rejected_rows)} source record(s) were rejected or invalid.",
            len(rejected_rows),
        ),
        (
            "missing_measurements",
            "warning" if null_measurement_count else "passed",
            f"{null_measurement_count} expected measurement value(s) are null; "
            "nulls were not zero-filled.",
            null_measurement_count,
        ),
        (
            "duplicate_events",
            "warning" if duplicate_count else "passed",
            f"{duplicate_count} repeated source/event record(s) were safely ignored.",
            duplicate_count,
        ),
    ]
    if domain == "weather" and expected_hours is not None:
        expected_locations = batch.request_parameters.get("locations")
        location_count = (
            len(expected_locations) if isinstance(expected_locations, list) else len(batch.stations)
        )
        expected_total = expected_hours * max(1, location_count)
        actual_total = len(batch.records)
        gap_count = max(0, expected_total - actual_total)
        checks.append(
            (
                "hourly_completeness",
                "warning" if gap_count else "passed",
                f"Received {actual_total}/{expected_total} expected UTC hourly rows.",
                gap_count,
            )
        )
    if latest_event is not None:
        threshold = timedelta(hours=6 if domain == "river" else 120)
        age = now - latest_event
        freshness_status = "warning" if age > threshold or age < timedelta(hours=-1) else "passed"
        checks.append(
            (
                "latest_event_freshness",
                freshness_status,
                f"Latest event is {age.total_seconds() / 3600:.1f} hour(s) from ingestion time.",
                int(freshness_status == "warning"),
            )
        )
    for name, status, details, affected_count in checks:
        session.add(
            QualityResult(
                run_id=run_id,
                check_name=name,
                status=status,
                details=details,
                affected_count=affected_count,
                checked_at=now,
                phase="ingestion",
            )
        )
    has_quality_warning = any(status in {"warning", "failed"} for _, status, _, _ in checks)
    run.status = (
        "completed"
        if not rejected_rows
        and not batch.warnings
        and not invalid_count
        and not source_errors
        and batch.records
        and not has_quality_warning
        else "degraded"
    )
    run.completed_at = utc_now()
    session.commit()
    logger.info(
        "real_ingestion_finished",
        extra={
            "run_id": run_id,
            "source_id": batch.source_id,
            "status": run.status,
            "record_count": run.record_count,
            "accepted_count": run.accepted_count,
            "rejected_count": run.rejected_count,
            "checksum": checksum,
        },
    )
    return run


@serialized_ingestion("river")
def ingest_real_river(
    session: Session,
    bronze_dir: Path,
    *,
    force: bool = False,
    connector: AustrianHydrographyConnector | None = None,
) -> IngestionRun:
    connector = connector or AustrianHydrographyConnector()
    request_parameters = {
        "collection": "i000501:pegel_aktuell",
        "station_ids": list(connector.station_ids),
        "scope": "current observations only",
    }
    request_key = (
        "ehyd-current-" + hashlib.sha256(_canonical_bytes(request_parameters)).hexdigest()[:24]
    )
    cached = _cache_hit(session, connector.source_id, request_key, timedelta(minutes=15), force)
    if cached is not None:
        return cached
    try:
        batch = connector.fetch()
    except SourceUnavailable as error:
        session.rollback()
        return _record_source_failure(
            session,
            bronze_dir,
            source_id=connector.source_id,
            error=str(error),
            request_parameters=request_parameters,
        )
    return _persist_real_batch(
        session,
        bronze_dir,
        batch,
        domain="river",
        request_key=request_key,
    )


def default_weather_window(today: date | None = None) -> tuple[date, date]:
    """Return the latest seven complete UTC days with a conservative archive lag."""
    end_date = (today or utc_now().date()) - timedelta(days=2)
    return end_date - timedelta(days=6), end_date


@serialized_ingestion("weather")
def ingest_real_weather(
    session: Session,
    bronze_dir: Path,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    models: str | None = None,
    force: bool = False,
    connectors: list[OpenMeteoArchiveConnector] | None = None,
) -> IngestionRun:
    if start_date is None and end_date is None:
        start_date, end_date = default_weather_window()
    elif start_date is None or end_date is None:
        raise ValueError("Provide both start_date and end_date for weather backfills.")
    assert start_date is not None and end_date is not None
    if end_date < start_date or (end_date - start_date).days > 30:
        raise ValueError("Weather range must be ordered and no longer than 31 days.")

    if connectors is None:
        river_stations = session.scalars(
            select(Station)
            .where(
                Station.source_id == "ehyd-ogc-api",
                Station.id.in_([f"ehyd:{station_id}" for station_id in DEFAULT_DANUBE_STATIONS]),
            )
            .order_by(Station.id)
        ).all()
        if not river_stations:
            raise ValueError("Ingest real river stations before requesting their weather.")
        connectors = [
            OpenMeteoArchiveConnector(
                latitude=station.latitude,
                longitude=station.longitude,
                station_id=f"open-meteo:{station.id.removeprefix('ehyd:')}",
                station_name=f"Open-Meteo reanalysis near {station.name}",
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                models=models,
            )
            for station in river_stations
            if station.latitude is not None and station.longitude is not None
        ]
    if not connectors:
        raise ValueError("No real hydrographic station coordinates are available for weather.")

    request_parameters: dict[str, Any] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "models": models,
        "variables": list(OPEN_METEO_VARIABLES),
        "locations": [
            {
                "station_id": connector.station_id,
                "latitude": connector.latitude,
                "longitude": connector.longitude,
            }
            for connector in connectors
        ],
    }
    request_key = (
        "open-meteo-" + hashlib.sha256(_canonical_bytes(request_parameters)).hexdigest()[:32]
    )
    cached = _cache_hit(session, "open-meteo-archive", request_key, None, force)
    if cached is not None:
        return cached

    records: list[dict[str, Any]] = []
    stations: list[dict[str, Any]] = []
    responses: dict[str, Any] = {}
    errors: list[str] = []
    for connector in connectors:
        try:
            batch = connector.fetch()
        except SourceUnavailable as error:
            errors.append(f"{connector.station_id}: {error}")
            continue
        records.extend(batch.records)
        stations.extend(batch.stations)
        responses[connector.station_id] = batch.raw_response
    if not responses:
        return _record_source_failure(
            session,
            bronze_dir,
            source_id="open-meteo-archive",
            error="; ".join(errors) or "Open-Meteo returned no station responses.",
            request_parameters=request_parameters,
        )
    raw_response = {"locations": responses, "errors": errors}
    batch = SourceBatch(
        source_id="open-meteo-archive",
        retrieved_at=utc_now(),
        published_at=None,
        records=records,
        provenance="real",
        checksum=hashlib.sha256(_canonical_bytes(raw_response)).hexdigest(),
        raw_response=raw_response,
        stations=stations,
        rejected_records=[],
        request_parameters={**request_parameters, "source_errors": errors},
    )
    expected_hours = (end_date - start_date).days * 24 + 24
    return _persist_real_batch(
        session,
        bronze_dir,
        batch,
        domain="weather",
        request_key=request_key,
        expected_hours=expected_hours,
        source_errors=errors,
    )


def run_demo(
    session: Session,
    bronze_dir: Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Load the labeled fixture and generate a recoverable schema-drift incident."""
    existing = session.scalar(
        select(IngestionRun).where(IngestionRun.source_id == "synthetic-river")
    )
    if existing and not force:
        return {
            "status": "already_loaded",
            "message": "Synthetic fixtures are already loaded.",
        }
    river, _ = _fixture("demo_hydrology.json")
    weather, _ = _fixture("demo_weather.json")
    drift, _ = _fixture("demo_schema_drift.json")
    river_run = ingest_records(
        session,
        bronze_dir,
        source_id="synthetic-river",
        domain="river",
        records=river["records"],
        raw_payload=river,
    )
    weather_run = ingest_records(
        session,
        bronze_dir,
        source_id="synthetic-weather",
        domain="weather",
        records=weather["records"],
        raw_payload=weather,
    )
    drift_run = ingest_records(
        session,
        bronze_dir,
        source_id="synthetic-schema",
        domain="river",
        records=drift["records"],
        raw_payload=drift,
        scenario="schema_drift",
    )
    session.commit()
    incident = session.scalar(
        select(Incident).where(Incident.run_id == drift_run.id, Incident.status == "detected")
    )
    return {
        "status": "loaded",
        "river_run_id": river_run.id,
        "weather_run_id": weather_run.id,
        "drift_run_id": drift_run.id,
        "incident_id": incident.id if incident else None,
        "provenance": "synthetic",
    }


def simulate_scenario(session: Session, bronze_dir: Path, scenario: str) -> Incident:
    """Create deterministic source failures for one investigation scenario."""
    if scenario not in {"schema_drift", "duplicate_records", "missing_late"}:
        raise ValueError(f"Unsupported incident scenario: {scenario}")
    existing = session.scalar(
        select(Incident)
        .where(Incident.scenario == scenario, Incident.status != "reset")
        .order_by(Incident.detected_at.desc())
    )
    if existing is not None:
        return existing

    if scenario == "schema_drift":
        payload, _ = _fixture("demo_schema_drift.json")
        run = ingest_records(
            session,
            bronze_dir,
            source_id="synthetic-schema",
            domain="river",
            records=payload["records"],
            raw_payload=payload,
            scenario=scenario,
        )
    elif scenario == "duplicate_records":
        event_time = "2026-10-24T01:00:00+00:00"
        records = [
            {
                "record_id": "duplicate-river-000",
                "station_id": "AT-DANUBE-DUPLICATE",
                "event_time": "2026-10-24T00:00:00+00:00",
                "water_level_m": 2.80,
                "discharge_m3_s": 1500.0,
            },
            {
                "record_id": "duplicate-river-001",
                "station_id": "AT-DANUBE-DUPLICATE",
                "event_time": event_time,
                "water_level_m": 2.82,
                "discharge_m3_s": 1510.0,
            },
            {
                "record_id": "duplicate-river-001-copy",
                "station_id": "AT-DANUBE-DUPLICATE",
                "event_time": event_time,
                "water_level_m": 8.20,
                "discharge_m3_s": 4400.0,
            },
            {
                "record_id": "duplicate-river-002",
                "station_id": "AT-DANUBE-DUPLICATE",
                "event_time": "2026-10-24T02:00:00+00:00",
                "water_level_m": 2.84,
                "discharge_m3_s": 1520.0,
            },
            {
                "record_id": "duplicate-river-003",
                "station_id": "AT-DANUBE-DUPLICATE",
                "event_time": "2026-10-24T03:00:00+00:00",
                "water_level_m": 2.83,
                "discharge_m3_s": 1515.0,
            },
        ]
        for row in records:
            row["source_published_at"] = (
                parse_utc(row["event_time"]) + timedelta(minutes=5)
            ).isoformat()
        run = ingest_records(
            session,
            bronze_dir,
            source_id="synthetic-duplicate",
            domain="river",
            records=records,
            raw_payload={"records": records},
            scenario=scenario,
            incident_category="duplicate_records",
        )
    else:
        first_hour = datetime(2026, 10, 15, tzinfo=UTC)
        records = []
        for hour, level, discharge in (
            (0, 2.80, 1500.0),
            (1, 2.82, 1510.0),
            (3, 2.86, 1530.0),
            (4, 2.88, 1540.0),
        ):
            event_time = first_hour + timedelta(hours=hour)
            records.append(
                {
                    "record_id": f"missing-late-river-{hour:03}",
                    "station_id": "AT-DANUBE-KREMS",
                    "event_time": event_time.isoformat(),
                    "source_published_at": (event_time + timedelta(days=5)).isoformat(),
                    "water_level_m": level,
                    "discharge_m3_s": discharge,
                }
            )
        missing_time = first_hour + timedelta(hours=2)
        backfill = {
            "record_id": "missing-late-river-002",
            "station_id": "AT-DANUBE-KREMS",
            "event_time": missing_time.isoformat(),
            "source_published_at": (missing_time + timedelta(days=5)).isoformat(),
            "water_level_m": 2.84,
            "discharge_m3_s": 1520.0,
        }
        run = ingest_records(
            session,
            bronze_dir,
            source_id="synthetic-missing-late",
            domain="river",
            records=records,
            raw_payload={"records": records, "recovery_records": [backfill]},
            scenario=scenario,
            incident_category="missing_late",
        )

    incident = session.scalar(select(Incident).where(Incident.run_id == run.id))
    if incident is None:
        raise RuntimeError(f"Scenario {scenario} did not create an incident.")
    if scenario == "duplicate_records":
        seen: set[tuple[str, str]] = set()
        annotated_records: list[dict[str, Any]] = []
        for item in incident.affected_records or []:
            item = dict(item)
            identity = (str(item.get("station_id")), str(item.get("event_time")))
            item["duplicate"] = identity in seen
            seen.add(identity)
            annotated_records.append(item)
        incident.affected_records = annotated_records
        incident.details = (
            "Duplicate station/event-time records are present. "
            "The pre-recovery Gold aggregate includes both records."
        )
    elif scenario == "missing_late":
        incident.affected_records = [
            *(incident.affected_records or []),
            {
                "source_record_id": "missing-late-river-002",
                "station_id": "AT-DANUBE-KREMS",
                "event_time": "2026-10-15T02:00:00+00:00",
                "state": "missing",
            },
        ]
        incident.details = (
            "The expected 02:00 UTC measurement is withheld from the initial batch; "
            "the surrounding observations were ingested more than 72 hours after event time."
        )
    session.flush()
    session.commit()
    return incident


def investigate_incident(session: Session, incident_id: str) -> Incident:
    incident = session.get(Incident, incident_id)
    if incident is None:
        raise LookupError(f"Incident {incident_id} was not found.")
    if incident.status == "detected":
        previous = incident.status
        incident.status = "investigating"
        _add_event(
            session,
            incident,
            "status_changed",
            previous,
            incident.status,
            {"reason": "Investigation opened by the user."},
        )
        session.commit()
    return incident


def _save_quality_results(
    session: Session,
    incident: Incident,
    checks: list[dict[str, Any]],
    phase: str,
    checked_at: datetime,
) -> None:
    for check in checks:
        session.add(
            QualityResult(
                run_id=incident.run_id,
                incident_id=incident.id,
                phase=phase,
                check_name=check["check_name"],
                status=check["status"],
                details=check["details"],
                affected_count=check["affected_count"],
                checked_at=checked_at,
            )
        )


def _upsert_river_observation(
    session: Session,
    run: IngestionRun,
    row: dict[str, Any],
    now: datetime,
) -> RiverObservation:
    _ensure_station(session, row["station_id"], run.source_id)
    observation = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == run.source_id,
            RiverObservation.source_record_id == row["record_id"],
        )
    )
    if observation is None:
        observation = RiverObservation(
            source_id=run.source_id,
            run_id=run.id,
            station_id=row["station_id"],
            source_record_id=row["record_id"],
            event_time=parse_utc(row["event_time"]),
            source_published_at=parse_utc(row["source_published_at"])
            if row.get("source_published_at")
            else None,
            ingested_at=now,
            processed_at=now,
            water_level_m=row.get("water_level_m"),
            discharge_m3_s=row.get("discharge_m3_s"),
            quality_status="valid" if row_is_valid(row, "river") else "invalid",
        )
        session.add(observation)
        session.flush()
        session.add(
            DataLineage(
                run_id=run.id,
                source_record_id=row["record_id"],
                target_table="river_observations",
                target_record_id=str(observation.id),
            )
        )
    else:
        observation.run_id = run.id
        observation.event_time = parse_utc(row["event_time"])
        observation.source_published_at = (
            parse_utc(row["source_published_at"]) if row.get("source_published_at") else None
        )
        observation.water_level_m = row.get("water_level_m")
        observation.discharge_m3_s = row.get("discharge_m3_s")
        observation.processed_at = now
        observation.quality_status = "valid" if row_is_valid(row, "river") else "invalid"
        lineage = session.scalar(
            select(DataLineage).where(
                DataLineage.run_id == run.id,
                DataLineage.source_record_id == row["record_id"],
                DataLineage.target_table == "river_observations",
                DataLineage.target_record_id == str(observation.id),
            )
        )
        if lineage is None:
            session.add(
                DataLineage(
                    run_id=run.id,
                    source_record_id=row["record_id"],
                    target_table="river_observations",
                    target_record_id=str(observation.id),
                )
            )
    return observation


def replay_is_verified(incident: Incident) -> bool:
    """Require a completed replay and a passing scenario-specific recovery check."""
    required_check = {
        "schema_change": "schema_change",
        "duplicate_records": "duplicate_events",
        "missing_late": "hourly_completeness",
    }.get(incident.category)
    if required_check is None:
        return False
    return any(
        replay.status == "completed"
        and replay.validation_results is not None
        and all(
            any(
                check.get("check_name") == name and check.get("status") == "passed"
                for check in replay.validation_results
            )
            for name in (required_check, "persisted_recovery", "downstream_gold")
        )
        for replay in incident.replays
    )


def replay_incident(session: Session, incident_id: str) -> ReplayHistory:
    """Recover from immutable Bronze, verify results, and retain both validation phases."""
    incident = session.get(Incident, incident_id)
    if incident is None:
        raise LookupError(f"Incident {incident_id} was not found.")
    if incident.status == "recovered" and replay_is_verified(incident):
        prior = session.scalar(
            select(ReplayHistory)
            .where(ReplayHistory.incident_id == incident_id, ReplayHistory.status == "completed")
            .order_by(ReplayHistory.replayed_at.desc())
        )
        if prior is not None:
            return prior
    if incident.status not in {
        "detected",
        "investigating",
        "recovering",
        "failed_recovery",
        "reset",
        "recovered",
    }:
        raise ValueError(f"Incident {incident_id} cannot be replayed from {incident.status}.")
    run = session.get(IngestionRun, incident.run_id)
    if run is None:
        raise RuntimeError(f"Ingestion run {incident.run_id} referenced by incident is missing.")
    bronze_path = Path(run.bronze_path)
    payload = json.loads(bronze_path.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"Bronze snapshot {bronze_path} has no records list.")
    checksum = hashlib.sha256(_canonical_bytes(records)).hexdigest()
    if checksum != run.checksum:
        previous_status = incident.status
        incident.status = "failed_recovery"
        message = "Bronze checksum mismatch; no Silver data was modified."
        _add_event(
            session,
            incident,
            "recovery_failed",
            previous_status,
            "failed_recovery",
            {"reason": message},
        )
        session.add(
            ReplayHistory(
                id=str(uuid.uuid4()),
                incident_id=incident.id,
                replayed_at=utc_now(),
                status="failed",
                message=message,
            )
        )
        session.commit()
        raise ValueError("Bronze checksum mismatch; recovery was stopped without modifying data.")

    now = utc_now()
    if not incident.affected_records:
        incident.affected_records = [
            {
                "source_record_id": row.get("record_id"),
                "event_time": row.get("event_time"),
                "station_id": row.get("station_id"),
            }
            for row in records
        ]
    affected_records = [row for row in records if row.get("station_id") and row.get("event_time")]
    before_metrics = _daily_metrics(session, affected_records)
    previous_status = incident.status
    incident.status = "recovering"
    _add_event(
        session,
        incident,
        "status_changed",
        previous_status,
        "recovering",
        {"run_id": run.id, "bronze_checksum": checksum},
    )
    session.flush()
    session.commit()
    try:
        recovery_records = payload.get("recovery_records", [])
        if not isinstance(recovery_records, list):
            raise ValueError("Bronze recovery_records must be a list.")
        repaired = [dict(row) for row in records]
        if incident.category == "schema_change":
            for row in repaired:
                if "water_level_cm" in row:
                    row["water_level_m"] = float(row.pop("water_level_cm")) / 100
            validated_records = repaired
        elif incident.category == "duplicate_records":
            unique: dict[tuple[str, datetime], dict[str, Any]] = {}
            for row in repaired:
                unique.setdefault((row["station_id"], parse_utc(row["event_time"])), row)
            validated_records = list(unique.values())
        elif incident.category == "missing_late":
            repaired.extend(dict(row) for row in recovery_records)
            validated_records = repaired
        else:
            raise ValueError(f"Replay repair is not configured for {incident.category}.")

        checks = inspect_records(validated_records, "river")
        if any(check["status"] == "failed" for check in checks):
            raise ValueError("Recovery validation failed; source snapshot remains unchanged.")
        if incident.category == "missing_late" and any(
            check["check_name"] == "hourly_completeness" and check["status"] != "passed"
            for check in checks
        ):
            raise ValueError("Recovery did not restore hourly coverage.")

        if incident.category == "duplicate_records":
            duplicate_ids = {row["record_id"] for row in repaired} - {
                row["record_id"] for row in validated_records
            }
            duplicate_rows = session.scalars(
                select(RiverObservation).where(
                    RiverObservation.source_id == run.source_id,
                    RiverObservation.source_record_id.in_(duplicate_ids),
                )
            ).all()
            for observation in duplicate_rows:
                observation.quality_status = "duplicate"
                observation.processed_at = now
        elif incident.category == "missing_late":
            for row in recovery_records:
                _upsert_river_observation(session, run, row, now)
        else:
            for row in repaired:
                _upsert_river_observation(session, run, row, now)

        if incident.category == "schema_change":
            quarantines = session.scalars(
                select(QuarantinedRecord).where(QuarantinedRecord.incident_id == incident.id)
            ).all()
            if not quarantines:
                failed_check = session.scalar(
                    select(QualityResult).where(
                        QualityResult.incident_id == incident.id,
                        QualityResult.phase == "detection",
                        QualityResult.check_name == "schema_change",
                    )
                )
                if failed_check is not None:
                    quarantines = [
                        QuarantinedRecord(
                            incident_id=incident.id,
                            source_record_id=str(row["record_id"]),
                            raw_record=row,
                            reason=failed_check.details,
                            status="recovered",
                            quarantined_at=failed_check.checked_at,
                        )
                        for row in records
                    ]
                    session.add_all(quarantines)
            for quarantine in quarantines:
                quarantine.status = "recovered"
        # API sessions disable autoflush: SQL must see the correction before verification.
        session.flush()
        after_metrics = _daily_metrics(
            session,
            [row for row in repaired if row.get("station_id") and row.get("event_time")],
        )
        checks.extend(
            _verify_persisted_recovery(session, run, records, validated_records, after_metrics)
        )
        _save_quality_results(session, incident, checks, "recovery", now)
        incident.after_metrics = after_metrics
        incident.status = "recovered"
        run.status = "recovered"
        run.completed_at = now
        _add_event(
            session,
            incident,
            "recovery_verified",
            "recovering",
            "recovered",
            {
                "before_metrics": before_metrics,
                "after_metrics": after_metrics,
                "validation": checks,
                "bronze_checksum": checksum,
            },
        )
        replay = ReplayHistory(
            id=str(uuid.uuid4()),
            incident=incident,
            replayed_at=now,
            status="completed",
            message=f"Recovered {incident.scenario} from the original Bronze snapshot.",
            before_metrics=before_metrics,
            after_metrics=after_metrics,
            validation_results=checks,
        )
        session.add(replay)
        session.commit()
    except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError):
        session.rollback()
        incident = session.get(Incident, incident_id)
        run = session.get(IngestionRun, incident.run_id) if incident else None
        if incident is None or run is None:
            raise
        previous_status = incident.status
        incident.status = "failed_recovery"
        run.status = "failed_recovery"
        _add_event(
            session,
            incident,
            "recovery_failed",
            previous_status,
            "failed_recovery",
            {
                "reason": (
                    "Recovery verification failed; transactional Silver changes were rolled back."
                )
            },
        )
        session.add(
            ReplayHistory(
                id=str(uuid.uuid4()),
                incident_id=incident.id,
                replayed_at=utc_now(),
                status="failed",
                message="Recovery verification failed; no Silver changes were committed.",
            )
        )
        session.commit()
        raise

    logger.info(
        "incident_replayed",
        extra={"incident_id": incident.id, "run_id": run.id, "status": replay.status},
    )
    return replay


def reset_demo(session: Session) -> dict[str, int]:
    """Restore scenario data without deleting immutable Bronze or audit records."""
    incidents = session.scalars(
        select(Incident).where(
            Incident.scenario.in_(("schema_drift", "duplicate_records", "missing_late")),
            Incident.status != "reset",
        )
    ).all()
    count = 0
    for incident in incidents:
        run = session.get(IngestionRun, incident.run_id)
        if run is None:
            continue
        previous = incident.status
        affected = incident.affected_records or []
        source_ids = {item["source_record_id"] for item in affected if item.get("source_record_id")}
        if incident.scenario == "schema_drift":
            rows = session.scalars(
                select(RiverObservation).where(
                    RiverObservation.source_id == run.source_id,
                    RiverObservation.source_record_id.in_(source_ids),
                )
            )
            for row in rows:
                row.quality_status = "quarantined"
            quarantines = session.scalars(
                select(QuarantinedRecord).where(QuarantinedRecord.incident_id == incident.id)
            )
            for quarantine in quarantines:
                quarantine.status = "quarantined"
        elif incident.scenario == "duplicate_records":
            duplicate_ids = [item["source_record_id"] for item in affected if item.get("duplicate")]
            duplicate_rows = session.scalars(
                select(RiverObservation).where(
                    RiverObservation.source_id == run.source_id,
                    RiverObservation.source_record_id.in_(duplicate_ids),
                )
            )
            for row in duplicate_rows:
                row.quality_status = "valid"
        elif incident.scenario == "missing_late":
            backfill = session.scalar(
                select(RiverObservation).where(
                    RiverObservation.source_id == run.source_id,
                    RiverObservation.source_record_id == "missing-late-river-002",
                )
            )
            if backfill is not None:
                backfill.quality_status = "quarantined"
        incident.status = "reset"
        incident.after_metrics = None
        _add_event(
            session,
            incident,
            "demo_reset",
            previous,
            "reset",
            {"bronze_preserved": True, "silver_restored": True},
        )
        count += 1
    session.commit()
    return {"incidents_reset": count}
