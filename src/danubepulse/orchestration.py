"""Airflow-independent execution audit and persisted Bronze/Silver/Gold checks."""

import hashlib
import json
import logging
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from danubepulse.config import get_settings
from danubepulse.energy import ingest_energy, verify_energy
from danubepulse.models import (
    EnergyObservation,
    IngestionRun,
    PipelineExecution,
    QualityResult,
    RiverObservation,
    WeatherObservation,
)
from danubepulse.pipeline import ingest_real_river, ingest_real_weather
from danubepulse.time_utils import parse_utc, utc_now

logger = logging.getLogger(__name__)
Domain = Literal["river", "weather", "energy"]


def execute_pipeline(
    session: Session,
    bronze_dir: Path,
    domain: Domain,
    orchestrator_run_id: str,
    target_date: date | None = None,
) -> PipelineExecution:
    if domain in {"weather", "energy"}:
        target_date = target_date or utc_now().date() - timedelta(days=2)
        if target_date > utc_now().date() - timedelta(days=2):
            raise ValueError("Automation requires at least two complete UTC days of lag.")
    elif target_date is not None:
        raise ValueError("The current river source does not support historical dates.")
    execution = PipelineExecution(
        id=str(uuid4()),
        domain=domain,
        orchestrator_run_id=orchestrator_run_id,
        target_date=datetime.combine(target_date, datetime.min.time(), UTC)
        if target_date
        else None,
        status="running",
        started_at=utc_now(),
    )
    session.add(execution)
    session.commit()
    try:
        if domain == "river":
            run = ingest_real_river(session, bronze_dir)
        elif domain == "weather":
            run = ingest_real_weather(
                session, bronze_dir, start_date=target_date, end_date=target_date
            )
        else:
            run = ingest_energy(
                session,
                bronze_dir,
                target_date,
                terms_accepted=get_settings().entsoe_terms_accepted,
            )
        execution.ingestion_run_id = run.id
        execution.status = "ingested" if run.status != "failed" else "failed"
        if run.status == "failed":
            checks = session.scalars(
                select(QualityResult).where(QualityResult.run_id == run.id)
            ).all()
            execution.error = "; ".join(check.details for check in checks)
            execution.completed_at = utc_now()
        session.commit()
    except Exception as error:
        # Preserve unexpected failures as evidence, then propagate them to the caller.
        session.rollback()
        execution = session.get(PipelineExecution, execution.id)
        if execution is None:
            raise RuntimeError("Pipeline audit record disappeared.") from error
        execution.status = "failed"
        execution.error = f"{type(error).__name__}: {error}"
        execution.completed_at = utc_now()
        session.commit()
        logger.exception("pipeline_execution_failed", extra={"run_id": execution.id})
        raise
    logger.info(
        "pipeline_ingestion_finished",
        extra={"run_id": execution.id, "status": execution.status},
    )
    return execution


def _same_number(actual: Any, expected: Any) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return math.isclose(float(actual), float(expected), rel_tol=1e-8, abs_tol=1e-8)


def _verify_data(session: Session, run: IngestionRun, domain: Domain) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"check": name, "passed": passed, "detail": detail})

    quality = session.scalars(select(QualityResult).where(QualityResult.run_id == run.id)).all()
    required = {
        "source_availability",
        "schema_validation",
        "unit_validation",
        "missing_measurements",
    }
    required.add("hourly_completeness" if domain == "weather" else "latest_event_freshness")
    results = {row.check_name: row.status for row in quality}
    check(
        "source_quality",
        run.status in {"completed", "degraded"}
        and all(results.get(name) == "passed" for name in required)
        and run.rejected_count == 0,
        json.dumps(results, sort_keys=True),
    )
    envelope = json.loads(Path(run.bronze_path).read_text(encoding="utf-8"))
    raw = envelope.get("raw_response")
    checksum = hashlib.sha256(
        json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    check(
        "bronze_integrity",
        raw is not None and checksum == run.checksum and envelope.get("provenance") == "real",
        "Immutable raw-response checksum and real-source provenance.",
    )
    records = envelope.get("records", [])
    model = RiverObservation if domain == "river" else WeatherObservation
    fields = (
        ("water_level_m", "discharge_m3_s")
        if domain == "river"
        else ("temperature_c", "precipitation_mm", "relative_humidity_percent", "wind_speed_m_s")
    )
    persisted = session.scalars(select(model).where(model.source_id == run.source_id)).all()
    by_record = {row.source_record_id: row for row in persisted}
    valid_rows = [row for row in persisted if row.quality_status == "valid"]
    event_keys = [
        (
            row.station_id,
            parse_utc(row.event_time),
            row.measurement_type if domain == "river" else "weather",
        )
        for row in valid_rows
    ]
    check(
        "semantic_uniqueness",
        len(event_keys) == len(set(event_keys)),
        "Only one trusted row per station/event/measurement type.",
    )
    check(
        "persisted_silver",
        bool(records)
        and all(
            (row := by_record.get(record["record_id"])) is not None
            and row.quality_status == "valid"
            and row.station_id == record["station_id"]
            and parse_utc(row.event_time) == parse_utc(record["event_time"])
            and all(_same_number(getattr(row, field), record.get(field)) for field in fields)
            for record in records
        ),
        "Every normalized Bronze record matches a trusted persisted measurement.",
    )
    impacted = {
        (record["station_id"], parse_utc(record["event_time"]).date()) for record in records
    }
    gold_matches = bool(impacted)
    for station_id, day in impacted:
        rows = [
            row
            for row in valid_rows
            if row.station_id == station_id and parse_utc(row.event_time).date() == day
        ]
        view = "gold_real_river_daily" if domain == "river" else "gold_real_weather_daily"
        gold = (
            session.execute(
                text(f"SELECT * FROM {view} WHERE location = :station AND observation_date = :day"),
                {"station": station_id, "day": day.isoformat()},
            )
            .mappings()
            .all()
        )
        gold_matches &= sum(row["observation_count"] for row in gold) == len(rows)
        for group in gold:
            group_rows = (
                [
                    row
                    for row in rows
                    if row.measurement_type == group["measurement_type"]
                    and row.source_unit == group["source_unit"]
                ]
                if domain == "river"
                else rows
            )
            for field in fields:
                values = [
                    getattr(row, field) for row in group_rows if getattr(row, field) is not None
                ]
                column = (
                    "precipitation_total_mm" if field == "precipitation_mm" else f"average_{field}"
                )
                expected = (
                    (sum(values) if field == "precipitation_mm" else sum(values) / len(values))
                    if values
                    else None
                )
                gold_matches &= _same_number(group[column], expected)
    check("downstream_gold", gold_matches, "Affected daily counts/aggregates match trusted Silver.")
    return checks


def verify_pipeline(session: Session, execution_id: str) -> PipelineExecution:
    execution = session.get(PipelineExecution, execution_id)
    if execution is None:
        raise LookupError("Pipeline execution not found.")
    if execution.status == "success":
        return execution
    run = (
        session.get(IngestionRun, execution.ingestion_run_id)
        if execution.ingestion_run_id
        else None
    )
    if run is None:
        raise ValueError("Pipeline execution has no persisted ingestion run to verify.")
    try:
        execution.verification = (
            verify_energy(session, run)
            if execution.domain == "energy"
            else _verify_data(session, run, execution.domain)
        )
        execution.status = (
            "success" if all(check["passed"] for check in execution.verification) else "failed"
        )
        execution.error = (
            None
            if execution.status == "success"
            else "; ".join(
                check["detail"] for check in execution.verification if not check["passed"]
            )
        )
    except (OSError, ValueError, KeyError, TypeError) as error:
        execution.status = "failed"
        execution.error = f"Verification failed: {type(error).__name__}: {error}"
        logger.exception("pipeline_verification_failed", extra={"run_id": execution.id})
    execution.completed_at = utc_now()
    session.commit()
    return execution


def execution_dict(execution: PipelineExecution) -> dict[str, Any]:
    return {
        "id": execution.id,
        "domain": execution.domain,
        "orchestrator_run_id": execution.orchestrator_run_id,
        "target_date": parse_utc(execution.target_date).date().isoformat()
        if execution.target_date
        else None,
        "ingestion_run_id": execution.ingestion_run_id,
        "status": execution.status,
        "started_at_utc": parse_utc(execution.started_at).isoformat(),
        "completed_at_utc": parse_utc(execution.completed_at).isoformat()
        if execution.completed_at
        else None,
        "error": execution.error,
        "verification": execution.verification,
    }


def pipeline_status(session: Session) -> list[dict[str, Any]]:
    status = []
    for domain, hours in (("river", 2), ("weather", 30), ("energy", 30)):
        source_id, model = {
            "river": ("ehyd-ogc-api", RiverObservation),
            "weather": ("open-meteo-archive", WeatherObservation),
            "energy": ("entsoe-energy", EnergyObservation),
        }[domain]
        latest_event = session.scalar(
            select(func.max(model.event_time)).where(model.source_id == source_id)
        )
        executions = session.scalars(
            select(PipelineExecution)
            .where(PipelineExecution.domain == domain)
            .order_by(PipelineExecution.started_at.desc())
            .limit(20)
        ).all()
        latest = executions[0] if executions else None
        successful = session.scalar(
            select(PipelineExecution)
            .where(PipelineExecution.domain == domain, PipelineExecution.status == "success")
            .order_by(PipelineExecution.started_at.desc())
            .limit(1)
        )
        failure = session.scalar(
            select(PipelineExecution)
            .where(PipelineExecution.domain == domain, PipelineExecution.status == "failed")
            .order_by(PipelineExecution.started_at.desc())
            .limit(1)
        )
        successful_ingestion = (
            session.get(IngestionRun, successful.ingestion_run_id) if successful else None
        )
        stale = successful_ingestion is None or utc_now() - parse_utc(
            successful_ingestion.started_at
        ) > timedelta(hours=hours)
        status.append(
            {
                "domain": domain,
                "source_id": source_id,
                "latest_event_time_utc": parse_utc(latest_event).isoformat()
                if latest_event
                else None,
                "latest_execution": execution_dict(latest) if latest else None,
                "latest_success": execution_dict(successful) if successful else None,
                "latest_verified_ingestion_at_utc": (
                    parse_utc(successful_ingestion.started_at).isoformat()
                    if successful_ingestion
                    else None
                ),
                "latest_failure": execution_dict(failure) if failure else None,
                "stale": stale,
                "stale_after_hours": hours,
                "recent_executions": [execution_dict(row) for row in executions],
                "note": (
                    "Application execution evidence only; consult Airflow for scheduler health."
                ),
            }
        )
    return status
