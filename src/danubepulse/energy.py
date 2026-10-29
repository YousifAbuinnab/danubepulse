"""Energy Bronze ingestion, revision-aware Silver and independently checked Gold."""

import hashlib
import json
import logging
import math
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from danubepulse.config import get_settings
from danubepulse.energy_sources import (
    AUSTRIA,
    REFERENCE,
    EnergyPoint,
    EnergySourceError,
    Product,
    fetch_xml,
    parse_csv,
    parse_xml,
    product_gold,
    product_source,
)
from danubepulse.ingestion_lock import serialized_ingestion
from danubepulse.models import (
    DataLineage,
    DataSource,
    EnergyObservation,
    IngestionRun,
    QualityResult,
)
from danubepulse.time_utils import parse_utc, utc_now

logger = logging.getLogger(__name__)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def _identity(point: EnergyPoint) -> str:
    return hashlib.sha256(_canonical(asdict(point))).hexdigest()


def _value(row: EnergyObservation) -> float | None:
    return getattr(
        row,
        {
            "generation": "generation_mw",
            "price": "price_eur_mwh",
            "load_forecast": "load_forecast_mw",
        }[row.metric],
    )


def _same_observation(point: EnergyPoint, row: EnergyObservation) -> bool:
    return (
        row.region == AUSTRIA
        and row.metric == point.metric
        and row.production_type == point.production_type
        and parse_utc(row.event_time) == point.start
        and parse_utc(row.interval_end) == point.end
        and row.duration_hours == point.hours
        and row.source_unit == point.unit
        and _value(row) == point.value
    )


def _revision_evidence(
    session: Session,
    current: list[EnergyObservation],
    source_id: str,
) -> dict[str, EnergyPoint]:
    """Unchanged responses still advance revision evidence, without mutating Silver."""
    targets = {row.source_record_id for row in current}
    if not targets:
        return {}
    links = session.execute(
        select(DataLineage, IngestionRun)
        .join(IngestionRun, IngestionRun.id == DataLineage.run_id)
        .where(
            IngestionRun.source_id == source_id,
            DataLineage.target_table == "energy_observations",
            DataLineage.target_record_id.in_(targets),
            DataLineage.source_record_id != DataLineage.target_record_id,
        )
        .order_by(IngestionRun.started_at, DataLineage.id)
    ).all()
    envelopes: dict[str, dict[str, EnergyPoint]] = {}
    evidence = {
        row.source_record_id: EnergyPoint(
            parse_utc(row.event_time),
            parse_utc(row.interval_end),
            row.metric,
            row.production_type,
            _value(row),
            row.source_unit,
            row.document_id,
            row.revision,
            parse_utc(row.source_published_at),
        )
        for row in current
    }
    for link, run in links:
        if run.id not in envelopes:
            envelope = json.loads(Path(run.bronze_path).read_text())
            if hashlib.sha256(_canonical(envelope["raw_response"])).hexdigest() != run.checksum:
                raise EnergySourceError("Unchanged-response revision evidence checksum mismatch.")
            start = datetime.combine(
                date.fromisoformat(run.request_parameters["target_date"]),
                datetime.min.time(),
                UTC,
            )
            parser = parse_csv if run.request_parameters["format"] == "csv" else parse_xml
            points = [
                p
                for kind, body in envelope["raw_response"].items()
                for p in parser(body.encode(), kind, start, start + timedelta(days=1))
            ]
            envelopes[run.id] = {_identity(p): p for p in points}
        point = envelopes[run.id].get(link.source_record_id)
        if point is None:
            raise EnergySourceError("Unchanged-response lineage has no matching Bronze point.")
        old = evidence.get(link.target_record_id)
        if old is None or (
            point.revision > old.revision
            if point.document_id == old.document_id
            else point.published_at > old.published_at
        ):
            evidence[link.target_record_id] = point
    return evidence


def _newer(point: EnergyPoint, old: EnergyObservation | EnergyPoint) -> bool:
    if point.document_id == old.document_id:
        if point.revision == old.revision:
            raise EnergySourceError("Conflicting values under the same document revision.")
        return point.revision > old.revision
    timestamp = old.published_at if isinstance(old, EnergyPoint) else old.source_published_at
    if point.published_at == parse_utc(timestamp):
        raise EnergySourceError("Conflicting documents have the same publication time.")
    return point.published_at > parse_utc(timestamp)


@serialized_ingestion("energy")
def ingest_energy(
    session: Session,
    bronze_dir: Path,
    target_date: date,
    *,
    payloads: dict[str, str] | None = None,
    import_format: str = "xml",
    reference: str = REFERENCE,
    terms_accepted: bool = False,
    product: Product = "energy",
    load_forecast_terms_accepted: bool = False,
) -> IngestionRun:
    """One UTC day per run. Imports require operator-confirmed source and reuse rights."""
    if target_date >= utc_now().date():
        raise EnergySourceError("Ingest only completed UTC days; no partial-day defaults.")
    start = datetime.combine(target_date, datetime.min.time(), UTC)
    end = start + timedelta(days=1)
    source_id = product_source(product)
    if product == "load_forecast" and not load_forecast_terms_accepted:
        raise EnergySourceError(
            "A65 needs separate permission review and --accept-load-forecast-terms; "
            "ENTSOE_TERMS_ACCEPTED does not authorize this product."
        )
    if product == "energy" and not terms_accepted:
        raise EnergySourceError("Confirm ENTSO-E reuse terms with ENTSOE_TERMS_ACCEPTED=true.")
    if reference != REFERENCE:
        raise EnergySourceError(
            "Only ENTSO-E Austrian exports are supported, not unverified APG CSV."
        )
    if import_format not in {"xml", "csv"}:
        raise EnergySourceError("Supported import formats are xml and normalized csv.")
    source = session.get(DataSource, source_id)
    if source is None:
        session.add(
            DataSource(
                id=source_id,
                name=(
                    "ENTSO-E Austrian day-ahead total load forecast (A65/A01, 6.1.B)"
                    if product == "load_forecast"
                    else "ENTSO-E Austrian aggregate generation and day-ahead prices"
                ),
                provenance="real",
                license=(
                    "ENTSO-E 6.1.B open-data list, CC BY 4.0; product-specific operator review"
                    if product == "load_forecast"
                    else "ENTSO-E Transparency Platform terms; operator acceptance"
                ),
                reference_url=REFERENCE,
                enabled=True,
            )
        )
        session.flush()
    run_id = str(uuid4())
    retrieved = utc_now()
    run = IngestionRun(
        id=run_id,
        source_id=source_id,
        started_at=retrieved,
        status="running",
        checksum="",
        bronze_path="",
        record_count=0,
        accepted_count=0,
        rejected_count=0,
        request_parameters={
            "target_date": target_date.isoformat(),
            "region": AUSTRIA,
            "format": import_format,
            "access": "operator_import" if payloads is not None else "api",
            "reference": reference,
            "product": product,
            "permission_review": (
                "A65 6.1.B operator-confirmed source/reuse review"
                if product == "load_forecast"
                else "A75/A44 operator-confirmed reuse review"
            ),
        },
    )
    session.add(run)
    session.commit()
    raw: dict[str, str] = {}
    points: list[EnergyPoint] = []
    checks: list[tuple[str, bool, int, str]] = []
    try:
        if payloads is None:
            settings = get_settings()
            for kind in (
                ("load_forecast",) if product == "load_forecast" else ("generation", "price")
            ):
                raw[kind] = fetch_xml(settings.entsoe_api_token, kind, start, end).decode("utf-8")
        else:
            allowed = {"load_forecast"} if product == "load_forecast" else {"generation", "price"}
            if not payloads or set(payloads) - allowed:
                raise EnergySourceError(f"Payloads must belong only to product {product}.")
            raw = payloads
        for kind, body in raw.items():
            parser = parse_xml if import_format == "xml" else parse_csv
            points.extend(parser(body.encode(), kind, start, end))
        checks.append(
            ("schema_and_units", True, 0, "Austria, metric, units and intervals validated.")
        )
        # Validate all revision decisions before mutating any trusted records.
        current = session.scalars(
            select(EnergyObservation).where(
                EnergyObservation.source_id == source_id,
                EnergyObservation.quality_status.in_(("valid", "missing")),
                EnergyObservation.event_time < end,
                EnergyObservation.interval_end > start,
            )
        ).all()
        existing_ids = set(
            session.scalars(
                select(EnergyObservation.source_record_id).where(
                    EnergyObservation.source_id == source_id
                )
            )
        )
        additions: list[tuple[EnergyPoint, str]] = []
        replacements: dict[int, EnergyObservation] = {}
        unchanged: list[tuple[EnergyPoint, EnergyObservation]] = []
        evidence = _revision_evidence(session, current, source_id)
        duplicates = 0
        for point in points:
            if _identity(point) in existing_ids:
                duplicates += 1
                continue
            overlaps = [
                old
                for old in current
                if old.metric == point.metric
                and old.production_type == point.production_type
                and parse_utc(old.event_time) < point.end
                and parse_utc(old.interval_end) > point.start
            ]
            if len(overlaps) == 1 and _same_observation(point, overlaps[0]):
                unchanged.append((point, overlaps[0]))
                duplicates += 1
                continue
            decisions = [_newer(point, evidence.get(old.source_record_id, old)) for old in overlaps]
            if decisions and any(decisions) and not all(decisions):
                raise EnergySourceError("Mixed older/newer overlapping provider revisions.")
            active = not decisions or all(decisions)
            if active:
                for old in overlaps:
                    matching = [
                        p
                        for p in points
                        if p.metric == old.metric
                        and p.production_type == old.production_type
                        and p.start >= parse_utc(old.event_time)
                        and p.end <= parse_utc(old.interval_end)
                    ]
                    # A coarser revision may wholly replace several finer intervals.
                    if not (
                        point.start <= parse_utc(old.event_time)
                        and point.end >= parse_utc(old.interval_end)
                    ) and not math.isclose(sum(p.hours for p in matching), old.duration_hours):
                        raise EnergySourceError(
                            "A revision would partially replace an old interval."
                        )
                    replacements[old.id] = old
            additions.append(
                (point, ("missing" if point.value is None else "valid") if active else "superseded")
            )
        for old in replacements.values():
            old.quality_status = "superseded"
        for point, old in unchanged:
            session.add(
                DataLineage(
                    run_id=run.id,
                    source_record_id=_identity(point),
                    target_table="energy_observations",
                    target_record_id=old.source_record_id,
                )
            )
        for point, status in additions:
            identity = _identity(point)
            session.add(
                EnergyObservation(
                    source_id=source_id,
                    source_record_id=identity,
                    run_id=run.id,
                    region=AUSTRIA,
                    event_time=point.start,
                    interval_end=point.end,
                    duration_hours=point.hours,
                    metric=point.metric,
                    production_type=point.production_type,
                    document_id=point.document_id,
                    revision=point.revision,
                    source_unit=point.unit,
                    source_published_at=point.published_at,
                    ingested_at=retrieved,
                    processed_at=utc_now(),
                    generation_mw=point.value if point.metric == "generation" else None,
                    price_eur_mwh=point.value if point.metric == "price" else None,
                    load_forecast_mw=point.value if point.metric == "load_forecast" else None,
                    quality_status=status,
                )
            )
            session.add(
                DataLineage(
                    run_id=run.id,
                    source_record_id=identity,
                    target_table="energy_observations",
                    target_record_id=identity,
                )
            )
        missing = sum(point.value is None for point in points)
        groups = {(point.metric, point.production_type) for point in points}
        incomplete = sum(
            not math.isclose(
                sum(
                    p.hours
                    for p in points
                    if (p.metric, p.production_type) == key and p.value is not None
                ),
                24.0,
            )
            for key in groups
        )
        checks.extend(
            [
                ("missing_values", missing == 0, missing, "Nulls are retained, never zero-filled."),
                (
                    "interval_coverage",
                    incomplete == 0,
                    incomplete,
                    "Each returned series requires 24 valid hours for a complete UTC day; "
                    "absent production categories are not inferred to be zero.",
                ),
                (
                    "duplicate_events",
                    duplicates == 0,
                    duplicates,
                    "Unchanged interval values are reused even when response metadata changes; "
                    "Bronze and lineage retain each response, including revision evidence.",
                ),
            ]
        )
        run.record_count = len(points)
        run.source_published_at = max(point.published_at for point in points)
        run.accepted_count = len(additions)
        run.rejected_count = missing
        run.status = "degraded" if missing or incomplete else "completed"
    except (EnergySourceError, UnicodeError) as error:
        run.status = "failed"
        run.rejected_count = max(len(points), 1)
        checks.append(("schema_and_units", False, run.rejected_count, str(error)))
        logger.warning("energy_ingestion_rejected", extra={"run_id": run.id, "status": "failed"})
    # Bronze is immutable, including failed responses and original revisions.
    directory = bronze_dir / source_id
    directory.mkdir(parents=True, exist_ok=True)
    run.checksum = hashlib.sha256(_canonical(raw)).hexdigest()
    path = directory / f"{run.id}.json"
    envelope = {
        "provenance": "real",
        "source_id": source_id,
        "run_id": run.id,
        "retrieved_at": retrieved.isoformat(),
        "request_parameters": run.request_parameters,
        "checksum": run.checksum,
        "raw_response": raw,
        "import_notice": (
            "Operator-provided export; not independently authenticated."
            if payloads is not None
            else "ENTSO-E API request; consult run status for whether a response was obtained."
        ),
        "points": [asdict(point) for point in points],
    }
    with path.open("x", encoding="utf-8") as handle:
        json.dump(envelope, handle, default=str)
    run.bronze_path = str(path)
    run.completed_at = utc_now()
    for name, passed, count, detail in checks:
        session.add(
            QualityResult(
                run_id=run.id,
                check_name=name,
                status="passed" if passed else "warning" if run.status != "failed" else "failed",
                affected_count=count,
                details=detail,
                checked_at=utc_now(),
                phase="initial",
            )
        )
    session.commit()
    logger.info(
        "energy_ingestion_finished",
        extra={
            "run_id": run.id,
            "status": run.status,
            "record_count": run.record_count,
        },
    )
    return run


def verify_energy(session: Session, run: IngestionRun) -> list[dict[str, Any]]:
    """Check raw checksum, persisted latest revisions, overlap and actual SQL Gold."""
    envelope = json.loads(Path(run.bronze_path).read_text())
    product = "load_forecast" if run.source_id == product_source("load_forecast") else "energy"
    checks = [
        {
            "check": "bronze_integrity",
            "passed": hashlib.sha256(_canonical(envelope["raw_response"])).hexdigest()
            == run.checksum
            and envelope["provenance"] == "real"
            and envelope["source_id"] == run.source_id,
            "detail": "Immutable provider response and provenance.",
        }
    ]
    quality = session.scalars(select(QualityResult).where(QualityResult.run_id == run.id)).all()
    required = {"schema_and_units", "missing_values", "interval_coverage"}
    quality_status = {row.check_name: row.status for row in quality}
    checks.append(
        {
            "check": "source_quality",
            "passed": run.status == "completed"
            and all(quality_status.get(name) == "passed" for name in required),
            "detail": "Strict schema, units, non-null values and complete returned series.",
        }
    )
    current = session.scalars(
        select(EnergyObservation)
        .where(
            EnergyObservation.source_id == run.source_id,
            EnergyObservation.quality_status == "valid",
        )
        .order_by(
            EnergyObservation.metric,
            EnergyObservation.production_type,
            EnergyObservation.event_time,
        )
    ).all()
    previous: dict[tuple, EnergyObservation] = {}
    unique = True
    for row in current:
        key = (row.metric, row.production_type)
        old = previous.get(key)
        if old and parse_utc(old.interval_end) > parse_utc(row.event_time):
            unique = False
        previous[key] = row
    checks.append(
        {
            "check": "semantic_uniqueness",
            "passed": unique,
            "detail": "No trusted overlapping intervals per metric/production type.",
        }
    )
    start = datetime.combine(
        date.fromisoformat(run.request_parameters["target_date"]), datetime.min.time(), UTC
    )
    end = start + timedelta(days=1)
    kind_parser = parse_csv if run.request_parameters["format"] == "csv" else parse_xml
    points = [
        p
        for kind, body in envelope["raw_response"].items()
        for p in kind_parser(body.encode(), kind, start, end)
    ]
    history = {
        row.source_record_id: row
        for row in session.scalars(
            select(EnergyObservation).where(EnergyObservation.source_id == run.source_id)
        )
    }
    links = {
        row.source_record_id: row.target_record_id
        for row in session.scalars(
            select(DataLineage).where(
                DataLineage.run_id == run.id,
                DataLineage.target_table == "energy_observations",
            )
        )
    }
    persisted = True
    for point in points:
        identity = _identity(point)
        row = history.get(links.get(identity, identity))
        shared = links.get(identity, identity) != identity
        if (
            row is None
            or not _same_observation(point, row)
            or (
                not shared
                and (
                    row.document_id != point.document_id
                    or row.revision != point.revision
                    or parse_utc(row.source_published_at) != point.published_at
                )
            )
        ):
            persisted = False
            continue
        if row.quality_status == "superseded":
            newer = [
                candidate
                for candidate in current
                if candidate.metric == point.metric
                and candidate.production_type == point.production_type
                and parse_utc(candidate.event_time) < point.end
                and parse_utc(candidate.interval_end) > point.start
            ]
            covered = sum(
                (
                    min(parse_utc(c.interval_end), point.end)
                    - max(parse_utc(c.event_time), point.start)
                ).total_seconds()
                / 3600
                for c in newer
            )
            if not math.isclose(covered, point.hours):
                persisted = False
        elif row.quality_status != ("missing" if point.value is None else "valid"):
            persisted = False
    checks.append(
        {
            "check": "persisted_silver",
            "passed": persisted,
            "detail": "Original values persist directly or via run-specific unchanged-value "
            "lineage; response metadata remains in immutable Bronze.",
        }
    )
    expected: dict[tuple, dict[str, float]] = {}
    for row in current:
        key = (parse_utc(row.event_time).date().isoformat(), row.metric, row.production_type)
        item = expected.setdefault(
            key,
            {
                "count": 0,
                "hours": 0,
                "weighted": 0,
                "minimum": math.inf,
                "maximum": -math.inf,
            },
        )
        item["count"] += 1
        item["hours"] += row.duration_hours
        value = _value(row)
        item["weighted"] += value * row.duration_hours
        item["minimum"] = min(item["minimum"], value)
        item["maximum"] = max(item["maximum"], value)
    gold = session.execute(text(f"SELECT * FROM {product_gold(product)}")).mappings().all()
    correct = len(gold) == len(expected)
    for row in gold:
        key = (str(row["observation_date"]), row["metric"], row["production_type"])
        item = expected.get(key)
        if item is None:
            correct = False
            continue
        actual = (
            row["forecast_demand_mwh"]
            if product == "load_forecast"
            else (
                row["generation_mwh"]
                if row["metric"] == "generation"
                else (row["average_price_eur_mwh"] * row["price_coverage_hours"])
            )
        )
        correct &= (
            row["observation_count"] == item["count"]
            and math.isclose(row["coverage_hours"], item["hours"])
            and actual is not None
            and math.isclose(actual, item["weighted"], abs_tol=1e-8)
        )
        average = (
            row["average_load_forecast_mw"]
            if product == "load_forecast"
            else (
                row["average_generation_mw"]
                if row["metric"] == "generation"
                else row["average_price_eur_mwh"]
            )
        )
        correct &= average is not None and math.isclose(
            average, item["weighted"] / item["hours"], abs_tol=1e-8
        )
        if row["metric"] == "price":
            correct &= (
                row["minimum_price_eur_mwh"] == item["minimum"]
                and row["maximum_price_eur_mwh"] == item["maximum"]
            )
    checks.append(
        {
            "check": "downstream_gold",
            "passed": bool(expected) and correct,
            "detail": "Duration-weighted MWh/prices and coverage agree with persisted Silver.",
        }
    )
    return checks
