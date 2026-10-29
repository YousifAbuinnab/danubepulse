"""HTTP API for data discovery, trusted analytics, and incident replay."""

import hmac
import json
import logging
import logging.config
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, object_session

from danubepulse.config import get_settings
from danubepulse.db import get_session
from danubepulse.energy_sources import (
    AUSTRIA,
    PRODUCTION_TYPES,
    Product,
    product_gold,
    product_source,
)
from danubepulse.ingestion_lock import IngestionBusy
from danubepulse.lakehouse import InvalidManifest, lakehouse_status
from danubepulse.models import (
    DataSource,
    EnergyObservation,
    Incident,
    IngestionRun,
    QualityResult,
    QuarantinedRecord,
    RiverObservation,
    Station,
    StreamingOutbox,
    WeatherObservation,
)
from danubepulse.orchestration import (
    execute_pipeline,
    execution_dict,
    pipeline_status,
    verify_pipeline,
)
from danubepulse.pipeline import (
    ingest_real_river,
    ingest_real_weather,
    investigate_incident,
    replay_incident,
    replay_is_verified,
    reset_demo,
    run_demo,
    simulate_scenario,
)
from danubepulse.streaming_events import EVENT_TOPIC, Notification, enqueue
from danubepulse.streaming_monitor import streaming_status
from danubepulse.time_utils import parse_utc, utc_now, vienna_time


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.now().astimezone().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in ("run_id", "source_id", "incident_id", "status", "record_count", "checksum"):
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        return json.dumps(entry, default=str)


handler = logging.StreamHandler()
handler.setFormatter(JsonLogFormatter())
logging.basicConfig(level=get_settings().log_level, handlers=[handler], force=True)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DanubePulse",
    version="0.1.0",
    description=(
        "Austrian river, weather, and energy data exploration. "
        "Correlations are descriptive and do not establish causation."
    ),
)


@app.get("/v1/lakehouse")
def lakehouse_explorer() -> dict[str, Any]:
    try:
        return lakehouse_status(get_settings().lakehouse_root)
    except (InvalidManifest, OSError) as error:
        logger.exception("lakehouse_manifest_read_failed")
        raise HTTPException(
            status_code=503, detail="Lakehouse status is unreadable; inspect the API logs."
        ) from error


@app.get("/v1/streaming")
def streaming_monitor(session: Session = Depends(get_session)) -> dict[str, Any]:
    return streaming_status(session)


@app.get("/v1/streaming/schema")
def streaming_schema() -> dict[str, Any]:
    return Notification.model_json_schema()


def _authorize_demo_action(token: str | None) -> None:
    expected = get_settings().demo_control_token
    if not expected or len(expected) < 32:
        raise HTTPException(
            status_code=503,
            detail="Demo actions require a DEMO_CONTROL_TOKEN of at least 32 characters.",
        )
    if token is None or not hmac.compare_digest(token.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="A valid demo control token is required.")


@app.get("/v1/energy")
def energy_explorer(
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(500, ge=1, le=5000),
    product: Product = "energy",
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must not follow end_date.")
    source_id = product_source(product)
    statement = select(EnergyObservation).where(
        EnergyObservation.source_id == source_id, EnergyObservation.quality_status == "valid"
    )
    if start_date:
        statement = statement.where(
            EnergyObservation.event_time >= datetime.combine(start_date, datetime.min.time(), UTC)
        )
    if end_date:
        statement = statement.where(
            EnergyObservation.event_time <= datetime.combine(end_date, datetime.max.time(), UTC)
        )
    rows = session.scalars(
        statement.order_by(EnergyObservation.event_time.desc()).limit(limit)
    ).all()
    daily = (
        session.execute(
            text(
                f"SELECT * FROM {product_gold(product)} "
                "WHERE (CAST(:start AS VARCHAR) IS NULL "
                "OR CAST(observation_date AS VARCHAR) >= :start) "
                "AND (CAST(:end AS VARCHAR) IS NULL OR CAST(observation_date AS VARCHAR) <= :end) "
                "ORDER BY observation_date DESC, metric, production_type LIMIT 5000"
            ),
            {
                "start": start_date.isoformat() if start_date else None,
                "end": end_date.isoformat() if end_date else None,
            },
        )
        .mappings()
        .all()
    )
    run = session.scalar(
        select(IngestionRun)
        .where(IngestionRun.source_id == source_id)
        .order_by(IngestionRun.started_at.desc())
        .limit(1)
    )
    checks = (
        session.scalars(select(QualityResult).where(QualityResult.run_id == run.id)).all()
        if run
        else []
    )
    latest = session.scalar(
        select(func.max(EnergyObservation.event_time)).where(
            EnergyObservation.source_id == source_id, EnergyObservation.quality_status == "valid"
        )
    )
    return {
        "provenance": "real",
        "product": product,
        "source_id": source_id,
        "product_label": (
            "A65/A01 Austrian day-ahead total load forecast (6.1.B), NOT actual demand"
            if product == "load_forecast"
            else "A75 generation / A44 day-ahead prices"
        ),
        "source": "ENTSO-E Transparency Platform; Austrian bidding/control area aggregates",
        "scope": "APG control area / Austrian bidding zone, never an individual power plant.",
        "region": AUSTRIA,
        "access_configured": bool(get_settings().entsoe_api_token),
        "terms_accepted": get_settings().entsoe_terms_accepted,
        "permission_note": (
            "A65 requires a separate one-run --accept-load-forecast-terms review. "
            "API authentication is not reuse permission; no automatic ingestion is enabled."
            if product == "load_forecast"
            else "A75/A44 remain gated by ENTSOE_TERMS_ACCEPTED."
        ),
        "polling": "Airflow energy DAG is initially paused; operator approval required.",
        "attribution": (
            "Source: ENTSO-E Transparency Platform; original data providers including APG. "
            + (
                "A65 6.1.B day-ahead total load forecast; normalized to UTC and integrated "
                "to forecast MWh; CC BY 4.0 https://creativecommons.org/licenses/by/4.0/; "
                "review https://transparencyplatform.zendesk.com/hc/en-us/articles/"
                "40921911218961-Legal-Terms-and-Conditions"
                if product == "load_forecast"
                else ""
            )
        ),
        "latest_event_time_utc": parse_utc(latest).isoformat() if latest else None,
        "latest_retrieval_time_utc": parse_utc(run.started_at).isoformat() if run else None,
        "latest_run_status": run.status if run else "not_connected",
        "stale": latest is None or utc_now() - parse_utc(latest) > timedelta(days=4),
        "freshness_note": "Latest event older than four days; independent of a recent re-import.",
        "quality": [
            {
                "check": q.check_name,
                "status": q.status,
                "details": q.details,
                "affected_count": q.affected_count,
            }
            for q in checks
        ],
        "observations": [
            {
                "event_time": parse_utc(row.event_time).isoformat(),
                "event_time_vienna": vienna_time(row.event_time),
                "interval_end": parse_utc(row.interval_end).isoformat(),
                "metric": row.metric,
                "production_type": row.production_type,
                "production_name": (
                    "Day-ahead total load forecast"
                    if row.metric == "load_forecast"
                    else PRODUCTION_TYPES.get(row.production_type, "Day-ahead price")
                ),
                "generation_mw": row.generation_mw,
                "price_eur_mwh": row.price_eur_mwh,
                "load_forecast_mw": row.load_forecast_mw,
                "duration_hours": row.duration_hours,
                "revision": row.revision,
            }
            for row in rows
        ],
        "daily": [dict(row) for row in daily],
        "note": (
            "Forecast, not measured consumption or generation. Duration-weighted forecast MW "
            "and integrated forecast demand MWh over the covered UTC intervals only. "
            "Missing intervals remain gaps; a UTC day is not a Vienna market day. "
            "This is latest-revision history, not a reconstruction of forecasts "
            "known at gate closure."
        )
        if product == "load_forecast"
        else (
            "UTC daily totals; MW is interval-average power, MWh is power times duration. "
            "Prices are time-weighted EUR/MWh, not volume-weighted market prices. "
            "Partial series and absent production types are not zero-filled. "
            "Current pages are bounded; narrow the date filter for larger histories."
        ),
    }


@app.get("/v1/energy/comparisons")
def energy_comparisons(
    river_station_id: str = Query(min_length=1),
    weather_station_id: str = Query(min_length=1),
    product: Product = "energy",
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    rows = (
        session.execute(
            text(f"""
        SELECT e.*, r.average_water_level_m, r.average_discharge_m3_s,
               r.observation_count AS river_count, r.measurement_type,
               w.average_temperature_c, w.precipitation_total_mm,
               w.observation_count AS weather_count
        FROM {product_gold(product)} e
        JOIN gold_real_river_daily r ON e.observation_date=r.observation_date
        JOIN gold_real_weather_daily w ON e.observation_date=w.observation_date
        WHERE r.location=:river AND w.location=:weather
        ORDER BY e.observation_date DESC, e.metric, e.production_type, r.measurement_type
        LIMIT 2000
    """),
            {"river": river_station_id, "weather": weather_station_id},
        )
        .mappings()
        .all()
    )
    return {
        "provenance": "real",
        "product": product,
        "rows": [dict(row) for row in rows],
        "river_station": river_station_id,
        "weather_grid": weather_station_id,
        "note": (
            "Matched UTC dates only, with per-series coverage displayed. Sparse current river "
            "snapshots are not daily means of a continuous record. Weather is reanalysis. "
            "National/area energy generation does not represent a selected river power plant. "
            "A65 is a forecast, not observed demand; latest revisions can include hindsight. "
            "Associations do not establish causation. Empty overlap is not interpolated."
        ),
    }


@app.exception_handler(IngestionBusy)
async def ingestion_busy_handler(_request, error: IngestionBusy) -> JSONResponse:
    logger.warning("ingestion_overlap_rejected", extra={"status": "busy"})
    return JSONResponse(status_code=409, content={"detail": str(error)})


class PipelineRequest(BaseModel):
    orchestrator_run_id: str = Field(min_length=1, max_length=300)
    target_date: date | None = None


@app.post("/v1/pipelines/{domain}/ingest")
def pipeline_ingest_endpoint(
    domain: Literal["river", "weather", "energy"],
    request: PipelineRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "real":
        raise HTTPException(status_code=409, detail="Automated ingestion requires DATA_MODE=real.")
    try:
        execution = execute_pipeline(
            session,
            get_settings().bronze_dir,
            domain,
            request.orchestrator_run_id,
            request.target_date,
        )
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    return execution_dict(execution)


@app.post("/v1/pipelines/executions/{execution_id}/verify")
def pipeline_verify_endpoint(
    execution_id: str, session: Session = Depends(get_session)
) -> dict[str, Any]:
    if get_settings().data_mode != "real":
        raise HTTPException(
            status_code=409, detail="Pipeline verification requires DATA_MODE=real."
        )
    try:
        return execution_dict(verify_pipeline(session, execution_id))
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.get("/v1/pipelines")
def pipelines_endpoint(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    return pipeline_status(session)


@app.get("/health")
def health(session: Session = Depends(get_session)) -> dict[str, str]:
    session.execute(text("SELECT 1"))
    return {"status": "ok", "service": "danubepulse-api"}


@app.get("/v1/sources")
def sources(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    rows = session.scalars(select(DataSource).order_by(DataSource.id)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "provenance": row.provenance,
            "license": row.license,
            "reference_url": row.reference_url,
            "enabled": row.enabled,
        }
        for row in rows
    ]


@app.get("/v1/summary")
def summary(session: Session = Depends(get_session)) -> dict[str, Any]:
    river_count = (
        session.scalar(
            select(func.count())
            .select_from(RiverObservation)
            .join(DataSource, DataSource.id == RiverObservation.source_id)
            .where(DataSource.provenance == "synthetic")
        )
        or 0
    )
    weather_count = (
        session.scalar(
            select(func.count())
            .select_from(WeatherObservation)
            .join(DataSource, DataSource.id == WeatherObservation.source_id)
            .where(DataSource.provenance == "synthetic")
        )
        or 0
    )
    river_latest = session.scalar(
        select(func.max(RiverObservation.event_time))
        .join(DataSource, DataSource.id == RiverObservation.source_id)
        .where(DataSource.provenance == "synthetic")
    )
    weather_latest = session.scalar(
        select(func.max(WeatherObservation.event_time))
        .join(DataSource, DataSource.id == WeatherObservation.source_id)
        .where(DataSource.provenance == "synthetic")
    )
    active_statuses = ("detected", "investigating", "recovering", "failed_recovery", "reset")
    open_incidents = (
        session.scalar(
            select(func.count()).select_from(Incident).where(Incident.status.in_(active_statuses))
        )
        or 0
    )
    recovered = session.scalars(select(Incident).where(Incident.status == "recovered")).all()
    unverified_incidents = sum(not replay_is_verified(incident) for incident in recovered)
    daily = session.execute(
        text(
            "SELECT observation_date, domain, location, observation_count, "
            "average_primary, average_secondary FROM gold_daily_observations "
            "ORDER BY observation_date, domain, location"
        )
    ).mappings()
    return {
        "provenance": "synthetic",
        "data_mode": get_settings().data_mode,
        "river_observations": river_count,
        "weather_observations": weather_count,
        "energy_observations": 0,
        "latest_event_time_utc": {
            "river": river_latest.isoformat() if river_latest else None,
            "weather": weather_latest.isoformat() if weather_latest else None,
        },
        "latest_event_time_vienna": {
            "river": vienna_time(river_latest).isoformat() if river_latest else None,
            "weather": vienna_time(weather_latest).isoformat() if weather_latest else None,
        },
        "open_incidents": open_incidents,
        "unverified_incidents": unverified_incidents,
        "analytics_trust": ("degraded" if open_incidents or unverified_incidents else "trusted"),
        "correlation_note": (
            "This overview is synthetic-only; real energy is in Energy Explorer. "
            "No causal claim is made; "
            "any future correlations are descriptive only."
        ),
        "daily": [dict(row) for row in daily],
    }


def _real_run_dict(session: Session, run: IngestionRun | None) -> dict[str, Any] | None:
    if run is None:
        return None
    checks = session.scalars(
        select(QualityResult).where(QualityResult.run_id == run.id).order_by(QualityResult.id)
    ).all()
    return {
        "id": run.id,
        "source_id": run.source_id,
        "status": run.status,
        "record_count": run.record_count,
        "accepted_count": run.accepted_count,
        "rejected_count": run.rejected_count,
        "checksum": run.checksum,
        "started_at_utc": parse_utc(run.started_at).isoformat(),
        "completed_at_utc": parse_utc(run.completed_at).isoformat() if run.completed_at else None,
        "request_parameters": run.request_parameters,
        "quality": [
            {
                "check_name": check.check_name,
                "status": check.status,
                "details": check.details,
                "affected_count": check.affected_count,
            }
            for check in checks
        ],
    }


@app.get("/v1/real/catalog")
def real_catalog(session: Session = Depends(get_session)) -> dict[str, Any]:
    real_source_ids = ("ehyd-ogc-api", "open-meteo-archive")
    sources_by_id = {
        source.id: source
        for source in session.scalars(
            select(DataSource).where(DataSource.id.in_(real_source_ids))
        ).all()
    }
    stations = session.scalars(
        select(Station)
        .join(DataSource, DataSource.id == Station.source_id)
        .where(DataSource.provenance == "real")
        .order_by(Station.kind, Station.name)
    ).all()
    station_rows = []
    for station in stations:
        if station.kind == "river":
            observations = session.scalars(
                select(RiverObservation)
                .where(RiverObservation.station_id == station.id)
                .order_by(RiverObservation.event_time.desc())
                .limit(1)
            ).all()
            latest = observations[0] if observations else None
            count = (
                session.scalar(
                    select(func.count())
                    .select_from(RiverObservation)
                    .where(RiverObservation.station_id == station.id)
                )
                or 0
            )
            latest_values = (
                {
                    "recorded_value": latest.source_value,
                    "source_unit": latest.source_unit,
                    "measurement_type": latest.measurement_type,
                    "water_level_m": latest.water_level_m,
                    "discharge_m3_s": latest.discharge_m3_s,
                    "measurement_reference": latest.measurement_reference,
                    "quality_status": latest.quality_status,
                    "event_time_utc": parse_utc(latest.event_time).isoformat(),
                    "event_time_vienna": vienna_time(latest.event_time).isoformat(),
                }
                if latest
                else None
            )
        else:
            observations = session.scalars(
                select(WeatherObservation)
                .where(WeatherObservation.station_id == station.id)
                .order_by(WeatherObservation.event_time.desc())
                .limit(1)
            ).all()
            latest = observations[0] if observations else None
            count = (
                session.scalar(
                    select(func.count())
                    .select_from(WeatherObservation)
                    .where(WeatherObservation.station_id == station.id)
                )
                or 0
            )
            latest_values = (
                {
                    "temperature_c": latest.temperature_c,
                    "precipitation_mm": latest.precipitation_mm,
                    "relative_humidity_percent": latest.relative_humidity_percent,
                    "wind_speed_m_s": latest.wind_speed_m_s,
                    "source_model": latest.source_model,
                    "source_units": latest.source_units,
                    "quality_status": latest.quality_status,
                    "event_time_utc": parse_utc(latest.event_time).isoformat(),
                    "event_time_vienna": vienna_time(latest.event_time).isoformat(),
                }
                if latest
                else None
            )
        source = sources_by_id[station.source_id]
        station_rows.append(
            {
                "id": station.id,
                "name": station.name,
                "kind": station.kind,
                "source_id": station.source_id,
                "source_name": source.name,
                "latitude": station.latitude,
                "longitude": station.longitude,
                "observation_count": count,
                "latest": latest_values,
            }
        )

    latest_runs = {}
    latest_success = {}
    for source_id in real_source_ids:
        latest = session.scalar(
            select(IngestionRun)
            .where(IngestionRun.source_id == source_id)
            .order_by(IngestionRun.started_at.desc())
            .limit(1)
        )
        successful = session.scalar(
            select(IngestionRun)
            .where(
                IngestionRun.source_id == source_id,
                IngestionRun.status.in_(("completed", "degraded")),
            )
            .order_by(IngestionRun.started_at.desc())
            .limit(1)
        )
        latest_runs[source_id] = _real_run_dict(session, latest)
        latest_success[source_id] = _real_run_dict(session, successful)

    source_descriptions = {
        "ehyd-ogc-api": {
            "attribution": "Datenquelle: ehyd.gv.at",
            "attribution_url": "https://ehyd.gv.at/",
            "license_url": None,
            "data_character": (
                "Observed current gauge readings; this collection is not a historical series."
            ),
        },
        "open-meteo-archive": {
            "attribution": "Weather data by Open-Meteo.com",
            "attribution_url": "https://open-meteo.com/",
            "license_url": "https://creativecommons.org/licenses/by/4.0/",
            "data_character": (
                "Historical weather model/reanalysis; not a local station observation."
            ),
        },
    }
    return {
        "data_mode": get_settings().data_mode,
        "sources": [
            {
                "id": source_id,
                "name": sources_by_id[source_id].name if source_id in sources_by_id else source_id,
                "license": sources_by_id[source_id].license if source_id in sources_by_id else None,
                "reference_url": (
                    sources_by_id[source_id].reference_url if source_id in sources_by_id else None
                ),
                **source_descriptions[source_id],
            }
            for source_id in real_source_ids
        ],
        "stations": station_rows,
        "latest_runs": latest_runs,
        "latest_successful_runs": latest_success,
    }


@app.get("/v1/real/observations")
def real_observations(
    domain: Literal["river", "weather"] = Query(),
    station_id: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
    session: Session = Depends(get_session),
) -> list[dict[str, Any]]:
    if domain == "river":
        statement = (
            select(RiverObservation, Station)
            .join(Station, Station.id == RiverObservation.station_id)
            .join(DataSource, DataSource.id == RiverObservation.source_id)
            .where(DataSource.provenance == "real")
            .order_by(RiverObservation.event_time.desc())
            .limit(limit)
        )
        if station_id:
            statement = statement.where(RiverObservation.station_id == station_id)
        rows = session.execute(statement).all()
        return [
            {
                "domain": domain,
                "station_id": station.id,
                "station_name": station.name,
                "latitude": station.latitude,
                "longitude": station.longitude,
                "source_record_id": observation.source_record_id,
                "source_id": observation.source_id,
                "event_time_utc": parse_utc(observation.event_time).isoformat(),
                "event_time_vienna": vienna_time(observation.event_time).isoformat(),
                "measurement_type": observation.measurement_type,
                "source_value": observation.source_value,
                "source_unit": observation.source_unit,
                "water_level_m": observation.water_level_m,
                "discharge_m3_s": observation.discharge_m3_s,
                "measurement_reference": observation.measurement_reference,
                "quality_status": observation.quality_status,
            }
            for observation, station in rows
        ]
    statement = (
        select(WeatherObservation, Station)
        .join(Station, Station.id == WeatherObservation.station_id)
        .join(DataSource, DataSource.id == WeatherObservation.source_id)
        .where(DataSource.provenance == "real")
        .order_by(WeatherObservation.event_time.desc())
        .limit(limit)
    )
    if station_id:
        statement = statement.where(WeatherObservation.station_id == station_id)
    rows = session.execute(statement).all()
    return [
        {
            "domain": domain,
            "station_id": station.id,
            "station_name": station.name,
            "latitude": station.latitude,
            "longitude": station.longitude,
            "source_record_id": observation.source_record_id,
            "source_id": observation.source_id,
            "event_time_utc": parse_utc(observation.event_time).isoformat(),
            "event_time_vienna": vienna_time(observation.event_time).isoformat(),
            "temperature_c": observation.temperature_c,
            "precipitation_mm": observation.precipitation_mm,
            "relative_humidity_percent": observation.relative_humidity_percent,
            "wind_speed_m_s": observation.wind_speed_m_s,
            "source_model": observation.source_model,
            "source_units": observation.source_units,
            "quality_status": observation.quality_status,
        }
        for observation, station in rows
    ]


@app.get("/v1/real/analytics")
def real_analytics(session: Session = Depends(get_session)) -> dict[str, Any]:
    river = session.execute(
        text(
            "SELECT observation_date, location, measurement_type, source_unit, "
            "observation_count, average_water_level_m, minimum_water_level_m, "
            "maximum_water_level_m, average_discharge_m3_s, minimum_discharge_m3_s, "
            "maximum_discharge_m3_s, latest_event_time "
            "FROM gold_real_river_daily ORDER BY observation_date, location"
        )
    ).mappings()
    weather = session.execute(
        text(
            "SELECT observation_date, location, observation_count, "
            "temperature_observation_count, average_temperature_c, precipitation_total_mm, "
            "humidity_observation_count, average_relative_humidity_percent, "
            "wind_observation_count, average_wind_speed_m_s, latest_event_time "
            "FROM gold_real_weather_daily ORDER BY observation_date, location"
        )
    ).mappings()
    return {
        "provenance": "real",
        "river_daily": [dict(row) for row in river],
        "weather_daily": [dict(row) for row in weather],
    }


def _real_ingestion_response(session: Session, run: IngestionRun) -> dict[str, Any]:
    return _real_run_dict(session, run) or {}


@app.post("/v1/real/ingest/river")
def ingest_real_river_endpoint(
    force: bool = Query(default=False),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "real":
        raise HTTPException(
            status_code=409,
            detail=(
                "Real-source ingestion requires DATA_MODE=real; synthetic demo mode is isolated."
            ),
        )
    try:
        run = ingest_real_river(session, Path(get_settings().bronze_dir), force=force)
    except (OSError, ValueError, RuntimeError) as error:
        session.rollback()
        logger.exception("real_river_ingestion_failed")
        raise HTTPException(status_code=502, detail=str(error)) from error
    if run.status == "failed":
        raise HTTPException(status_code=502, detail=_real_ingestion_response(session, run))
    return _real_ingestion_response(session, run)


@app.post("/v1/real/ingest/weather")
def ingest_real_weather_endpoint(
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    models: str | None = Query(default=None, max_length=80),
    force: bool = Query(default=False),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "real":
        raise HTTPException(
            status_code=409,
            detail=(
                "Real-source ingestion requires DATA_MODE=real; synthetic demo mode is isolated."
            ),
        )
    try:
        run = ingest_real_weather(
            session,
            Path(get_settings().bronze_dir),
            start_date=start_date,
            end_date=end_date,
            models=models,
            force=force,
        )
    except (OSError, ValueError, RuntimeError) as error:
        session.rollback()
        logger.exception("real_weather_ingestion_failed")
        raise HTTPException(status_code=502, detail=str(error)) from error
    if run.status == "failed":
        raise HTTPException(status_code=502, detail=_real_ingestion_response(session, run))
    return _real_ingestion_response(session, run)


@app.get("/v1/observations")
def observations(
    domain: str = Query(pattern="^(river|weather)$"),
    limit: int = Query(default=500, ge=1, le=5000),
    session: Session = Depends(get_session),
) -> list[dict[str, Any]]:
    if domain == "river":
        rows = session.scalars(
            select(RiverObservation)
            .join(DataSource, DataSource.id == RiverObservation.source_id)
            .where(DataSource.provenance == "synthetic")
            .order_by(RiverObservation.event_time.desc())
            .limit(limit)
        )
        return [
            {
                "domain": domain,
                "station_id": row.station_id,
                "source_record_id": row.source_record_id,
                "event_time_utc": row.event_time.isoformat(),
                "event_time_vienna": vienna_time(row.event_time).isoformat(),
                "water_level_m": row.water_level_m,
                "discharge_m3_s": row.discharge_m3_s,
                "quality_status": row.quality_status,
                "source_id": row.source_id,
            }
            for row in rows
        ]
    rows = session.scalars(
        select(WeatherObservation)
        .join(DataSource, DataSource.id == WeatherObservation.source_id)
        .where(DataSource.provenance == "synthetic")
        .order_by(WeatherObservation.event_time.desc())
        .limit(limit)
    )
    return [
        {
            "domain": domain,
            "station_id": row.station_id,
            "source_record_id": row.source_record_id,
            "event_time_utc": row.event_time.isoformat(),
            "event_time_vienna": vienna_time(row.event_time).isoformat(),
            "temperature_c": row.temperature_c,
            "precipitation_mm": row.precipitation_mm,
            "quality_status": row.quality_status,
            "source_id": row.source_id,
        }
        for row in rows
    ]


@app.get("/v1/runs")
def runs(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    result = []
    for run in session.scalars(select(IngestionRun).order_by(IngestionRun.started_at.desc())).all():
        checks = session.scalars(
            select(QualityResult).where(QualityResult.run_id == run.id).order_by(QualityResult.id)
        ).all()
        result.append(
            {
                "id": run.id,
                "source_id": run.source_id,
                "status": run.status,
                "record_count": run.record_count,
                "checksum": run.checksum,
                "started_at": run.started_at.isoformat(),
                "bronze_path": run.bronze_path,
                "quality": [
                    {
                        "check_name": check.check_name,
                        "status": check.status,
                        "details": check.details,
                        "affected_count": check.affected_count,
                        "phase": check.phase,
                    }
                    for check in checks
                ],
            }
        )
    return result


def _incident_dict(incident: Incident) -> dict[str, Any]:
    recovery_verified = replay_is_verified(incident)
    quality = [
        {
            "check_name": check.check_name,
            "status": check.status,
            "details": check.details,
            "affected_count": check.affected_count,
            "phase": check.phase,
            "checked_at": check.checked_at.isoformat(),
        }
        for check in sorted(
            incident_quality_results(incident),
            key=lambda item: (item.phase or "", item.checked_at, item.id),
        )
    ]
    quarantined = [
        {
            "source_record_id": row.source_record_id,
            "raw_record": row.raw_record,
            "reason": row.reason,
            "status": row.status,
            "quarantined_at": row.quarantined_at.isoformat(),
        }
        for row in incident_quarantine(incident)
    ]
    return {
        "id": incident.id,
        "run_id": incident.run_id,
        "category": incident.category,
        "scenario": incident.scenario or incident.category,
        "severity": incident.severity,
        "status": (
            "unverified"
            if incident.status == "recovered" and not recovery_verified
            else incident.status
        ),
        "stored_status": incident.status,
        "recovery_verified": recovery_verified,
        "title": incident.title,
        "details": incident.details,
        "downstream_impact": incident.downstream_impact,
        "affected_records": incident.affected_records or [],
        "before_metrics": incident.before_metrics,
        "after_metrics": incident.after_metrics,
        "detected_at": incident.detected_at.isoformat(),
        "quality_results": quality,
        "quarantined_records": quarantined,
        "audit_trail": [
            {
                "event_type": event.event_type,
                "from_status": event.from_status,
                "to_status": event.to_status,
                "details": event.details,
                "created_at": event.created_at.isoformat(),
            }
            for event in sorted(incident.events, key=lambda item: (item.created_at, item.id))
        ],
        "replays": [
            {
                "id": replay.id,
                "status": replay.status,
                "message": replay.message,
                "replayed_at": replay.replayed_at.isoformat(),
                "before_metrics": replay.before_metrics,
                "after_metrics": replay.after_metrics,
                "validation_results": replay.validation_results,
            }
            for replay in incident.replays
        ],
    }


def incident_quality_results(incident: Incident) -> list[QualityResult]:
    return list(incident_quality_query(incident))


def incident_quality_query(incident: Incident) -> list[QualityResult]:
    session = object_session(incident)
    if session is None:
        return []
    return session.scalars(
        select(QualityResult).where(QualityResult.incident_id == incident.id)
    ).all()


def incident_quarantine(incident: Incident) -> list[QuarantinedRecord]:
    session = object_session(incident)
    if session is None:
        return []
    return session.scalars(
        select(QuarantinedRecord).where(QuarantinedRecord.incident_id == incident.id)
    ).all()


@app.get("/v1/incidents")
def incidents(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    return [
        _incident_dict(incident)
        for incident in session.scalars(
            select(Incident).order_by(Incident.detected_at.desc())
        ).all()
    ]


@app.post("/v1/demo/scenarios/{scenario}/simulate")
def simulate(
    scenario: Literal["schema_drift", "duplicate_records", "missing_late"],
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "demo":
        raise HTTPException(status_code=409, detail="Scenario simulation requires DATA_MODE=demo.")
    _authorize_demo_action(demo_token)
    try:
        incident = simulate_scenario(session, Path(get_settings().bronze_dir), scenario)
    except (OSError, ValueError, RuntimeError):
        session.rollback()
        logger.exception("scenario_simulation_failed", extra={"scenario": scenario})
        raise HTTPException(status_code=500, detail="Incident simulation failed.") from None
    return _incident_dict(incident)


@app.post("/v1/incidents/{incident_id}/investigate")
def investigate(
    incident_id: str,
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "demo":
        raise HTTPException(
            status_code=409, detail="Incident investigation is disabled in real mode."
        )
    _authorize_demo_action(demo_token)
    try:
        incident = investigate_incident(session, incident_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _incident_dict(incident)


@app.post("/v1/incidents/{incident_id}/replay")
def replay(
    incident_id: str,
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "demo":
        raise HTTPException(status_code=409, detail="Incident replay is disabled in real mode.")
    _authorize_demo_action(demo_token)
    try:
        result = replay_incident(session, incident_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError) as exc:
        session.rollback()
        logger.exception("incident_replay_failed", extra={"incident_id": incident_id})
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "incident_id": result.incident_id,
        "status": result.status,
        "message": result.message,
        "replayed_at": parse_utc(result.replayed_at).isoformat(),
    }


@app.post("/v1/demo/reset")
def reset(
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, int]:
    if get_settings().data_mode != "demo":
        raise HTTPException(status_code=409, detail="Demo reset requires DATA_MODE=demo.")
    _authorize_demo_action(demo_token)
    return reset_demo(session)


@app.post("/v1/demo/ingest")
def ingest_demo(
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "demo":
        raise HTTPException(
            status_code=409,
            detail="Demo ingestion is disabled outside DATA_MODE=demo.",
        )
    _authorize_demo_action(demo_token)
    try:
        result = run_demo(session, Path(get_settings().bronze_dir))
    except (OSError, ValueError, RuntimeError):
        session.rollback()
        logger.exception("demo_ingestion_failed")
        raise HTTPException(
            status_code=500,
            detail="Synthetic demo ingestion failed.",
        ) from None
    return result


@app.post("/v1/demo/streaming")
def create_interview_streaming_demo(
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "demo":
        raise HTTPException(status_code=409, detail="Streaming demo requires DATA_MODE=demo.")
    _authorize_demo_action(demo_token)
    event_ids = []
    for notification in ("ingestion_completed", "verification_failed", "quality_incident"):
        row = enqueue(
            session,
            "streaming.test",
            "interview-demo",
            "synthetic-streaming-test",
            {
                "scenario": "replay",
                "notification": notification,
                "measurements_modified": False,
            },
            identity=f"interview-demo:kafka:v1:{notification}",
        )
        event_ids.append(row.id)
    session.commit()
    return {
        "status": "queued_or_existing",
        "provenance": "synthetic-streaming-test",
        "measurements_modified": False,
        "event_ids": event_ids,
    }


@app.post("/v1/demo/streaming/{event_id}/replay")
def replay_interview_streaming_demo(
    event_id: str,
    demo_token: str | None = Header(default=None, alias="X-Demo-Control-Token"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if get_settings().data_mode != "demo":
        raise HTTPException(status_code=409, detail="Streaming demo requires DATA_MODE=demo.")
    _authorize_demo_action(demo_token)
    original = session.get(StreamingOutbox, event_id)
    if (
        original is None
        or original.topic != EVENT_TOPIC
        or original.payload.get("event_type") != "streaming.test"
        or original.payload.get("provenance") != "synthetic-streaming-test"
    ):
        raise HTTPException(status_code=404, detail="Synthetic interview event was not found.")
    replay_id = str(uuid5(NAMESPACE_URL, f"interview-demo:kafka:replay:{event_id}"))
    replay_row = session.get(StreamingOutbox, replay_id)
    if replay_row is None:
        replay_row = StreamingOutbox(
            id=replay_id,
            topic=EVENT_TOPIC,
            created_at=utc_now(),
            next_attempt_at=utc_now(),
            attempts=0,
            payload=original.payload,
        )
        session.add(replay_row)
        session.commit()
    return {
        "status": "queued_or_existing",
        "provenance": "synthetic-streaming-test",
        "event_id": event_id,
        "replay_outbox_id": replay_row.id,
        "measurements_modified": False,
    }
