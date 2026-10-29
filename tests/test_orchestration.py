import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from danubepulse import main, orchestration
from danubepulse.db import get_session
from danubepulse.ingestion_lock import IngestionBusy, serialized_ingestion
from danubepulse.models import PipelineExecution, RiverObservation, WeatherObservation
from danubepulse.orchestration import execute_pipeline, pipeline_status, verify_pipeline
from danubepulse.pipeline import ingest_real_river, ingest_real_weather
from danubepulse.sources import AustrianHydrographyConnector, OpenMeteoArchiveConnector

FIXTURES = Path(__file__).parent / "fixtures"


def river_connector() -> AustrianHydrographyConnector:
    payload = json.loads((FIXTURES / "ehyd_features.json").read_text())
    for feature in payload["features"]:
        feature["properties"]["zeitpunkt"] = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    return AustrianHydrographyConnector(station_ids=(207019, 207068), http_get=lambda _url: payload)


def setup_river(monkeypatch) -> None:
    connector = river_connector()
    monkeypatch.setattr(
        orchestration,
        "ingest_real_river",
        lambda session, bronze: ingest_real_river(session, bronze, connector=connector),
    )


def test_pipeline_verifies_persisted_data_and_retains_attempts(
    session_factory, tmp_path, monkeypatch
) -> None:
    setup_river(monkeypatch)
    with session_factory() as session:
        first = execute_pipeline(session, tmp_path, "river", "manual:first")
        assert first.status == "ingested"
        assert first.completed_at is None
        first = verify_pipeline(session, first.id)
        assert first.status == "success"
        assert all(check["passed"] for check in first.verification)
        assert {check["check"] for check in first.verification} == {
            "source_quality",
            "bronze_integrity",
            "semantic_uniqueness",
            "persisted_silver",
            "downstream_gold",
        }
        original = first.verification
        assert verify_pipeline(session, first.id).verification == original
        second = execute_pipeline(session, tmp_path, "river", "manual:retry")
        assert second.id != first.id
        assert second.ingestion_run_id == first.ingestion_run_id
        assert verify_pipeline(session, second.id).status == "success"
        assert session.scalar(select(func.count()).select_from(PipelineExecution)) == 2
        assert session.scalar(select(func.count()).select_from(RiverObservation)) == 2
        status = pipeline_status(session)
        assert status[0]["stale"] is False
        assert status[0]["latest_failure"] is None
        assert status[1]["stale"] is True


@pytest.mark.parametrize("damage", ["bronze", "silver", "gold"])
def test_pipeline_fails_verification_for_corrupted_persisted_state(
    damage, session_factory, tmp_path, monkeypatch
) -> None:
    setup_river(monkeypatch)
    with session_factory() as session:
        execution = execute_pipeline(session, tmp_path, "river", "manual:corrupt")
        if damage == "bronze":
            bronze = next((tmp_path / "ehyd-ogc-api").glob("*.json"))
            envelope = json.loads(bronze.read_text())
            envelope["raw_response"]["changed"] = True
            bronze.write_text(json.dumps(envelope))
        elif damage == "silver":
            row = session.scalar(
                select(RiverObservation).where(RiverObservation.measurement_type == "water_level")
            )
            row.water_level_m = 999
            session.commit()
        else:
            session.execute(text("DROP VIEW gold_real_river_daily"))
            session.execute(
                text(
                    "CREATE VIEW gold_real_river_daily AS SELECT "
                    "DATE(event_time) AS observation_date, station_id AS location, "
                    "measurement_type, source_unit, 999 AS observation_count, "
                    "999 AS average_water_level_m, 999 AS average_discharge_m3_s "
                    "FROM river_observations"
                )
            )
            session.commit()
        result = verify_pipeline(session, execution.id)
        assert result.status == "failed"
        assert result.error
        assert any(not check["passed"] for check in result.verification)
        assert pipeline_status(session)[0]["latest_failure"]["id"] == result.id


def test_unexpected_ingestion_failure_is_audited(session_factory, tmp_path, monkeypatch) -> None:
    def fail(*_args):
        raise RuntimeError("Deliberate integration failure")

    monkeypatch.setattr(orchestration, "ingest_real_river", fail)
    with session_factory() as session:
        with pytest.raises(RuntimeError, match="Deliberate integration failure"):
            execute_pipeline(session, tmp_path, "river", "manual:failure")
        execution = session.scalar(select(PipelineExecution))
        assert execution.status == "failed"
        assert "Deliberate integration failure" in execution.error


def test_complete_weather_day_verifies_and_incomplete_day_fails(
    session_factory, tmp_path, monkeypatch
) -> None:
    payload = json.loads((FIXTURES / "open_meteo_archive.json").read_text())
    day = date(2026, 10, 18)
    payload["hourly"]["time"] = [f"{day.isoformat()}T{hour:02d}:00" for hour in range(24)]
    for field in payload["hourly"]:
        if field != "time":
            payload["hourly"][field] = [1.0] * 24
    connector = OpenMeteoArchiveConnector(
        latitude=48.3,
        longitude=14.2,
        station_id="open-meteo:207068",
        station_name="Mock weather only",
        start_date=day.isoformat(),
        end_date=day.isoformat(),
        http_get=lambda _url: payload,
    )
    monkeypatch.setattr(
        orchestration,
        "ingest_real_weather",
        lambda session, bronze, **kw: ingest_real_weather(
            session, bronze, connectors=[connector], force=True, **kw
        ),
    )
    with session_factory() as session:
        execution = execute_pipeline(session, tmp_path, "weather", "scheduled:test", day)
        assert verify_pipeline(session, execution.id).status == "success"
        assert session.scalar(select(func.count()).select_from(WeatherObservation)) == 24
        for field in payload["hourly"]:
            payload["hourly"][field] = payload["hourly"][field][:-1]
        incomplete = execute_pipeline(session, tmp_path, "weather", "scheduled:incomplete", day)
        assert verify_pipeline(session, incomplete.id).status == "failed"
        assert any(
            check["check"] == "source_quality" and not check["passed"]
            for check in incomplete.verification
        )
        assert session.scalar(select(func.count()).select_from(WeatherObservation)) == 24
        with pytest.raises(ValueError, match="two complete UTC days"):
            execute_pipeline(session, tmp_path, "weather", "bad", datetime.now(UTC).date())


def test_source_lock_prevents_overlap_and_releases_after_failure(session_factory) -> None:
    @serialized_ingestion("river")
    def inner(session):
        return "acquired"

    @serialized_ingestion("river")
    def outer(session):
        with pytest.raises(IngestionBusy):
            inner(session)
        raise RuntimeError("force release")

    with session_factory() as session:
        with pytest.raises(RuntimeError, match="force release"):
            outer(session)
        assert inner(session) == "acquired"


def test_pipeline_api_mode_guard_and_verification(session_factory, tmp_path, monkeypatch) -> None:
    setup_river(monkeypatch)

    def override():
        with session_factory() as session:
            yield session

    settings = SimpleNamespace(data_mode="demo", bronze_dir=tmp_path)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    main.app.dependency_overrides[get_session] = override
    try:
        with TestClient(main.app) as client:
            payload = {"orchestrator_run_id": "manual:api"}
            assert client.post("/v1/pipelines/river/ingest", json=payload).status_code == 409
            settings.data_mode = "real"
            response = client.post("/v1/pipelines/river/ingest", json=payload)
            assert response.status_code == 200
            execution = response.json()
            result = client.post(f"/v1/pipelines/executions/{execution['id']}/verify")
            assert result.json()["status"] == "success"
            assert client.get("/v1/pipelines").json()[0]["latest_success"]["id"] == execution["id"]
    finally:
        main.app.dependency_overrides.pop(get_session, None)
