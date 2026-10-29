import hashlib
import json
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from danubepulse import main
from danubepulse.db import get_session
from danubepulse.models import (
    DataSource,
    IngestionRun,
    QualityResult,
    RiverObservation,
    WeatherObservation,
)
from danubepulse.pipeline import (
    default_weather_window,
    ingest_real_river,
    ingest_real_weather,
    run_demo,
)
from danubepulse.sources import (
    AustrianHydrographyConnector,
    OpenMeteoArchiveConnector,
    SourceUnavailable,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _payload(filename: str) -> dict:
    return json.loads((FIXTURES / filename).read_text())


def test_hydro_connector_normalizes_danube_level_and_discharge() -> None:
    payload = _payload("ehyd_features.json")
    connector = AustrianHydrographyConnector(
        station_ids=(207019, 207068),
        http_get=lambda _url: payload,
    )

    batch = connector.fetch()

    level = next(row for row in batch.records if row["measurement_type"] == "water_level")
    discharge = next(row for row in batch.records if row["measurement_type"] == "discharge")
    assert level["station_id"] == "ehyd:207068"
    assert level["station_name"] == "Linz (mock fixture)"
    assert level["source_value"] == 345
    assert level["source_unit"] == "cm"
    assert level["water_level_m"] == pytest.approx(3.45)
    assert level["discharge_m3_s"] is None
    assert level["measurement_reference"] == "gauge_reference_unspecified"
    assert level["event_time"] == "2026-10-25T22:30:00+00:00"
    assert discharge["station_id"] == "ehyd:207019"
    assert discharge["source_unit"] == "m³/s"
    assert discharge["discharge_m3_s"] == pytest.approx(423)
    assert discharge["water_level_m"] is None
    assert batch.provenance == "real"
    assert batch.raw_response["pages"][0]["features"][0]["properties"]["wert"] == 345


def test_hydro_connector_rejects_unknown_unit_and_keeps_raw_payload() -> None:
    payload = _payload("ehyd_features.json")
    payload["features"][0]["properties"]["einheit"] = "ft"
    batch = AustrianHydrographyConnector(
        station_ids=(207068,),
        http_get=lambda _url: payload,
    ).fetch()

    assert batch.records == []
    assert batch.rejected_records[0]["reason"].startswith("Unsupported parameter/unit")
    assert batch.raw_response["pages"][0]["features"][0]["properties"]["einheit"] == "ft"


def test_open_meteo_connector_requests_utc_variables_and_keeps_missing_values() -> None:
    payload = _payload("open_meteo_archive.json")
    requests: list[str] = []

    def fake_get(url: str) -> dict:
        requests.append(url)
        return payload

    connector = OpenMeteoArchiveConnector(
        latitude=48.3069,
        longitude=14.2849,
        station_id="open-meteo:207068",
        station_name="Mock reanalysis point",
        start_date="2026-10-18",
        end_date="2026-10-24",
        http_get=fake_get,
    )
    batch = connector.fetch()

    query = parse_qs(urlparse(requests[0]).query)
    assert query["timezone"] == ["UTC"]
    assert query["wind_speed_unit"] == ["ms"]
    assert set(query["hourly"][0].split(",")) == {
        "temperature_2m",
        "precipitation",
        "relative_humidity_2m",
        "wind_speed_10m",
    }
    first, second = batch.records
    assert first["event_time"] == "2026-10-18T00:00:00+00:00"
    assert first["temperature_c"] == pytest.approx(8.1)
    assert first["wind_speed_m_s"] == pytest.approx(3.0)
    assert second["temperature_c"] is None
    assert second["precipitation_mm"] == pytest.approx(0.2)
    assert first["source_units"]["wind_speed_10m"] == "km/h"
    assert batch.stations[0]["latitude"] == pytest.approx(48.31)
    assert batch.raw_response["response"]["hourly"]["temperature_2m"][1] is None


def test_open_meteo_rejects_inconsistent_response_and_long_period() -> None:
    payload = _payload("open_meteo_archive.json")
    payload["hourly"]["wind_speed_10m"].pop()
    connector = OpenMeteoArchiveConnector(
        latitude=48,
        longitude=14,
        station_id="grid",
        station_name="Mock",
        start_date="2026-10-18",
        end_date="2026-10-24",
        http_get=lambda _url: payload,
    )
    with pytest.raises(SourceUnavailable, match="inconsistent values"):
        connector.fetch()
    with pytest.raises(ValueError, match="limited to 31 days"):
        OpenMeteoArchiveConnector(
            latitude=48,
            longitude=14,
            station_id="grid",
            station_name="Mock",
            start_date="2026-01-01",
            end_date="2026-02-01",
        )


def test_default_weather_window_is_seven_days_with_archive_lag() -> None:
    start, end = default_weather_window(date(2026, 10, 26))
    assert (start, end) == (date(2026, 10, 18), date(2026, 10, 24))


def test_real_river_ingestion_is_idempotent_and_isolated_from_demo(
    tmp_path: Path, session_factory
) -> None:
    payload = deepcopy(_payload("ehyd_features.json"))
    recent = datetime.now(UTC) - timedelta(minutes=10)
    for index, feature in enumerate(payload["features"]):
        feature["properties"]["zeitpunkt"] = (recent - timedelta(minutes=index)).isoformat()
    with session_factory() as session:
        run_demo(session, tmp_path)
        demo_count_before = session.execute(
            text(
                "SELECT sum(observation_count) FROM gold_daily_observations WHERE domain = 'river'"
            )
        ).scalar_one()
        request_count = 0

        def fake_get(_url: str) -> dict:
            nonlocal request_count
            request_count += 1
            return payload

        connector = AustrianHydrographyConnector(
            station_ids=(207019, 207068),
            http_get=fake_get,
        )
        first = ingest_real_river(session, tmp_path, connector=connector)
        bronze_path = Path(first.bronze_path)
        original = bronze_path.read_bytes()
        envelope = json.loads(original)
        raw_checksum = hashlib.sha256(
            json.dumps(
                envelope["raw_response"],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode()
        ).hexdigest()
        assert first.status == "completed"
        assert first.record_count == first.accepted_count == 2
        assert first.rejected_count == 0
        assert first.checksum == raw_checksum
        assert envelope["provenance"] == "real"
        assert envelope["raw_response"]["pages"][0]["features"]
        cached = ingest_real_river(session, tmp_path, connector=connector)
        assert cached.id == first.id
        assert request_count == 1

        forced = ingest_real_river(session, tmp_path, connector=connector, force=True)
        assert forced.id != first.id
        assert request_count == 2
        source = session.scalar(select(DataSource).where(DataSource.id == "ehyd-ogc-api"))
        assert source is not None
        assert source.provenance == "real"
        assert source.license.startswith("No redistribution license is declared")
        assert (
            session.scalar(
                select(RiverObservation).where(
                    RiverObservation.source_id == "ehyd-ogc-api",
                    RiverObservation.measurement_type == "water_level",
                )
            ).measurement_reference
            == "gauge_reference_unspecified"
        )
        assert (
            session.scalar(
                select(RiverObservation).where(
                    RiverObservation.source_id == "ehyd-ogc-api",
                    RiverObservation.measurement_type == "discharge",
                )
            ).water_level_m
            is None
        )
        assert (
            session.scalar(
                select(QualityResult).where(
                    QualityResult.run_id == forced.id,
                    QualityResult.check_name == "duplicate_events",
                )
            ).status
            == "warning"
        )
        demo_count_after = session.execute(
            text(
                "SELECT sum(observation_count) FROM gold_daily_observations WHERE domain = 'river'"
            )
        ).scalar_one()
        assert demo_count_after == demo_count_before
        real_aggregate_count = session.execute(
            text("SELECT sum(observation_count) FROM gold_real_river_daily")
        ).scalar_one()
        assert real_aggregate_count == 2
        assert bronze_path.read_bytes() == original
        assert (
            session.scalar(select(DataSource.provenance).where(DataSource.id == "synthetic-river"))
            == "synthetic"
        )


def test_real_weather_ingestion_uses_real_silver_and_gold(tmp_path: Path, session_factory) -> None:
    payload = _payload("open_meteo_archive.json")
    with session_factory() as session:
        run_demo(session, tmp_path)
        connector = OpenMeteoArchiveConnector(
            latitude=48.3069,
            longitude=14.2849,
            station_id="open-meteo:207068",
            station_name="Open-Meteo reanalysis near Linz",
            start_date="2026-10-18",
            end_date="2026-10-24",
            http_get=lambda _url: payload,
        )
        run = ingest_real_weather(
            session,
            tmp_path,
            start_date=date(2026, 10, 18),
            end_date=date(2026, 10, 24),
            connectors=[connector],
        )
        bronze_path = Path(run.bronze_path)
        original_bronze = bronze_path.read_bytes()
        rows = session.scalars(
            select(WeatherObservation).where(WeatherObservation.source_id == run.source_id)
        ).all()
        assert run.status == "degraded"
        assert run.record_count == run.accepted_count == 2
        assert len(rows) == 2
        assert rows[1].temperature_c is None
        assert rows[1].precipitation_mm == pytest.approx(0.2)
        assert rows[0].wind_speed_m_s == pytest.approx(3.0)
        checks = session.scalars(select(QualityResult).where(QualityResult.run_id == run.id)).all()
        coverage = next(check for check in checks if check.check_name == "hourly_completeness")
        assert coverage.status == "warning"
        assert coverage.affected_count == 166
        gold = (
            session.execute(
                text(
                    "SELECT observation_count, temperature_observation_count, "
                    "precipitation_total_mm, average_relative_humidity_percent, "
                    "average_wind_speed_m_s FROM gold_real_weather_daily"
                )
            )
            .mappings()
            .one()
        )
        assert gold["observation_count"] == 2
        assert gold["temperature_observation_count"] == 1
        assert gold["precipitation_total_mm"] == pytest.approx(0.2)
        assert gold["average_relative_humidity_percent"] == pytest.approx(83)
        assert gold["average_wind_speed_m_s"] == pytest.approx(2.5)
        demo_weather_count = session.execute(
            text(
                "SELECT sum(observation_count) FROM gold_daily_observations "
                "WHERE domain = 'weather'"
            )
        ).scalar_one()
        assert demo_weather_count == 6
        cached = ingest_real_weather(
            session,
            tmp_path,
            start_date=date(2026, 10, 18),
            end_date=date(2026, 10, 24),
            connectors=[connector],
        )
        assert cached.id == run.id
        forced = ingest_real_weather(
            session,
            tmp_path,
            start_date=date(2026, 10, 18),
            end_date=date(2026, 10, 24),
            connectors=[connector],
            force=True,
        )
        assert forced.id != run.id
        assert (
            session.scalar(
                select(func.count())
                .select_from(WeatherObservation)
                .where(WeatherObservation.source_id == run.source_id)
            )
            == 2
        )
        assert (
            session.scalar(
                select(QualityResult).where(
                    QualityResult.run_id == forced.id,
                    QualityResult.check_name == "duplicate_events",
                )
            ).status
            == "warning"
        )
        assert bronze_path.read_bytes() == original_bronze


def test_weather_source_failure_is_audited_without_creating_observations(
    tmp_path: Path, session_factory
) -> None:
    with session_factory() as session:
        connector = OpenMeteoArchiveConnector(
            latitude=48.3,
            longitude=14.2,
            station_id="open-meteo:207068",
            station_name="Open-Meteo near Linz",
            start_date="2026-10-18",
            end_date="2026-10-24",
            http_get=lambda _url: (_ for _ in ()).throw(SourceUnavailable("Provider timeout")),
        )
        run = ingest_real_weather(
            session,
            tmp_path,
            start_date=date(2026, 10, 18),
            end_date=date(2026, 10, 24),
            connectors=[connector],
        )
        assert run.status == "failed"
        assert "Provider timeout" in Path(run.bronze_path).read_text()
        assert (
            session.scalar(
                select(QualityResult).where(
                    QualityResult.run_id == run.id,
                    QualityResult.check_name == "source_availability",
                    QualityResult.status == "failed",
                )
            )
            is not None
        )
        assert (
            session.scalar(
                select(WeatherObservation).where(
                    WeatherObservation.source_id == "open-meteo-archive"
                )
            )
            is None
        )


def test_partial_weather_source_failure_is_retried_without_duplicate_rows(
    tmp_path: Path, session_factory
) -> None:
    payload = _payload("open_meteo_archive.json")
    calls = {"successful": 0, "failed": 0}

    def fetch_successful(_url: str) -> dict[str, Any]:
        calls["successful"] += 1
        return payload

    def fetch_failed(_url: str) -> dict[str, Any]:
        calls["failed"] += 1
        raise SourceUnavailable("Provider timeout")

    connectors = [
        OpenMeteoArchiveConnector(
            latitude=48.3,
            longitude=14.2,
            station_id="open-meteo:207068",
            station_name="Open-Meteo near Linz",
            start_date="2026-10-18",
            end_date="2026-10-24",
            http_get=fetch_successful,
        ),
        OpenMeteoArchiveConnector(
            latitude=48.0,
            longitude=14.0,
            station_id="open-meteo:207019",
            station_name="Open-Meteo near Achleiten",
            start_date="2026-10-18",
            end_date="2026-10-24",
            http_get=fetch_failed,
        ),
    ]
    with session_factory() as session:
        first = ingest_real_weather(
            session,
            tmp_path,
            start_date=date(2026, 10, 18),
            end_date=date(2026, 10, 24),
            connectors=connectors,
        )
        second = ingest_real_weather(
            session,
            tmp_path,
            start_date=date(2026, 10, 18),
            end_date=date(2026, 10, 24),
            connectors=connectors,
        )

        assert first.status == second.status == "degraded"
        assert first.id != second.id
        assert first.request_parameters["source_errors"] == ["open-meteo:207019: Provider timeout"]
        assert calls == {"successful": 2, "failed": 2}
        assert (
            session.scalar(
                select(func.count())
                .select_from(WeatherObservation)
                .where(WeatherObservation.source_id == "open-meteo-archive")
            )
            == 2
        )
        assert session.scalar(select(func.count()).select_from(IngestionRun)) == 2


def test_real_api_keeps_data_modes_separate(tmp_path: Path, session_factory, monkeypatch) -> None:
    with session_factory() as session:
        run_demo(session, tmp_path)
        ingest_real_river(
            session,
            tmp_path,
            connector=AustrianHydrographyConnector(
                station_ids=(207019, 207068),
                http_get=lambda _url: _payload("ehyd_features.json"),
            ),
        )

    def override_session():
        with session_factory() as session:
            yield session

    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(data_mode="demo", bronze_dir=tmp_path, log_level="INFO"),
    )
    main.app.dependency_overrides[get_session] = override_session
    try:
        with TestClient(main.app) as client:
            assert client.post("/v1/real/ingest/river").status_code == 409
            demo_rows = client.get("/v1/observations?domain=river").json()
            real_rows = client.get(
                "/v1/real/observations?domain=river&station_id=ehyd:207068"
            ).json()
            daily = client.get("/v1/summary").json()["daily"]
            real_catalog = client.get("/v1/real/catalog").json()
            assert demo_rows and all(row["source_id"].startswith("synthetic-") for row in demo_rows)
            assert len(real_rows) == 1
            assert real_rows[0]["source_id"] == "ehyd-ogc-api"
            assert real_rows[0]["station_name"] == "Linz (mock fixture)"
            assert all(row["location"] != "ehyd:207068" for row in daily)
            assert real_catalog["sources"][0]["attribution"] == "Datenquelle: ehyd.gv.at"
            monkeypatch.setattr(
                main,
                "get_settings",
                lambda: SimpleNamespace(data_mode="real", bronze_dir=tmp_path, log_level="INFO"),
            )
            assert client.post("/v1/demo/scenarios/duplicate_records/simulate").status_code == 409
            assert client.post("/v1/demo/reset").status_code == 409
    finally:
        main.app.dependency_overrides.pop(get_session, None)
