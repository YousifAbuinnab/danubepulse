"""Synthetic protocol fixtures in isolated schemas; never loaded as real application data."""

import csv
import io
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from danubepulse import energy, energy_sources, main, orchestration
from danubepulse.db import get_session
from danubepulse.energy import ingest_energy, verify_energy
from danubepulse.energy_sources import (
    AUSTRIA,
    EnergySourceError,
    fetch_xml,
    parse_csv,
    parse_xml,
    strict_utc,
)
from danubepulse.models import (
    DataSource,
    EnergyObservation,
    IngestionRun,
    RiverObservation,
    Station,
    WeatherObservation,
)

DAY = date(2024, 10, 27)
START = datetime(2024, 10, 27, tzinfo=UTC)
END = START + timedelta(days=1)


def document(kind="generation", value=100, revision=1, hours=24, resolution=60, curve="A01"):
    """Invented values for protocol testing, not an ENTSO-E download."""
    slots = int(hours * 60 / resolution)
    positions = [1] if curve == "A03" else range(1, slots + 1)
    measure = "quantity" if kind == "generation" else "price.amount"
    points = "".join(
        f"<Point><position>{i}</position><{measure}>{value}</{measure}></Point>" for i in positions
    )
    incoming = f"<inBiddingZone_Domain.mRID>{AUSTRIA}</inBiddingZone_Domain.mRID>"
    series = (
        f"{incoming}<quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>"
        "<MktPSRType><psrType>B11</psrType></MktPSRType>"
        if kind == "generation"
        else f"{incoming}<outBiddingZone_Domain.mRID>{AUSTRIA}</outBiddingZone_Domain.mRID>"
        "<currency_Unit.name>EUR</currency_Unit.name>"
        "<price_Measure_Unit.name>MWH</price_Measure_Unit.name>"
        "<contract_MarketAgreement.type>A01</contract_MarketAgreement.type>"
    )
    end = START + timedelta(hours=hours)
    return (
        '<GL_MarketDocument xmlns="urn:synthetic:protocol-test">'
        f"<mRID>synthetic-{kind}</mRID><revisionNumber>{revision}</revisionNumber>"
        f"<type>{'A75' if kind == 'generation' else 'A44'}</type>"
        "<process.processType>A16</process.processType>"
        f"<createdDateTime>2024-10-28T0{revision}:00:00Z</createdDateTime>"
        f"<TimeSeries>{series}<curveType>{curve}</curveType><Period>"
        f"<timeInterval><start>{START.isoformat()}</start><end>{end.isoformat()}</end>"
        f"</timeInterval><resolution>PT{resolution}M</resolution>{points}"
        "</Period></TimeSeries></GL_MarketDocument>"
    )


def ingest(session, path, **kwargs):
    return ingest_energy(session, path, DAY, terms_accepted=True, **kwargs)


def test_xml_units_curves_dst_and_prices():
    generated = parse_xml(document(curve="A03").encode(), "generation", START, END)
    assert len(generated) == 24
    assert sum(p.hours * p.value for p in generated) == 2400
    prices = parse_xml(
        document(kind="price", value=-25, resolution=15).encode(), "price", START, END
    )
    assert len(prices) == 96
    assert all(p.value == -25 for p in prices)
    # Both occurrences of Vienna's autumn 02:00 are distinct instants.
    assert strict_utc("2024-10-27T02:00:00+02:00") != strict_utc("2024-10-27T02:00:00+01:00")
    with pytest.raises(EnergySourceError, match="explicit UTC offset"):
        strict_utc("2024-10-27T02:00:00")


@pytest.mark.parametrize("hours,offset", [(23, 1), (25, 2)])
def test_provider_dst_market_day_intervals_remain_distinct(hours, offset):
    business_start = START - timedelta(hours=offset)
    business_end = business_start + timedelta(hours=hours)
    body = (
        document(hours=hours, resolution=15)
        .replace(START.isoformat(), business_start.isoformat())
        .replace((START + timedelta(hours=hours)).isoformat(), business_end.isoformat())
    )
    points = parse_xml(body.encode(), "generation", business_start, business_end)
    assert len(points) == hours * 4
    assert len({p.start for p in points}) == hours * 4
    assert sum(p.hours * p.value for p in points) == hours * 100


@pytest.mark.parametrize(
    "old,new",
    [
        ("MAW", "MWH"),
        ("B11", "unknown"),
        ("A16", "A01"),
        ("<quantity>100", "<quantity>nan"),
        ("<quantity>100", "<quantity>-1"),
        (AUSTRIA, "10YFR-RTE------C"),
        ("PT60M", "PT5M"),
    ],
)
def test_schema_and_impossible_values_rejected(old, new):
    with pytest.raises(EnergySourceError):
        parse_xml(document().replace(old, new).encode(), "generation", START, END)


def test_dangerous_xml_duplicate_positions_and_consumption():
    with pytest.raises(EnergySourceError, match="forbidden"):
        parse_xml(b'<!DOCTYPE a [<!ENTITY b "bad">]><a/>', "generation", START, END)
    duplicate = document().replace("<position>2</position>", "<position>1</position>")
    with pytest.raises(EnergySourceError, match="Duplicate"):
        parse_xml(duplicate.encode(), "generation", START, END)
    consumption = document().replace("inBiddingZone", "outBiddingZone")
    with pytest.raises(EnergySourceError, match="No supported"):
        parse_xml(consumption.encode(), "generation", START, END)


def test_connector_parameters_and_secret_redaction():
    class Response(io.BytesIO):
        pass

    with patch.object(energy_sources, "urlopen", return_value=Response(b"<test/>")) as get:
        assert fetch_xml("sensitive-token", "generation", START, END) == b"<test/>"
        url = get.call_args.args[0].full_url
        assert "in_Domain=10YAT-APG------L" in url and "processType=A16" in url
    with patch.object(
        energy_sources,
        "urlopen",
        side_effect=HTTPError("https://example/?securityToken=SECRET", 429, "SECRET", {}, None),
    ):
        with pytest.raises(EnergySourceError) as caught:
            fetch_xml("SECRET", "price", START, END)
        assert "SECRET" not in str(caught.value)
        assert caught.value.__suppress_context__
    with pytest.raises(EnergySourceError, match="ENTSOE_API_TOKEN"):
        fetch_xml(None, "price", START, END)


def test_idempotency_revisions_and_duration_weighted_gold(session_factory, tmp_path):
    with session_factory() as session:
        first = ingest(
            session,
            tmp_path,
            payloads={
                "generation": document(),
                "price": document("price", value=-10),
            },
        )
        assert first.status == "completed"
        assert all(c["passed"] for c in verify_energy(session, first))
        again = ingest(
            session,
            tmp_path,
            payloads={
                "generation": document(),
                "price": document("price", value=-10),
            },
        )
        assert again.accepted_count == 0
        assert all(c["passed"] for c in verify_energy(session, again))
        revised = ingest(
            session,
            tmp_path,
            payloads={
                "generation": document(value=200, revision=2, resolution=15),
            },
        )
        assert revised.status == "completed"
        gold = (
            session.execute(text("SELECT * FROM gold_real_energy_daily WHERE metric='generation'"))
            .mappings()
            .one()
        )
        assert gold["observation_count"] == 96
        assert gold["generation_mwh"] == 4800
        assert gold["average_generation_mw"] == 200
        assert all(c["passed"] for c in verify_energy(session, revised))
        assert (
            session.scalar(
                select(func.count())
                .select_from(EnergyObservation)
                .where(EnergyObservation.quality_status == "superseded")
            )
            == 24
        )
        # Replaying the older document must not overwrite its newer correction.
        ingest(session, tmp_path, payloads={"generation": document()})
        assert (
            session.scalar(
                text("SELECT generation_mwh FROM gold_real_energy_daily WHERE metric='generation'")
            )
            == 4800
        )
        assert (
            session.scalar(
                text(
                    "SELECT average_price_eur_mwh FROM gold_real_energy_daily WHERE metric='price'"
                )
            )
            == -10
        )
        assert Path(first.bronze_path).exists()
        assert session.scalar(select(func.count()).select_from(IngestionRun)) == 4


def test_null_gaps_and_invalid_revision_leave_auditable_evidence(session_factory, tmp_path):
    with session_factory() as session:
        partial = ingest(session, tmp_path, payloads={"generation": document(hours=2)})
        assert partial.status == "degraded"
        assert not all(c["passed"] for c in verify_energy(session, partial))
        original = session.scalar(
            text("SELECT generation_mwh FROM gold_real_energy_daily WHERE metric='generation'")
        )
        conflict = ingest(session, tmp_path, payloads={"generation": document(value=200)})
        assert conflict.status == "failed"
        assert (
            session.scalar(
                text("SELECT generation_mwh FROM gold_real_energy_daily WHERE metric='generation'")
            )
            == original
        )
        assert json.loads(Path(conflict.bronze_path).read_text())["raw_response"]
        null = ingest(
            session,
            tmp_path,
            payloads={
                "price": document("price").replace(
                    "<price.amount>100</price.amount>", "<price.amount></price.amount>", 1
                )
            },
        )
        assert null.status == "degraded" and null.rejected_count == 1
        price = (
            session.execute(text("SELECT * FROM gold_real_energy_daily WHERE metric='price'"))
            .mappings()
            .one()
        )
        assert price["observation_count"] == 23
        assert price["price_coverage_hours"] == 23
        assert price["average_price_eur_mwh"] == 100


def test_normalized_csv_contract_and_mixed_duration_weighting(session_factory, tmp_path):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=sorted(energy_sources.CSV_COLUMNS))
    writer.writeheader()
    original = parse_xml(document(kind="price", value=50).encode(), "price", START, END)
    mixed = original[:-1] + [
        replace(
            original[-1],
            start=original[-1].start + timedelta(minutes=15 * i),
            end=original[-1].start + timedelta(minutes=15 * (i + 1)),
            value=100,
        )
        for i in range(4)
    ]
    for point in mixed:
        writer.writerow(
            {
                "interval_start": point.start.isoformat(),
                "interval_end": point.end.isoformat(),
                "metric": "price",
                "production_type": "price",
                "value": point.value,
                "unit": "EUR/MWh",
                "document_id": point.document_id,
                "revision": point.revision,
                "published_at": point.published_at.isoformat(),
                "region": AUSTRIA,
            }
        )
    csv_body = stream.getvalue()
    assert len(parse_csv(csv_body.encode(), "price", START, END)) == 27
    with session_factory() as session:
        run = ingest(session, tmp_path, payloads={"price": csv_body}, import_format="csv")
        assert run.status == "completed"
        assert all(c["passed"] for c in verify_energy(session, run))
        assert session.scalar(
            text("SELECT average_price_eur_mwh FROM gold_real_energy_daily")
        ) == pytest.approx(1250 / 24)
    with pytest.raises(EnergySourceError, match="headers"):
        parse_csv(b"unknown,value\nx,1", "price", START, END)


def test_energy_verification_detects_silver_and_gold_corruption(session_factory, tmp_path):
    with session_factory() as session:
        run = ingest(session, tmp_path, payloads={"generation": document()})
        row = session.scalar(select(EnergyObservation))
        row.generation_mw = 999
        session.commit()
        assert not next(c for c in verify_energy(session, run) if c["check"] == "persisted_silver")[
            "passed"
        ]
        row.generation_mw = 100
        session.flush()
        session.execute(
            text("CREATE TABLE test_energy_gold_snapshot AS SELECT * FROM gold_real_energy_daily")
        )
        session.execute(text("DROP VIEW gold_real_energy_daily"))
        session.execute(text("UPDATE test_energy_gold_snapshot SET average_generation_mw=999"))
        session.execute(
            text("CREATE VIEW gold_real_energy_daily AS SELECT * FROM test_energy_gold_snapshot")
        )
        session.commit()
        assert not next(c for c in verify_energy(session, run) if c["check"] == "downstream_gold")[
            "passed"
        ]


def test_energy_api_and_orchestrated_verification(session_factory, tmp_path, monkeypatch):
    settings = main.get_settings().model_copy(
        update={
            "data_mode": "real",
            "bronze_dir": tmp_path,
            "entsoe_api_token": None,
            "entsoe_terms_accepted": True,
        }
    )
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(orchestration, "get_settings", lambda: settings)
    monkeypatch.setattr(
        orchestration,
        "ingest_energy",
        lambda session, path, day, **kwargs: ingest(
            session, path, payloads={"generation": document()}
        ),
    )

    def sessions():
        with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_session] = sessions
    try:
        with TestClient(main.app) as client:
            empty = client.get("/v1/energy").json()
            assert empty["observations"] == [] and empty["latest_run_status"] == "not_connected"
            response = client.post(
                "/v1/pipelines/energy/ingest",
                json={
                    "orchestrator_run_id": "test:energy",
                    "target_date": DAY.isoformat(),
                },
            )
            assert response.status_code == 200
            execution = response.json()
            verified = client.post(f"/v1/pipelines/executions/{execution['id']}/verify")
            assert verified.json()["status"] == "success"
            result = client.get("/v1/energy?start_date=2024-10-27&end_date=2024-10-27")
            assert result.status_code == 200
            assert len(result.json()["observations"]) == 24
            assert result.json()["daily"][0]["generation_mwh"] == 2400
            assert (
                client.get("/v1/energy?start_date=2024-10-28&end_date=2024-10-27").status_code
                == 422
            )
            assert (
                client.get(
                    "/v1/energy/comparisons",
                    params={
                        "river_station_id": "no-station",
                        "weather_station_id": "no-grid",
                    },
                ).json()["rows"]
                == []
            )
    finally:
        main.app.dependency_overrides.clear()


def test_energy_dashboard_renders_real_api_payloads(session_factory, tmp_path, monkeypatch):
    """Streamlit renders test-schema data through the genuine FastAPI endpoints."""
    from urllib.parse import urlsplit

    from streamlit.testing.v1 import AppTest

    settings = main.get_settings().model_copy(
        update={
            "data_mode": "real",
            "bronze_dir": tmp_path,
            "entsoe_api_token": None,
        }
    )
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with session_factory() as session:
        ingest(
            session,
            tmp_path,
            payloads={
                "generation": document(),
                "price": document("price", value=-20),
            },
        )

    def sessions():
        with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_session] = sessions
    try:
        with TestClient(main.app) as client:

            def local_api(request, **_kwargs):
                url = urlsplit(request.full_url)
                result = client.request(request.method, url.path + "?" + url.query)
                result.raise_for_status()
                return io.BytesIO(result.content)

            monkeypatch.setattr("urllib.request.urlopen", local_api)
            monkeypatch.setenv("AIRFLOW_HEALTH_URL", "https://disabled.invalid/")
            page = AppTest.from_file(
                str(Path(__file__).resolve().parents[1] / "dashboard/app.py")
            ).run(timeout=45)
            assert not page.exception
            assert not page.error, [item.value for item in page.error]
            assert any(item.value == "Austrian Energy Explorer" for item in page.subheader)
            assert any("Generation production types" == item.label for item in page.multiselect)
            assert len(page.get("vega_lite_chart")) >= 2
            assert any("generation_mwh" in frame.value.columns for frame in page.dataframe)
    finally:
        main.app.dependency_overrides.clear()


def test_disabled_credentials_produce_failed_audit_not_fake_records(
    session_factory, tmp_path, monkeypatch
):
    settings = main.get_settings().model_copy(update={"entsoe_api_token": None})
    monkeypatch.setattr(energy, "get_settings", lambda: settings)
    with session_factory() as session:
        run = ingest(session, tmp_path)
        assert run.status == "failed"
        assert run.accepted_count == 0
        assert session.scalar(select(func.count()).select_from(EnergyObservation)) == 0
        assert json.loads(Path(run.bronze_path).read_text())["raw_response"] == {}
        assert not all(c["passed"] for c in verify_energy(session, run))


def test_comparisons_match_selected_real_locations_without_join_inflation(
    session_factory, tmp_path
):
    with session_factory() as session:
        run = ingest(
            session,
            tmp_path,
            payloads={
                "generation": document(),
                "price": document("price", value=40),
            },
        )
        session.add(
            DataSource(
                id="test-environment",
                name="Synthetic protocol test ONLY",
                provenance="real",
                license="Test schema only",
            )
        )
        session.flush()
        for station_id, kind in (
            ("test-river", "river"),
            ("test-grid", "weather"),
            ("other-grid", "weather"),
        ):
            session.add(
                Station(
                    id=station_id,
                    source_id="test-environment",
                    name=station_id,
                    kind=kind,
                )
            )
        session.flush()
        session.add(
            IngestionRun(
                id="environment-run",
                source_id="test-environment",
                started_at=START,
                completed_at=END,
                status="completed",
                checksum="test-only",
                bronze_path="test-only",
            )
        )
        session.flush()
        common = {
            "source_id": "test-environment",
            "run_id": "environment-run",
            "event_time": START,
            "ingested_at": END,
            "processed_at": END,
            "quality_status": "valid",
        }
        session.add(
            RiverObservation(
                **common,
                source_record_id="r",
                station_id="test-river",
                water_level_m=3,
                measurement_type="water_level",
            )
        )
        session.add(
            WeatherObservation(
                **common,
                source_record_id="w",
                station_id="test-grid",
                temperature_c=10,
                precipitation_mm=2,
            )
        )
        session.add(
            WeatherObservation(
                **common,
                source_record_id="unrelated",
                station_id="other-grid",
                temperature_c=99,
                precipitation_mm=90,
            )
        )
        session.commit()
        result = main.energy_comparisons(
            river_station_id="test-river",
            weather_station_id="test-grid",
            session=session,
        )
        assert len(result["rows"]) == 2  # One generation type and one price series.
        assert all(row["average_temperature_c"] == 10 for row in result["rows"])
        assert all(row["weather_count"] == 1 for row in result["rows"])
        assert (
            next(row for row in result["rows"] if row["metric"] == "generation")["generation_mwh"]
            == 2400
        )
        revised = ingest(
            session,
            tmp_path,
            payloads={
                "generation": document(value=200, revision=2),
            },
        )
        assert revised.status == "completed"
        assert all(c["passed"] for c in verify_energy(session, run))
        assert session.scalar(select(func.count()).select_from(RiverObservation)) == 1
        assert session.scalar(select(func.count()).select_from(WeatherObservation)) == 2
