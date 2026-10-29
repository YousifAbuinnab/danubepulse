"""A65 tests use invented protocol values only in isolated test schemas."""

import io
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from danubepulse import cli, energy, energy_sources, main, orchestration
from danubepulse.db import get_session
from danubepulse.energy import ingest_energy, verify_energy
from danubepulse.energy_sources import (
    AUSTRIA,
    LOAD_FORECAST_SOURCE_ID,
    EnergySourceError,
    fetch_xml,
    parse_xml,
)
from danubepulse.models import (
    DataLineage,
    DataSource,
    EnergyObservation,
    IngestionRun,
    QualityResult,
)

DAY = date(2024, 10, 27)
START = datetime(2024, 10, 27, tzinfo=UTC)
END = START + timedelta(days=1)


def forecast_xml(value=5000, revision=1, slots=24, curve="A01"):
    positions = [1] if curve == "A03" else range(1, slots + 1)
    points = "".join(
        f"<Point><position>{position}</position><quantity>{value}</quantity></Point>"
        for position in positions
    )
    return (
        '<GL_MarketDocument xmlns="urn:synthetic:a65-protocol-test">'
        "<mRID>synthetic-load-forecast</mRID>"
        f"<revisionNumber>{revision}</revisionNumber><type>A65</type>"
        "<process.processType>A01</process.processType>"
        f"<createdDateTime>2024-10-26T1{revision}:00:00Z</createdDateTime>"
        "<TimeSeries><mRID>synthetic-series</mRID><businessType>A04</businessType>"
        f"<outBiddingZone_Domain.mRID>{AUSTRIA}</outBiddingZone_Domain.mRID>"
        "<quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>"
        f"<curveType>{curve}</curveType><Period>"
        f"<timeInterval><start>{START.isoformat()}</start><end>{END.isoformat()}</end>"
        f"</timeInterval><resolution>PT60M</resolution>{points}</Period>"
        "</TimeSeries></GL_MarketDocument>"
    )


def ingest_forecast(session, path, **kwargs):
    return ingest_energy(
        session,
        path,
        DAY,
        product="load_forecast",
        load_forecast_terms_accepted=True,
        **kwargs,
    )


def test_a65_exact_query_contract_and_no_generation_or_price_requests(monkeypatch):
    requests = []

    def response(request, **_kwargs):
        requests.append(parse_qs(urlsplit(request.full_url).query))
        return io.BytesIO(forecast_xml().encode())

    monkeypatch.setattr(energy_sources, "urlopen", response)
    monkeypatch.setattr(energy_sources.time, "sleep", lambda _seconds: None)
    raw = fetch_xml("test-token-not-a-credential", "load_forecast", START, END)
    assert requests == [
        {
            "securityToken": ["test-token-not-a-credential"],
            "documentType": ["A65"],
            "processType": ["A01"],
            "outBiddingZone_Domain": [AUSTRIA],
            "periodStart": ["202410270000"],
            "periodEnd": ["202410280000"],
            "curveType": ["A01"],
        }
    ]
    points = parse_xml(raw, "load_forecast", START, END)
    assert len(points) == 24
    assert all(p.metric == "load_forecast" and p.unit == "MAW" for p in points)
    assert all(p.production_type == "load" and p.hours == 1 for p in points)
    assert len(parse_xml(forecast_xml(curve="A03").encode(), "load_forecast", START, END)) == 24


@pytest.mark.parametrize(
    "old,new",
    [
        ("<type>A65</type>", "<type>A75</type>"),
        ("<process.processType>A01", "<process.processType>A16"),
        ("<businessType>A04", "<businessType>A01"),
        (AUSTRIA, "10YCZ-CEPS-----N"),
        ("MAW", "MWH"),
        ("<quantity>5000", "<quantity>-1"),
        ("<quantity>5000", "<quantity>nan"),
        ("<position>2</position>", "<position>1</position>"),
    ],
)
def test_reject_actual_load_wrong_area_units_and_bad_points(old, new):
    with pytest.raises(EnergySourceError):
        parse_xml(forecast_xml().replace(old, new).encode(), "load_forecast", START, END)


def test_product_specific_permission_does_not_unlock_other_products(session_factory, tmp_path):
    with session_factory() as session:
        with pytest.raises(EnergySourceError, match="separate permission"):
            ingest_energy(
                session,
                tmp_path,
                DAY,
                product="load_forecast",
                terms_accepted=True,
                payloads={"load_forecast": forecast_xml()},
            )
        with pytest.raises(EnergySourceError, match="ENTSOE_TERMS_ACCEPTED"):
            ingest_energy(
                session,
                tmp_path,
                DAY,
                load_forecast_terms_accepted=True,
            )
        assert session.scalar(select(func.count()).select_from(IngestionRun)) == 0
        assert not list(tmp_path.iterdir())
        run = ingest_forecast(session, tmp_path, payloads={"price": forecast_xml()})
        assert run.status == "failed"
        assert session.scalar(select(func.count()).select_from(EnergyObservation)) == 0


def test_a65_repeated_ingestion_revisions_and_separate_gold(session_factory, tmp_path):
    with session_factory() as session:
        first = ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        assert first.status == "completed" and first.accepted_count == 24
        assert first.source_id == LOAD_FORECAST_SOURCE_ID
        assert all(check["passed"] for check in verify_energy(session, first))
        second = ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        assert second.accepted_count == 0
        assert all(check["passed"] for check in verify_energy(session, second))
        gold = session.execute(text("SELECT * FROM gold_real_load_forecast_daily")).mappings().one()
        assert gold["forecast_demand_mwh"] == 120000
        assert gold["average_load_forecast_mw"] == 5000
        assert gold["coverage_hours"] == 24 and gold["observation_count"] == 24
        assert session.scalar(text("SELECT count(*) FROM gold_real_energy_daily")) == 0
        revised = ingest_forecast(
            session,
            tmp_path,
            payloads={
                "load_forecast": forecast_xml(value=6000, revision=2),
            },
        )
        assert revised.accepted_count == 24
        assert all(check["passed"] for check in verify_energy(session, revised))
        assert (
            session.scalar(text("SELECT forecast_demand_mwh FROM gold_real_load_forecast_daily"))
            == 144000
        )
        # Older identical input cannot re-activate a superseded forecast.
        ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        assert all(check["passed"] for check in verify_energy(session, first))
        rows = session.scalars(select(EnergyObservation)).all()
        assert len(rows) == 48
        assert sum(row.quality_status == "valid" for row in rows) == 24
        assert sum(row.quality_status == "superseded" for row in rows) == 24
        assert all(row.generation_mw is None and row.price_eur_mwh is None for row in rows)
        assert Path(first.bronze_path).read_text() != Path(revised.bronze_path).read_text()
        assert session.scalar(
            select(DataSource).where(DataSource.id == LOAD_FORECAST_SOURCE_ID)
        ).license.startswith("ENTSO-E 6.1.B")


@pytest.mark.parametrize(
    "body",
    [
        forecast_xml(slots=23),
        forecast_xml().replace("<quantity>5000</quantity>", "<quantity/>", 1),
    ],
)
def test_a65_gaps_and_nulls_are_not_filled(session_factory, tmp_path, body):
    with session_factory() as session:
        run = ingest_forecast(session, tmp_path, payloads={"load_forecast": body})
        assert run.status == "degraded"
        assert not all(check["passed"] for check in verify_energy(session, run))
        gold = session.execute(text("SELECT * FROM gold_real_load_forecast_daily")).mappings().one()
        assert gold["forecast_demand_mwh"] == 115000
        assert gold["coverage_hours"] == 23
        assert gold["average_load_forecast_mw"] == 5000


def test_a65_csv_uses_same_metric_and_permission_validation(session_factory, tmp_path):
    header = (
        "interval_start,interval_end,metric,production_type,value,unit,"
        "document_id,revision,published_at,region\n"
    )
    body = header + "".join(
        f"{(START + timedelta(hours=i)).isoformat()},"
        f"{(START + timedelta(hours=i + 1)).isoformat()},load_forecast,load,5000,MAW,"
        f"synthetic-csv,1,2024-10-26T12:00:00Z,{AUSTRIA}\n"
        for i in range(24)
    )
    with session_factory() as session:
        run = ingest_forecast(
            session,
            tmp_path,
            payloads={"load_forecast": body},
            import_format="csv",
        )
        assert run.status == "completed"
        assert all(check["passed"] for check in verify_energy(session, run))


def test_a65_api_and_dashboard_separate_product(session_factory, tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest

    settings = main.get_settings().model_copy(
        update={
            "data_mode": "real",
            "entsoe_api_token": None,
            "entsoe_terms_accepted": False,
        }
    )
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(orchestration, "get_settings", lambda: settings)
    with session_factory() as session:
        ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})

    def sessions():
        with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_session] = sessions
    try:
        with TestClient(main.app) as client:
            forecast = client.get("/v1/energy?product=load_forecast").json()
            assert forecast["terms_accepted"] is False
            assert forecast["source_id"] == LOAD_FORECAST_SOURCE_ID
            assert len(forecast["observations"]) == 24
            assert forecast["daily"][0]["forecast_demand_mwh"] == 120000
            assert "NOT actual demand" in forecast["product_label"]
            assert client.get("/v1/energy").json()["observations"] == []
            assert client.get("/v1/energy?product=unknown").status_code == 422
            # Existing automated energy ingestion cannot bypass A75/A44's closed gate.
            response = client.post(
                "/v1/pipelines/energy/ingest",
                json={
                    "target_date": DAY.isoformat(),
                    "orchestrator_run_id": "test:gated",
                },
            )
            assert response.status_code == 422

            def local_api(request, **_kwargs):
                url = urlsplit(request.full_url)
                response = client.request(request.method, url.path + "?" + url.query)
                response.raise_for_status()
                return io.BytesIO(response.content)

            monkeypatch.setattr("urllib.request.urlopen", local_api)
            monkeypatch.setenv("AIRFLOW_HEALTH_URL", "https://disabled.invalid/")
            page = AppTest.from_file(
                str(Path(__file__).resolve().parents[1] / "dashboard/app.py")
            ).run(timeout=45)
            selector = next(item for item in page.selectbox if item.label == "Energy data product")
            selector.set_value("load_forecast")
            page.run(timeout=45)
            assert not page.exception
            assert len(page.error) == 1
            assert "Latest failed execution:" in page.error[0].value
            assert "ENTSOE_TERMS_ACCEPTED=true" in page.error[0].value
            assert len(page.get("vega_lite_chart")) >= 1
            assert any("forecast_demand_mwh" in frame.value.columns for frame in page.dataframe)
    finally:
        main.app.dependency_overrides.clear()


def test_a65_cli_requires_explicit_one_run_review_before_migrating(monkeypatch):
    called = []
    monkeypatch.setattr(cli, "_upgrade", lambda: called.append("migration"))
    monkeypatch.setattr(
        "sys.argv",
        [
            "danubepulse",
            "ingest-energy",
            "--date",
            "2024-10-27",
            "--product",
            "load_forecast",
            "--accept-terms",
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2 and called == []


def test_a65_manual_fetch_requests_only_a65_with_general_gate_closed(
    session_factory, tmp_path, monkeypatch
):
    settings = main.get_settings().model_copy(
        update={
            "entsoe_api_token": "test-not-a-real-token",
            "entsoe_terms_accepted": False,
        }
    )
    monkeypatch.setattr(energy, "get_settings", lambda: settings)
    requests = []

    def fetch(token, kind, start, end):
        requests.append((kind, start, end))
        return forecast_xml().encode()

    monkeypatch.setattr(energy, "fetch_xml", fetch)
    with session_factory() as session:
        run = ingest_forecast(session, tmp_path)
        assert run.status == "completed"
        assert requests == [("load_forecast", START, END)]
        assert run.request_parameters["product"] == "load_forecast"
        assert "test-not-a-real-token" not in Path(run.bronze_path).read_text()


def test_documented_cli_command_verifies_a65_without_changing_general_permission(
    session_factory, tmp_path, monkeypatch, capsys
):
    settings = main.get_settings().model_copy(
        update={
            "data_mode": "real",
            "bronze_dir": tmp_path,
            "entsoe_terms_accepted": False,
        }
    )
    monkeypatch.setattr(cli, "_upgrade", lambda: None)
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "make_session_factory", lambda: session_factory)
    monkeypatch.setattr(energy, "fetch_xml", lambda *args: forecast_xml().encode())
    monkeypatch.setattr(
        "sys.argv",
        [
            "danubepulse",
            "ingest-energy",
            "--product",
            "load_forecast",
            "--date",
            DAY.isoformat(),
            "--accept-load-forecast-terms",
        ],
    )
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert result["verified"] is True and result["records"] == 24
    assert settings.entsoe_terms_accepted is False


def test_a65_does_not_change_existing_generation_or_its_gold(session_factory, tmp_path):
    generation = (
        forecast_xml()
        .replace("<type>A65", "<type>A75")
        .replace("<process.processType>A01", "<process.processType>A16")
        .replace("outBiddingZone_Domain", "inBiddingZone_Domain")
        .replace(
            "<businessType>A04</businessType>",
            "<businessType>A01</businessType><MktPSRType><psrType>B11</psrType></MktPSRType>",
        )
    )
    with session_factory() as session:
        original = ingest_energy(
            session,
            tmp_path,
            DAY,
            payloads={"generation": generation},
            terms_accepted=True,
        )
        before = [
            dict(row)
            for row in session.execute(
                text("SELECT * FROM energy_observations ORDER BY id")
            ).mappings()
        ]
        gold_before = [
            dict(row)
            for row in session.execute(text("SELECT * FROM gold_real_energy_daily")).mappings()
        ]
        ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        ingest_forecast(
            session,
            tmp_path,
            payloads={
                "load_forecast": forecast_xml(value=6000, revision=2),
            },
        )
        assert [
            dict(row)
            for row in session.execute(
                text(
                    "SELECT * FROM energy_observations WHERE source_id='entsoe-energy' ORDER BY id"
                )
            ).mappings()
        ] == before
        assert [
            dict(row)
            for row in session.execute(text("SELECT * FROM gold_real_energy_daily")).mappings()
        ] == gold_before
        assert all(check["passed"] for check in verify_energy(session, original))


def response_metadata(body, document, timestamp):
    return body.replace(
        "<mRID>synthetic-load-forecast</mRID>",
        f"<mRID>{document}</mRID>",
        1,
    ).replace("2024-10-26T11:00:00Z", timestamp, 1)


def test_identical_96_intervals_changed_response_metadata_reuse_silver(session_factory, tmp_path):
    body = forecast_xml(slots=96).replace("PT60M", "PT15M")
    response_a = response_metadata(body, "response-a", "2024-10-28T00:06:59Z")
    response_b = response_metadata(body, "response-b", "2024-10-28T00:09:03Z")
    with session_factory() as session:
        first = ingest_forecast(session, tmp_path, payloads={"load_forecast": response_a})
        original_rows = session.execute(text("SELECT * FROM energy_observations ORDER BY id")).all()
        gold_before = session.execute(text("SELECT * FROM gold_real_load_forecast_daily")).all()
        second = ingest_forecast(session, tmp_path, payloads={"load_forecast": response_b})
        third = ingest_forecast(session, tmp_path, payloads={"load_forecast": response_b})
        assert first.accepted_count == 96
        assert second.record_count == third.record_count == 96
        assert second.accepted_count == third.accepted_count == 0
        assert (
            session.execute(text("SELECT * FROM energy_observations ORDER BY id")).all()
            == original_rows
        )
        assert (
            session.execute(text("SELECT * FROM gold_real_load_forecast_daily")).all()
            == gold_before
        )
        gold = session.execute(text("SELECT * FROM gold_real_load_forecast_daily")).mappings().one()
        assert gold["observation_count"] == 96 and gold["forecast_demand_mwh"] == 120000
        assert gold["coverage_hours"] == 24 and gold["average_load_forecast_mw"] == 5000
        assert first.checksum != second.checksum
        assert len({first.bronze_path, second.bronze_path, third.bronze_path}) == 3
        assert session.scalar(select(func.count()).select_from(IngestionRun)) == 3
        assert (
            session.scalar(
                select(func.count())
                .select_from(EnergyObservation)
                .where(EnergyObservation.quality_status == "superseded")
            )
            == 0
        )
        links = session.scalars(select(DataLineage).where(DataLineage.run_id == second.id)).all()
        assert len(links) == 96
        assert all(link.source_record_id != link.target_record_id for link in links)
        assert all(
            check["passed"]
            for run in (first, second, third)
            for check in verify_energy(session, run)
        )
        # A real changed-value response with a later envelope still creates only changed versions.
        changed = response_metadata(
            body.replace("<quantity>5000</quantity>", "<quantity>6000</quantity>", 1),
            "response-c",
            "2024-10-28T00:10:00Z",
        )
        correction = ingest_forecast(session, tmp_path, payloads={"load_forecast": changed})
        assert correction.accepted_count == 1
        assert session.scalar(select(func.count()).select_from(EnergyObservation)) == 97
        gold = session.execute(text("SELECT * FROM gold_real_load_forecast_daily")).mappings().one()
        assert gold["observation_count"] == 96 and gold["forecast_demand_mwh"] == 120250
        assert all(
            check["passed"]
            for run in (first, second, correction)
            for check in verify_energy(session, run)
        )


@pytest.mark.parametrize("same_document", [True, False])
def test_metadata_only_responses_preserve_revision_ordering(
    session_factory, tmp_path, same_document
):
    body = forecast_xml()
    with session_factory() as session:
        first = ingest_forecast(session, tmp_path, payloads={"load_forecast": body})
        newer = (
            forecast_xml(revision=3)
            if same_document
            else response_metadata(body, "newer-response", "2024-10-26T13:00:00Z")
        )
        noop = ingest_forecast(session, tmp_path, payloads={"load_forecast": newer})
        assert noop.accepted_count == 0
        # An intervening stale correction cannot override the latest unchanged response.
        stale = (
            forecast_xml(value=6000, revision=2)
            if same_document
            else response_metadata(
                forecast_xml(value=6000),
                "stale-response",
                "2024-10-26T12:00:00Z",
            )
        )
        historical = ingest_forecast(session, tmp_path, payloads={"load_forecast": stale})
        assert historical.status == "completed"
        assert (
            session.scalar(text("SELECT forecast_demand_mwh FROM gold_real_load_forecast_daily"))
            == 120000
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(EnergyObservation)
                .where(EnergyObservation.quality_status == "valid")
            )
            == 24
        )
        latest = (
            forecast_xml(value=7000, revision=4)
            if same_document
            else response_metadata(
                forecast_xml(value=7000),
                "latest-response",
                "2024-10-26T14:00:00Z",
            )
        )
        correction = ingest_forecast(session, tmp_path, payloads={"load_forecast": latest})
        assert correction.accepted_count == 24
        assert (
            session.scalar(text("SELECT forecast_demand_mwh FROM gold_real_load_forecast_daily"))
            == 168000
        )
        assert all(
            check["passed"]
            for run in (first, noop, historical, correction)
            for check in verify_energy(session, run)
        )


def test_duplicate_lineage_verification_detects_corruption(session_factory, tmp_path):
    with session_factory() as session:
        ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        response = response_metadata(
            forecast_xml(),
            "different-response",
            "2024-10-26T12:00:00Z",
        )
        noop = ingest_forecast(session, tmp_path, payloads={"load_forecast": response})
        link = session.scalar(select(DataLineage).where(DataLineage.run_id == noop.id))
        link.target_record_id = "nonexistent-silver-record"
        session.commit()
        checks = verify_energy(session, noop)
        assert not next(c for c in checks if c["check"] == "persisted_silver")["passed"]


def test_null_observations_with_changed_metadata_do_not_create_versions(session_factory, tmp_path):
    body = forecast_xml().replace("<quantity>5000</quantity>", "<quantity/>", 1)
    with session_factory() as session:
        ingest_forecast(session, tmp_path, payloads={"load_forecast": body})
        noop = ingest_forecast(
            session,
            tmp_path,
            payloads={
                "load_forecast": response_metadata(body, "response-b", "2024-10-26T12:00:00Z"),
            },
        )
        assert noop.status == "degraded" and noop.accepted_count == 0
        assert session.scalar(select(func.count()).select_from(EnergyObservation)) == 24
        assert next(c for c in verify_energy(session, noop) if c["check"] == "persisted_silver")[
            "passed"
        ]


def test_stale_unchanged_response_cannot_lower_retained_revision(session_factory, tmp_path):
    with session_factory() as session:
        ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml(revision=3)})
        noop = ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        assert noop.accepted_count == 0
        stale = ingest_forecast(
            session,
            tmp_path,
            payloads={
                "load_forecast": forecast_xml(value=6000, revision=2),
            },
        )
        assert stale.status == "completed"
        assert (
            session.scalar(text("SELECT forecast_demand_mwh FROM gold_real_load_forecast_daily"))
            == 120000
        )


def test_a65_verifier_detects_corruption(session_factory, tmp_path):
    with session_factory() as session:
        run = ingest_forecast(session, tmp_path, payloads={"load_forecast": forecast_xml()})
        row = session.scalar(select(EnergyObservation))
        row.load_forecast_mw = 9999
        session.commit()
        assert not next(c for c in verify_energy(session, run) if c["check"] == "persisted_silver")[
            "passed"
        ]
        row.load_forecast_mw = 5000
        session.commit()
        Path(run.bronze_path).write_text(
            json.dumps(
                {
                    **json.loads(Path(run.bronze_path).read_text()),
                    "provenance": "synthetic",
                }
            )
        )
        assert not next(c for c in verify_energy(session, run) if c["check"] == "bronze_integrity")[
            "passed"
        ]
        # Missing original checks cannot produce a verified run.
        check = session.scalar(
            select(QualityResult).where(
                QualityResult.run_id == run.id, QualityResult.check_name == "interval_coverage"
            )
        )
        session.delete(check)
        session.commit()
        assert not next(c for c in verify_energy(session, run) if c["check"] == "source_quality")[
            "passed"
        ]
