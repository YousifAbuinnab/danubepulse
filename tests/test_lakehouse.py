import io
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient
from lakehouse.snapshot import normalize, timestamp

from danubepulse import main
from danubepulse.db import get_session
from danubepulse.lakehouse import InvalidManifest, lakehouse_status
from danubepulse.pipeline import run_demo


def test_snapshot_normalization_preserves_units_lineage_and_identity():
    time = datetime(2026, 10, 25, tzinfo=UTC)
    row = {
        "provenance": "real",
        "source_id": "entsoe-load-forecast",
        "region": "AT",
        "event_time": time,
        "interval_end": time + timedelta(minutes=15),
        "duration_hours": 0.25,
        "metric": "load_forecast",
        "load_forecast_mw": 6000,
        "source_record_id": "record",
        "run_id": "run",
        "document_id": "provider-doc",
        "revision": 1,
        "source_published_at": time,
        "ingested_at": time,
        "processed_at": time,
        "bronze_path": "/raw/response.xml",
        "checksum": "a" * 64,
        "source_name": "ENTSO-E",
        "license": "CC BY 4.0",
        "reference_url": "https://entsoe.eu",
    }
    (point,) = normalize("energy", row, "real")
    assert point["value"] == 6000 and point["unit"] == "MW"
    assert point["metric"] == "load_forecast_mw" and point["location"] == "AT"
    assert point["bronze_checksum"] == "a" * 64 and point["revision"] == 1
    (again,) = normalize("energy", dict(row, load_forecast_mw=6100), "real")
    assert point["key"] == again["key"] and point["content_hash"] != again["content_hash"]
    with pytest.raises(ValueError, match="provenance"):
        normalize("energy", row, "synthetic")
    with pytest.raises(ValueError, match="duration disagree"):
        normalize("energy", dict(row, duration_hours=1), "real")
    with pytest.raises(ValueError, match="metric/value"):
        normalize("energy", dict(row, metric="generation"), "real")
    with pytest.raises(ValueError, match="non-finite"):
        normalize("energy", dict(row, load_forecast_mw=float("nan")), "real")
    assert normalize("energy", dict(row, load_forecast_mw=None), "real") == []


def test_snapshot_requires_explicit_timezone_and_normalizes_dst():
    assert timestamp("2026-10-25T02:30:00+02:00") == "2026-10-25T00:30:00+00:00"
    assert timestamp("2026-10-25T02:30:00+01:00") == "2026-10-25T01:30:00+00:00"
    with pytest.raises(ValueError, match="timezone-aware"):
        timestamp(datetime(2026, 10, 25))


def test_lakehouse_api_is_read_only_and_reports_unprocessed_or_failed(tmp_path, monkeypatch):
    settings = main.get_settings().model_copy(update={"lakehouse_root": tmp_path})
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as client:
        response = client.get("/v1/lakehouse")
        assert response.status_code == 200
        assert all(
            state["status"] == "not_processed" for state in response.json()["scopes"].values()
        )
        assert client.post("/v1/lakehouse").status_code == 405
        real = tmp_path / "real"
        real.mkdir()
        success = {
            "status": "success",
            "id": "verified",
            "provenance": "real",
            "finished_at": "2026-10-28T00:00:00+00:00",
            "silver_active_rows": 0,
            "gold_preview": [],
            "sources": [],
            "tables": {
                layer: {"rows": 0, "files": 0, "size_bytes": 0, "version": 0, "history": []}
                for layer in ("bronze", "silver", "gold")
            },
        }
        (real / "status.json").write_text(json.dumps(success))
        (real / "latest_attempt.json").write_text(
            json.dumps({"status": "failed", "error": "schema mismatch"})
        )
        state = client.get("/v1/lakehouse").json()["scopes"]["real"]
        assert state["status"] == "failed" and state["last_success"] == success
        (real / "status.json").write_text("broken JSON")
        assert client.get("/v1/lakehouse").status_code == 503
        with pytest.raises(InvalidManifest):
            lakehouse_status(tmp_path)


def test_dashboard_renders_verified_lakehouse_and_separate_scale_results(
    session_factory, tmp_path, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    settings = main.get_settings().model_copy(update={"lakehouse_root": tmp_path})
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with session_factory() as session:
        run_demo(session, tmp_path / "bronze")

    def sessions():
        with session_factory() as session:
            yield session

    report = {
        "status": "success",
        "id": "test-run",
        "finished_at": "2026-10-27T00:00:00+00:00",
        "provenance": "real",
        "silver_active_rows": 96,
        "sources": [],
        "gold_preview": [{"metric": "load_forecast_mw", "integrated_mwh": 164825.0}],
        "tables": {
            layer: {"version": 2, "rows": 96, "files": 2, "size_bytes": 1000, "history": []}
            for layer in ("bronze", "silver", "gold")
        },
    }
    scale = dict(
        report,
        provenance="synthetic-scale-test",
        note="Artificial scale data only",
        results=[{"unique_rows": 10000, "input_rows": 20000, "seconds": 1.0, "history": []}],
    )
    for scope, data in (("real", report), ("synthetic-scale", scale)):
        (tmp_path / scope).mkdir()
        for name in ("status", "latest_attempt"):
            (tmp_path / scope / f"{name}.json").write_text(json.dumps(data))
    main.app.dependency_overrides[get_session] = sessions
    try:
        with TestClient(main.app) as client:

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
            assert not page.exception
            assert any("event_type" in item.value.columns for item in page.dataframe)
            assert any("Workers have not started" in item.value for item in page.info)
            assert any("integrated_mwh" in item.value.columns for item in page.dataframe)
            selector = next(item for item in page.selectbox if item.label == "Lakehouse namespace")
            selector.set_value("synthetic-scale")
            page.run(timeout=45)
            assert not page.exception
            assert any("SYNTHETIC-SCALE-TEST" in item.value for item in page.warning)
            assert any("input_rows" in item.value.columns for item in page.dataframe)
    finally:
        main.app.dependency_overrides.clear()
