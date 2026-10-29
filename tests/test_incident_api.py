from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from danubepulse import main
from danubepulse.db import get_session


def test_scenario_api_exposes_validation_impact_recovery_and_reset(
    tmp_path: Path,
    monkeypatch,
    session_factory,
) -> None:
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(
            data_mode="demo",
            bronze_dir=tmp_path,
            log_level="INFO",
            demo_control_token="test-demo-control-token-" + "x" * 32,
        ),
    )

    def override_session():
        with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_session] = override_session
    try:
        with TestClient(main.app) as client:
            headers = {"X-Demo-Control-Token": "test-demo-control-token-" + "x" * 32}
            response = client.post("/v1/demo/scenarios/duplicate_records/simulate", headers=headers)
            assert response.status_code == 200
            initial = response.json()
            incident_id = initial["id"]
            assert initial["status"] == "detected"
            assert client.get("/v1/summary").json()["analytics_trust"] == "degraded"
            assert any(
                row["check_name"] == "duplicate_events" and row["phase"] == "detection"
                for row in initial["quality_results"]
            )
            investigating = client.post(f"/v1/incidents/{incident_id}/investigate", headers=headers)
            assert investigating.status_code == 200
            assert investigating.json()["status"] == "investigating"

            recovered = client.post(f"/v1/incidents/{incident_id}/replay", headers=headers)
            assert recovered.status_code == 200
            detail = client.get("/v1/incidents").json()[0]
            assert detail["status"] == "recovered"
            assert detail["recovery_verified"] is True
            assert detail["before_metrics"] != detail["after_metrics"]
            assert detail["before_metrics"]["daily"][0]["observation_count"] == 5
            assert detail["after_metrics"]["daily"][0]["observation_count"] == 4
            assert detail["after_metrics"]["daily"][0]["average_water_level_m"] == 2.8225
            summary = client.get("/v1/summary").json()
            assert summary["analytics_trust"] == "trusted"
            gold = next(row for row in summary["daily"] if row["domain"] == "river")
            assert gold["observation_count"] == 4
            assert gold["average_primary"] == 2.8225
            assert gold["average_secondary"] == 1511.25
            assert any(row["phase"] == "recovery" for row in detail["quality_results"])
            assert detail["audit_trail"][-1]["event_type"] == "recovery_verified"
            repeated = client.post(f"/v1/incidents/{incident_id}/replay", headers=headers)
            assert repeated.status_code == 200
            assert repeated.json() == recovered.json()
            assert len(client.get("/v1/incidents").json()[0]["replays"]) == 1

            reset = client.post("/v1/demo/reset", headers=headers)
            assert reset.status_code == 200
            assert reset.json()["incidents_reset"] == 1
            assert client.get("/v1/incidents").json()[0]["status"] == "reset"
    finally:
        main.app.dependency_overrides.clear()
