import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import func, select, text

from danubepulse import pipeline
from danubepulse.models import (
    Incident,
    IngestionRun,
    QualityResult,
    QuarantinedRecord,
    ReplayHistory,
    RiverObservation,
    WeatherObservation,
)
from danubepulse.pipeline import (
    ingest_records,
    replay_incident,
    replay_is_verified,
    reset_demo,
    run_demo,
    simulate_scenario,
)


@pytest.fixture
def session_and_bronze(tmp_path: Path, session_factory):
    with session_factory() as session:
        assert session.autoflush is False
        yield session, tmp_path


def _metric(incident: Incident, key: str) -> dict:
    assert incident.before_metrics is not None
    return incident.before_metrics["daily"][0][key]


def test_schema_drift_is_quarantined_then_corrected_and_verified(session_and_bronze) -> None:
    session, bronze_dir = session_and_bronze
    result = run_demo(session, bronze_dir)
    incident = session.get(Incident, result["incident_id"])
    assert incident is not None and incident.status == "detected"
    assert incident.scenario == "schema_drift"
    assert incident.affected_records[0]["source_record_id"] == "demo-river-schema-007"

    raw_record = session.scalar(
        select(QuarantinedRecord).where(QuarantinedRecord.incident_id == incident.id)
    )
    assert raw_record is not None
    assert raw_record.status == "quarantined"
    assert "water_level_cm" in raw_record.raw_record
    assert (
        session.scalar(
            select(RiverObservation).where(
                RiverObservation.source_id == "synthetic-schema",
                RiverObservation.source_record_id == "demo-river-schema-007",
            )
        )
        is None
    )

    detection = session.scalar(
        select(QualityResult).where(
            QualityResult.incident_id == incident.id,
            QualityResult.phase == "detection",
            QualityResult.check_name == "schema_change",
        )
    )
    assert detection is not None and detection.status == "failed"
    bronze_path = Path(
        session.get(IngestionRun, incident.run_id).bronze_path  # type: ignore[union-attr]
    )
    original_checksum = hashlib.sha256(bronze_path.read_bytes()).hexdigest()

    replay = replay_incident(session, incident.id)
    observation = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-schema",
            RiverObservation.source_record_id == "demo-river-schema-007",
        )
    )
    assert observation is not None
    assert observation.water_level_m == pytest.approx(2.88)
    assert observation.quality_status == "valid"
    assert incident.status == "recovered"
    assert incident.after_metrics is not None
    assert _metric(incident, "observation_count") == 5
    assert incident.after_metrics["daily"][0]["observation_count"] == 6
    assert raw_record.status == "recovered"
    assert replay.validation_results is not None
    assert (
        next(
            check for check in replay.validation_results if check["check_name"] == "schema_change"
        )["status"]
        == "passed"
    )
    assert (
        session.scalar(
            select(QualityResult).where(
                QualityResult.incident_id == incident.id,
                QualityResult.phase == "detection",
                QualityResult.check_name == "schema_change",
            )
        ).status
        == "failed"
    )
    assert hashlib.sha256(bronze_path.read_bytes()).hexdigest() == original_checksum

    replay_again = replay_incident(session, incident.id)
    assert replay_again.id == replay.id
    assert (
        session.scalar(
            select(func.count())
            .select_from(RiverObservation)
            .where(
                RiverObservation.source_id == "synthetic-schema",
                RiverObservation.source_record_id == "demo-river-schema-007",
            )
        )
        == 1
    )


def test_duplicate_recovery_deduplicates_gold_and_is_idempotent(session_and_bronze) -> None:
    session, bronze_dir = session_and_bronze
    run_demo(session, bronze_dir)
    weather_before = session.scalar(select(func.count()).select_from(WeatherObservation))
    river_before = session.scalar(
        select(func.count())
        .select_from(RiverObservation)
        .where(RiverObservation.source_id == "synthetic-river")
    )
    incident = simulate_scenario(session, bronze_dir, "duplicate_records")
    assert incident.status == "detected"
    assert _metric(incident, "observation_count") == 5
    assert _metric(incident, "average_water_level_m") > 3.8
    duplicate_check = session.scalar(
        select(QualityResult).where(
            QualityResult.incident_id == incident.id,
            QualityResult.phase == "detection",
            QualityResult.check_name == "duplicate_events",
        )
    )
    assert duplicate_check is not None and duplicate_check.status == "warning"
    source_run = session.get(IngestionRun, incident.run_id)
    assert source_run is not None
    bronze_path = Path(source_run.bronze_path)
    original_bronze = bronze_path.read_bytes()

    first = replay_incident(session, incident.id)
    assert incident.status == "recovered"
    assert first.after_metrics is not None
    assert first.after_metrics["daily"][0]["observation_count"] == 4
    assert first.after_metrics["daily"][0]["average_water_level_m"] == pytest.approx(2.8225)
    assert first.after_metrics["daily"][0]["average_discharge_m3_s"] == pytest.approx(1511.25)
    gold = (
        session.execute(
            text("SELECT * FROM gold_daily_observations WHERE location = 'AT-DANUBE-DUPLICATE'")
        )
        .mappings()
        .one()
    )
    assert gold["observation_count"] == 4
    assert gold["average_primary"] == pytest.approx(2.8225)
    assert gold["average_secondary"] == pytest.approx(1511.25)
    recovery_check = next(
        row for row in first.validation_results if row["check_name"] == "duplicate_events"
    )
    assert recovery_check["status"] == "passed"
    duplicate_row = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-duplicate",
            RiverObservation.source_record_id == "duplicate-river-001-copy",
        )
    )
    assert duplicate_row is not None and duplicate_row.quality_status == "duplicate"

    replay_again = replay_incident(session, incident.id)
    assert replay_again.id == first.id
    bronze_payload = json.loads(original_bronze)
    repeat_run = ingest_records(
        session,
        bronze_dir,
        source_id=source_run.source_id,
        domain="river",
        records=bronze_payload["records"],
        raw_payload={"records": bronze_payload["records"]},
    )
    session.commit()
    assert repeat_run.record_count == 0
    assert bronze_path.read_bytes() == original_bronze
    assert (
        session.execute(
            text(
                "SELECT observation_count FROM gold_daily_observations "
                "WHERE domain = 'river' AND location = 'AT-DANUBE-DUPLICATE'"
            )
        ).scalar_one()
        == 4
    )
    assert (
        session.scalar(
            select(func.count())
            .select_from(RiverObservation)
            .where(RiverObservation.source_id == "synthetic-duplicate")
        )
        == 5
    )
    assert (
        session.scalar(
            select(func.count())
            .select_from(RiverObservation)
            .where(
                RiverObservation.source_id == "synthetic-duplicate",
                RiverObservation.quality_status == "valid",
            )
        )
        == 4
    )
    assert (
        session.scalar(
            select(func.count())
            .select_from(RiverObservation)
            .where(RiverObservation.source_id == "synthetic-river")
        )
        == river_before
    )
    assert session.scalar(select(func.count()).select_from(WeatherObservation)) == weather_before
    assert len(incident.replays) == 1
    assert len(incident.events) >= 3


def test_missing_late_backfill_restores_hourly_coverage(session_and_bronze) -> None:
    session, bronze_dir = session_and_bronze
    run_demo(session, bronze_dir)
    weather_before = session.scalar(select(func.count()).select_from(WeatherObservation))
    incident = simulate_scenario(session, bronze_dir, "missing_late")
    assert incident.status == "detected"
    assert _metric(incident, "observation_count") == 4
    check_by_name = {
        row.check_name: row
        for row in session.scalars(
            select(QualityResult).where(
                QualityResult.incident_id == incident.id,
                QualityResult.phase == "detection",
            )
        )
    }
    assert check_by_name["hourly_completeness"].status == "warning"
    assert check_by_name["late_arrival"].status == "warning"
    late_row = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-missing-late",
            RiverObservation.source_record_id == "missing-late-river-003",
        )
    )
    assert late_row is not None
    assert late_row.ingested_at > late_row.event_time
    assert late_row.source_published_at > late_row.event_time
    neighbors = session.scalars(
        select(RiverObservation).where(RiverObservation.source_id == "synthetic-missing-late")
    ).all()
    neighbor_snapshot = {
        row.source_record_id: (
            row.id,
            row.run_id,
            row.event_time,
            row.source_published_at,
            row.ingested_at,
            row.processed_at,
            row.water_level_m,
            row.discharge_m3_s,
            row.quality_status,
        )
        for row in neighbors
    }

    replay = replay_incident(session, incident.id)
    backfilled = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-missing-late",
            RiverObservation.source_record_id == "missing-late-river-002",
        )
    )
    assert backfilled is not None and backfilled.quality_status == "valid"
    assert backfilled.ingested_at > backfilled.event_time
    assert replay.after_metrics is not None
    assert replay.after_metrics["daily"][0]["observation_count"] == 5
    recovered_checks = {item["check_name"]: item for item in replay.validation_results}
    assert recovered_checks["hourly_completeness"]["status"] == "passed"
    assert recovered_checks["late_arrival"]["status"] == "warning"
    assert incident.status == "recovered"
    assert session.scalar(select(func.count()).select_from(WeatherObservation)) == weather_before
    assert {
        row.source_record_id: (
            row.id,
            row.run_id,
            row.event_time,
            row.source_published_at,
            row.ingested_at,
            row.processed_at,
            row.water_level_m,
            row.discharge_m3_s,
            row.quality_status,
        )
        for row in session.scalars(
            select(RiverObservation).where(
                RiverObservation.source_id == "synthetic-missing-late",
                RiverObservation.source_record_id != "missing-late-river-002",
            )
        )
    } == neighbor_snapshot


def test_reset_preserves_bronze_and_audit_history(session_and_bronze) -> None:
    session, bronze_dir = session_and_bronze
    run_demo(session, bronze_dir)
    incidents = [
        simulate_scenario(session, bronze_dir, scenario)
        for scenario in ("schema_drift", "duplicate_records", "missing_late")
    ]
    bronze_snapshots = {}
    for incident in incidents:
        run = session.get(IngestionRun, incident.run_id)
        assert run is not None
        path = Path(run.bronze_path)
        bronze_snapshots[incident.id] = (path, path.read_bytes())
        replay_incident(session, incident.id)

    result = reset_demo(session)
    assert result["incidents_reset"] == 3
    assert all(incident.status == "reset" for incident in incidents)
    for incident in incidents:
        path, original = bronze_snapshots[incident.id]
        assert path.read_bytes() == original
        assert len(incident.replays) == 1
        assert any(event.event_type == "demo_reset" for event in incident.events)
    schema_row = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-schema",
            RiverObservation.source_record_id == "demo-river-schema-007",
        )
    )
    assert schema_row is not None and schema_row.quality_status == "quarantined"
    duplicate_row = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-duplicate",
            RiverObservation.source_record_id == "duplicate-river-001-copy",
        )
    )
    assert duplicate_row is not None and duplicate_row.quality_status == "valid"
    backfill = session.scalar(
        select(RiverObservation).where(
            RiverObservation.source_id == "synthetic-missing-late",
            RiverObservation.source_record_id == "missing-late-river-002",
        )
    )
    assert backfill is not None and backfill.quality_status == "quarantined"

    new_incident = simulate_scenario(session, bronze_dir, "schema_drift")
    assert new_incident.id != incidents[0].id
    assert new_incident.status == "detected"


def test_tampered_bronze_records_failed_recovery_without_silver_changes(session_and_bronze) -> None:
    session, bronze_dir = session_and_bronze
    incident = simulate_scenario(session, bronze_dir, "schema_drift")
    run = session.get(IngestionRun, incident.run_id)
    assert run is not None
    path = Path(run.bronze_path)
    payload = json.loads(path.read_text())
    payload["records"][0]["water_level_cm"] = 999
    path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="checksum mismatch"):
        replay_incident(session, incident.id)
    assert incident.status == "failed_recovery"
    assert (
        session.scalar(
            select(RiverObservation).where(
                RiverObservation.source_id == "synthetic-schema",
                RiverObservation.source_record_id == "demo-river-schema-007",
            )
        )
        is None
    )
    assert (
        session.scalar(
            select(ReplayHistory).where(
                ReplayHistory.incident_id == incident.id,
                ReplayHistory.status == "failed",
            )
        )
        is not None
    )
    assert any(event.event_type == "recovery_failed" for event in incident.events)


def test_recovered_label_without_recovery_evidence_is_not_trusted() -> None:
    incident = Incident(
        id="legacy-incident",
        run_id="legacy-run",
        category="schema_change",
        scenario="schema_change",
        severity="high",
        status="recovered",
        title="Legacy recovered incident",
        details="No recovery validation was stored by the previous implementation.",
        downstream_impact={},
        detected_at=datetime.now(UTC),
    )
    incident.replays = [
        ReplayHistory(
            id="legacy-replay",
            incident_id=incident.id,
            replayed_at=incident.detected_at,
            status="completed",
            message="Legacy replay with no validation snapshot.",
        )
    ]
    assert not replay_is_verified(incident)


def test_stale_gold_snapshot_fails_recovery_and_rolls_back(session_and_bronze, monkeypatch) -> None:
    session, bronze_dir = session_and_bronze
    incident = simulate_scenario(session, bronze_dir, "duplicate_records")
    before = incident.before_metrics
    monkeypatch.setattr(pipeline, "_daily_metrics", lambda *_: before)

    with pytest.raises(ValueError, match="Downstream Gold verification failed"):
        replay_incident(session, incident.id)

    assert incident.status == "failed_recovery"
    assert not replay_is_verified(incident)
    assert (
        session.execute(
            text(
                "SELECT observation_count FROM gold_daily_observations "
                "WHERE location = 'AT-DANUBE-DUPLICATE'"
            )
        ).scalar_one()
        == 5
    )
    assert (
        session.scalar(
            select(RiverObservation.quality_status).where(
                RiverObservation.source_id == "synthetic-duplicate",
                RiverObservation.source_record_id == "duplicate-river-001-copy",
            )
        )
        == "valid"
    )
    assert (
        session.scalar(
            select(func.count())
            .select_from(ReplayHistory)
            .where(ReplayHistory.incident_id == incident.id, ReplayHistory.status == "failed")
        )
        == 1
    )


def test_legacy_duplicate_replay_is_reverified_without_rewriting_history(
    session_and_bronze,
) -> None:
    session, bronze_dir = session_and_bronze
    incident = simulate_scenario(session, bronze_dir, "duplicate_records")
    first = replay_incident(session, incident.id)
    first.validation_results = [
        check
        for check in first.validation_results
        if check["check_name"] not in {"persisted_recovery", "downstream_gold"}
    ]
    first.after_metrics = first.before_metrics
    incident.after_metrics = first.before_metrics
    incident.affected_records = []
    session.commit()
    assert not replay_is_verified(incident)

    repaired = replay_incident(session, incident.id)
    assert repaired.id != first.id
    assert replay_is_verified(incident)
    assert repaired.before_metrics["daily"][0]["observation_count"] == 4
    assert repaired.after_metrics["daily"][0]["observation_count"] == 4
    assert first.after_metrics["daily"][0]["observation_count"] == 5
    assert (
        session.scalar(
            select(func.count())
            .select_from(ReplayHistory)
            .where(ReplayHistory.incident_id == incident.id)
        )
        == 2
    )
