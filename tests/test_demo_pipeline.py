from pathlib import Path

from sqlalchemy import select

from danubepulse.models import Incident, IngestionRun, QualityResult, QuarantinedRecord
from danubepulse.pipeline import replay_incident, run_demo


def test_demo_pipeline_and_replay(tmp_path: Path, session_factory) -> None:
    with session_factory() as session:
        result = run_demo(session, tmp_path)
        assert result["status"] == "loaded"
        incident = session.scalar(select(Incident).where(Incident.status == "detected"))
        assert incident is not None
        assert incident.category == "schema_change"
        failed_schema_check = session.scalar(
            select(QualityResult).where(
                QualityResult.incident_id == incident.id,
                QualityResult.phase == "detection",
                QualityResult.check_name == "schema_change",
            )
        )
        assert failed_schema_check is not None and failed_schema_check.status == "failed"
        assert session.scalar(
            select(QuarantinedRecord).where(QuarantinedRecord.incident_id == incident.id)
        )

        completed_runs = session.scalars(
            select(IngestionRun).where(IngestionRun.status == "completed")
        ).all()
        assert len(completed_runs) >= 2

        replay = replay_incident(session, incident.id)
        assert replay.status == "completed"
        updated = session.get(Incident, incident.id)
        assert updated is not None and updated.status == "recovered"
        assert replay.validation_results is not None
