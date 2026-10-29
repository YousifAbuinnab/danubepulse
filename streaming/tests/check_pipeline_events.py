"""Real Kafka delivery from existing synthetic ingestion in a disposable PostgreSQL schema."""

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from danubepulse.models import PipelineExecution, StreamingOutbox, StreamingReceipt
from danubepulse.pipeline import replay_incident, run_demo, simulate_scenario
from danubepulse.streaming import consume, make_producer, publish_batch
from danubepulse.time_utils import utc_now


def main() -> None:
    schema = "test_streaming_" + uuid4().hex
    admin = create_engine(os.environ["DATABASE_URL"])
    with admin.begin() as connection:
        connection.execute(CreateSchema(schema))
    engine = create_engine(
        os.environ["DATABASE_URL"], connect_args={"options": f"-csearch_path={schema}"}
    )
    try:
        with engine.begin() as connection:
            config = Config("/app/alembic.ini")
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
        with TemporaryDirectory(prefix="streaming-fixture-") as directory, factory() as session:
            run_demo(session, Path(directory))
            incident = simulate_scenario(session, Path(directory), "schema_drift")
            replay = replay_incident(session, incident.id)
            assert replay.status == "completed" and incident.status == "recovered"
            execution = PipelineExecution(
                id=str(uuid4()),
                domain="river",
                orchestrator_run_id="synthetic-kafka-contract",
                ingestion_run_id=incident.run_id,
                status="running",
                started_at=utc_now(),
            )
            session.add(execution)
            session.commit()
            execution.status = "failed"
            execution.verification = [
                {"check": "synthetic_verification_failure", "passed": False, "detail": "test only"}
            ]
            session.commit()
            event_ids = list(session.scalars(select(StreamingOutbox.id)))
            event_types = {
                row.payload["event_type"] for row in session.scalars(select(StreamingOutbox))
            }
            assert {
                "ingestion.failed",
                "ingestion.completed",
                "incident.changed",
                "quality.alert",
                "pipeline.verification_failed",
            } <= event_types
            producer = make_producer("kafka:9092")
            published = publish_batch(factory, producer)
            assert published["failed"] == 0 and published["published"] == len(event_ids)
            counts = consume(factory, "kafka:9092", schema, seconds=15)
            with factory() as verification:
                for event_id in event_ids:
                    receipt = verification.get(StreamingReceipt, event_id)
                    assert receipt is not None and receipt.payload["provenance"] == "synthetic"
                for event in verification.scalars(select(StreamingOutbox)):
                    if event.id in event_ids:
                        assert event.published_at is not None
            print(
                json.dumps(
                    {
                        "synthetic_pipeline_events_verified": len(event_ids),
                        "event_types": sorted(event_types),
                        "consumer": counts,
                        "incident_recovery_preserved": True,
                        "production_measurements_modified": False,
                    }
                )
            )
            producer.flush(1)
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        admin.dispose()


if __name__ == "__main__":
    main()
