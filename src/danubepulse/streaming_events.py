"""Versioned notification contract and transactional SQLAlchemy outbox capture."""

import json
from datetime import datetime
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from sqlalchemy import event, inspect
from sqlalchemy.orm import Session

from danubepulse.models import (
    DataSource,
    Incident,
    IngestionRun,
    PipelineExecution,
    QualityResult,
    StreamingOutbox,
)
from danubepulse.time_utils import utc_now

EVENT_TOPIC = "danubepulse.events.v1"
DLQ_TOPIC = "danubepulse.dead-letters.v1"


class EventData(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class IngestionData(EventData):
    source_id: str
    status: Literal["completed", "degraded", "failed"]
    record_count: int | None
    accepted_count: int | None
    rejected_count: int | None


class PipelineData(EventData):
    domain: Literal["river", "weather", "energy"]
    status: Literal["success", "failed"]
    ingestion_run_id: str | None
    failed_checks: list[str]


class IncidentData(EventData):
    category: str
    severity: str
    status: str
    run_id: str
    scenario: str | None


class QualityData(EventData):
    check: str
    status: str
    affected_count: int | None
    phase: str | None
    incident_id: str | None
    checked_at: str


class TestData(EventData):
    scenario: Literal["outage-recovery", "replay"]
    notification: Literal["ingestion_completed", "verification_failed", "quality_incident"]
    measurements_modified: Literal[False]


class Notification(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal[1]
    event_id: str
    event_type: Literal[
        "ingestion.completed",
        "ingestion.failed",
        "pipeline.verified",
        "pipeline.verification_failed",
        "pipeline.failed",
        "incident.changed",
        "quality.alert",
        "streaming.test",
    ]
    occurred_at: str
    provenance: Literal["real", "synthetic", "operational", "synthetic-streaming-test"]
    entity_id: str
    data: IngestionData | PipelineData | IncidentData | QualityData | TestData

    @model_validator(mode="after")
    def matching_data_contract(self) -> "Notification":
        expected = {
            "ingestion": IngestionData,
            "pipeline": PipelineData,
            "incident": IncidentData,
            "quality": QualityData,
            "streaming": TestData,
        }[self.event_type.split(".")[0]]
        if not isinstance(self.data, expected):
            raise ValueError("Event type and data contract disagree.")
        return self

    @field_validator("event_id")
    @classmethod
    def valid_uuid(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("Noncanonical event UUID.")
        return value

    @field_validator("occurred_at")
    @classmethod
    def aware_timestamp(cls, value: str) -> str:
        if datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("Event timestamp must include a timezone.")
        return value


def enqueue(
    session: Session,
    event_type: str,
    entity_id: str,
    provenance: str,
    data: dict[str, Any],
    *,
    identity: str | None = None,
) -> StreamingOutbox:
    # Identity excludes publication/processing times so retries retain the original UUID.
    key = identity or json.dumps([event_type, entity_id, data], sort_keys=True)
    event_id = str(uuid5(NAMESPACE_URL, "danubepulse:v1:" + key))
    pending = next(
        (row for row in session.new if isinstance(row, StreamingOutbox) and row.id == event_id),
        None,
    )
    existing = pending or session.get(StreamingOutbox, event_id)
    if existing is not None:
        return existing
    now = utc_now()
    payload = Notification(
        schema_version=1,
        event_id=event_id,
        event_type=event_type,
        entity_id=entity_id,
        provenance=provenance,
        occurred_at=now.isoformat(),
        data=data,
    ).model_dump()
    row = StreamingOutbox(
        id=event_id,
        topic=EVENT_TOPIC,
        payload=payload,
        created_at=now,
        next_attempt_at=now,
        attempts=0,
    )
    session.add(row)
    return row


def _find(session: Session, model: type, key: str) -> Any:
    return next(
        (row for row in session.new if isinstance(row, model) and row.id == key), None
    ) or session.get(model, key)


def _provenance(session: Session, run_id: str | None = None, source_id: str | None = None) -> str:
    run = _find(session, IngestionRun, run_id) if run_id else None
    source_id = source_id or (run.source_id if run else None)
    source = _find(session, DataSource, source_id) if source_id else None
    return source.provenance if source else "operational"


@event.listens_for(Session, "before_flush")
def capture_notifications(session: Session, _context: Any, _instances: Any) -> None:
    """Capture all ORM ingestion paths, including CLI/manual ingestion and incident replay."""
    for row in list(session.new) + list(session.dirty):
        if isinstance(row, QualityResult) and row in session.new and row.status != "passed":
            enqueue(
                session,
                "quality.alert",
                row.run_id,
                _provenance(session, run_id=row.run_id),
                {
                    "check": row.check_name,
                    "status": row.status,
                    "affected_count": row.affected_count,
                    "phase": row.phase,
                    "incident_id": row.incident_id,
                    "checked_at": row.checked_at.isoformat(),
                },
            )
            continue
        if not isinstance(row, (IngestionRun, PipelineExecution, Incident)):
            continue
        if row not in session.new and not inspect(row).attrs.status.history.has_changes():
            continue
        if isinstance(row, IngestionRun) and row.status in {"completed", "degraded", "failed"}:
            enqueue(
                session,
                "ingestion.failed" if row.status == "failed" else "ingestion.completed",
                row.id,
                _provenance(session, source_id=row.source_id),
                {
                    "source_id": row.source_id,
                    "status": row.status,
                    "record_count": row.record_count,
                    "accepted_count": row.accepted_count,
                    "rejected_count": row.rejected_count,
                },
            )
        elif isinstance(row, PipelineExecution) and row.status in {"success", "failed"}:
            kind = (
                "pipeline.verified"
                if row.status == "success"
                else "pipeline.verification_failed"
                if row.verification is not None
                else "pipeline.failed"
            )
            enqueue(
                session,
                kind,
                row.id,
                _provenance(session, run_id=row.ingestion_run_id),
                {
                    "domain": row.domain,
                    "status": row.status,
                    "ingestion_run_id": row.ingestion_run_id,
                    "failed_checks": [
                        item["check"] for item in (row.verification or []) if not item["passed"]
                    ],
                },
            )
        elif isinstance(row, Incident):
            enqueue(
                session,
                "incident.changed",
                row.id,
                _provenance(session, run_id=row.run_id),
                {
                    "category": row.category,
                    "severity": row.severity,
                    "status": row.status,
                    "run_id": row.run_id,
                    "scenario": row.scenario,
                },
            )
