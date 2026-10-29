"""Database-only streaming status; deliberately never connect the API to Kafka."""

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from danubepulse.models import (
    StreamingDeadLetter,
    StreamingOutbox,
    StreamingReceipt,
    StreamingWorker,
)
from danubepulse.time_utils import parse_utc, utc_now


def streaming_status(session: Session) -> dict[str, Any]:
    pending = session.scalar(
        select(func.count())
        .select_from(StreamingOutbox)
        .where(StreamingOutbox.published_at.is_(None))
    )
    retrying = session.scalar(
        select(func.count())
        .select_from(StreamingOutbox)
        .where(StreamingOutbox.published_at.is_(None), StreamingOutbox.attempts > 0)
    )
    oldest = session.scalar(
        select(func.min(StreamingOutbox.created_at)).where(StreamingOutbox.published_at.is_(None))
    )
    workers = []
    for row in session.scalars(select(StreamingWorker).order_by(StreamingWorker.id)):
        age = (utc_now() - parse_utc(row.heartbeat_at)).total_seconds()
        workers.append(
            {
                "id": row.id,
                "role": row.role,
                "status": row.status if age < 45 else "stale",
                "last_reported_status": row.status,
                "heartbeat_at": parse_utc(row.heartbeat_at).isoformat(),
                "heartbeat_age_seconds": round(age, 1),
                "error": row.error,
                "details": row.details,
            }
        )
    dead_rows = session.scalars(
        select(StreamingDeadLetter).order_by(StreamingDeadLetter.created_at.desc()).limit(50)
    ).all()
    dead_outboxes = {row.id: session.get(StreamingOutbox, row.id) for row in dead_rows}
    dead_event_ids = {
        row.payload.get("source_event_id") for row in dead_outboxes.values() if row is not None
    }
    events = []
    for row in session.scalars(
        select(StreamingOutbox)
        .where(StreamingOutbox.topic == "danubepulse.events.v1")
        .order_by(StreamingOutbox.created_at.desc())
        .limit(100)
    ):
        receipt = session.get(StreamingReceipt, row.payload.get("event_id", row.id))
        events.append(
            {
                "event_id": row.id,
                "event_type": row.payload.get("event_type", "invalid-test-schema"),
                "provenance": row.payload.get("provenance", "synthetic-streaming-test"),
                "entity_id": row.payload.get("entity_id"),
                "data": row.payload.get("data"),
                "created_at": parse_utc(row.created_at).isoformat(),
                "status": "dead_letter"
                if row.id in dead_event_ids or row.payload.get("event_id", row.id) in dead_event_ids
                else "processed"
                if receipt
                else "published"
                if row.published_at
                else "pending",
                "publish_attempts": row.attempts,
                "last_error": row.last_error,
                "deliveries": receipt.deliveries if receipt else 0,
            }
        )
    return {
        "delivery_guarantee": (
            "at-least-once; idempotent database inbox; not end-to-end exactly-once"
        ),
        "pending": pending,
        "retrying": retrying,
        "oldest_pending_age_seconds": (
            round((utc_now() - parse_utc(oldest)).total_seconds(), 1) if oldest else None
        ),
        "processed": session.scalar(select(func.count()).select_from(StreamingReceipt)),
        "receipts": [
            {
                "event_id": row.event_id,
                "event_type": row.payload["event_type"],
                "provenance": row.payload["provenance"],
                "entity_id": row.payload["entity_id"],
                "data": row.payload["data"],
                "received_at": parse_utc(row.received_at).isoformat(),
                "deliveries": row.deliveries,
            }
            for row in session.scalars(
                select(StreamingReceipt).order_by(StreamingReceipt.received_at.desc()).limit(100)
            )
        ],
        "dead_letters": [
            {
                "id": row.id,
                "topic": row.topic,
                "partition": row.partition,
                "offset": row.offset,
                "error": row.error,
                "created_at": parse_utc(row.created_at).isoformat(),
                "dlq_published": bool((outbox := dead_outboxes[row.id]) and outbox.published_at),
            }
            for row in dead_rows
        ],
        "workers": workers,
        "events": events,
        "note": "Notifications only, not a live external measurement feed. Kafka is optional. "
        "An empty worker list means not started; heartbeat freshness is not a broker health probe.",
    }
