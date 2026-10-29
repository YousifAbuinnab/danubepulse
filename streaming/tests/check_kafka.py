"""Manual integration against Compose Kafka; writes notification-only test records."""

import argparse
import json
import time
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import select

from danubepulse.db import make_session_factory
from danubepulse.models import StreamingDeadLetter, StreamingOutbox, StreamingReceipt
from danubepulse.streaming import consume, demo, make_producer, publish_batch, send
from danubepulse.streaming_events import DLQ_TOPIC, EVENT_TOPIC
from danubepulse.time_utils import utc_now


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("outage", "recover", "resume", "replay"))
    parser.add_argument("--key", required=True)
    args = parser.parse_args()
    factory = make_session_factory()
    ids = demo(factory, "outage-recovery", args.key)
    poison_id = str(uuid5(NAMESPACE_URL, "danubepulse-integration-poison:" + args.key))
    with factory() as session:
        if session.get(StreamingOutbox, poison_id) is None:
            now = utc_now()
            session.add(
                StreamingOutbox(
                    id=poison_id,
                    topic=EVENT_TOPIC,
                    created_at=now,
                    next_attempt_at=now,
                    attempts=0,
                    payload={"synthetic_streaming_test": True, "key": args.key},
                )
            )
            session.commit()
        before = sum(
            row.deliveries
            for row in session.scalars(
                select(StreamingReceipt).where(StreamingReceipt.event_id.in_(ids))
            )
        )
    producer = make_producer("kafka:9092")
    group = "integration-" + args.key
    if args.phase == "outage":
        result = publish_batch(factory, producer)
        assert result["failed"] == 1
        with factory() as session:
            assert all(session.get(StreamingOutbox, key).published_at is None for key in ids)
            assert (
                session.scalar(
                    select(StreamingOutbox).where(StreamingOutbox.last_error.is_not(None))
                )
                is not None
            )
        print(json.dumps({"phase": args.phase, "durable_pending_test_events": 4, **result}))
    else:
        if args.phase == "recover":
            # Simulate the ACK→database-commit crash gap: send but leave outbox pending.
            with factory() as session:
                row = session.get(StreamingOutbox, ids[0])
                send(producer, row.topic, row.id, row.payload)
            time.sleep(3)  # allow the outage backoff to expire
            result = publish_batch(factory, producer)
            assert result["failed"] == 0
        with factory() as session:
            assert all(session.get(StreamingOutbox, key).published_at for key in ids)
        if args.phase == "replay":
            group += "-replay"
        counts = consume(factory, "kafka:9092", group, seconds=18)
        with factory() as session:
            receipts = session.scalars(
                select(StreamingReceipt).where(StreamingReceipt.event_id.in_(ids))
            ).all()
            assert len(receipts) == 3
            deliveries = sum(row.deliveries for row in receipts)
            if args.phase == "recover":
                assert deliveries == 4  # ACK gap duplicates delivery, not processing rows.
            elif args.phase == "resume":
                assert deliveries == before
                assert all(count == 0 for count in counts.values())
            else:
                assert deliveries == before + 4
            dead = session.scalars(select(StreamingDeadLetter)).all()
            assert dead
        dlq = publish_batch(factory, producer)
        assert dlq["failed"] == 0
        with factory() as session:
            assert (
                session.scalar(
                    select(StreamingOutbox).where(
                        StreamingOutbox.topic == DLQ_TOPIC,
                        StreamingOutbox.published_at.is_not(None),
                    )
                )
                is not None
            )
        print(
            json.dumps(
                {
                    "phase": args.phase,
                    "unique_test_receipts": 3,
                    "test_delivery_count": deliveries,
                    "consumer": counts,
                    "dlq_publication": dlq,
                    "notification_only": True,
                    "measurement_writes": 0,
                }
            )
        )
    producer.flush(1)


if __name__ == "__main__":
    main()
