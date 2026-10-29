"""Optional Kafka workers: at-least-once delivery with durable SQL inbox/outbox."""

import argparse
import base64
import hashlib
import json
import logging
import os
import signal
import time
from datetime import timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker

from danubepulse.db import make_session_factory
from danubepulse.models import (
    StreamingDeadLetter,
    StreamingOutbox,
    StreamingReceipt,
    StreamingWorker,
)
from danubepulse.streaming_events import DLQ_TOPIC, EVENT_TOPIC, Notification, enqueue
from danubepulse.time_utils import utc_now

logger = logging.getLogger(__name__)
MAX_BYTES = 64 * 1024


class PublicationFailed(RuntimeError):
    """A safe broker error code, never a payload or credential-bearing exception message."""


class WorkerLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": utc_now().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in ("event_id", "reason", "offset", "code"):
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        if record.exc_info:
            entry["exception_type"] = record.exc_info[0].__name__
        return json.dumps(entry)


def heartbeat(
    session: Session,
    worker: str,
    role: str,
    status: str,
    error: str | None = None,
    **details: Any,
) -> None:
    row = session.get(StreamingWorker, worker)
    if row is None:
        row = StreamingWorker(id=worker, role=role)
        session.add(row)
    row.status, row.error, row.details = status, error, details
    row.heartbeat_at = utc_now()


def process_record(
    session: Session,
    topic: str,
    partition: int,
    offset: int,
    raw: bytes,
    event_key: bytes | None = None,
) -> str:
    """Persist processing or a poison record; caller commits DB before Kafka offset."""
    try:
        if len(raw) > MAX_BYTES:
            raise ValueError("event_too_large")
        event = Notification.model_validate_json(raw)
        payload = event.model_dump()
        try:
            json.dumps(payload, allow_nan=False)
        except ValueError:
            raise ValueError("nonfinite_event_data") from None
        receipt = session.get(StreamingReceipt, event.event_id)
        if receipt is not None:
            if receipt.payload != payload:
                raise ValueError("event_id_payload_conflict")
            session.execute(
                update(StreamingReceipt)
                .where(StreamingReceipt.event_id == event.event_id)
                .values(deliveries=StreamingReceipt.deliveries + 1)
            )
            return "duplicate"
        session.add(
            StreamingReceipt(
                event_id=event.event_id,
                payload=payload,
                received_at=utc_now(),
                deliveries=1,
            )
        )
        return "processed"
    except (ValidationError, ValueError) as error:
        # Do not log untrusted contents or validation exception text (may contain secrets).
        reason = "invalid_event_schema" if isinstance(error, ValidationError) else str(error)
        dead_id = str(uuid5(NAMESPACE_URL, f"danubepulse:dead:{topic}:{partition}:{offset}"))
        if session.get(StreamingDeadLetter, dead_id) is None:
            now = utc_now()
            # Retain exact bounded bytes, otherwise a bounded preview and original checksum.
            raw_base64 = base64.b64encode(raw[:MAX_BYTES]).decode("ascii")
            session.add(
                StreamingDeadLetter(
                    id=dead_id,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    raw_base64=raw_base64,
                    error=reason,
                    created_at=now,
                )
            )
            session.add(
                StreamingOutbox(
                    id=dead_id,
                    topic=DLQ_TOPIC,
                    created_at=now,
                    next_attempt_at=now,
                    attempts=0,
                    payload={
                        "schema_version": 1,
                        "event_id": dead_id,
                        "event_type": "streaming.dead_letter",
                        "source_event_id": (
                            event_key.decode("utf-8", errors="replace")[:36] if event_key else None
                        ),
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                        "error": reason,
                        "raw_base64": raw_base64,
                        "sha256": hashlib.sha256(raw).hexdigest(),
                        "original_bytes": len(raw),
                        "truncated": len(raw) > MAX_BYTES,
                    },
                )
            )
        logger.warning("streaming_dead_letter", extra={"reason": reason, "offset": offset})
        return "dead_letter"


def make_producer(bootstrap: str) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 10000,
            "request.timeout.ms": 5000,
            "retry.backoff.ms": 500,
            "socket.timeout.ms": 5000,
            "queue.buffering.max.kbytes": 4096,
        }
    )


def send(producer: Any, topic: str, key: str, payload: dict[str, Any]) -> None:
    outcome: list[Any] = []
    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(payload, allow_nan=False).encode(),
        on_delivery=lambda error, _message: outcome.append(error),
    )
    deadline = time.monotonic() + 15
    while not outcome and time.monotonic() < deadline:
        producer.poll(0.2)
    if not outcome:
        raise PublicationFailed("Kafka_delivery_timeout")
    if outcome[0] is not None:
        raise PublicationFailed("Kafka_" + outcome[0].name())


def publish_batch(
    factory: sessionmaker[Session],
    producer: Any,
    *,
    limit: int = 100,
) -> dict[str, int]:
    if not 1 <= limit <= 1000:
        raise ValueError("Publication batch limit must be between 1 and 1000.")
    result = {"published": 0, "failed": 0}
    for _ in range(limit):
        with factory() as session:
            row = session.scalar(
                select(StreamingOutbox)
                .where(
                    StreamingOutbox.published_at.is_(None),
                    StreamingOutbox.next_attempt_at <= utc_now(),
                )
                .order_by(StreamingOutbox.created_at, StreamingOutbox.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if row is None:
                heartbeat(session, "publisher", "publisher", "healthy", **result)
                session.commit()
                break
            row.attempts += 1
            try:
                send(producer, row.topic, row.id, row.payload)
            except Exception as error:
                # Preserve retryable publication state even if the broker is entirely absent.
                row.last_error = (
                    str(error) if isinstance(error, PublicationFailed) else type(error).__name__
                )
                row.next_attempt_at = utc_now() + timedelta(
                    seconds=min(60, 2 ** min(row.attempts, 6))
                )
                heartbeat(session, "publisher", "publisher", "retrying", row.last_error)
                result["failed"] += 1
                session.commit()
                logger.exception(
                    "streaming_publication_failed",
                    extra={"event_id": row.id, "reason": row.last_error},
                )
                break
            row.published_at, row.last_error = utc_now(), None
            result["published"] += 1
            heartbeat(session, "publisher", "publisher", "healthy", **result)
            session.commit()
    return result


def consume(
    factory: sessionmaker[Session],
    bootstrap: str,
    group: str,
    *,
    seconds: float | None = None,
    stop: Any = lambda: False,
) -> dict[str, int]:
    if not 1 <= len(group) <= 128:
        raise ValueError("Consumer group must contain 1 to 128 characters.")
    if seconds is not None and seconds <= 0:
        raise ValueError("Consumer duration must be positive.")
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 10000,
            "max.poll.interval.ms": 120000,
            "fetch.message.max.bytes": 131072,
        }
    )
    worker = "consumer:" + group
    counts = {"processed": 0, "duplicate": 0, "dead_letter": 0}
    started = time.monotonic()
    last_heartbeat = 0.0
    healthy = False
    try:
        consumer.subscribe([EVENT_TOPIC])
        while not stop() and (seconds is None or time.monotonic() - started < seconds):
            message = consumer.poll(1)
            if message is None:
                if time.monotonic() - last_heartbeat > 5:
                    try:
                        metadata = consumer.list_topics(EVENT_TOPIC, timeout=3)
                        topic = metadata.topics.get(EVENT_TOPIC)
                        healthy = topic is not None and topic.error is None
                    except KafkaException:
                        healthy = False
                    with factory() as session:
                        heartbeat(
                            session,
                            worker,
                            "consumer",
                            "healthy" if healthy else "retrying",
                            None if healthy else "broker_or_topic_unavailable",
                            **counts,
                        )
                        session.commit()
                    last_heartbeat = time.monotonic()
                continue
            if message.error():
                logger.error(
                    "streaming_consumer_broker_error", extra={"code": message.error().code()}
                )
                with factory() as session:
                    heartbeat(session, worker, "consumer", "retrying", "Kafka_poll_error", **counts)
                    session.commit()
                healthy = False
                continue
            try:
                with factory() as session:
                    outcome = process_record(
                        session,
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        message.value() or b"",
                        message.key(),
                    )
                    heartbeat(
                        session,
                        worker,
                        "consumer",
                        "healthy",
                        topic=message.topic(),
                        partition=message.partition(),
                        next_offset=message.offset() + 1,
                        **counts,
                    )
                    session.commit()
                # A crash/commit failure here causes redelivery; SQL inbox deduplicates it.
                consumer.commit(message=message, asynchronous=False)
                counts[outcome] += 1
                healthy = True
            except Exception:
                logger.exception("streaming_consumer_processing_failed")
                with factory() as session:
                    heartbeat(
                        session, worker, "consumer", "retrying", "processing_or_offset_failure"
                    )
                    session.commit()
                consumer.seek(
                    TopicPartition(message.topic(), message.partition(), message.offset())
                )
                time.sleep(1)
    finally:
        consumer.close()
        with factory() as session:
            heartbeat(session, worker, "consumer", "stopped", **counts)
            session.commit()
    return counts


def demo(factory: sessionmaker[Session], scenario: str, key: str) -> list[str]:
    """Create notification-only test data; never touch measurements or provider settings."""
    ids = []
    with factory() as session:
        for kind in ("ingestion_completed", "verification_failed", "quality_incident"):
            row = enqueue(
                session,
                "streaming.test",
                key,
                "synthetic-streaming-test",
                {"scenario": scenario, "notification": kind, "measurements_modified": False},
                identity=f"streaming-demo:{key}:{scenario}:{kind}",
            )
            ids.append(row.id)
        session.commit()
    return ids


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--once", action="store_true")
    publish.add_argument("--limit", type=int, default=100)
    receive = commands.add_parser("consume")
    receive.add_argument("--group", default="danubepulse-monitor-v1")
    receive.add_argument("--seconds", type=float)
    simulate = commands.add_parser("demo")
    simulate.add_argument("--key", required=True)
    simulate.add_argument("--scenario", choices=("outage-recovery", "replay"), required=True)
    poison = commands.add_parser("poison")
    poison.add_argument("--key", required=True)
    args = parser.parse_args()
    if args.command == "publish" and not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000.")
    if args.command == "consume":
        if not 1 <= len(args.group) <= 128:
            parser.error("--group must contain 1 to 128 characters.")
        if args.seconds is not None and args.seconds <= 0:
            parser.error("--seconds must be positive.")
    if args.command in {"demo", "poison"} and not 1 <= len(args.key) <= 128:
        parser.error("--key must contain 1 to 128 characters.")
    handler = logging.StreamHandler()
    handler.setFormatter(WorkerLogFormatter())
    logging.basicConfig(level="INFO", handlers=[handler], force=True)
    factory = make_session_factory()
    stopping = False

    def stop(_signal: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if args.command == "demo":
        print(json.dumps({"test_event_ids": demo(factory, args.scenario, args.key)}))
    elif args.command == "consume":
        print(
            json.dumps(
                consume(
                    factory, args.bootstrap, args.group, seconds=args.seconds, stop=lambda: stopping
                )
            )
        )
    else:
        producer = make_producer(args.bootstrap)
        if args.command == "poison":
            # Deliberately invalid contract, durably queued; it cannot represent observations.
            now = utc_now()
            with factory() as session:
                event_id = str(uuid5(NAMESPACE_URL, "danubepulse:poison:" + args.key))
                if session.get(StreamingOutbox, event_id) is None:
                    session.add(
                        StreamingOutbox(
                            id=event_id,
                            topic=EVENT_TOPIC,
                            created_at=now,
                            next_attempt_at=now,
                            attempts=0,
                            payload={
                                "synthetic_streaming_test": True,
                                "purpose": "invalid-schema",
                                "key": args.key,
                            },
                        )
                    )
                session.commit()
            print(json.dumps({"poison_event_id": event_id}))
        else:
            while not stopping:
                result = publish_batch(factory, producer, limit=args.limit)
                if args.once:
                    print(json.dumps(result))
                    if result["failed"]:
                        raise SystemExit(1)
                    break
                time.sleep(2)
        producer.flush(1)


if __name__ == "__main__":
    main()
