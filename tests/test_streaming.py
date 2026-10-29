import json
import sys
from collections import namedtuple
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from danubepulse import main, streaming
from danubepulse.db import get_session
from danubepulse.models import (
    EnergyObservation,
    IngestionRun,
    PipelineExecution,
    RiverObservation,
    StreamingDeadLetter,
    StreamingOutbox,
    StreamingReceipt,
    StreamingWorker,
    WeatherObservation,
)
from danubepulse.pipeline import run_demo, simulate_scenario
from danubepulse.streaming_events import DLQ_TOPIC, EVENT_TOPIC, Notification
from danubepulse.streaming_monitor import streaming_status
from danubepulse.time_utils import utc_now


def test_outbox_is_atomic_with_ingestion_incidents_and_quality(session_factory, tmp_path):
    with session_factory() as session:
        run_demo(session, tmp_path)
        events = session.scalars(select(StreamingOutbox)).all()
        types = {row.payload["event_type"] for row in events}
        assert {"ingestion.completed", "quality.alert", "incident.changed"} <= types
        assert all(row.payload["provenance"] == "synthetic" for row in events)
        assert all(Notification.model_validate(row.payload) for row in events)
        assert all(row.published_at is None for row in events)
        before = len(events)
        run = session.scalar(select(IngestionRun).limit(1))
        original = run.status
        run.status = "failed"
        session.flush()
        assert session.scalar(select(func.count()).select_from(StreamingOutbox)) > before
        session.rollback()
        assert session.get(IngestionRun, run.id).status == original
        assert session.scalar(select(func.count()).select_from(StreamingOutbox)) == before
        incident = simulate_scenario(session, tmp_path, "duplicate_records")
        assert any(
            row.payload["entity_id"] == incident.id
            for row in session.scalars(select(StreamingOutbox))
        )


def test_pipeline_verification_failure_emits_safe_notification(session_factory):
    with session_factory() as session:
        execution = PipelineExecution(
            id=str(uuid4()),
            domain="energy",
            orchestrator_run_id="test",
            status="running",
            started_at=utc_now(),
        )
        session.add(execution)
        session.commit()
        execution.status = "failed"
        execution.error = "PRIVATE: do not publish error details"
        execution.verification = [{"check": "persisted_gold", "passed": False, "detail": "private"}]
        session.commit()
        row = session.scalar(select(StreamingOutbox))
        assert row.payload["event_type"] == "pipeline.verification_failed"
        assert row.payload["data"]["failed_checks"] == ["persisted_gold"]
        assert "PRIVATE" not in json.dumps(row.payload) and "private" not in json.dumps(row.payload)


def test_stable_ids_demo_does_not_modify_measurements_and_replay_deduplicates(session_factory):
    first = streaming.demo(session_factory, "replay", "deterministic")
    assert streaming.demo(session_factory, "replay", "deterministic") == first
    with session_factory() as session:
        rows = session.scalars(select(StreamingOutbox)).all()
        assert len(rows) == 3
        for offset, row in enumerate(rows):
            payload = json.dumps(row.payload).encode()
            assert streaming.process_record(session, EVENT_TOPIC, 0, offset, payload) == "processed"
            session.commit()
            assert streaming.process_record(session, EVENT_TOPIC, 0, offset, payload) == "duplicate"
            session.commit()
        assert session.scalar(select(func.count()).select_from(StreamingReceipt)) == 3
        assert all(row.deliveries == 2 for row in session.scalars(select(StreamingReceipt)))
        assert session.scalar(select(func.count()).select_from(IngestionRun)) == 0


def test_interview_demo_actions_are_authenticated_and_replay_deduplicates(
    tmp_path, monkeypatch, session_factory
):
    token = "test-demo-control-token-" + "x" * 32
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(
            data_mode="demo",
            bronze_dir=tmp_path,
            log_level="INFO",
            demo_control_token=token,
        ),
    )

    def override_session():
        with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_session] = override_session
    try:
        with TestClient(main.app) as client:
            unauthorized = client.post("/v1/demo/streaming")
            assert unauthorized.status_code == 401
            monkeypatch.setattr(
                main,
                "get_settings",
                lambda: SimpleNamespace(
                    data_mode="demo",
                    bronze_dir=tmp_path,
                    log_level="INFO",
                    demo_control_token=None,
                ),
            )
            unconfigured = client.post(
                "/v1/demo/streaming", headers={"X-Demo-Control-Token": token}
            )
            assert unconfigured.status_code == 503
            monkeypatch.setattr(
                main,
                "get_settings",
                lambda: SimpleNamespace(
                    data_mode="demo",
                    bronze_dir=tmp_path,
                    log_level="INFO",
                    demo_control_token=token,
                ),
            )
            headers = {"X-Demo-Control-Token": token}
            first = client.post("/v1/demo/streaming", headers=headers)
            repeated = client.post("/v1/demo/streaming", headers=headers)
            assert first.status_code == 200 and repeated.status_code == 200
            assert first.json() == repeated.json()
            payload = first.json()
            assert payload["provenance"] == "synthetic-streaming-test"
            assert payload["measurements_modified"] is False
            assert len(payload["event_ids"]) == 3

            event_id = payload["event_ids"][0]
            replay = client.post(f"/v1/demo/streaming/{event_id}/replay", headers=headers)
            duplicate = client.post(f"/v1/demo/streaming/{event_id}/replay", headers=headers)
            assert replay.status_code == duplicate.status_code == 200
            assert replay.json() == duplicate.json()
            assert replay.json()["event_id"] == event_id
            assert replay.json()["replay_outbox_id"] != event_id

            with session_factory() as session:
                original = session.get(StreamingOutbox, event_id)
                replay_row = session.get(StreamingOutbox, replay.json()["replay_outbox_id"])
                assert original is not None and replay_row is not None
                assert replay_row.payload == original.payload
                original_bytes = json.dumps(original.payload).encode()
                assert streaming.process_record(session, EVENT_TOPIC, 0, 0, original_bytes) == (
                    "processed"
                )
                session.commit()
                assert streaming.process_record(session, EVENT_TOPIC, 0, 1, original_bytes) == (
                    "duplicate"
                )
                session.commit()
                receipt = session.get(StreamingReceipt, event_id)
                assert receipt is not None and receipt.deliveries == 2
                monitor = streaming_status(session)
                monitored_receipt = next(
                    item for item in monitor["receipts"] if item["event_id"] == event_id
                )
                replay_event = next(
                    item
                    for item in monitor["events"]
                    if item["event_id"] == replay.json()["replay_outbox_id"]
                )
                assert monitored_receipt["deliveries"] == 2
                assert replay_event["status"] == "processed"
                assert replay_event["deliveries"] == 2
                assert session.scalar(select(func.count()).select_from(RiverObservation)) == 0
                assert session.scalar(select(func.count()).select_from(WeatherObservation)) == 0
                assert session.scalar(select(func.count()).select_from(EnergyObservation)) == 0
                assert session.scalar(select(func.count()).select_from(IngestionRun)) == 0
    finally:
        main.app.dependency_overrides.clear()


def test_poison_schema_conflict_and_dlq_are_durable_and_idempotent(session_factory):
    ids = streaming.demo(session_factory, "replay", "conflict")
    with session_factory() as session:
        row = session.get(StreamingOutbox, ids[0])
        payload = json.dumps(row.payload).encode()
        assert streaming.process_record(session, EVENT_TOPIC, 0, 0, payload) == "processed"
        session.commit()
        changed = dict(
            row.payload,
            data=dict(row.payload["data"], notification="quality_incident"),
        )
        if changed == row.payload:
            changed["data"]["notification"] = "verification_failed"
        assert (
            streaming.process_record(session, EVENT_TOPIC, 0, 1, json.dumps(changed).encode())
            == "dead_letter"
        )
        session.commit()
        for _ in range(2):
            assert (
                streaming.process_record(session, EVENT_TOPIC, 0, 2**40, b"not JSON")
                == "dead_letter"
            )
            session.commit()
        assert session.scalar(select(func.count()).select_from(StreamingDeadLetter)) == 2
        assert (
            session.scalar(
                select(func.count())
                .select_from(StreamingOutbox)
                .where(StreamingOutbox.topic == DLQ_TOPIC)
            )
            == 2
        )
        assert session.get(StreamingReceipt, ids[0]).payload == row.payload
        assert session.get(StreamingReceipt, ids[0]).deliveries == 1


def test_publisher_outage_preserves_pending_events_and_recovers(session_factory, monkeypatch):
    streaming.demo(session_factory, "outage-recovery", "offline")

    def unavailable(*_args):
        raise RuntimeError("unavailable")

    monkeypatch.setattr(streaming, "send", unavailable)
    assert streaming.publish_batch(session_factory, object()) == {"published": 0, "failed": 1}
    with session_factory() as session:
        failed = session.scalar(select(StreamingOutbox).where(StreamingOutbox.attempts > 0))
        assert failed.published_at is None and failed.last_error == "RuntimeError"
        failed.next_attempt_at = utc_now() - timedelta(seconds=1)
        session.commit()
    sent = []
    monkeypatch.setattr(
        streaming, "send", lambda _p, topic, key, payload: sent.append((topic, key))
    )
    assert streaming.publish_batch(session_factory, object()) == {"published": 3, "failed": 0}
    assert len(set(sent)) == 3
    assert streaming.publish_batch(session_factory, object()) == {"published": 0, "failed": 0}


def test_consumer_db_rollback_leaves_no_receipt_or_deadletter(session_factory):
    ids = streaming.demo(session_factory, "replay", "rollback")
    with session_factory() as session:
        payload = session.get(StreamingOutbox, ids[0]).payload
        streaming.process_record(session, EVENT_TOPIC, 0, 10, json.dumps(payload).encode())
        session.rollback()
        assert session.get(StreamingReceipt, ids[0]) is None
        streaming.process_record(session, EVENT_TOPIC, 0, 11, b"{}")
        session.rollback()
        assert session.scalar(select(func.count()).select_from(StreamingDeadLetter)) == 0


def test_streaming_api_reports_no_workers_stale_health_and_omits_raw_deadletters(session_factory):
    def sessions():
        with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_session] = sessions
    try:
        with TestClient(main.app) as client:
            assert client.get("/v1/streaming").json()["workers"] == []
            assert client.post("/v1/streaming").status_code == 405
            with session_factory() as session:
                session.add(
                    StreamingWorker(
                        id="test-worker",
                        role="consumer",
                        status="healthy",
                        heartbeat_at=utc_now() - timedelta(minutes=5),
                        details={},
                    )
                )
                streaming.process_record(session, EVENT_TOPIC, 0, 1, b'{"PRIVATE":"secret"}')
                session.commit()
                assert streaming_status(session)["workers"][0]["status"] == "stale"
            result = client.get("/v1/streaming")
            assert result.status_code == 200 and result.json()["dead_letters"]
            assert "PRIVATE" not in result.text and "raw_base64" not in result.text
    finally:
        main.app.dependency_overrides.clear()


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"event_id": "not-a-uuid"},
        {"occurred_at": "2026-10-19T00:00:00"},
        {"event_type": "pipeline.verified"},
    ],
)
def test_invalid_or_mismatched_event_contract_goes_to_dlq(session_factory, change):
    ids = streaming.demo(session_factory, "replay", "schema-contract")
    with session_factory() as session:
        payload = dict(session.get(StreamingOutbox, ids[0]).payload, **change)
        assert (
            streaming.process_record(session, EVENT_TOPIC, 0, 0, json.dumps(payload).encode())
            == "dead_letter"
        )
        session.commit()
        assert session.get(StreamingReceipt, ids[0]) is None


def test_offset_commit_failure_retries_after_db_commit_without_duplicate_receipt(
    session_factory, monkeypatch
):
    ids = streaming.demo(session_factory, "replay", "offset-gap")
    with session_factory() as session:
        raw = json.dumps(session.get(StreamingOutbox, ids[0]).payload).encode()
    message = SimpleNamespace(
        topic=lambda: EVENT_TOPIC,
        partition=lambda: 0,
        offset=lambda: 7,
        value=lambda: raw,
        key=lambda: ids[0].encode(),
        error=lambda: None,
    )
    commits = []
    seeks = []
    partition = namedtuple("TopicPartition", "topic partition offset")

    class FakeConsumer:
        def __init__(self, config):
            assert config["enable.auto.commit"] is False
            assert config["enable.auto.offset.store"] is False

        def subscribe(self, topics):
            assert topics == [EVENT_TOPIC]

        def poll(self, _timeout):
            return message

        def commit(self, *, message, asynchronous):
            assert not asynchronous
            with session_factory() as session:
                # The durable receipt MUST exist before any offset commit attempt.
                assert session.get(StreamingReceipt, ids[0]) is not None
            commits.append(message.offset())
            if len(commits) == 1:
                raise RuntimeError("Injected offset commit failure")

        def seek(self, position):
            seeks.append(position)

        def close(self):
            pass

    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka",
        SimpleNamespace(
            Consumer=FakeConsumer,
            KafkaException=RuntimeError,
            TopicPartition=partition,
        ),
    )
    monkeypatch.setattr(streaming.time, "sleep", lambda _seconds: None)
    result = streaming.consume(
        session_factory,
        "test-only",
        "offset-contract",
        stop=lambda: len(commits) == 2,
    )
    assert commits == [7, 7] and seeks == [partition(EVENT_TOPIC, 0, 7)]
    assert result == {"processed": 0, "duplicate": 1, "dead_letter": 0}
    with session_factory() as session:
        assert session.get(StreamingReceipt, ids[0]).deliveries == 2
        assert session.scalar(select(func.count()).select_from(StreamingReceipt)) == 1


@pytest.mark.parametrize(
    "arguments",
    [
        ["publish", "--limit", "0"],
        ["consume", "--seconds", "-1"],
        ["consume", "--group", "x" * 129],
        ["poison", "--key", ""],
    ],
)
def test_invalid_worker_arguments_fail_before_connecting(monkeypatch, arguments):
    monkeypatch.setattr(sys, "argv", ["streaming", *arguments])
    with pytest.raises(SystemExit) as error:
        streaming.main()
    assert error.value.code == 2
