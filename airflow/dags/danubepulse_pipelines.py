"""Thin orchestration: all ingestion and verification stays in DanubePulse."""

import json
import logging
import os
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from airflow.sdk import DAG, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

logger = logging.getLogger(__name__)
API_URL = os.environ.get("DANUBEPULSE_API_URL", "http://api:8000").rstrip("/")


def _post(path: str, payload: dict | None = None) -> dict:
    request = Request(
        f"{API_URL}{path}",
        data=json.dumps(payload or {}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=480) as response:
            result = json.load(response)
    except HTTPError as error:
        detail = error.read().decode(errors="replace")[:1500]
        raise RuntimeError(f"DanubePulse HTTP {error.code}: {detail}") from error
    logger.info("DanubePulse response: %s", json.dumps(result, sort_keys=True))
    if result["status"] == "failed":
        raise RuntimeError(f"Pipeline execution failed: {result}")
    return result


def build_pipeline(domain: str, schedule: str, paused: bool) -> DAG:
    with DAG(
        dag_id=f"danubepulse_{domain}",
        description=f"Real {domain}: Bronze/Silver ingestion then persisted Gold verification.",
        schedule=CronDataIntervalTimetable(schedule, timezone="UTC"),
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        is_paused_upon_creation=paused,
        dagrun_timeout=timedelta(minutes=40),
        default_args={
            "owner": "danubepulse",
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "retry_exponential_backoff": True,
            "max_retry_delay": timedelta(minutes=10),
            "execution_timeout": timedelta(minutes=10),
        },
        tags=["danubepulse", "real", domain],
    ) as dag:

        @task
        def ingest() -> str:
            context = get_current_context()
            run = context["dag_run"]
            payload = {"orchestrator_run_id": f"{domain}:{run.run_id}"}
            if domain in {"weather", "energy"}:
                # Derive the target from the interval, not wall time, so retries are stable.
                interval_end = context.get("data_interval_end") or run.run_after
                payload["target_date"] = (interval_end.date() - timedelta(days=2)).isoformat()
            result = _post(f"/v1/pipelines/{domain}/ingest", payload)
            if result["status"] != "ingested":
                raise RuntimeError(f"Expected ingested pipeline execution, got {result}")
            return result["id"]

        @task
        def verify(execution_id: str) -> None:
            result = _post(f"/v1/pipelines/executions/{execution_id}/verify")
            if result["status"] != "success":
                raise RuntimeError(f"Persisted data verification did not pass: {result}")

        verify(ingest())
    return dag


# Paused until the operator confirms eHYD automated-polling permission.
river = build_pipeline("river", "0 * * * *", paused=True)
weather = build_pipeline("weather", "0 6 * * *", paused=False)
# No external polling until account, reuse terms and schedule are confirmed.
energy = build_pipeline("energy", "0 7 * * *", paused=True)
