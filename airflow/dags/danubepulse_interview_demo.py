"""Manual-only Airflow workflow over a small, isolated synthetic fixture."""

import json
import logging
import math
import os
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from airflow.sdk import DAG, task

logger = logging.getLogger(__name__)
FIXTURE_PATH = Path(
    os.environ.get("DANUBEPULSE_DEMO_FIXTURE", "/opt/airflow/fixtures/interview_demo.json")
)


def load_and_validate_fixture() -> dict[str, Any]:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    records = fixture.get("records")
    if fixture.get("provenance") != "synthetic" or not isinstance(records, list) or not records:
        raise ValueError("Interview fixture must contain explicitly synthetic records.")
    required = {"domain", "event_time", "metric", "record_id", "unit", "value"}
    for row in records:
        if not isinstance(row, dict) or set(row) != required:
            raise ValueError("Interview fixture record schema is invalid.")
        if not isinstance(row["value"], (int, float)) or not math.isfinite(row["value"]):
            raise ValueError("Interview fixture contains a non-finite value.")
        event_time = datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
        if event_time.tzinfo is None:
            raise ValueError("Interview fixture timestamps must include a timezone.")
    result = {
        "dataset": fixture["dataset"],
        "provenance": fixture["provenance"],
        "record_count": len(records),
        "records": records,
    }
    logger.info(
        "fixture_validation_passed dataset=%s provenance=%s records=%s",
        result["dataset"],
        result["provenance"],
        result["record_count"],
    )
    return result


def aggregate_records(validated: dict[str, Any]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in validated["records"]:
        groups[(row["domain"], row["metric"], row["unit"])].append(float(row["value"]))
    aggregates = [
        {
            "domain": domain,
            "metric": metric,
            "unit": unit,
            "observation_count": len(values),
            "average": sum(values) / len(values),
        }
        for (domain, metric, unit), values in sorted(groups.items())
    ]
    result = {
        "dataset": validated["dataset"],
        "provenance": validated["provenance"],
        "input_count": validated["record_count"],
        "groups": aggregates,
    }
    logger.info("fixture_aggregation_complete result=%s", json.dumps(result, sort_keys=True))
    return result


def verify_aggregates(result: dict[str, Any]) -> dict[str, Any]:
    validated = load_and_validate_fixture()
    expected = aggregate_records(validated)
    if result != expected or result["provenance"] != "synthetic":
        raise ValueError("Persisted task output does not match the synthetic fixture.")
    verified = {
        "status": "verified",
        "dataset": result["dataset"],
        "provenance": result["provenance"],
        "input_count": result["input_count"],
        "groups": result["groups"],
    }
    logger.info("fixture_verification_passed result=%s", json.dumps(verified, sort_keys=True))
    return verified


with DAG(
    dag_id="danubepulse_interview_demo",
    description="Manual-only fixture validation, aggregation and independent verification.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    tags=["danubepulse", "interview-demo", "synthetic"],
) as interview_demo:
    validated_fixture = task(task_id="validate_fixture")(load_and_validate_fixture)()
    aggregated_fixture = task(task_id="aggregate_demo")(aggregate_records)(validated_fixture)
    task(task_id="verify_aggregate")(verify_aggregates)(aggregated_fixture)
