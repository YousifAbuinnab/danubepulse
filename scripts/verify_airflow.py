"""Check real Airflow/PostgreSQL execution evidence without printing login secrets."""

import json
import subprocess
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

BASE = "http://localhost:8080"


def request(path: str, token: str | None = None, payload: dict | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(
        BASE + path,
        headers=headers,
        data=json.dumps(payload).encode() if payload is not None else None,
    )
    with urlopen(req, timeout=45) as response:
        return json.load(response)


try:
    request("/api/v2/dags")
except HTTPError as error:
    if error.code != 401:
        raise
else:
    raise AssertionError("Airflow administrative API unexpectedly permits anonymous access.")

credentials = json.loads(
    subprocess.check_output(
        [
            "docker",
            "compose",
            "--profile",
            "airflow",
            "exec",
            "-T",
            "airflow-api",
            "python",
            "-c",
            "import json, os; "
            "print(json.dumps({'username': os.environ['AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS']"
            ".split(':')[0], 'password': json.load(open("
            "os.environ['AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE']))["
            "os.environ['AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS'].split(':')[0]]}))",
        ],
        text=True,
    )
)
token = request("/auth/token", payload=credentials)["access_token"]
health = request("/api/v2/monitor/health", token)
assert health["metadatabase"]["status"] == "healthy", health
assert health["scheduler"]["status"] == "healthy", health
assert health["dag_processor"]["status"] == "healthy", health
print("Authenticated Airflow API, scheduler, metadata DB and DAG processor: healthy")

for domain in ("river", "weather"):
    dag_id = f"danubepulse_{domain}"
    dag = request(f"/api/v2/dags/{dag_id}", token)
    print(f"{dag_id}: paused={dag['is_paused']}, max_active_runs={dag['max_active_runs']}")
    runs = request(f"/api/v2/dags/{dag_id}/dagRuns?limit=20&order_by=-run_after", token)["dag_runs"]
    for kind in ("manual", "scheduled"):
        successful = [run for run in runs if run["run_type"] == kind and run["state"] == "success"]
        assert successful, f"No successful {kind} execution for {dag_id}: {runs}"
        run = successful[0]
        run_id = quote(run["dag_run_id"], safe="")
        tasks = request(f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances", token)[
            "task_instances"
        ]
        assert {task["task_id"] for task in tasks} == {"ingest", "verify"}, tasks
        assert all(task["state"] == "success" for task in tasks), tasks
        print(f"  {kind}: {run['dag_run_id']} -- ingest/verify success")
        for task in tasks:
            log = request(
                f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/"
                f"{task['task_id']}/logs/{task['try_number']}",
                token,
            )
            assert log.get("content"), f"No retained task logs: {task['task_id']}"
    # The original readiness failure, if any, remains in Airflow's retry history.
print("All four manual/scheduled DAG executions and retained task logs verified.")
