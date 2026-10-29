# Stage 4: Automated real-data pipelines

## Architecture and scope

The optional `airflow` Compose profile uses Apache Airflow **3.3.2**, Python
3.12 and LocalExecutor. This version was verified against the official stable
[Docker documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
There is no Redis, Celery worker, triggerer, Spark or Kafka. Two executor slots,
one DAG-parser process and one API worker keep the WSL footprint small. Each
long-running Airflow service has a 1 GiB memory ceiling. Leave approximately
4 GiB free for the combined development stack; these are development settings,
not an internet-facing production deployment.

The DAGs send HTTP requests to DanubePulse:

1. `ingest`: call the existing source connectors and Bronze/Silver processing
   through `/v1/pipelines/{domain}/ingest`.
2. `verify`: independently check the immutable Bronze checksum, source quality,
   persisted Silver values and semantic uniqueness, and actual affected Gold
   counts/averages/precipitation sums through
   `/v1/pipelines/executions/{id}/verify`.

The DAG file contains no source parsing, unit conversions or analytical SQL.
PostgreSQL SQL views recompute Gold directly from valid Silver data; there is
no materialized-table refresh step. A green Airflow run requires both tasks
to succeed, not just HTTP 200 or an in-memory validation result.

Airflow owns the separate `danubepulse_airflow` database using the non-superuser
`danubepulse_airflow` role. It shares the existing PostgreSQL server and volume
but neither stores metadata in application tables nor receives the application
database password. The metadata role has no grants on application tables.
Initialization works with an already-initialized PostgreSQL volume; it does
not rely on first-boot `/docker-entrypoint-initdb.d` scripts. Do not delete
volumes to initialize or upgrade Airflow.

## Schedules and source permission

Stage 5 adds `danubepulse_energy`, initially paused, with a proposed 07:00 UTC
daily schedule and two-day lag. It uses the same `ingest -> verify` application
endpoints and energy advisory lock. Do not enable it until ENTSO-E access,
reuse permissions and scheduling are reviewed. See the [energy guide](energy.md).
Existing river pause state and weather schedule are unchanged.

| DAG | UTC schedule | Initial state | Requested data |
|---|---|---|---|
| `danubepulse_river` | `0 * * * *` | **Paused** | Current eHYD Danube snapshots |
| `danubepulse_weather` | `0 6 * * *` | Unpaused | One complete UTC day ending two days before the interval-end date |
| `danubepulse_energy` | `0 7 * * *` | **Paused** | Permission-gated ENTSO-E energy products |
| `danubepulse_interview_demo` | Manual only (`schedule=None`) | **Paused** | Isolated four-record synthetic fixture |

Scheduled DAGs use explicit cron data-interval timetables and `catchup=False`. Initial
activation may run the **latest completed interval once**, never every missed
historical interval. Historical river backfill is impossible with the current
provider collection. Weather retries use the persisted data interval, or the
persisted trigger time for a manual run, not the retry's wall clock.

The interview fixture DAG is separate from all real-source workflows. It reads
`airflow/fixtures/interview_demo.json`, validates explicit synthetic provenance,
aggregates the fixture, and verifies the result in a three-task chain. It is
paused and unscheduled. Run it safely with the one-shot, network-disabled
command in the [interview demo guide](demo.md); that command uses disposable
SQLite metadata and does not start the Airflow scheduler or modify the
persistent Airflow database. The regular weather DAG is enabled by default, so
do not start the scheduler merely to execute this fixture workflow.

**eHYD permission is not asserted.** Its collection metadata does not state a
redistribution license or explicit automated-polling allowance/rate limit.
Confirm appropriate usage with the provider before unpausing hourly collection.
The hourly schedule is prepared but not enabled by default. Attribution remains
`Datenquelle: ehyd.gv.at`. A controlled scheduled-run test can reuse a freshly
cached manual fetch without making further eHYD requests.

[Open-Meteo terms](https://open-meteo.com/en/terms) allow non-commercial free API
use with fewer than 10,000 calls/day, 5,000/hour and 600/minute. The daily DAG
normally makes three location requests, plus bounded retries. Use a licensed
paid plan for commercial use. Weather is model/reanalysis, not local station
measurements; retain the Open-Meteo attribution and CC BY 4.0 notice.
The two-day lag avoids the most recent incomplete archive period, but provider
delays are still possible: incomplete coverage **fails verification**, not
silently zero-filling or reporting success.

## Setup and login

Keep the existing `.env` and its original PostgreSQL credential. For a fresh
installation only, copy `.env.example` to `.env` and set `POSTGRES_PASSWORD`.
Use URL-safe passwords because Compose constructs database URLs.

```bash
cd /home/yousif/projects/danubepulse
python3 scripts/setup_airflow.py
DATA_MODE=real docker compose --profile airflow up -d --build
docker compose --profile airflow ps -a
```

The setup script appends only missing random secrets to the git-ignored `.env`,
sets restrictive file permissions and never prints the values. Do not replace
the Airflow metadata password when reusing its database: database initialization
does not reset existing role passwords. Keep the JWT and Fernet keys stable.
The `airflow-db-init` and `airflow-init` services should exit successfully.
API-server readiness gates scheduler startup, including WSL's slower cold starts.

- Application dashboard: <http://localhost:8501>
- Application API: <http://localhost:8000/docs>
- Airflow: <http://localhost:8080> (bound only to loopback)

The development-only SimpleAuthManager uses `AIRFLOW_ADMIN_USER` (default
`admin`) and generates a random password on first use. The password is retained
in the `airflow_auth` named volume. Read it **locally**, not in a shared report:

```bash
docker compose --profile airflow exec airflow-api \
  python -c 'import json,os; print(json.load(open(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])))'
```

Airflow may also print its generated password in startup logs; protect those
logs. Never commit the password file, `.env`, log files or tokens.
Do not expose this local SimpleAuthManager deployment to the internet.
To change the local password, update its JSON file inside the auth volume
as described in the official SimpleAuthManager documentation and restart the
Airflow API service. User roles are configured through `AIRFLOW_ADMIN_USER`.

## Run and verify

The weather pipeline requires known real gauge coordinates. If this is a fresh
database, perform a permitted initial gauge fetch before running weather:

```bash
docker compose exec -T api danubepulse ingest-real-river
```

After obtaining permission for unattended eHYD polling:

```bash
docker compose --profile airflow exec airflow-scheduler \
  airflow dags unpause danubepulse_river
```

Trigger manually in the Airflow UI, or:

```bash
docker compose --profile airflow exec airflow-scheduler \
  airflow dags trigger danubepulse_river
docker compose --profile airflow exec airflow-scheduler \
  airflow dags trigger danubepulse_weather
```

A paused DAG keeps manual runs queued; unpause it only when appropriate.
Do not add `--logical-date` unless deliberately testing a known interval.
Check scheduled/manual runs, task retries, `ingest` and `verify` logs, and XCom's
execution UUID in the Airflow UI. Then inspect **Automated Pipelines** in
Streamlit or `GET /v1/pipelines`. The dashboard distinguishes:

- latest source event time;
- latest verified execution and its original source-retrieval time;
- failed attempts, errors, unfinished verification and recent execution history;
- stale retrievals (river: 2 hours; weather: 30 hours).

Rechecking a cached response does **not** refresh the source-retrieval clock.
This dashboard is application-side evidence, not a replacement for Airflow
scheduler health. If Airflow/API communications fail before reaching the
application, the task error exists in Airflow logs, not an application run.

## Reliability and recovery

- Two task retries, five-minute initial backoff (exponential, capped at ten
  minutes), ten-minute execution timeout, 40-minute DAG deadline.
- Default `all_success` dependency: failed ingestion cannot yield a green
  downstream verification task or successful DAG.
- `max_active_runs=1` and `max_active_tasks=1` per DAG. Real-source ingestion
  also holds a dedicated **PostgreSQL session-level advisory lock** through
  internal pipeline commits, protecting all API, CLI and Airflow callers.
  Overlap returns HTTP 409 and is auditable/retryable.
- SQLite retains a same-process lock for offline regression tests; it is not
  a distributed scheduler lock. Scheduled deployments use PostgreSQL.
- Stable source-record identities and safe deduplication prevent replays,
  schedule retries and manual fetches from inflating Gold.
- Every application ingestion attempt creates a new `pipeline_executions`
  audit entry, even when reusing the existing 15-minute river cache or fixed
  weather response cache. Ingestion failures are persisted and propagated.
  Partial weather failures are retried rather than cached permanently.
- Successful verification is idempotent. Original failures and previous
  attempts are retained. Verification does not mutate raw or analytical data.
- If a task is killed after the HTTP request reached the application, server
  work can finish independently. Retry safely: source locks and idempotency
  prevent corruption. `running`/`ingested` audit entries are **not** reported
  as successes; investigate abandoned entries and Airflow task logs.

Clear/retry a failed Airflow task only after inspecting its source/DQ error.
A failed complete-period check must not be turned green by changing its expected
row count. If a fresh database's first weather execution lacks coordinates,
load permitted real river data, then retry the weather task.

## Validation commands

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
docker compose --profile airflow config --quiet

# Set this to the existing local database URL; each test uses an isolated schema.
TEST_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@localhost:5432/DATABASE' \
  .venv/bin/pytest -q

# Uses the exact installed Airflow parser and task implementations; no provider calls.
docker compose --profile airflow exec -T airflow-scheduler \
  python /opt/airflow/tests/check_dags.py

# After both scheduled AND manual runs have completed for each DAG:
.venv/bin/python scripts/verify_airflow.py
```

The final script checks authenticated API access, scheduler/parser health,
successful task states for both kinds of run, and retained task logs. It does
not trigger provider calls or print login secrets. Tests use explicitly mocked
payloads only in isolated test schemas; the live DAGs never load test fixtures.

## Running without Airflow

```bash
docker compose --profile airflow stop airflow-scheduler airflow-dag-processor airflow-api
DATA_MODE=real docker compose up -d api dashboard
```

To demonstrate the synthetic incidents, restart the application with
`DATA_MODE=demo`; stop Airflow first so it does not retry rejected real-ingestion
requests. Real and synthetic observations remain separate. All three original
incident scenarios, detection checks, Bronze files and replay histories persist.
For a complete shutdown, `docker compose --profile airflow down` preserves named
volumes. **Never use `down -v`.**
