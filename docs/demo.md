# DanubePulse interview demo

This is a repeatable **10–15 minute** local walkthrough of data engineering
decisions, failure handling and verification. It uses no live provider requests.
The Airflow fixture, incident examples, Kafka event and Spark scale records are
clearly labeled synthetic; the Austrian observations shown in the last section
are read-only records already stored in PostgreSQL.

The Demo Control Center protects synthetic mutations with a private
`DEMO_CONTROL_TOKEN`, requires explicit confirmation, and exposes no delete/reset
control. The API and Streamlit host ports bind to loopback. Keep the token and
`.env` private; this local application is not suitable for internet exposure.
If `DATA_MODE` is `real`, all synthetic controls remain disabled.

## Start the application

From the repository root, preserve the current `.env` and run the helper to add
a random demo-action token only if one is missing. The helper does not print the
secret or change provider permissions. Set demo mode for this shell only:

```bash
python3 scripts/setup_demo_control.py
export DATA_MODE=demo
docker compose up -d --build --wait db api dashboard
```

The shell override takes precedence over `.env` without editing its existing
`DATA_MODE=real` setting. In demo mode live-ingestion endpoints are disabled;
the real-data explorers below remain read-only database queries. Keep this
shell open for the Compose commands in this guide.

Confirm readiness:

```bash
curl -fsS http://localhost:8000/health
```

Open [Streamlit](http://localhost:8501) → **Demo Control Center**. The first
row verifies FastAPI/PostgreSQL together; dashboard availability is evidenced
by the loaded page. Airflow and Kafka are optional and show unavailable until
their isolated demo components are started. Spark/Delta becomes ready after a
verified demo manifest is present.

The API container loads the token from `.env`, and Compose passes the same
secret to Streamlit for server-side requests. After changing it, recreate both
containers. Do not copy the token into the browser, a command argument, issue,
log or screenshot. A configured token is authorization for local demonstration
actions, not production authentication.

## 12-minute presenter script

### 0:00–1:30 — Architecture and trust boundaries

In the Control Center, start with readiness and provenance:

- PostgreSQL is the system of record; FastAPI checks its connection.
- Airflow schedules orchestration but the demo DAG is manual-only.
- Kafka handles durable **notifications**, not live sensor measurements.
- Spark/Delta is optional and used to demonstrate transactions, not because
  the data volume needs a distributed engine.
- Previously ingested real observations are presented from the database, with
  no connector call from this page.

### 1:30–3:30 — Real Austrian data, read-only

Expand **Previously ingested Austrian data**. Show stored eHYD gauge readings,
Open-Meteo model/reanalysis estimates, and ENTSO-E A65 Austrian national
day-ahead load forecast intervals when present.

Explain that gauge levels are relative, weather is modeled rather than a local
station observation, and A65 is forecast demand—not measured load, generation,
or an individual power plant. The page reads existing database rows only.
Correlation is not causation. If a data product is absent, say so; do not
trigger an ingestion to fill the gap.

### 3:30–5:30 — Airflow fixture DAG and task logs

The manual `danubepulse_interview_demo` DAG reads a four-row JSON fixture,
validates its synthetic provenance/schema/timezones, calculates two grouped
averages, then reloads the fixture and independently verifies the aggregate.
The three real-source DAGs are not triggered.

Run the command shown in the Control Center (also below). Point out the
`validate_fixture`, `aggregate_demo`, and `verify_aggregate` task results and
the structured task log lines in the terminal. This one-shot Airflow CLI
execution uses an ephemeral SQLite metadata database inside a disposable
container, network isolation, read-only DAG/fixture mounts, and no scheduler.
It does not touch the persistent Airflow metadata database.

### 5:30–7:30 — Kafka outbox and deduplication

Start the optional broker and workers using the controls' Compose commands.
Check readiness, then confirm **Queue deterministic demo notifications**.
The API inserts three labeled notifications into the existing transactional
outbox; retrying the control returns the same event IDs rather than inserting
duplicates.

Use **Replay first event with the same event ID** to enqueue one deliberate
duplicate delivery. Refresh the page and inspect Streaming Monitor: the SQL
inbox should show one unique receipt with an increased delivery count. This is
at-least-once delivery with idempotent processing, **not** end-to-end
exactly-once. A broker outage leaves the outbox durable and does not change
measurements.

### 7:30–10:30 — Data-quality investigation and recovery

Select **Duplicate records**, confirm the synthetic action, then simulate.
Show the failed `duplicate_events` detection result and affected record.
Investigate the incident, execute recovery, and compare the Gold count and
audit trail. The canonical fresh fixture's trusted river count changes from
five to four after deduplication; the API verifies persisted Silver, Gold and
replay idempotency before marking recovery complete.

For a shorter or different story, select **Schema drift** to show quarantine,
field mapping and replay, or **Missing and late measurements** to show event
time versus ingestion time and aggregate backfill. Do not reset the database
during an interview; the recovery controls add synthetic audit evidence and do
not delete records.

### 10:30–12:30 — PySpark and Delta Lake

Run the two lakehouse commands shown in the Control Center. The first performs
a read-only PostgreSQL snapshot export into the isolated synthetic Delta
namespace and verifies Bronze/Silver/Gold counts and results. The second runs a
small 1,000-row, clearly labeled scale contract in the separate
`synthetic-scale` namespace: duplicate handling, schema rejection, an update,
and a historical Delta version read. Refresh Streamlit to inspect persisted
layer versions, aggregates and time-travel evidence.

This adds demo audit/results to the existing synthetic lakehouse folders; it
never deletes volumes or touches real observation records. Expect the Spark
container to use at most 3 GiB and two CPUs; do not run it alongside other
heavy optional workloads on a constrained laptop.

## Exact isolated Airflow command

Run from the repository root. Image pulls may need internet the first time.
The task container itself has **no network**:

```bash
docker run --rm --network none --memory=1g --cpus=1 \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db \
  -e DANUBEPULSE_DEMO_FIXTURE=/opt/airflow/fixtures/interview_demo.json \
  -v "$PWD/airflow/dags:/opt/airflow/dags:ro" \
  -v "$PWD/airflow/fixtures:/opt/airflow/fixtures:ro" \
  -v "$PWD/airflow/tests:/opt/airflow/tests:ro" \
  --entrypoint bash apache/airflow:3.3.2-python3.12 -ec \
  'airflow db migrate && python /opt/airflow/tests/check_dags.py && \
   airflow dags test danubepulse_interview_demo 2026-10-27'
```

It migrates only the disposable container's SQLite metadata, runs the pinned
Airflow DAG contract suite, then executes the DAG and prints actual task logs.
No scheduler is started. In particular, do **not** start the optional Airflow
Compose scheduler just for this demo: the separate weather DAG is enabled by
default and may attempt provider requests. River/energy remain paused, but the
interview workflow does not require the scheduler at all.

## Kafka commands

The stack is optional. In the shell where `DATA_MODE=demo` is exported, start
Kafka, create topics idempotently, and then start the durable publisher and
consumer workers:

```bash
docker compose --profile kafka up -d --wait kafka
docker compose --profile kafka run --rm kafka-init
docker compose --profile kafka up -d streaming-publisher streaming-consumer
```

Use the Control Center to publish/replay; use **Streaming Monitor** for
outbox state, receipts, delivery counts, dead letters and worker heartbeats.
If Kafka is unavailable, newly queued events remain pending in PostgreSQL.
Do not run the destructive/outage integration harness against the interview
stack; its purpose is automated isolated testing.

## Spark and Delta commands

```bash
docker compose --profile lakehouse run --rm lakehouse run --scope demo
docker compose --profile lakehouse run --rm lakehouse benchmark --sizes 1000
```

`demo` only exports trusted synthetic application records. It never writes to
PostgreSQL. `benchmark` stores a separate synthetic test generation and
validates time travel from the prior Delta version. The API exposes verified
manifests read-only; refresh Streamlit after the jobs complete.

## Shutdown without data loss

Stop only services started for the demonstration. These commands do not remove
containers' named volumes or database records:

```bash
docker compose --profile kafka stop streaming-consumer streaming-publisher kafka
docker compose stop dashboard api db
```

The isolated Airflow command exits on completion and uses disposable container
storage. Do not run `docker compose down -v`, delete named volumes, clear
lakehouse directories, reset PostgreSQL, or trigger real-provider ingestion.
Airflow metadata and existing history remain intact.

## Safety and limitations

- `ENTSOE_TERMS_ACCEPTED=false`; A75/A44 remain gated; river and energy DAGs
  remain paused. The demo never changes these settings.
- The read-only real-data panel calls only local DanubePulse API endpoints.
- Synthetic incidents and Kafka messages are new auditable demo records. They
  are not external measurements.
- The local API has no general user identity system. The dedicated demo
  mutation routes require a 256-bit random local token and confirmation; this
  does not make the application suitable for public exposure.
- The full-history publication blocker and deferred code license remain
  unresolved. Do not push or publish as part of this demo.
