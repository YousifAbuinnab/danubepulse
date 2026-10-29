# Stage 7: optional Kafka notification streaming

## What this adds (and what it does not)

Kafka transports **ingestion, validation, incident and pipeline notifications**,
not new external measurements. Existing PostgreSQL ingestion remains authoritative.
No eHYD, Open-Meteo or ENTSO-E request is made by the workers. A75/A44 remain gated,
`ENTSOE_TERMS_ACCEPTED=false` remains unchanged, and river/energy Airflow DAGs
remain paused. Spark, Delta and Airflow are not required to run Kafka.

Work is on branch `stage-7-kafka`, based on `stage-6-lakehouse-verified`.
There is deliberately no Stage 7 commit or milestone tag yet.

## Runtime and verified compatibility

- Official **`apache/kafka:4.1.2`** JVM image, single-node combined
  broker/controller **KRaft** mode; no ZooKeeper.
- Pinned **`confluent-kafka==2.12.2`** Python client, isolated in worker images.
  FastAPI/Streamlit do not install or import the Kafka client.
- Apache publishes the image/version and single-node configuration:
  [official Docker guide](https://kafka.apache.org/41/getting-started/docker/),
  [4.1.2 single-node example](https://github.com/apache/kafka/blob/4.1.2/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).
- The [client package documentation](https://pypi.org/project/confluent-kafka/2.12.2/)
  and [API reference](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
  document broker compatibility, delivery callbacks and synchronous offset commits.
  This exact image/client pair is also exercised by the Docker integration checks.

The broker has a 512 MiB JVM heap, 1 GiB container limit and 1 CPU cap.
Each worker is capped at 256 MiB / 0.5 CPU. A short-lived storage initializer
sets ownership of **only the new Kafka volume's root directory** for the image's
non-root UID 1000. No existing volume is removed or recursively rewritten.

Kafka has no published host port and uses plaintext on the local Compose network.
This is a laptop demonstration, **not an authenticated production Kafka service**.
Do not expose it to untrusted clients. A production deployment needs TLS/SASL,
ACLs, dedicated database roles, retention/backup policy and multiple brokers.

## Architecture and guarantees

```text
Pipeline state / ingestion / quality / incident ORM changes
              │ same PostgreSQL transaction
              ▼
       streaming_outbox ── publisher ──► Kafka events.v1
              ▲                              │
              │                     manual-offset consumer
              │                              ▼
       dead-letter outbox             streaming_receipts
              │                       OR dead-letter row
              ▼                              │
       Kafka dead-letters.v1          database COMMIT first
                                             │
                                      Kafka offset COMMIT

       FastAPI /v1/streaming ← database-only monitoring → Streamlit
```

### Transactional outbox

An application `Session.before_flush` listener captures new terminal ingestion
states, pipeline verification/failure states, non-passed quality checks and
incident state changes. It is registered by the shared session-management module,
covering existing CLI, API and Airflow-driven ORM paths without duplicating
business logic. Raw SQL/external database writes do not invoke this listener.

The notification and the relevant business state commit or roll back together.
No Kafka connection is opened in an ingestion transaction. Existing rows are
**not** retrospectively backfilled into notifications.

Events have stable UUIDv5 IDs derived from type, entity and semantic notification
data, excluding delivery time. Repeated publication uses the stored ID/payload;
an unchanged re-emission reuses its outbox row. A new ingestion run still has its
own identity even when measurements were deduplicated.

The publisher locks one pending row (`FOR UPDATE SKIP LOCKED` on PostgreSQL),
sends with producer idempotence and `acks=all`, waits for the delivery callback,
then marks it published. Failures retain the row and record attempts, safe error class/code
and capped exponential retry timing (2–60 seconds). There is no retry count that
silently discards an event. SQLite is supported for unit/demo tests with a single
publisher; concurrent SQLite writers are not a supported deployment.

There is necessarily an **ACK → database commit crash window**. A message can
reach Kafka while its outbox row remains pending and is sent again. Producer
idempotence does not remove application-level resends across sessions.

### Consumer, idempotency and offsets

The consumer disables auto-commit and auto-offset-store. It validates each event,
then atomically inserts one receipt per event UUID. A matching redelivery
increments an audit delivery count but does not insert a second processing row.
A reused UUID with different validated content is quarantined as a conflict.
Delivery-count increments use an atomic SQL update.

Only after that database transaction commits does the consumer synchronously
commit the **next Kafka offset**. Crashes or commit failures cause redelivery,
which the inbox handles. Database/processing failures do not advance the offset:
the worker logs, seeks back and retries. Topic/partition/next-offset details and
heartbeats provide operational evidence; committed positions are owned by Kafka's
consumer group, not guessed from a database maximum.

The only processing side effect currently implemented is the monitored SQL
receipt. There are no measurement writes, external notifications or analytical
updates in the consumer. Adding any such side effect requires its own idempotency
and transaction design.

**Guarantee: at-least-once delivery with an idempotent database inbox. Not
end-to-end exactly-once.** A single broker/replica also cannot survive loss of its
disk/volume. `acks=all` on one replica is not multi-node redundancy.

There is no global ordering guarantee: events use UUID keys across two partitions.
The monitor must not interpret arrival order as authoritative incident state.

### Dead letters

Malformed JSON, unsupported contracts, oversized events and UUID/content conflicts
are poison records, not transient database failures. The consumer atomically
persists a dead-letter row **and a DLQ outbox row**, then commits the source offset.
If Kafka is unavailable afterward, the DLQ remains pending until publication
succeeds. Dead-letter identity is stable for topic/partition/offset, including
across different replay groups.
Worker counters describe acknowledged deliveries, so replayed poison records
count again there; the database dead-letter rows and DLQ outbox remain deduplicated.

The database retains bounded raw bytes as base64; the DLQ envelope includes an
original-payload SHA-256, byte length, truncation flag and source coordinates.
The limit is 64 KiB; oversized values retain a preview, not unlimited input.
The API/dashboard omit raw poison payloads, and application logs omit event
contents and exception messages that could contain sensitive input.

The consumer subscribes **only** to `danubepulse.events.v1`, so DLQ records do not
loop back through the normal validator. Fixing a poison contract is an explicit
operator action: inspect it privately, correct the source contract, and emit a
new valid event. Merely replaying unchanged poison data does not fix it.

## Tables, topics and event schema

Migration `0008_streaming` only adds:

| Table | Purpose |
|---|---|
| `streaming_outbox` | Immutable notification payload + retry/publication state |
| `streaming_receipts` | One processed payload per UUID + redelivery evidence |
| `streaming_dead_letters` | Durable quarantine by Kafka position |
| `streaming_workers` | Latest publisher/consumer heartbeat and operational status |

No old observation, incident, lineage or audit table is rewritten. Historical
database/volume backups remain intact.
`0009_streaming_offsets` widens the new dead-letter offset column to the Kafka
protocol's 64-bit range. Its pre-migration backup was separately test-restored.

Explicit topics (auto-create disabled):

| Topic | Partitions | Replicas | Retention |
|---|---:|---:|---|
| `danubepulse.events.v1` | 2 | 1 | 7 days |
| `danubepulse.dead-letters.v1` | 1 | 1 | 7 days |

The named `kafka_data` volume retains logs and group offsets across restarts.
PostgreSQL outbox/receipt/dead-letter rows have no automatic deletion policy in
this implementation. An indefinite outage therefore grows the outbox: monitor
disk usage and backlog age. Kafka retention is finite; replay can only read
records still retained there. Do not delete/recreate topics under retained
consumer audits: topic/partition/offset identities assume unchanged topic history.

The Pydantic contract is exposed at:

```bash
curl -fsS http://localhost:8000/v1/streaming/schema
```

Every event has `schema_version=1`, canonical UUID `event_id`, aware
`occurred_at`, `event_type`, `entity_id`, explicit `provenance` and typed `data`.
Unknown fields/types or mismatched type/data shapes are rejected.

| Type | Data |
|---|---|
| `ingestion.completed`, `ingestion.failed` | Source, status, record/accepted/rejected counts |
| `pipeline.verified`, `pipeline.verification_failed`, `pipeline.failed` | Domain, status, ingestion-run reference, failed check names |
| `quality.alert` | Check/status, affected count, phase, incident/run references |
| `incident.changed` | Category, severity, status, run and scenario |
| `streaming.test` | Explicit test scenario, simulated notification kind, `measurements_modified=false` |

Counts are nullable where the pre-existing demo run does not assign them; they
are not fabricated as zero. Event time means notification creation, not the
underlying measurement's event time or provider publication time. References
lead back to existing pipeline audits and immutable Bronze.

Real-source notifications keep `provenance=real`; they still are not a live
external measurement feed. Demo-source notifications are `synthetic`; the
notification-only CLI scenarios use `synthetic-streaming-test`. Tests never
relabel synthetic measurements as real data.

## Back up before migration

Before deploying the new image against an existing database, create custom-format
PostgreSQL dumps of both the application and Airflow metadata databases, restore
them into uniquely named disposable databases, and compare public-table row
counts/checksums. Merely listing a dump's contents does not test restoration.
Keep credentials private and never commit dumps.

For this implementation, both backups were created and test-restored **before**
`0008_streaming` was applied. A second verified backup preceded
`0009_streaming_offsets`. All public-table fingerprints matched the originals.
Backups and the pre-change baseline are retained in the private session files
directory under `stage7-backup/`.

The API's existing startup command runs `alembic upgrade head`. Worker startup
waits for API health but does not migrate or create application tables itself.

## Exact startup commands

From the repository, using the existing private `.env`:

```bash
# Original app works without Kafka. On an existing DB, back up/test-restore first.
docker compose up -d --build db api dashboard

# Build only the optional workers with their pinned client.
docker compose --profile kafka build streaming-publisher streaming-consumer

# Starts Kafka, explicit topic initialization and local notification workers.
# Does not start Airflow or fetch any provider data.
docker compose --profile kafka up -d \
  kafka kafka-init streaming-publisher streaming-consumer

curl -fsS http://localhost:8000/v1/streaming
```

Open [Streamlit](http://localhost:8501) → **Streaming Monitor**. It shows the last
100 outbox events, last 100 received events, pending/retry counts, unique processed count, worker heartbeats,
delivery counts, failures and the last 50 dead letters. `pending`, `published`,
`processed`, and `dead_letter` describe different stages, not pipeline validity.
Refresh the page after CLI operations.

Worker state is `healthy`, `retrying`, `stopped`, or `stale` after 45 seconds
without a heartbeat. “No workers” means not started. The consumer checks broker
metadata while idle rather than treating an empty poll as health; the publisher's
idle heartbeat only proves that its database loop is running, not broker health.
The API never probes Kafka, so a stopped broker cannot break the dashboard.

```bash
# Stop only optional components; preserve every volume.
docker compose --profile kafka stop streaming-publisher streaming-consumer kafka

# Read worker logs (Python logs are structured JSON; librdkafka also logs transport errors).
docker compose --profile kafka logs --tail=80 streaming-publisher streaming-consumer
```

## Manual notification-only demos

Use a new key for a new scenario, and reuse it to prove deterministic event IDs.
These commands never change measurements, source permissions or Airflow settings.

```bash
# Prepare the optional topics once.
docker compose --profile kafka run --rm kafka-init

# Stop daemons so the checks have deterministic ownership of delivery/offsets.
docker compose --profile kafka stop streaming-publisher streaming-consumer kafka

# Queue three notification-only tests and one poison record while broker is down.
docker compose --profile kafka run --rm --no-deps \
  -v "$PWD/streaming/tests:/checks:ro" streaming-publisher \
  python /checks/check_kafka.py outage --key manual-streaming-01

# Bring back the persisted broker. ACK-gap test deliberately sends one event twice.
docker compose --profile kafka up -d --wait kafka
docker compose --profile kafka run --rm --no-deps \
  -v "$PWD/streaming/tests:/checks:ro" streaming-publisher \
  python /checks/check_kafka.py recover --key manual-streaming-01

# Same group: committed offsets mean no further records.
docker compose --profile kafka run --rm --no-deps \
  -v "$PWD/streaming/tests:/checks:ro" streaming-consumer \
  python /checks/check_kafka.py resume --key manual-streaming-01

# New group: replay retained history, retain UUIDs, deduplicate receipts.
docker compose --profile kafka run --rm --no-deps \
  -v "$PWD/streaming/tests:/checks:ro" streaming-consumer \
  python /checks/check_kafka.py replay --key manual-streaming-01
```

Expected for the selected test key: outage retains four messages; recovery creates
three valid receipts with four deliveries and one durable DLQ record; same-group
resume reads nothing; replay adds four deliveries but still only three receipts
and does not create another DLQ for the same poison position. Other retained
events may also be consumed; the script distinguishes its own valid UUIDs.
The phase sequence intentionally is not repeatable with the same key after it has
finished; use a fresh key for a new integration experiment.

General CLI examples:

```bash
docker compose --profile kafka run --rm --no-deps streaming-publisher \
  python -m danubepulse.streaming demo --scenario replay --key cli-example-01
docker compose --profile kafka run --rm --no-deps streaming-publisher \
  python -m danubepulse.streaming publish --once
docker compose --profile kafka run --rm --no-deps streaming-consumer \
  python -m danubepulse.streaming consume --group replay-cli-example-01 --seconds 20
docker compose --profile kafka run --rm --no-deps streaming-publisher \
  python -m danubepulse.streaming poison --key invalid-example-01
```

`publish --once` exits nonzero when a send fails, retaining pending state. Running
it during backoff can find nothing due yet; check pending/retry counts, not only
the number published. Each fresh replay group starts at the earliest retained
offset, while an existing group's committed position takes precedence.

The integration demo asserts only bounded exact counters for its key. Do not run
its phases concurrently with daemon consumers or other publisher tests.

## Automated and Docker checks

```bash
.venv/bin/pytest -o addopts='' -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
docker compose --profile kafka --profile airflow --profile lakehouse config --quiet
```

The application suite also runs under the existing private `TEST_DATABASE_URL`
convention using unique disposable PostgreSQL schemas. It proves transaction
rollback, real ingestion/incident capture, quality alerts, stable IDs, outbox
retry, UUID-conflict quarantine, DLQ deduplication, inbox rollback and monitoring.
Streamlit AppTest exercises the Streaming Monitor against real FastAPI responses.

For real Kafka delivery from the **existing synthetic schema-drift pipeline**:

```bash
docker compose --profile kafka run --rm --no-deps \
  -v "$PWD/streaming/tests:/checks:ro" streaming-publisher \
  python /checks/check_pipeline_events.py
```

This creates/migrates a unique disposable PostgreSQL test schema, runs existing
fixture ingestion and verified recovery, publishes its genuine pipeline
notifications, consumes them and verifies receipts. It drops only that test
schema, never production tables. Its synthetic notifications remain in Kafka
until topic retention expires; they are not genuine external measurements.

CI has a separate Kafka integration job exercising broker outage, recovery, ACK
gap, durable offsets, replay, DLQ publication and the existing synthetic pipeline.
It performs no provider polling.

## Verified local results

- Application suite: **94 SQLite tests** and **94 PostgreSQL tests**; this includes
  schema/type validation, a dead-letter position at `2**40`, rollback tests and
  an injected Kafka offset-commit failure after the database receipt commits.
- Existing compatibility checks: **3 Spark/Delta tests**, **4 Airflow DAG checks**.
- Broker-outage test: four notification-only inputs remained durable; core API,
  A65 discovery and Lakehouse Explorer stayed available with Kafka stopped.
- Recovery: three valid test UUIDs produced three unique receipts and four
  deliveries, including the intentional ACK-gap resend. The poison input was
  durably quarantined and its DLQ event published.
- Same-group restart: zero further deliveries. New-group replay: the selected
  three UUIDs still had three receipts, now with eight total deliveries; the same
  poison position did not create another DLQ outbox row.
- Existing synthetic ingestion/recovery integration: **15 pipeline notifications**
  delivered and verified, including ingestion completion/failure, quality alerts,
  incident transitions and verification failure. Genuine observations were not
  used as mutable streaming-test data.
- Original application/Airflow row counts **and content checksums** were preserved.
  A65 Gold remained 96 intervals, 24 hours, 164,825 MWh. No provider permission or
  paused-DAG state changed.
- The deployed default monitor processed **30 unique notifications** from the
  retained, explicitly synthetic checks, with zero pending publications and two
  durably published dead letters. A running-consumer broker outage changed health
  to `retrying`; broker restart restored `healthy` with the same 30 receipts.
- The deployed Streaming Monitor was checked in the browser as well as AppTest.

These are local measured checks, not a claim that the GitHub-hosted CI job has
already run or that a single broker provides production high availability.

## Operational limits and learning points

- Monitor outbox growth, oldest pending age, dead letters and stale workers.
- PostgreSQL row locks coordinate publisher concurrency; there is one consumer
  monitor group by default. Cross-group duplicate receipt inserts can temporarily
  contend; the unique key plus retry prevents a second processed row.
- A continually failing DB/business handler blocks its partition rather than
  dropping the event. Malformed contracts use the durable poison path.
- Worker heartbeat rows summarize latest state, not every liveness transition.
  Consumer delivery counts, outbox attempts, receipts and DLQ rows retain evidence.
- Notifications capture states at flush time. They are audit facts, not an
  instruction to overwrite current application incident status.
- Schema evolution is explicit: incompatible changes need a new contract/topic.
  Schema Registry, CDC, Kafka Streams and streaming measurement ingestion are
  intentionally outside this stage.
- Replay is historical notification processing. It neither repairs PostgreSQL
  measurements nor runs Delta transforms; those remain existing explicit flows.
