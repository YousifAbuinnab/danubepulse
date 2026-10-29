# Stage 6: local PySpark and Delta Lake

## Purpose and boundaries

Stage 6 adds a **manual, optional lakehouse**, not a replacement for the working
PostgreSQL pipeline. Spark is useful here for learning distributed execution
plans, partitioned transformations and transactional file tables; the current
observations are small enough that PostgreSQL is simpler and sufficient.

The Stage 5 baseline is tagged `stage-5-a65-verified`; development is on
`stage-6-lakehouse`. No application migration, provider request, schedule change
or historical-row correction is needed. A75/A44 remain gated, general ENTSO-E
terms remain unaccepted, and river/energy Airflow DAGs remain paused.

### Runtime

- Python 3.12, OpenJDK 17, PySpark **3.5.6**, Delta Lake **3.3.2**, Scala 2.12.
- [Official Delta/Spark compatibility table](https://docs.delta.io/releases/):
  Delta 3.3.x supports Spark 3.5.x.
- Dedicated Docker image; the application and Airflow do not install Spark.
- `local[2]`: one JVM, two local worker threads, two shuffle partitions, 1 GiB
  driver heap; Docker caps the job at 2 CPUs and 3 GiB.
- Tested on Linux/amd64 under WSL. The image's Java path targets amd64.
- Python dependencies and Delta JVM artifacts are resolved at image build time.
  Once built, existing-data runs and synthetic benchmarks need no provider or
  Maven access. Initial builds require package-repository access.

Run one Spark job at a time on a laptop. Airflow is not required. Do not start
the entire Airflow profile just to use the lakehouse.

## Architecture and provenance

```text
Existing immutable source Bronze → existing PostgreSQL validated Silver
                                             │
                         REPEATABLE READ, READ ONLY snapshot
                                             │
                     provenance-separated long-format measurements
                                             │
               Delta Bronze → Delta Silver → Delta Gold → verified manifest
                                                              │
                                                  FastAPI → Streamlit
```

PostgreSQL remains authoritative. **Lakehouse Bronze is a trusted PostgreSQL
export**, not a second copy of the original unvalidated provider response.
Original source Bronze files remain untouched. Every exported measurement
retains its source/run/record identifiers, original Bronze path/checksum,
event/ingestion/processing times, provider document metadata, units, attribution,
license and reference URL. A Bronze path is a reference into the API's storage,
not a file mounted into the Spark container.

Only observations with `quality_status='valid'` are exported. Superseded,
quarantined and duplicate rows do not become trusted measurements. Null values
remain missing, never zero. One PostgreSQL weather observation can export four
measurement rows, so lakehouse measurement counts are not source-event counts.

The named `lakehouse_data` volume has separate directories:

| Directory | Contents |
|---|---|
| `real/` | Existing real river, weather and energy measurements |
| `demo/` | Existing synthetic incident-demo measurements |
| `synthetic-scale/` | Artificial benchmark data, never joined to observations |

Each observation namespace contains `bronze/`, `silver/`, `gold/` Delta tables,
content-addressed `snapshots/`, immutable per-attempt `runs/` JSON files,
`latest_attempt.json`, and the last verified `status.json`. Benchmark runs retain
their own UUID directories and audit reports.

An exclusive filesystem lock prevents overlapping writers to the volume.
The source transaction enforces read-only SQL even though local Compose reuses
the configured database login. For deployment beyond this local demonstration,
provision a dedicated SELECT-only PostgreSQL role and override the job's `PGUSER`
and `PGPASSWORD`; do not share owner credentials or expose the volume publicly.

### What is incremental

The small authoritative PostgreSQL snapshot is **fully re-read** each run; this
is not CDC or a high-water-mark source extractor.

- Bronze uses insert-only `MERGE` on snapshot hash + measurement key. An
  identical snapshot adds no Bronze rows; distinct snapshots retain history.
- Silver `MERGE` uses a stable source/location/metric/UTC interval identity and
  content hash. Only changed/new observations are updated/inserted. Missing
  observations become `active=false`, retaining previous rows rather than
  silently keeping withdrawn/quarantined data trusted.
- Gold is recomputed from active Silver; `MERGE` updates changed groups, inserts
  new groups and removes obsolete groups from the **current** view. Prior groups
  remain accessible through Delta versions. This is incremental persistence,
  not incremental aggregate computation.
- Repeated runs retain separate job audits. Delta may record no-op transactions;
  idempotency means stable observation counts/values, not frozen version numbers.

An empty successful source snapshot deliberately deactivates the namespace's
Silver rows and empties current Gold. An extraction error fails the job instead:
it is never replaced with an empty-success export.

### Gold semantics

Daily grouping is by **UTC date**, provenance, source, domain, location, metric,
production type and unit. No join multiplies weather, river or energy records.

- River readings and weather variables expose sample count/mean/min/max.
  Gauge levels are not absolute elevations; means of irregular readings are
  not time-weighted continuous river averages.
- Precipitation also exposes the sum of available interval amounts in mm.
  Missing hours are not assumed dry.
- Energy intervals expose covered hours and a duration-weighted average.
  `MW × hours` integrates generation or load forecast to MWh.
- Prices use duration-weighted EUR/MWh; they are not summed or integrated to MWh.
- A65 is **national day-ahead demand forecast**, not actual demand or generation.
  Generation data, if authorized/imported separately, is national production by
  type, not a particular hydropower plant.
- Covered hours do not prove complete coverage. Existing source quality checks
  remain authoritative; different intervals/stations/metrics are not added into
  a misleading cross-domain “total”.
- Weather remains Open-Meteo model/reanalysis data. Source permissions are not
  expanded by exporting to Delta. Do not redistribute eHYD data without reviewing
  its still-unclear redistribution terms. Correlation is not causation.

### Transactions and failure recovery

Each Delta table has ACID transactions, enforced schemas, operation metrics,
version history and version-based time travel. Commit metadata includes the
lakehouse run ID and namespace. There is **no cross-table transaction**.

Silver is verified against the complete trusted export after persistence. Gold
is read back and checked against the aggregate transformation. Only then is
`status.json` atomically published. Its table versions and preview describe a
single verified generation, not an unverified read of the latest table pointers.

A failed job preserves the previous successful manifest, publishes the failure
separately, and exits nonzero. A crash can leave newer table versions than the
published manifest; read the manifest's version explicitly, or rerun the same
job to finish recovery. Tests inject a post-write failure and verify rerun repair.
An interrupted process may leave `latest_attempt.json` in `running`; confirm no
container is active before retrying. OS file locks are released on process exit.

No automatic `VACUUM`, history cleanup, database reset or volume deletion is
performed. Delta's default log-retention policies still apply when future
checkpoints are written; indefinite time travel is not promised. Preserve
snapshots/backups and establish an explicit retention policy before production use.

## Run locally

Use the existing private `.env`; do not change provider permissions or tokens.

```bash
docker compose up -d --build db api dashboard
docker compose --profile lakehouse build lakehouse

# Read only existing trusted observations. No external API requests.
docker compose --profile lakehouse run --rm lakehouse run --scope real
# Repeat: metrics must stay identical, with another audit run.
docker compose --profile lakehouse run --rm lakehouse run --scope real

# Independent synthetic demo namespace; does not load/reset PostgreSQL fixtures.
docker compose --profile lakehouse run --rm lakehouse run --scope demo

# Artificial scale workload: duplicates + schema rejection + update/time travel.
docker compose --profile lakehouse run --rm lakehouse benchmark --sizes 10000 100000 1000000
```

`docker compose run` may start the database dependency but never the Airflow
scheduler. Jobs exit after completion. The new named volume persists afterward;
the API mounts it **read-only**. Do not run `docker compose down -v`.

Open [Streamlit](http://localhost:8501) → **Lakehouse Explorer**. Choose `real`,
`demo` or `synthetic-scale` to see provenance, last attempt/verified result,
table versions, row/file counts, transaction histories, source attribution and
Gold preview or scale results. Refresh the dashboard after a completed CLI job.
The Explorer never triggers Spark or source ingestion. The manifest API is:

```bash
curl -fsS http://localhost:8000/v1/lakehouse
```

No manifest yet is reported as `not_processed`; malformed/unreadable status
returns HTTP 503 with an explicit logged error, not a fabricated healthy result.
Gold preview is limited to 100 groups; history summaries show the last 10 commits.

### Inspect history and time travel

```bash
# Current table and recent operations.
docker compose --profile lakehouse run --rm lakehouse inspect --scope real --table silver
# Original Delta version (or choose a published manifest's table version).
docker compose --profile lakehouse run --rm lakehouse inspect --scope real --table silver --version 0
docker compose --profile lakehouse run --rm lakehouse inspect --scope real --table gold --version 0
```

The synthetic benchmark creates `2N` identical-key inputs, deduplicates to exactly
`N`, and checks the stored sum against an arithmetic expectation. An unexpected
column append must be rejected **without a new commit**. It then updates one
value from `0` to `999`; current reads must see `999` and `versionAsOf=0` must
still see `0`. Reports retain table paths and both transaction histories.

Timings include generation, deduplication, disk I/O, verification, schema rejection,
updates and time-travel checks. They exclude Spark-session startup; cold/warm JVM
and local-machine contention matter. These are reproducible correctness/scale
experiments, **not evidence that Spark beats PostgreSQL or provides multi-node
speedup**. Inputs are capped at one million unique rows per size to limit misuse
on laptops.

## Verify

```bash
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/ruff format --check .
docker compose --profile lakehouse --profile airflow config --quiet

# Offline Spark contracts, disposable container filesystem; no DB/provider access.
docker run --rm --memory=3g --cpus=2 --entrypoint python \
  danubepulse-lakehouse -m pytest lakehouse/tests -q

# Actual existing PostgreSQL data: real export twice + separate demo export.
# Independently compare every Gold group with SQL and fingerprint all application
# tables before/after. Writes only the lakehouse volume, not PostgreSQL.
docker compose --profile lakehouse run --rm --entrypoint python \
  lakehouse lakehouse/tests/verify_postgres.py
```

Application tests also support the existing `TEST_DATABASE_URL` disposable-schema
PostgreSQL convention. Supply it privately; never print credentials in a report.
The existing incident/replay and energy tests run unchanged on both databases.
CI separately builds the pinned Spark image and runs offline Delta contracts.

## Verified local results

The existing PostgreSQL export contained **2,121 real measurement rows** and
**40 synthetic demo measurement rows**. Independent SQL matched all **91** real
Gold groups. The repeated real export did not inflate Bronze or Silver.
For 2026-10-19 A65 specifically:

| Metric | PostgreSQL and Delta |
|---|---:|
| Valid intervals | 96 |
| Covered hours | 24 |
| Forecast demand | 164,825 MWh |
| Duration-weighted forecast | 6,867.708333333333 MW |

The initial real job took 32.468 s and its warm repeat 16.458 s, excluding JVM
startup. Historical PostgreSQL rows, migrations and audit records were unchanged.
Actual scale timings vary; the Explorer retains the measured report for this
deployment rather than promising a portable performance threshold.

## Learning exercises

1. Compare observation counts with long-format measurement counts.
2. Inspect Bronze and Silver version 0, then repeat ingestion and inspect `MERGE`
   operation metrics and run IDs.
3. Run offline contracts: a corrected forecast changes integrated MWh; a removed
   measurement becomes inactive without erasing its history.
4. Inspect the failed schema append and the two successful synthetic commits.
5. Compare time travel with a replay: time travel reads historical state;
   PostgreSQL incident replay repairs authoritative data and produces new audits.
6. Study why duration weighting, UTC interval boundaries, null handling,
   provenance separation and single-writer coordination matter even at small scale.
