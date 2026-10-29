# Incident Investigation Learning Guide

DanubePulse demonstrates three deterministic source failures using synthetic data.
Each incident has a detection snapshot, a separate recovery-validation snapshot,
before/after Gold metrics, and a lifecycle event trail. Bronze JSON snapshots are
immutable and carry a checksum. Replaying a snapshot verifies the checksum before
changing Silver.

## Scenario A — Schema drift

### Why it happens

The synthetic river provider changes `water_level_m` to `water_level_cm`. The
incoming data still looks numerically plausible, but the contract no longer
matches the standardized model.

### Detection

The schema contract check records an original `failed` result. The unmodified
record is persisted in `quarantined_records`, but is not loaded as a valid Silver
observation and is excluded from Gold. The incident API exposes the source row,
run, checksum, and impacted daily aggregate.

### Recovery

Replay reads the Bronze snapshot, converts centimeters to meters, runs the full
validation suite again, and upserts a standardized observation. Quarantine is
marked recovered; neither the Bronze payload nor the original failed result is
rewritten.

### Verification

Compare detection-phase `schema_change=failed` with recovery-phase
`schema_change=passed`. The after metric must include the recovered observation,
and replaying the same incident again returns the original completed replay
without inserting another observation.

## Scenario B — Duplicate observations

### Why it happens

The synthetic source publishes two different record identifiers for the same
station and event timestamp. A uniqueness constraint on a source record ID alone
cannot prevent this semantic duplicate.

### Detection and impact

The batch duplicate check groups by station/event time. Both rows are initially
retained in Silver so the demonstration can show the impact: the detected
incident marks analytics degraded and its before snapshot includes both values.

### Recovery

Replay deterministically retains the first source row and changes the duplicate
row's Silver `quality_status` to `duplicate`. The Gold SQL view only includes
`quality_status='valid'`, so the duplicated row no longer contributes to daily
counts or averages. Raw Bronze is unchanged.

### Verification

The recovery validation reports no duplicate event, the Gold count falls by one,
and its averages are recalculated. Repeated replay is idempotent. Repeated source
ingestion does not create a second copy of the same source record.

For a fresh simulation, query the actual Gold view:

```sql
SELECT observation_date, observation_count,
       average_primary AS average_water_level_m,
       average_secondary AS average_discharge_m3_s
FROM gold_daily_observations
WHERE domain = 'river' AND location = 'AT-DANUBE-DUPLICATE';
```

| State | Count | Mean water level (m) | Mean discharge (m3/s) |
|---|---:|---:|---:|
| Detected, analytics degraded | 5 | 3.898 | 2089 |
| Recovered and verified | 4 | 2.8225 | 1511.25 |

The five physical Silver rows and their original values remain available:

```sql
SELECT source_record_id, event_time, quality_status,
       water_level_m, discharge_m3_s
FROM river_observations
WHERE source_id = 'synthetic-duplicate'
ORDER BY event_time, source_record_id;
```

Only `duplicate-river-001-copy` becomes `duplicate`; the four canonical rows
remain `valid`. These deliberately inflated pre-recovery results are never
advertised as trusted: the active incident degrades analytics trust.

### Snapshot correctness and existing incidents

Production sessions disable SQLAlchemy autoflush. The original recovery changed
the ORM object's status but queried analytics before flushing that change. It
therefore saved an incorrect five-record after snapshot. Commit subsequently
persisted the duplicate status, so the actual PostgreSQL Gold view already
contained four records. Tests using default, autoflush-enabled sessions missed
this timing bug.

Recovery now captures its before snapshot before correction, flushes Silver
changes, queries the corrected aggregates, and adds two independent checks:

- `persisted_recovery`: SQL rows must match the corrected source records, with
  redundant records explicitly excluded.
- `downstream_gold`: the actual Gold view must match those persisted aggregates.

The Gold view is not materialized: it reflects flushed Silver changes without
a refresh command. Verification failure rolls back Silver changes and records
`failed_recovery`; passing validation of an in-memory payload is not sufficient.

Older completed replays lacking these checks are shown as **unverified**.
Executing recovery again appends new evidence and updates the incident's after
snapshot without deleting or rewriting old replay/check/event history. If the
database was already corrected, this new replay correctly records **4 -> 4**,
not an artificial **5 -> 4**. The original detection snapshot still shows the
original five-record impact. Subsequent recovery requests return the verified
completed replay without creating more rows or replay entries.

## Scenario C — Missing and late observations

### Why it happens

The synthetic source omits the 02:00 UTC measurement and provides neighboring
observations more than 72 hours after their event times. Their `event_time`,
`source_published_at`, and `ingested_at` remain distinct.

### Detection and impact

Hourly completeness detects the gap, while freshness detects late events. The
before Gold snapshot includes only the four delivered observations.

### Recovery

The original Bronze run includes a separately identified deterministic
`recovery_records` payload, which represents the source's delayed backfill.
Replay inserts the missing 02:00 record with its original event/publication
times and a new ingestion/processing time. It does not fabricate a current
measurement or change the neighboring observations.

### Verification

The recovered Gold day contains five hourly measurements and the recovery
hourly-completeness check passes. The `late_arrival` recovery check remains a
warning: lateness is a historical fact, even after the missing value has been
backfilled. This preserves useful operational evidence rather than silently
rewriting it.

## Lifecycle and reset

Incidents move through `detected`, `investigating`, `recovering`, and
`recovered`. Integrity or validation failures move to `failed_recovery`.
`incident_events` records each transition, replay, or reset. Detection and
recovery checks are stored with separate `phase` values.

**Reset demo scenarios** only restores these synthetic incidents to their
detected data condition. It marks recovered rows quarantined or restores
deduplicated rows to valid, keeps every run/replay/check/event, and never deletes
or edits Bronze snapshots. A subsequent simulation starts a new incident and
run.

## Engineering concepts demonstrated

- Immutable raw storage, SHA-256 integrity checks, and replayable ingestion runs
- Source identity versus semantic event identity
- Quarantine and data-quality gates between Bronze, Silver, and Gold
- Idempotent upserts and safe deduplication
- Event time, publication time, ingestion time, and processing time
- Incremental analytics impact inspection and SQL-view recomputation
- Separate historical detection and post-recovery validation evidence
- Data lineage, audit events, lifecycle state, and failure recovery
- Synthetic provenance and reproducibility without external APIs

The fixtures are synthetic examples, not measurements from real Austrian
stations. Energy observations are not part of these scenarios. No correlation
or relationship in this project establishes causation.

## Stage 3 — Real Austrian observations

Real-source ingestion is separate from the deterministic incident examples.
The real connectors do not call the demo ingestion or replay functions, use
different `data_sources` and station IDs, and write into real-only Gold views.
`DATA_MODE=real` enables the live ingestion endpoints/CLI and disables demo
ingestion, reset, investigation and replay operations. The synthetic Gold view
and `/v1/observations` remain synthetic-only; `/v1/real/*` and
`gold_real_*` contain only records whose source provenance is `real`.

### Austrian gauge readings

The source is Hydrographie Österreich's documented OGC API Features
`i000501:pegel_aktuell` collection. Danube station identifiers are allowlisted
as Achleiten (`207019`), Linz (`207068`) and Kienstock (`207357`), then verified
against the response's hydrographic station ID, official name, water body,
timestamp and point coordinates.

This collection reports current station values rather than a historical time
series. Each manual or scheduled ingestion stores another immutable snapshot;
the dashboard only draws a time trend after it has at least two snapshots.
Hydrology attribution is **Datenquelle: ehyd.gv.at**.

The API's `parameter=W` identifies water-level readings. Known centimeter
values are converted to meters while the original numeric value and unit remain
in Silver and the raw response remains in Bronze. The reference datum is not
exposed by this collection, so normalized values are labelled
`gauge_reference_unspecified`; they must not be interpreted as elevation above
sea level. `parameter=Q` is discharge and is normalized only from known
m³/s-compatible units. Unknown unit/type combinations are quarantined from
trusted Gold rather than guessed or converted.

### Historical weather

Open-Meteo's Historical Weather API returns hourly gridded model/reanalysis
estimates at the requested gauge coordinates. They are not direct measurements
from a local weather station. The initial fetch covers seven complete UTC days
with a two-day archive lag and requests temperature at 2 m, precipitation,
relative humidity at 2 m and wind speed at 10 m. Date ranges can be backfilled
explicitly up to 31 days per request. The response's grid coordinates, units,
model metadata where supplied, and hourly records are stored.

Internal event times are UTC; dashboard labels also provide Europe/Vienna time,
including daylight-saving offsets. Precipitation is an hourly accumulation;
Gold daily totals sum available values, while averages/counts ignore nulls.
Missing values remain null, not zero. A real weather row may be analytically
valid when some variables are absent, but the missing-measurement and
completeness checks remain visible.

Open-Meteo requires attribution (**Weather data by Open-Meteo.com**) and its
free API is for non-commercial use under CC BY 4.0. The hydrography feature
metadata does not itself declare redistribution terms; display the requested
attribution and consult provider terms before redistributing data.

### Bronze, Silver, Gold and operations

- **Bronze:** response pages/payloads, query parameters, retrieval time,
  provenance, rejects and a SHA-256 checksum are written to a new immutable
  file under the mounted source directory.
- **Silver:** station metadata and validated rows are idempotently inserted by
  `(source_id, source_record_id)`. UTC timestamps, original units and normalized
  values are retained. Unsupported units, invalid types/ranges, duplicates,
  missing values, source outages, freshness and hourly gaps become
  `quality_results`.
- **Gold:** `gold_real_river_daily` groups by station and physical measurement
  type; `gold_real_weather_hourly` and `gold_real_weather_daily` use only real
  provenance. These are SQL views over valid Silver rows, not fabricated or
  prefilled time series.

Hydrology requests cache for 15 minutes; fixed historical weather request
periods are cached by coordinates/date/model. `--force` bypasses caching.
`danubepulse ingest-real-river` and `danubepulse ingest-real-weather` can be
called manually or scheduled with cron/systemd; failed source requests create
failed ingestion runs and quality evidence instead of successful-looking
empty data.

The dashboard's **Real Austrian Data Explorer** separates real gauges, weather
reanalysis and daily coverage from the synthetic demo. It shows source
attribution, station coordinates, current readings, historical charts when
available, local/UTC event times, latest run status and quality warnings.
Open-Meteo relationships with river readings are descriptive only and do not
establish causation.
