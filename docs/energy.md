# Stage 5: Austrian energy integration

## A65 extension: separate day-ahead electricity-demand forecasts

The operator has reported a successful authenticated Austrian A65 request
(HTTP 200, `GL_MarketDocument`). This verifies that request's access, **not**
an A75/A44 entitlement, blanket reuse permission, or a successful DanubePulse
ingestion. No live A65 records were fetched or imported while implementing
this extension.

The official Postman reference's **6.1.B Day-ahead Total Load Forecast** specifies:

```text
documentType=A65
processType=A01
outBiddingZone_Domain=10YAT-APG------L
periodStart / periodEnd in UTC YYYYMMDDHHmm
```

DanubePulse also requests `curveType=A01`. Response validation checks A65/A01,
Austrian outgoing bidding zone, consumption business type A04, MAW (MW),
aligned intervals, finite nonnegative quantities and document/revision metadata.
Actual-load A16 responses and generation/price documents are rejected rather
than relabeled as demand forecasts. Both fixed A01 and step A03 curves are supported.

### Permission review required before the first live A65 date

Keep `ENTSOE_TERMS_ACCEPTED=false`. Do **not** enable it for this product.
Keep both `danubepulse_energy` and `danubepulse_river` paused. There is no A65
scheduler or automatic fetch button.

Before passing the separate manual acceptance flag:

1. Read the current [ENTSO-E Terms of Use and open-data list](https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961-Legal-Terms-and-Conditions).
   The linked **18 October 2023** list explicitly includes **item 1,
   day-ahead forecast of total load per market time unit, Article 6.1.b**, under
   **CC BY 4.0**. Austria is not among the listed exceptions. Check whether a
   newer list, provider-specific condition or export notice changes that.
   Authentication alone is not this review.
2. Confirm that the actual downloaded document describes the Austrian
   `10YAT-APG------L` total-load **day-ahead forecast**, not measured load, another
   forecast horizon, generation or market prices. Retain the original provider
   and document identity. Obtain permission if the current conditions exclude
   this particular data/export or intended use.
3. Retain attribution to **ENTSO-E Transparency Platform and the original data
   provider (APG for the Austrian data, subject to the document's provenance)**;
   include a [CC BY 4.0 license link](https://creativecommons.org/licenses/by/4.0/),
   retain notices and identify changes: unit normalization, UTC interval
   normalization, latest-revision selection and duration-weighted aggregation.
   Do not imply endorsement.
4. Review the [400 requests/minute per-token limit](https://transparencyplatform.zendesk.com/hc/en-us/articles/12783148966036-API-Rate-Limit-Part-1)
   across all users of the token. This manual operation makes one A65 request
   with one-second request spacing; it does not request A75 or A44.

The one-run flag `--accept-load-forecast-terms` records that review in the
ingestion audit. It does not write an environment setting or unlock another
product. Generic `--accept-terms` does **not** authorize A65.

After backing up PostgreSQL and rebuilding the application (no ingestion):

```bash
DATA_MODE=real docker compose up -d --build api dashboard
```

**Only after completing the review**, the first manual API ingestion can be:

```bash
docker compose exec -T api danubepulse ingest-energy \
  --product load_forecast --date 2026-10-25 --accept-load-forecast-terms
```

This uses the existing private `ENTSOE_API_TOKEN`. Do not put a token in the
command or URL. Change the date deliberately if needed. The pipeline currently
imports **completed UTC delivery days only**, so this is a historical
day-ahead forecast, not an ingestion of tomorrow's forecast. A UTC delivery day
may include intervals from two Vienna market days. CLI output includes
received/accepted counts and independent verification; a nonzero exit means
the run is not verified. Repeating the same command retains a new run/immutable
Bronze envelope without duplicating Silver observations or Gold totals.

For an original A65 XML export instead of a network request:

```bash
docker compose exec -T api danubepulse ingest-energy \
  --product load_forecast --date 2026-10-25 \
  --load-forecast-file /app/data/energy-imports/a65-2026-10-25.xml \
  --accept-load-forecast-terms
```

The normalized CSV option also works with `metric=load_forecast`,
`production_type=load`, `unit=MAW` and the same original document metadata.
It is not a native-provider CSV format. Operator imports remain explicitly
labeled as not independently authenticated.

### Storage, analytics and presentation

- Separate source: `entsoe-load-forecast`; separate metric: `load_forecast`.
- Silver uses the existing interval/revision/audit machinery and a nullable
  `load_forecast_mw` value. Generation and price fields remain null for A65.
- Additive migration `0007_load_forecast` adds
  `gold_real_load_forecast_daily`: covered hours, observation count,
  duration-weighted forecast MW, min/max forecast MW and integrated
  **forecast demand MWh**, never measured consumption.
- Forecast rows are explicitly excluded from the generation/price Gold view.
  Original Bronze, old revisions, lineage and historical failures are preserved.
- An API response's XML `mRID` and `createdDateTime` are retained exactly as
  supplied, but are **not stable observation identity or proof of original
  forecast publication**. In the two inspected October 19, 2026 downloads,
  both fields changed with the API response while all 96 values and revision 1
  stayed identical. For those responses they describe the generated response
  envelope, not evidence that the underlying forecast changed. The legacy
  `source_published_at` field currently stores that XML document creation time;
  it must not be used to claim when the original forecast became available.
- Idempotency compares current source/region, metric, production type, UTC
  interval boundaries, duration, unit and exact normalized value (including
  null), **before** interpreting response/revision metadata. Unchanged intervals
  reuse Silver without changing its original metadata or status. Every run still
  preserves a separate immutable Bronze response and quality results.
  Run-specific lineage maps the response record hash to the reused Silver
  record. `accepted_count` counts newly stored Silver versions, not received
  intervals. A metadata-only retry receives 96 but accepts 0.
- Genuine changed values/intervals still use document revision ordering (same
  document) or document creation time ordering (different response documents).
  Metadata from unchanged responses remains available through Bronze/lineage
  as revision evidence, so a later stale correction cannot override a newer
  unchanged revision. Different-response ordering remains a latest-response
  policy, not proof of original provider publication chronology. Original
  historical versions are not deleted or retroactively rewritten by this fix.
- `GET /v1/energy?product=load_forecast` and Energy Explorer's **Energy data
  product** selector expose the separate product, attribution, permission note,
  forecast chart, freshness, coverage and quality evidence.
- `GET /v1/energy/comparisons?product=load_forecast&river_station_id=...&weather_station_id=...`
  matches actual overlapping dates without fabricating observations. Forecast
  values remain forecasts; environmental observations do not make them actual
  demand. Latest revisions can introduce hindsight: this is **not** an
  as-published-at-gate-closure forecast backtest.
- A75/A44 automated endpoints retain their original closed permission gate.
  A65 manual successes are visible in its product-specific API/Dashboard/run
  history, not misreported as a successful generation/price Airflow execution.

## Verified access and current limitation

The implemented connector uses ENTSO-E's official XML REST API. At the initial
Stage 5 implementation no API token was present. The operator subsequently
verified A65 authentication as described above; **A75/A44 live access and
ingestion are not claimed**. Offline protocol tests contain deliberately
invented values only in isolated test schemas.

Official documentation checked:

- [API integration sitemap](https://transparencyplatform.zendesk.com/hc/en-us/articles/15692855254548):
  links to the official [Postman reference](https://documenter.getpostman.com/view/7009892/2s93JtP3F6).
- [Production endpoint](https://transparencyplatform.zendesk.com/hc/en-us/articles/15696677194644):
  `https://web-api.tp.entsoe.eu/api`.
- [Security-token setup](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188):
  register, verify email, request “RESTful API access” at
  `transparency@entsoe.eu`, then generate a token in My Account after approval.
- [Rate limits](https://transparencyplatform.zendesk.com/hc/en-us/articles/12783148966036):
  400 requests/minute per token, shared across all your deployments; excess
  requests may temporarily ban the token. DanubePulse serializes energy
  ingestion and spaces requests by one second. A daily run makes two requests;
  Airflow retries use its existing bounded backoff. HTTP 429 is explicit failure,
  not an immediate retry loop. Account-wide usage outside this app is your responsibility.
- [Legal terms and open-data list](https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961).
  **Do not assume all Transparency Platform data is CC BY 4.0.** The linked
  18 October 2023 free-reuse list does not include generation 16.1.B/C or
  day-ahead prices 12.1.D. Verify current terms and obtain any necessary
  primary-owner permissions before ingesting or redistributing these products.
  For A75/A44 only, `ENTSOE_TERMS_ACCEPTED=true` is the operator's confirmation of this review
  and necessary permissions, not a license granted by this project.

### APG investigation

[APG transparency](https://markt.apg.at/en/transparency/) offers an API access
request form; its specification/access must be requested from APG, including
`marktinformation@apg.at`. No public undocumented endpoint has been reverse-engineered.

APG's official
[generation description](https://markt.apg.at/en/transparency/generation/generation-per-production-type/)
describes control-area net public-grid feed-in, partly measured and partly
estimated/scheduled, normally published with an hour's delay. Wind figures can
be revised after clearing. The area is not exactly every Austrian installation
(APG describes a Vorarlberg corridor exception). This is not output from an
individual hydropower plant.

APG's [price description](https://markt.apg.at/en/transparency/transmission/day-ahead-prices/)
distinguishes market-coupling, EXAA and a fallback reference price. They must not
be combined into a single interchangeable series. The present connector targets
ENTSO-E A44 **day-ahead** Austrian bidding-zone prices only.

Direct APG API/native CSV ingestion is deferred until APG supplies an approved
schema, authentication, reuse terms and request limits. Native File Library
CSV/ZIP layouts are also not guessed.

## Products and units

| Product | Verified request parameters | Scope |
|---|---|---|
| Actual generation per type | `documentType=A75`, `processType=A16`, `in_Domain=10YAT-APG------L`, `curveType=A01` | APG/Austrian area aggregate interval-average MW |
| Day-ahead prices | `documentType=A44`, matching Austrian `in_Domain` and `out_Domain` | EUR/MWh |

Requests are bounded to one completed UTC day. ENTSO-E starts in 2015, with
earlier legacy downloads separate; availability for any particular product/date
is not guaranteed. No-data acknowledgements fail explicitly.

- ENTSO-E `MAW` means MW, not MWh. Gold integrates power times interval hours
  into MWh. It does not add MW values as if they were energy.
- Price daily averages are duration-weighted, **not volume-weighted**.
  Negative prices are valid; negative generation and non-finite numbers are not.
- Supported resolutions are aligned 15, 30 and 60 minutes. Both A01 fixed
  blocks and documented A03 step blocks are handled. A03 repeats a value until
  the next position or period end; absent A01 points are gaps, not zero.
- B10, B11 and B12 distinguish pumped-storage output, run-of-river/poundage and
  reservoir hydro. Outgoing-domain pumped consumption is excluded.
- Null values remain null with `missing` status and do not enter trusted Gold.
  Missing production categories are **not** assumed zero or inferred from installed capacity.
- Every row has interval start/end, original unit, document/revision identity,
  publication, ingestion and processing times. Internally all times are UTC.
  Dashboard timestamps include Vienna's offset, so repeated autumn 02:00
  values remain distinct. UTC daily Gold has 24 hours; it is not a Vienna
  market-day total (those may cover 23 or 25 hours).
- Same-document revisions use the provider revision number. Different documents
  use publication time; ambiguous same-time conflicts are rejected. Old rows
  become `superseded`, not deleted. Original Bronze and lineage stay intact.
  Coarser/finer revisions must completely cover replaced intervals; partial
  replacement is rejected rather than losing data.
- Cancelled/withdrawn documents are rejected for investigation, not silently
  applied. Unsupported units, unexpected formats or overlaps fail the entire
  response before trusted records are changed.

## Safe startup

Keep the existing environment file, application data and Airflow secrets.
Before an upgrade, back up **both databases**. Do not delete Docker volumes.
For the Stage 5 upgrade the implementation created custom-format PostgreSQL
backups and successfully restored both into separate check databases **before**
running the migration. The backup paths and checksums are available in the
completion report/session artifacts; backups are not source-controlled.

For future upgrades, use your own private backup directory and verify by
restoring to disposable databases:

```bash
docker compose up -d db
docker compose exec -T db pg_dump -U danubepulse -d danubepulse -Fc > application.dump
docker compose exec -T db pg_dump -U danubepulse -d danubepulse_airflow -Fc > airflow.dump
# Restore each archive to a newly created, explicitly named check database
# with pg_restore --exit-on-error, and query its tables before migrating.
chmod 600 application.dump airflow.dump
```

Do not put these archives in the Git repository. Adapt role/database names if
your deployment uses custom names.

For A75/A44 only, after account approval and their separate permissions review,
the original configuration below would be needed. **Do not enable it for A65;
leave the general setting false in the current deployment.**

```dotenv
ENTSOE_API_TOKEN=<your-approved-token>
ENTSOE_TERMS_ACCEPTED=true
```

Never paste the token into a command URL, commit it, or share expanded Compose
configuration/logs containing secrets. The connector strips token-bearing URLs
from network errors and never stores request credentials in Bronze.

```bash
cd /home/yousif/projects/danubepulse
DATA_MODE=real docker compose up -d --build api dashboard
# Optional scheduler:
python3 scripts/setup_airflow.py
DATA_MODE=real docker compose --profile airflow up -d
```

The API applies additive migration `0006_energy`. The original application can
still run without Airflow. In demo mode real ingestion endpoints remain blocked.

Fetch one historical day after configuring access (use a date you have reviewed):

```bash
docker compose exec -T api danubepulse ingest-energy --date YYYY-MM-DD
```

The CLI prints the run ID, counts and independent verification results and
exits nonzero for incomplete/failed verification. Use `/v1/pipelines/energy/ingest`
then `/v1/pipelines/executions/{id}/verify` for the audited two-task workflow.
Audit attempts, failures and existing successful runs remain separate.

## Historical imports

Imports are an alternative for a legitimate ENTSO-E export obtained under your
approved access, not a way around provider restrictions. Put your original XML,
or carefully normalized CSV, in `data/energy-imports/`. This directory is
git-ignored and permanently mounted read-only into the API container; no manual
container copying is necessary.

```bash
docker compose exec -T api danubepulse ingest-energy --date YYYY-MM-DD \
  --generation-file /app/data/energy-imports/generation.xml \
  --price-file /app/data/energy-imports/prices.xml --accept-terms
```

Either file can be omitted. `--accept-terms` confirms authentic source origin
and applicable reuse permissions; the importer cannot independently authenticate
an operator-provided file. Bronze labels this explicitly and preserves its
contents and checksum.

### Normalized CSV contract

Use `--format csv`. This is a **DanubePulse normalization contract**, not a
claim that APG or ENTSO-E natively exports these headers:

```text
interval_start,interval_end,metric,production_type,value,unit,document_id,revision,published_at,region
```

Requirements:

- UTF-8; exact headers, explicit-offset ISO timestamps, one interval per row.
- `metric`: `generation` or `price`, matching the chosen file argument.
- `production_type`: the provider PSR code (e.g. `B11`), or `price`.
- `unit`: original `MAW` for generation; `EUR/MWh` for normalized price.
- `value`: original numeric value or empty for missing, never manufactured zeros.
- `document_id`, `revision`, `published_at`: original provider metadata.
- `region`: `10YAT-APG------L`.
- All intervals must fit the specified UTC day. Split files by UTC day without
  changing interval values. Preserve the original export outside the repository
  and document your normalization; missing revision/publication metadata is not
  fabricated to satisfy the importer.

A direct native-CSV adapter requires an actual verified export schema before it
can be added. No sample values in this guide are represented as real measurements.

## Dashboard, comparisons and verification

Open **Energy Explorer** at <http://localhost:8501>:

- production-type selections and MW time-series charts;
- separate hydro categories and price history;
- UTC daily MWh, duration-weighted prices and per-series coverage;
- original source retrieval/event times, stale-data and quality indicators;
- matched river-station/weather-grid UTC-date comparisons.

`GET /v1/energy` supports `start_date`, `end_date`, and bounded observation
`limit`. `GET /v1/energy/comparisons` requires `river_station_id` and
`weather_station_id`. Empty overlap is displayed honestly; no fabricated energy
values or synthetic observations enter these comparisons. Sparse river
snapshots are not continuous daily measurements. Weather is reanalysis.
Comparisons are descriptive and **do not establish causation**.

Gold is a SQL view over current valid real rows. Revision changes therefore
immediately change affected aggregates without a materialized refresh.
Verification separately checks Bronze checksum, quality/complete returned
series, original persisted Silver values, interval non-overlap and actual
duration-weighted Gold output. Retrying identical input adds an audit run but
not duplicate observations.

The `danubepulse_energy` DAG is **initially paused**, with a proposed 07:00 UTC
daily schedule and two-day lag. Do not unpause until account permissions,
provider revisions/availability and safe scheduling have been reviewed.
It reuses existing `ingest -> verify`, retry/backoff, timeout, catchup and
source-lock behavior. **Keep `danubepulse_river` paused.**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
docker compose --profile airflow config --quiet
# TEST_DATABASE_URL points to the existing local PostgreSQL server;
# tests migrate unique disposable schemas, never reset application tables.
docker compose --profile airflow exec -T airflow-scheduler \
  python /opt/airflow/tests/check_dags.py
```

Live authenticated energy acceptance testing remains necessary once credentials
are configured: fetch an approved date, check all verification gates, repeat it,
confirm unchanged trusted counts, inspect revisions/coverage and compare with
the provider's displayed totals. Offline parser tests are not proof of live access.
