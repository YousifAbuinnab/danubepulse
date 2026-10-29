# Stage 8A publication and CI preparation

## Current release status: blocked

Stage 8A and Stage 8B have been committed locally on branch stage-8-portfolio as commit 65efe56, with milestone tag stage-8-demo-verified.

A separate publication review copy, danubepulse-public-review, was created from the latest committed snapshot without copying the original Git history. The original development repository and its milestone tags remain preserved.

No GitHub push, public repository, or cloud deployment has been performed. Publication remains blocked pending final security, data-rights, attribution, and repository review.

### Publication blockers

1. **Historical database backup.** `backups/danubepulse_stage5.dump` was added
   in commit `f17f2b7` and removed later (`b95b0f1`), but remains reachable in
   history/tags. Removing a file at HEAD or adding `.gitignore` does not remove
   its historical content.
2. Code licensing decision. The project owner has chosen to publish the source code for portfolio review with All rights reserved and no
   open-source code license. Public visibility does not grant general permission to reuse or commercially exploit the original code. Synthetic CC0 fixtures and third-party materials retain their separate terms.
3. **Data use/publication rights require review.** In particular, eHYD
   redistribution and polling, Open-Meteo's promotional/non-commercial API
   boundary and ENTSO-E product-specific reuse require separate decisions.

The owner explicitly chose to document the history blocker while preserving
all history/tags. **Do not force-push or rewrite tags as part of this stage.**

## Audit evidence and limitations

The read-only security review examined all locally reachable commits/blobs and
unreachable blobs for credential patterns, tracked private files and generated
data. It did not confirm a live credential. This is not a guarantee of absence.

An additional pinned **Gitleaks v8.24.3** scan over all locally reachable refs
reported **zero findings**, with values redacted and no repository upload.
The custom-format historical backup was then restored into a unique disposable
database for private assessment, and its decoded SQL was scanned locally:
zero Gitleaks findings. No source rows or credentials were printed.

The historical dump **does contain actual source data**: 9 real river and 504
real weather rows, alongside synthetic data; it contained no energy rows.
That makes it inappropriate to publish by default even without a detected token.
Private audit reports are retained outside the repository. No production table
was altered by the restore assessment.

Boundaries:

- Scanners have false negatives; binary archives require decoding/inspection.
- Only locally available refs can be scanned. Remote forks, caches, PR refs,
  releases, old clone copies or secrets in external systems are not cleared.
- Ignored runtime configuration, real datasets, auth volumes and live secrets
  must remain private. This report does not reproduce their values.
- A source license review is not a legal opinion; a secret scan is not a full
  exploitable-vulnerability, image/CVE or transitive-dependency audit.
- API/dashboard have no production authentication layer. They are loopback-only
  now, not safe to expose directly to the internet.

## Implemented sharing safeguards

- `.gitignore` covers private env variants, keys, database files/sidecars,
  backups, provider imports, generated Delta/Parquet, logs and Terraform state.
- `.dockerignore` excludes private env variants, imports, backups and generated
  data from **all** image build contexts, even though COPY uses explicit paths.
- Example env contains only placeholders, no private token, and a closed
  ENTSO-E general gate. Never copy a real `.env` into docs or issues.
- API/dashboard/PostgreSQL/Airflow UI host ports bind to loopback; Kafka has no
  published host port. Reverse proxies/access control remain future work.
- `scripts/check_publication.py` fails on prohibited **paths**, with no file
  contents printed. It checks the tracked tree by default and all locally
  reachable objects with `--history`. Shallow history is rejected for the latter.
- CI is read-only, pins its GitHub Actions revisions, does not persist checkout
  credentials, has job timeouts/cancellation, and does not upload data/log dumps.

Ignore rules are guardrails, not encryption or history cleanup. They do not
stop `git add -f`, unrecognized filename formats or sensitive content in otherwise
allowed source files. Keep both path checks and content scans.

## CI jobs and network boundary

| Job | Validation | Provider access |
|---|---|---|
| Publication safety | Full-history redacted Gitleaks and private/generated path guard | None; **expected to fail on the known historical backup** |
| Python quality | Python 3.12, Ruff, full SQLite and PostgreSQL 16 migrated-schema suites; API/Streamlit tests | Tests reject non-loopback socket connections |
| Airflow | Pinned runtime DAG contracts | No scheduler, container runs with `--network none` |
| Spark / Delta | Pinned Docker build, synthetic MERGE/schema/history/time-travel/scale contracts | Runtime container uses `--network none` |
| Kafka | Disposable CI stack, outbox outage, ACK gap, offsets, replay, DLQ and synthetic pipeline integration | No private tokens; no Airflow; synthetic/local operations only |

Package/image/action downloads require internet. This is separate from making
live provider ingestion requests. CI explicitly clears the ENTSO-E token, keeps
the general gate false and uses demo mode.

The PostgreSQL service password in the workflow is a deliberate, isolated
CI-only value, not a production credential. Never substitute the live URL.
`TEST_DATABASE_URL` uses disposable schemas; no application database reset occurs.
Hosted CI has **not** run as part of Stage 8A: a push is explicitly prohibited.

## Local verification results

These are executed local checks, not a hosted GitHub Actions run:

| Check | Result |
|---|---|
| Application, API and Streamlit suite, SQLite | **114 passed**, 60 existing warnings |
| Same suite, PostgreSQL 16 with migrated disposable schemas | **114 passed**, 60 existing warnings |
| Ruff lint / format | Passed |
| Pinned actionlint against the complete workflow | Passed |
| Airflow DAG contracts, network disabled | **4 passed** |
| Spark / Delta contracts, network disabled | **3 passed** |
| All optional Compose profiles / diff whitespace | Passed |
| Actual Docker COPY build context | 82 files; source and demo fixtures present; prohibited private/generated paths absent |
| Local Gitleaks: reachable Git history / decoded historical dump / working source including new files | **0 findings** in each scan |
| Current tracked-tree path gate | Passed |
| Full-history path gate | **Blocked as intended** by the historical backup |
| New documentation's relative file links | Passed |
| Running API/dashboard health and energy/lakehouse/streaming reads | HTTP 200 after startup readiness |

Kafka integration ran on a separate Compose project with fresh named volumes,
no published ports, an internal-only Docker network and blank provider tokens.
No competing worker daemons were started for its phase assertions:

- Broker outage: **4 durable pending events**, none published.
- Recovery: **3 unique receipts / 4 deliveries**, one duplicate and one durable
  dead letter; its DLQ notification was published.
- Same-group restart: no extra receipts or deliveries.
- New-group replay: still **3 unique receipts**, now **8 audited deliveries**.
- Existing synthetic pipeline/recovery integration: **15 expected notifications
  verified**, covering all five event types.
- Notification failure/replay scenarios made **zero measurement writes**.
  The separate pipeline test used only synthetic data in a disposable schema.

The isolated services were stopped afterward; all named volumes were retained.
The existing application was checked separately: content fingerprints of all
**17 original application tables** and **71 Airflow tables** match the pre-test
baseline. Worker heartbeat rows are deliberately outside this comparison.
All existing Git tags are unchanged. River/energy DAGs remain paused and
`ENTSOE_TERMS_ACCEPTED=false`; no live provider ingestion was triggered.
Loopback-only API/dashboard bindings were applied and verified on running
containers, not just checked in YAML.

### Local commands

```bash
.venv/bin/pytest -o addopts='' -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
python3 scripts/check_publication.py
docker compose --profile airflow --profile lakehouse --profile kafka config --quiet

# Expected nonzero until the historical backup is removed from publication refs.
python3 scripts/check_publication.py --history

# Local content scan, redacted; full fetched history only.
docker run --rm -v "$PWD:/repo:ro" ghcr.io/gitleaks/gitleaks:v8.24.3 \
  git /repo --log-opts=--all --redact=100 --no-banner --log-level=error

# Offline optional contracts; image downloads/build require network first.
docker run --rm --network none --entrypoint python \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
  -v "$PWD/airflow/dags:/opt/airflow/dags:ro" \
  -v "$PWD/airflow/tests:/opt/airflow/tests:ro" \
  apache/airflow:3.3.2-python3.12 /opt/airflow/tests/check_dags.py
docker run --rm --network none --memory=3g --cpus=2 --entrypoint python \
  danubepulse-lakehouse -m pytest lakehouse/tests -q
```

For PostgreSQL, privately set `TEST_DATABASE_URL` to the local test target.
Never paste its real password into a report. See the [streaming guide](streaming.md)
for exact Kafka outage/replay checks; do not run its phase assertions concurrently
with publisher/consumer daemons against the same events.

## How to resolve history safely (future explicit approval required)

Do **not** run a mirror push of the current repository. A mirror includes tags
and refs retaining the backup. Renaming or deleting just the latest file is
insufficient.

After review, choose one separately authorized approach:

1. Make a **separate sanitized publication repository/history** from an audited
   code snapshot, preserving this private repository and all its tags as-is.
   Review every file, choose the code license, scan the new repository and only
   then decide whether to create/push a public remote.
2. Rewrite a **separate clone** with a history-filtering tool, removing forbidden
   data from every intended release ref/tag. Review changed commit identities,
   decide which sanitized milestones may be recreated, rescan, and obtain
   explicit approval before any remote update.

The first approach is now in progress: a separate, clean publication review copy has been created without the original Git history. The original repository and all milestone tags remain unchanged.

The publication copy has not yet been initialized as a new Git repository or uploaded to GitHub. Final security, data-rights, authorship, and publication checks are still required. The history-rewriting approach has not been used.

If exposure has already occurred elsewhere, removing history is not sufficient:
assess downloaded copies, rotate any exposed credentials, and follow the hosting
provider's sensitive-data-removal process as appropriate.

## Publication checklist

- [ ] Resolve historical backup in separately approved publication refs.
- [x] Confirm code licensing choice: All rights reserved, no open-source license; keep provider terms separate.
- [ ] Confirm intended-use rights and attribution for every public data asset.
- [ ] Publish only fabricated demo inputs unless real data is specifically approved.
- [ ] Run path/history and redacted content scans on the **exact** final refs.
- [ ] Review commit metadata for personal identities before public sharing.
- [ ] Build/test clean checkout; review dependency/image vulnerabilities and notices.
- [ ] Approve a remote and run hosted credential-free CI; no deployment secrets yet.
- [ ] Require passing status checks before merge; keep deployment manually gated.
- [ ] Review public screenshots/demo for secrets and unapproved source content.
- [ ] Separately approve first commit/push/tag/publication; none are implied here.

For future cloud operation, see the [design and cost plan](cloud-deployment.md).
