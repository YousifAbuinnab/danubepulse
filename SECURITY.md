# Security and responsible reporting

DanubePulse is a local portfolio system, not an internet-hardened hosted service.
It currently has no production authentication layer, uses local development
database roles and a single-node plaintext Kafka network, and is not supported
as a high-availability deployment.

## Do not publish sensitive evidence

Never put credentials, `.env`, provider responses, database dumps, auth files,
raw dead-letter contents or unredacted logs in a public issue or pull request.
Share only sanitized reproduction steps, affected code paths and version
references. Rotate an exposed credential rather than merely deleting its line.

If GitHub private vulnerability reporting is enabled after publication, use it.
Otherwise arrange a private reporting channel with the maintainer; no such
channel or response-time SLA is established by this preparation stage.

## Local safeguards

- Keep all host-published service ports bound to loopback.
- Do not forward the API, Streamlit, database, broker or Airflow UI publicly.
- Store private credentials outside version control and build contexts.
- Back up and test-restore PostgreSQL before migrations; retain original records.
- Keep A75/A44 restricted and river/energy Airflow DAGs paused until explicit
  provider/permission review. Security testing must not trigger provider polling.
- Review dependency/image updates and upstream security advisories before reuse.

## Known release blockers

A historical database dump remains reachable in Git history and contains real
source records. Code licensing is pending. Neither is resolved by ignore rules
or a clean secret-scan report. No public release should be approved until the
[publication checklist](docs/publication.md) is satisfied.

The local Gitleaks result is not a warranty that no secret or vulnerability
exists. Provider data rights remain separate from application security and
any future code license.
