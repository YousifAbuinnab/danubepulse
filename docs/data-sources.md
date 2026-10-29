# Source attribution and data publication boundaries

Code publication is not permission to republish provider datasets. A connector
being accessible, a successful API response, and an API token are not licenses.
Review the current primary-source terms for the specific intended use before
ingestion, redistribution, screenshots, downloadable exports or cloud hosting.
This is an engineering checklist, not legal advice.

## Source matrix

| Source | What the project actually uses | Access / terms / attribution | Public repository policy |
|---|---|---|---|
| Hydrographie Österreich / eHYD | OGC `i000501:pegel_aktuell`, current Danube W/Q readings; not historical hourly downloads | Public collection; attribution **Datenquelle: ehyd.gv.at**. Collection metadata does not establish a redistribution license or automated-polling permission | Publish connector and fabricated tests only; exclude raw/derived real readings. River automation stays paused |
| Open-Meteo Historical Weather | Historical model/reanalysis estimates at gauge coordinates, not measured station weather | Data attribution **Weather data by Open-Meteo.com**, CC BY 4.0 with changes identified. Free API has separate non-commercial intended-use and request-limit terms | Keep real cached/derived data out of Git by default; review whether a portfolio/promotional deployment needs a paid plan |
| ENTSO-E A65/A01, Article 6.1.B | Austrian national day-ahead total-load forecast, historical completed UTC days | Authenticated API; existing documented open-data list includes 6.1.B under CC BY 4.0 subject to current terms/exceptions. Attribute ENTSO-E and original provider, retain notices, identify transformations | Connector/tests can be shared after code-license review; any new live import requires explicit one-run terms review. No raw export or database publication by default |
| ENTSO-E A75/A44 | Generation-by-type / day-ahead price parser and interval pipeline | No blanket entitlement or redistribution claim. The reviewed open-data list did not include these products; require applicable permissions | Remain gated. Do not set the general acceptance flag just to demonstrate the portfolio |
| Direct APG | Documentation investigation only | API specification/access/reuse approval must be requested | No invented integration, undocumented endpoint or claim of live ingestion |
| Synthetic demo fixtures | Fabricated measurements and incident examples | Fixture documentation labels fabricated demo data CC0; station names do not make values real | Small explicitly labeled fixtures are permitted in the project; do not combine real exports into them |
| Mock connector fixtures | Fabricated response-shaped payloads for tests | Not provider observations; never loaded through real ingestion | Keep test-only provenance prominent |
| Synthetic scale / Kafka tests | Artificial scale data and notification-only scenarios | Not external live data; not a new provider feed | Keep generated output out of Git; commit generators and tests only |

### Primary references

- [Official OGC collection metadata](https://gis.lfrz.gv.at/api/geodata/i000501/ogc/features/v1/collections/i000501:pegel_aktuell)
- [Open-Meteo archive API](https://open-meteo.com/en/docs/historical-weather-api),
  [terms](https://open-meteo.com/en/terms) and
  [license](https://open-meteo.com/en/licence)
- [ENTSO-E legal terms / open-data list](https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961-Legal-Terms-and-Conditions),
  [API access](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188)
  and [API rate limits](https://transparencyplatform.zendesk.com/hc/en-us/articles/12783148966036-API-Rate-Limit-Part-1)
- [APG transparency](https://markt.apg.at/en/transparency/)
- [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

Open-Meteo terms checked during Stage 8A state fewer than 10,000 calls/day,
5,000/hour and 600/minute for the free API and explicitly classify some
promotional uses as commercial. A public portfolio is not automatically
authorized free-tier use. Do not make live requests merely to test these terms.
The detailed [energy guide](energy.md) retains the earlier legal/access review;
reconfirm the current ENTSO-E list before any new import or redistribution.

## What must remain private

- API tokens, passwords, private environment files, auth-volume files and dumps.
- Real Bronze responses, operator-import XML/CSV, PostgreSQL exports, Delta
  tables/manifests and downstream dataset downloads unless redistribution has
  been explicitly reviewed and approved.
- Logs, screenshots or monitoring captures that expose credentials, query
  tokens, identifiable operator information or unapproved data.
- Kafka dead-letter raw payloads: they may contain untrusted/private input even
  when the normal event schema does not.

An `.gitignore` rule does not remove a file already tracked or reachable in
history. The historical database backup remains a publication blocker; see
[publication preparation](publication.md).

## Accuracy and interpretation

River levels are gauge-relative, not absolute elevation. Weather estimates are
reanalysis/model data. A65 is a forecast, not actual demand; document creation
time is not proof of original publication time. National energy totals are not
individual power-plant output. Null/missing intervals are not zero, and
correlation does not establish causation.

## Code and dependency licenses

The project owner has chosen to make the source code publicly viewable for portfolio evaluation under **All rights reserved**, without granting an open-source code license. Public visibility does not grant general permission to reuse, modify, redistribute or commercially exploit the original project code, except as permitted by applicable law or GitHub's Terms of Service. The synthetic demonstration fixtures retain their separate CC0-1.0 designation. Third-party software and external provider data remain subject to their own licenses and terms.
Third-party software retains its own licenses/notices. Apache Kafka, Airflow,
Spark and Delta Lake have Apache project licensing; Python libraries and images
carry additional component notices. Before distributing built images or release
bundles, generate/review a dependency inventory, license notices and vulnerability
report for the exact built artifacts. This stage does not claim a complete legal
or transitive-dependency audit.
