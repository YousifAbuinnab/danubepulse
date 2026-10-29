"""Streamlit dashboard for local DanubePulse exploration."""

import json
import os
import socket
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import altair as alt
import pandas as pd
import streamlit as st

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000").rstrip("/")
DEMO_CONTROL_TOKEN = os.environ.get("DEMO_CONTROL_TOKEN", "")
AIRFLOW_HEALTH_URL = os.environ.get(
    "AIRFLOW_HEALTH_URL", "http://airflow-api:8080/api/v2/monitor/health"
)
AIRFLOW_UI_URL = os.environ.get("AIRFLOW_UI_URL", "http://localhost:8080")
KAFKA_HEALTHCHECK_ADDRESS = os.environ.get("KAFKA_HEALTHCHECK_ADDRESS", "kafka:9092")

st.set_page_config(page_title="DanubePulse", page_icon="🌊", layout="wide")
st.title("DanubePulse")
st.caption(
    "Austrian river, weather & energy data intelligence — provenance is shown on every dataset."
)


def api(path: str, method: str = "GET") -> object:
    headers = (
        {"X-Demo-Control-Token": DEMO_CONTROL_TOKEN}
        if method == "POST" and DEMO_CONTROL_TOKEN
        else {}
    )
    request = Request(f"{API_BASE_URL}{path}", headers=headers, method=method)
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def airflow_readiness() -> tuple[str, str]:
    parsed = urlsplit(AIRFLOW_HEALTH_URL)
    if parsed.scheme != "http" or parsed.hostname not in {
        "airflow-api",
        "localhost",
        "127.0.0.1",
    }:
        return "blocked", "Health probes are limited to local Airflow hosts."
    try:
        with urlopen(Request(AIRFLOW_HEALTH_URL, method="GET"), timeout=2) as response:
            health = json.loads(response.read())
        scheduler = health.get("scheduler", {}).get("status", "unknown")
        return "available", f"Airflow API responded; scheduler status: {scheduler}."
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError) as error:
        return "not running", f"Optional Airflow API is unavailable ({type(error).__name__})."


def kafka_readiness() -> tuple[str, str]:
    host, separator, raw_port = KAFKA_HEALTHCHECK_ADDRESS.rpartition(":")
    if not separator or host not in {"kafka", "localhost", "127.0.0.1"}:
        return "blocked", "Broker probes are limited to local Kafka hosts."
    try:
        port = int(raw_port)
        with socket.create_connection((host, port), timeout=1):
            return "available", "Kafka broker TCP listener responded."
    except (OSError, ValueError):
        return "not running", "Optional Kafka broker listener is unavailable."


def historical_chart(
    data: pd.DataFrame,
    value_field: str,
    value_label: str,
    *,
    mark: str = "line",
) -> alt.Chart:
    chart = alt.Chart(data).encode(
        x=alt.X(
            "event_time_vienna:N",
            sort=None,
            title="Vienna local time (UTC offset shown)",
            axis=alt.Axis(labelAngle=-45),
        ),
        y=alt.Y(f"{value_field}:Q", title=value_label),
        tooltip=[
            alt.Tooltip("event_time_vienna:N", title="Vienna local time"),
            alt.Tooltip(f"{value_field}:Q", title=value_label, format=".2f"),
        ],
    )
    return (chart.mark_bar() if mark == "bar" else chart.mark_line(point=True)).properties(
        height=220
    )


try:
    summary = api("/v1/summary")
except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
    st.error(f"API is unavailable: {error}")
    try:
        health = api("/health")
        api_state = "available"
        database_state = "available" if health.get("status") == "ok" else "unavailable"
        health_detail = "API responded; database SELECT 1 completed."
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as health_error:
        api_state = "unavailable"
        database_state = "unknown"
        health_detail = f"Database readiness could not be verified ({type(health_error).__name__})."
    airflow_state, airflow_detail = airflow_readiness()
    kafka_state, kafka_detail = kafka_readiness()
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Component": "Streamlit dashboard",
                    "State": "available",
                    "Evidence": "This page is active.",
                },
                {"Component": "FastAPI", "State": api_state, "Evidence": health_detail},
                {"Component": "PostgreSQL", "State": database_state, "Evidence": health_detail},
                {"Component": "Airflow API", "State": airflow_state, "Evidence": airflow_detail},
                {"Component": "Kafka broker", "State": kafka_state, "Evidence": kafka_detail},
                {
                    "Component": "Spark / Delta",
                    "State": "unknown",
                    "Evidence": "A verified manifest requires the DanubePulse API.",
                },
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.info(
        "Restore the local API/database connection, then reload this page "
        "for data and Delta readiness."
    )
    st.stop()

provenance = summary["provenance"]
st.warning(
    f"Dataset provenance: **{provenance.upper()}**. Synthetic records are illustrative only."
)
if summary["data_mode"] == "demo":
    if len(DEMO_CONTROL_TOKEN) < 32:
        st.info("Synthetic actions are locked until a private demo control token is configured.")
    if st.button(
        "Load synthetic demo",
        type="primary",
        disabled=len(DEMO_CONTROL_TOKEN) < 32,
    ):
        try:
            result = api("/v1/demo/ingest", method="POST")
            st.success(f"Demo ingestion: {result['status']}")
            st.rerun()
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            st.error(f"Demo ingestion failed: {error}")
else:
    st.info(
        "Real-data mode is active. Synthetic ingestion, reset, and incident recovery are disabled."
    )

col_river, col_weather, col_energy, col_trust = st.columns(4)
col_river.metric("River observations", summary["river_observations"])
col_weather.metric("Weather observations", summary["weather_observations"])
col_energy.metric("Energy observations", summary["energy_observations"])
col_trust.metric("Analytics trust", summary["analytics_trust"].upper())
st.caption(summary["correlation_note"])

(
    tab_observations,
    tab_analytics,
    tab_incidents,
    tab_quality,
    tab_demo_control,
    tab_investigation,
    tab_real_data,
    tab_pipelines,
    tab_energy,
    tab_lakehouse,
    tab_streaming,
) = st.tabs(
    [
        "Observations",
        "Daily analytics",
        "Incidents & replay",
        "Quality & runs",
        "Demo Control Center",
        "Incident Investigation",
        "Real Austrian Data Explorer",
        "Automated Pipelines",
        "Energy Explorer",
        "Lakehouse Explorer",
        "Streaming Monitor",
    ]
)
with tab_demo_control:
    st.subheader("Interview Demo Control Center")
    st.caption(
        "A guided, repeatable demonstration. Real-data panels are read-only; "
        "interactive actions create synthetic notifications or demo incidents only."
    )
    if summary["data_mode"] != "demo":
        st.info("Demo controls are disabled because application DATA_MODE is not demo.")
    elif len(DEMO_CONTROL_TOKEN) < 32:
        st.warning(
            "Demo actions are locked. Run `python3 scripts/setup_demo_control.py`, then "
            "recreate the API and dashboard containers. The secret value is never displayed."
        )
    else:
        st.success("Demo action token is configured; no reset or real-ingestion action is offered.")

    api_health = api("/health")
    airflow_state, airflow_detail = airflow_readiness()
    kafka_state, kafka_detail = kafka_readiness()
    lakehouse_status = api("/v1/lakehouse")
    demo_lakehouse = lakehouse_status["scopes"]["demo"]
    readiness = pd.DataFrame(
        [
            {
                "Component": "Streamlit dashboard",
                "State": "available",
                "Evidence": "This page is active.",
            },
            {
                "Component": "FastAPI",
                "State": "available" if api_health.get("status") == "ok" else "unavailable",
                "Evidence": "Health endpoint responded."
                if api_health.get("status") == "ok"
                else "Health endpoint did not pass.",
            },
            {
                "Component": "PostgreSQL",
                "State": "available" if api_health.get("status") == "ok" else "unavailable",
                "Evidence": "Health endpoint's database SELECT 1 passed."
                if api_health.get("status") == "ok"
                else "Database readiness check did not pass.",
            },
            {"Component": "Airflow API", "State": airflow_state, "Evidence": airflow_detail},
            {"Component": "Kafka broker", "State": kafka_state, "Evidence": kafka_detail},
            {
                "Component": "Spark / Delta demo namespace",
                "State": demo_lakehouse["status"],
                "Evidence": (
                    "Last verified synthetic Bronze/Silver/Gold result is available."
                    if demo_lakehouse["last_success"]
                    else "Not processed yet; run the isolated demo command below."
                ),
            },
        ]
    )
    st.dataframe(readiness, use_container_width=True, hide_index=True)
    if st.button("Refresh readiness and demo evidence"):
        st.rerun()
    st.caption(
        "Kafka TCP availability is only a listener probe; use worker heartbeats and event "
        "receipts below to verify actual processing. Spark/Delta availability is proven by "
        "a successful persisted manifest, not by an API health check."
    )

    with st.expander("1 · Airflow: manual fixture workflow and task logs", expanded=True):
        st.write(
            "This DAG is schedule=None and paused on creation. It validates four explicitly "
            "synthetic fixture records, groups two metrics, then independently verifies the "
            "aggregates. The one-shot command runs no scheduler, contacts no provider and "
            "does not connect to either database."
        )
        st.code(
            "docker run --rm --network none --memory=1g --cpus=1 \\\n"
            "  -e AIRFLOW__CORE__LOAD_EXAMPLES=false \\\n"
            "  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db \\\n"
            "  -e DANUBEPULSE_DEMO_FIXTURE=/opt/airflow/fixtures/interview_demo.json \\\n"
            '  -v "$PWD/airflow/dags:/opt/airflow/dags:ro" \\\n'
            '  -v "$PWD/airflow/fixtures:/opt/airflow/fixtures:ro" \\\n'
            '  -v "$PWD/airflow/tests:/opt/airflow/tests:ro" \\\n'
            "  --entrypoint bash apache/airflow:3.3.2-python3.12 -ec \\\n"
            "  'airflow db migrate && python /opt/airflow/tests/check_dags.py && \\\n"
            "   airflow dags test danubepulse_interview_demo 2026-10-27'",
            language="bash",
        )
        st.caption(
            "Task-level results and logs print in the terminal. For the optional Airflow UI, "
            f"open [{AIRFLOW_UI_URL}]({AIRFLOW_UI_URL}) only when it is already running; "
            "do not start the scheduler merely for this fixture demo."
        )

    with st.expander("2 · Kafka: publish and deduplicate synthetic notifications", expanded=True):
        st.write(
            "Start the optional Kafka publisher and consumer from the documented command. "
            "The durable outbox makes publication safe during a broker outage; repeated "
            "delivery uses the same event ID and increments the inbox delivery counter."
        )
        st.code(
            "docker compose --profile kafka up -d --wait kafka\n"
            "docker compose --profile kafka run --rm kafka-init\n"
            "docker compose --profile kafka up -d streaming-publisher streaming-consumer",
            language="bash",
        )
        kafka_confirmed = st.checkbox(
            "I understand this publishes notification-only synthetic test events; "
            "it does not modify measurements.",
            key="interview-kafka-confirm",
        )
        event_ids = st.session_state.get("interview_demo_event_ids", [])
        if st.button(
            "Queue deterministic demo notifications",
            disabled=(
                not kafka_confirmed or not DEMO_CONTROL_TOKEN or summary["data_mode"] != "demo"
            ),
        ):
            try:
                result = api("/v1/demo/streaming", method="POST")
                event_ids = result["event_ids"]
                st.session_state["interview_demo_event_ids"] = event_ids
                st.success(
                    f"{len(event_ids)} stable synthetic event IDs queued or already present. "
                    "Refresh this page after workers process them."
                )
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                st.error(f"Could not queue synthetic notifications: {error}")
        if event_ids:
            st.code("\n".join(event_ids), language="text")
            if st.button(
                "Replay first event with the same event ID",
                key="interview-kafka-replay",
                disabled=(
                    not kafka_confirmed or not DEMO_CONTROL_TOKEN or summary["data_mode"] != "demo"
                ),
            ):
                try:
                    replay_result = api(f"/v1/demo/streaming/{event_ids[0]}/replay", method="POST")
                    st.success(
                        "A second outbox delivery is queued with the original event ID. "
                        f"Replay outbox: {replay_result['replay_outbox_id']}"
                    )
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                    st.error(f"Could not queue duplicate delivery: {error}")
        try:
            stream_status = api("/v1/streaming")
            interview_events = [
                row for row in stream_status["events"] if row["entity_id"] == "interview-demo"
            ]
            receipts = [
                row for row in stream_status["receipts"] if row["entity_id"] == "interview-demo"
            ]
            st.write("**Interview notification outbox**")
            st.dataframe(pd.DataFrame(interview_events), use_container_width=True, hide_index=True)
            st.write("**Delivered receipts and duplicate counts**")
            st.dataframe(pd.DataFrame(receipts), use_container_width=True, hide_index=True)
            st.write("**Worker health**")
            st.dataframe(
                pd.DataFrame(stream_status["workers"]), use_container_width=True, hide_index=True
            )
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            st.error(f"Streaming evidence is unavailable: {error}")

    with st.expander(
        "3 · Data quality: investigate and recover a synthetic incident", expanded=True
    ):
        st.warning(
            "These controls require the configured demo token and DATA_MODE=demo. "
            "They do not expose reset or delete actions. Existing real observations are untouched."
        )
        scenario_labels = {
            "Schema drift": "schema_drift",
            "Duplicate records": "duplicate_records",
            "Missing and late measurements": "missing_late",
        }
        selected_label = st.selectbox(
            "Synthetic incident scenario",
            list(scenario_labels),
            key="interview-incident-scenario",
        )
        action_confirmed = st.checkbox(
            "I understand this adds a synthetic incident and audit events to the demo database.",
            key="interview-incident-confirm",
        )
        action_disabled = (
            not action_confirmed or not DEMO_CONTROL_TOKEN or summary["data_mode"] != "demo"
        )
        if st.button("Simulate selected incident", disabled=action_disabled):
            try:
                api(
                    f"/v1/demo/scenarios/{scenario_labels[selected_label]}/simulate",
                    method="POST",
                )
                st.rerun()
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                st.error(f"Incident simulation failed: {error}")
        try:
            demo_incidents = [
                row
                for row in api("/v1/incidents")
                if row["scenario"] == scenario_labels[selected_label]
            ]
            if demo_incidents:
                options = {
                    f"{row['title']} · {row['status']} · {row['id'][:8]}": row
                    for row in demo_incidents
                }
                chosen = st.selectbox(
                    "Incident evidence",
                    list(options),
                    key="interview-incident-selection",
                )
                incident = options[chosen]
                st.status(
                    f"{incident['status'].replace('_', ' ').title()} · "
                    f"{incident['severity'].upper()}",
                    state=("complete" if incident["status"] == "recovered" else "running"),
                )
                st.write("**Affected records**")
                st.dataframe(
                    pd.DataFrame(incident["affected_records"]),
                    use_container_width=True,
                    hide_index=True,
                )
                st.write("**Original failed checks**")
                st.dataframe(
                    pd.DataFrame(
                        row for row in incident["quality_results"] if row["phase"] == "detection"
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
                if incident["before_metrics"] is not None:
                    before, after = st.columns(2)
                    before.metric(
                        "Gold observations before",
                        incident["before_metrics"]["daily"][0]["observation_count"],
                    )
                    if incident["after_metrics"] is not None:
                        after.metric(
                            "Gold observations after",
                            incident["after_metrics"]["daily"][0]["observation_count"],
                        )
                if incident["status"] in {"detected", "open"} and st.button(
                    "Investigate incident",
                    key="interview-investigate-" + incident["id"],
                    disabled=action_disabled,
                ):
                    try:
                        api(f"/v1/incidents/{incident['id']}/investigate", method="POST")
                        st.rerun()
                    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                        st.error(f"Incident investigation failed: {error}")
                if incident["status"] in {
                    "detected",
                    "investigating",
                    "failed_recovery",
                    "unverified",
                } and st.button(
                    "Recover and verify",
                    key="interview-recover-" + incident["id"],
                    disabled=action_disabled,
                ):
                    try:
                        api(f"/v1/incidents/{incident['id']}/replay", method="POST")
                        st.rerun()
                    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                        st.error(f"Incident recovery failed: {error}")
                st.write("**Recovery checks and audit trail**")
                st.dataframe(
                    pd.DataFrame(incident["quality_results"]),
                    use_container_width=True,
                    hide_index=True,
                )
                st.dataframe(
                    pd.DataFrame(incident["audit_trail"]),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("Simulate the selected scenario to begin.")
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            st.error(f"Incident evidence could not be loaded: {error}")

    with st.expander("4 · PySpark / Delta: isolated demo layers and time travel", expanded=True):
        st.write(
            "The `demo` Delta namespace is explicitly synthetic and separate from `real`. "
            "Snapshot extraction is read-only. Running the benchmark adds a separate "
            "synthetic-scale Delta test and verifies version-based time travel."
        )
        st.code(
            "docker compose --profile lakehouse run --rm lakehouse run --scope demo\n"
            "docker compose --profile lakehouse run --rm lakehouse benchmark --sizes 1000",
            language="bash",
        )
        if demo_lakehouse["last_success"]:
            report = demo_lakehouse["last_success"]
            st.success(
                f"Verified synthetic namespace · {report['silver_active_rows']} Silver "
                f"measurements · run {report['id']}"
            )
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "layer": layer,
                            "version": values["version"],
                            "rows": values["rows"],
                            "files": values["files"],
                        }
                        for layer, values in report["tables"].items()
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.dataframe(
                pd.DataFrame(report["gold_preview"]), use_container_width=True, hide_index=True
            )
        scale_result = lakehouse_status["scopes"]["synthetic-scale"]["last_success"]
        if scale_result:
            st.write("**Synthetic Delta time-travel evidence**")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "rows": row.get("unique_rows"),
                            "input_rows": row.get("input_rows"),
                            "time_travel_original_value": row.get("time_travel_original_value"),
                            "current_value": row.get("current_value"),
                            "schema_rejected": row.get("schema_rejected_without_commit"),
                        }
                        for row in scale_result["results"]
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("5 · Previously ingested Austrian data (read-only)", expanded=True):
        st.caption(
            "Every request here reads PostgreSQL only. No eHYD, Open-Meteo or ENTSO-E request "
            "is made, and no refresh/ingestion control is provided."
        )
        try:
            river_rows = api("/v1/real/observations?domain=river&limit=1000")
            weather_rows = api("/v1/real/observations?domain=weather&limit=1000")
            forecast = api("/v1/energy?product=load_forecast&limit=1000")
            real_columns = st.columns(3)
            real_columns[0].metric("Stored real river readings", len(river_rows))
            real_columns[1].metric("Stored real weather readings", len(weather_rows))
            real_columns[2].metric("Stored ENTSO-E A65 intervals", len(forecast["observations"]))
            st.caption(
                "River attribution: Datenquelle: ehyd.gv.at · Weather attribution: "
                "Weather data by Open-Meteo.com · Energy attribution: " + forecast["attribution"]
            )
            st.caption(
                "A65 is an Austrian national day-ahead load forecast, not measured demand "
                "or individual-plant generation."
            )
            if forecast["observations"]:
                st.dataframe(
                    pd.DataFrame(forecast["observations"][:5]),
                    use_container_width=True,
                    hide_index=True,
                )
            if river_rows:
                st.write("Most recently stored river readings")
                st.dataframe(
                    pd.DataFrame(river_rows[:5]), use_container_width=True, hide_index=True
                )
            if weather_rows:
                st.write("Most recently stored weather estimates (model/reanalysis)")
                st.dataframe(
                    pd.DataFrame(weather_rows[:5]), use_container_width=True, hide_index=True
                )
            if not river_rows and not weather_rows and not forecast["observations"]:
                st.info("No previously ingested real observations are stored in this database.")
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, KeyError) as error:
            st.error(f"Read-only real-data presentation is unavailable: {error}")

with tab_streaming:
    st.subheader("Optional Kafka notification streaming")
    try:
        streaming = api("/v1/streaming")
        st.caption(streaming["note"])
        st.info(streaming["delivery_guarantee"])
        cols = st.columns(3)
        cols[0].metric("Pending publications", streaming["pending"])
        cols[1].metric("Retrying publications", streaming["retrying"])
        cols[2].metric("Unique processed events", streaming["processed"])
        if streaming["oldest_pending_age_seconds"] is not None:
            st.caption(
                f"Oldest pending notification: {streaming['oldest_pending_age_seconds']} seconds"
            )
        st.write("**Worker health**")
        if streaming["workers"]:
            st.dataframe(pd.DataFrame(streaming["workers"]), hide_index=True)
        else:
            st.info("Workers have not started. The application works without Kafka.")
        st.write("**Recent events (last 100)**")
        st.dataframe(pd.DataFrame(streaming["events"]), hide_index=True)
        st.write("**Processed notification history (last 100; includes received-only events)**")
        st.dataframe(pd.DataFrame(streaming["receipts"]), hide_index=True)
        st.write("**Dead letters (last 50; raw payloads deliberately omitted)**")
        st.dataframe(pd.DataFrame(streaming["dead_letters"]), hide_index=True)
        st.warning(
            "Synthetic-streaming-test events simulate notifications only. Replay reprocesses "
            "notification history; it does not fetch or modify river, weather or energy readings."
        )
        st.code(
            "docker compose --profile kafka up -d kafka kafka-init "
            "streaming-publisher streaming-consumer\n"
            "docker compose --profile kafka run --rm --no-deps streaming-publisher "
            "python -m danubepulse.streaming demo --scenario outage-recovery --key manual-01\n"
            "# Replay with a NEW consumer group; original event IDs are retained:\n"
            "docker compose --profile kafka run --rm --no-deps streaming-consumer "
            "python -m danubepulse.streaming consume --group replay-manual-01 --seconds 20",
            language="bash",
        )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Streaming status could not be loaded: {error}")

with tab_lakehouse:
    st.subheader("Optional local Delta lakehouse")
    st.caption(
        "Read-only trusted PostgreSQL exports → incremental Delta Bronze/Silver/Gold. "
        "Local Spark uses two worker threads, not a multi-node cluster. "
        "These small datasets do not require Spark."
    )
    try:
        lakehouse = api("/v1/lakehouse")
        scope = st.selectbox("Lakehouse namespace", ["real", "demo", "synthetic-scale"])
        state = lakehouse["scopes"][scope]
        st.info(f"Namespace: {scope} · latest processing status: {state['status']}")
        st.caption(lakehouse["note"])
        latest = state["latest_attempt"]
        if latest and latest["status"] == "failed":
            st.error(f"Latest lakehouse job failed: {latest['error']}")
        report = state["last_success"]
        if report is None:
            st.info("No verified processing result yet. Run the optional manual CLI below.")
        else:
            st.warning(f"Dataset provenance: {report['provenance'].upper()}")
            st.write(f"Last verified job: {report['id']} · {report['finished_at']}")
            if scope == "synthetic-scale":
                st.caption(report["note"])
                st.dataframe(
                    pd.DataFrame(
                        [
                            {key: value for key, value in item.items() if key != "history"}
                            for item in report["results"]
                        ]
                    ),
                    hide_index=True,
                )
                with st.expander("Synthetic Delta transaction / time-travel evidence"):
                    st.json(report["results"])
            else:
                st.metric("Active long-format measurements", report["silver_active_rows"])
                st.caption(
                    "One observation may contain several measurements. Gold days are UTC. "
                    "Load forecast is expected national demand, not actual demand or plant output."
                )
                for layer, detail in report["tables"].items():
                    st.write(
                        f"**{layer.title()}** · version {detail['version']} · "
                        f"{detail['rows']} rows · "
                        f"{detail['files']} files · {detail['size_bytes']} bytes"
                    )
                    with st.expander(f"{layer.title()} version history at verification"):
                        st.json(detail["history"])
                st.dataframe(pd.DataFrame(report["gold_preview"]), hide_index=True)
                st.caption("Gold preview is limited to 100 groups; CLI reads all groups.")
                with st.expander("Source attribution and licensing"):
                    st.dataframe(pd.DataFrame(report["sources"]), hide_index=True)
        st.code(
            "docker compose --profile lakehouse build lakehouse\n"
            "docker compose --profile lakehouse run --rm lakehouse run --scope real\n"
            "docker compose --profile lakehouse run --rm lakehouse benchmark --sizes 10000 100000",
            language="bash",
        )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Lakehouse status could not be loaded: {error}")

with tab_observations:
    domain = st.selectbox("Dataset", ["river", "weather"])
    try:
        rows = api(f"/v1/observations?domain={domain}")
        if rows:
            frame = pd.DataFrame(rows)
            st.dataframe(frame, use_container_width=True, hide_index=True)
            frame["event_time_vienna"] = (
                pd.to_datetime(frame["event_time_utc"], utc=True)
                .dt.tz_convert("Europe/Vienna")
                .dt.strftime("%Y-%m-%d %H:%M %z")
            )
            if domain == "river":
                chart_data = frame.dropna(subset=["water_level_m"]).sort_values("event_time_utc")
                if not chart_data.empty:
                    st.altair_chart(
                        historical_chart(chart_data, "water_level_m", "Water level (m)"),
                        use_container_width=True,
                    )
                else:
                    st.info("No water-level measurements are available for a trend chart.")
            else:
                chart_data = frame.dropna(subset=["temperature_c"]).sort_values("event_time_utc")
                if not chart_data.empty:
                    st.altair_chart(
                        historical_chart(chart_data, "temperature_c", "Temperature (°C)"),
                        use_container_width=True,
                    )
                else:
                    st.info("No temperature measurements are available for a trend chart.")
        else:
            st.info("No observations yet. Load the synthetic demo to populate this workspace.")
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Could not load observations: {error}")

with tab_analytics:
    daily = summary["daily"]
    if daily:
        daily_frame = pd.DataFrame(daily)
        st.dataframe(daily_frame, use_container_width=True, hide_index=True)
        st.info(
            "Gold values are daily descriptive aggregates. Weather/river/energy "
            "association is not causal evidence; energy data is not configured in demo mode."
        )
    else:
        st.info("Daily analytical views will be populated after ingestion.")

with tab_incidents:
    try:
        current_incidents = api("/v1/incidents")
        if not current_incidents:
            st.success("No incidents recorded.")
        for incident in current_incidents:
            with st.expander(
                f"{incident['severity'].upper()} · {incident['title']} · {incident['status']}"
            ):
                st.write(incident["details"])
                st.json(incident["downstream_impact"])
                if incident["status"] in {
                    "open",
                    "detected",
                    "investigating",
                    "failed_recovery",
                    "unverified",
                } and st.button(
                    "Replay from immutable Bronze",
                    key=f"replay-{incident['id']}",
                    disabled=summary["data_mode"] != "demo",
                ):
                    try:
                        outcome = api(f"/v1/incidents/{incident['id']}/replay", method="POST")
                        st.success(outcome["message"])
                        st.rerun()
                    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                        st.error(f"Replay failed: {error}")
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Could not load incidents: {error}")

with tab_quality:
    try:
        run_rows = api("/v1/runs")
        if run_rows:
            for run in run_rows:
                with st.expander(
                    f"{run['source_id']} · {run['status']} · {run['record_count']} record(s)"
                ):
                    st.caption(f"SHA-256: {run['checksum']} · Bronze: {run['bronze_path']}")
                    st.dataframe(
                        pd.DataFrame(run["quality"]), use_container_width=True, hide_index=True
                    )
        else:
            st.info("No ingestion runs yet.")
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Could not load quality results: {error}")

with tab_investigation:
    st.subheader("Incident Investigation and Recovery")
    st.caption(
        "Each simulation uses isolated, deterministic synthetic data. Recovery keeps the "
        "original Bronze snapshot and failed checks for audit."
    )
    demo_controls_enabled = summary["data_mode"] == "demo" and len(DEMO_CONTROL_TOKEN) >= 32
    if not demo_controls_enabled:
        st.info("Synthetic incident actions require demo mode and a private control token.")
    scenario_labels = {
        "Schema drift": "schema_drift",
        "Duplicate records": "duplicate_records",
        "Missing and late measurements": "missing_late",
    }
    selected_label = st.selectbox("Scenario", list(scenario_labels))
    selected_scenario = scenario_labels[selected_label]
    action_col, reset_col = st.columns(2)
    with action_col:
        if st.button(
            "Simulate selected scenario", type="primary", disabled=not demo_controls_enabled
        ):
            try:
                api(f"/v1/demo/scenarios/{selected_scenario}/simulate", method="POST")
                st.rerun()
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                st.error(f"Scenario simulation failed: {error}")
    with reset_col:
        if st.button("Reset demo scenarios", disabled=not demo_controls_enabled):
            try:
                reset_result = api("/v1/demo/reset", method="POST")
                st.success(
                    f"Reset {reset_result['incidents_reset']} scenario(s). "
                    "Bronze snapshots and audit history were preserved."
                )
                st.rerun()
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                st.error(f"Demo reset failed: {error}")

    try:
        investigation_incidents = api("/v1/incidents")
        matching = [row for row in investigation_incidents if row["scenario"] == selected_scenario]
        if not matching:
            st.info("No incident for this scenario yet. Simulate it to begin the investigation.")
        else:
            options = {
                f"{row['title']} · {row['status']} · {row['id'][:8]}": row for row in matching
            }
            selected_incident = st.selectbox("Incident", list(options))
            detail = options[selected_incident]
            status = detail["status"]
            status_config = {
                "detected": ("Detected", "running"),
                "investigating": ("Investigating", "running"),
                "recovering": ("Recovering", "running"),
                "recovered": ("Recovered and verified", "complete"),
                "failed_recovery": ("Failed recovery", "error"),
                "unverified": ("Marked recovered; verification evidence missing", "error"),
                "reset": ("Reset; ready to simulate", "complete"),
                "open": ("Detected", "running"),
                "resolved": ("Recovered", "complete"),
            }
            label, state = status_config.get(status, (status.title(), "error"))
            with st.status(label, state=state, expanded=True):
                st.write(f"**Severity:** {detail['severity'].upper()}")
                st.write(f"**Incident:** {detail['details']}")
                st.write("**Impacted datasets:**")
                st.json(detail["downstream_impact"])
                st.write("**Affected records:**")
                st.dataframe(
                    pd.DataFrame(detail["affected_records"]),
                    use_container_width=True,
                    hide_index=True,
                )

            if status in {"detected", "open"} and st.button(
                "Start investigation",
                key=f"investigate-{detail['id']}",
                disabled=not demo_controls_enabled,
            ):
                try:
                    api(f"/v1/incidents/{detail['id']}/investigate", method="POST")
                    st.rerun()
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                    st.error(f"Could not start investigation: {error}")

            if detail["quarantined_records"]:
                st.markdown("#### Quarantined source records")
                st.dataframe(
                    pd.DataFrame(detail["quarantined_records"]),
                    use_container_width=True,
                    hide_index=True,
                )

            quality_results = detail["quality_results"]
            detection_checks = [row for row in quality_results if row["phase"] == "detection"]
            recovery_checks = [row for row in quality_results if row["phase"] == "recovery"]
            checks_col, recovery_col = st.columns(2)
            with checks_col:
                st.markdown("#### Original detection checks")
                if detection_checks:
                    st.dataframe(
                        pd.DataFrame(detection_checks),
                        use_container_width=True,
                        hide_index=True,
                    )
            with recovery_col:
                st.markdown("#### Recovery verification checks")
                if recovery_checks:
                    st.dataframe(
                        pd.DataFrame(recovery_checks),
                        use_container_width=True,
                        hide_index=True,
                    )
                elif status not in {"reset"}:
                    st.info("Recovery has not run. Original failure results remain unchanged.")

            if detail["before_metrics"] is not None or detail["after_metrics"] is not None:
                st.markdown("#### Impacted Gold daily analytics")
                metric_col_before, metric_col_after = st.columns(2)
                with metric_col_before:
                    st.caption("Before recovery")
                    if detail["before_metrics"] is not None:
                        st.json(detail["before_metrics"])
                    else:
                        st.info("No before-recovery snapshot was recorded.")
                with metric_col_after:
                    st.caption("After recovery")
                    if detail["after_metrics"] is not None:
                        st.json(detail["after_metrics"])
                    else:
                        st.info("No after-recovery snapshot was recorded yet.")

            if status in {"detected", "investigating", "failed_recovery", "unverified"}:
                if st.button(
                    "Execute recovery and verify",
                    key=f"recover-{detail['id']}",
                    disabled=not demo_controls_enabled,
                ):
                    try:
                        result = api(
                            f"/v1/incidents/{detail['id']}/replay",
                            method="POST",
                        )
                        st.success(result["message"])
                        st.rerun()
                    except HTTPError as error:
                        st.error(f"Recovery failed: {error}")
                    except (URLError, TimeoutError, json.JSONDecodeError) as error:
                        st.error(f"Recovery request failed: {error}")

            st.markdown("#### Complete audit trail")
            audit = detail["audit_trail"]
            if audit:
                st.dataframe(pd.DataFrame(audit), use_container_width=True, hide_index=True)
            else:
                st.info("No lifecycle events recorded.")
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Could not load the incident investigation: {error}")

with tab_real_data:
    st.subheader("Real Austrian Data Explorer")
    st.caption(
        "Live eHYD gauge readings and Open-Meteo historical reanalysis are displayed "
        "separately from the synthetic incident-demo datasets."
    )
    try:
        catalog = api("/v1/real/catalog")
        real_stations = catalog["stations"]
        mode = catalog["data_mode"]
        if mode == "real":
            st.success("Real-data ingestion mode is enabled.")
        else:
            st.info(
                "Viewing real-data catalog is safe in demo mode; ingestion is disabled. "
                "Start Compose with DATA_MODE=real to fetch provider data."
            )

        source_columns = st.columns(len(catalog["sources"]))
        for column, source in zip(source_columns, catalog["sources"], strict=True):
            license_detail = source["license"] or (
                "Terms are not declared in the collection metadata"
            )
            if source.get("license_url"):
                license_detail = f"[{license_detail}]({source['license_url']})"
            column.markdown(
                f"**{source['name']}**  \n[{source['attribution']}]"
                f"({source['attribution_url']})  \n{source['data_character']}  \n"
                f"License/terms: {license_detail}"
            )

        river_stations = [row for row in real_stations if row["kind"] == "river"]
        weather_stations = [row for row in real_stations if row["kind"] == "weather"]
        river_ingest_col, weather_ingest_col = st.columns(2)
        with river_ingest_col:
            if st.button("Fetch current Danube gauge readings", disabled=mode != "real"):
                try:
                    outcome = api("/v1/real/ingest/river", method="POST")
                    st.success(
                        f"{outcome['status']}: {outcome['accepted_count']} accepted, "
                        f"{outcome['rejected_count']} rejected; SHA-256 {outcome['checksum']}"
                    )
                    st.rerun()
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                    st.error(f"Hydrography ingestion failed: {error}")
        with weather_ingest_col:
            if st.button(
                "Fetch seven complete historical weather days",
                disabled=mode != "real" or not river_stations,
            ):
                try:
                    outcome = api("/v1/real/ingest/weather", method="POST")
                    st.success(
                        f"{outcome['status']}: {outcome['accepted_count']} accepted, "
                        f"{outcome['rejected_count']} rejected; SHA-256 {outcome['checksum']}"
                    )
                    st.rerun()
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
                    st.error(f"Historical weather ingestion failed: {error}")
        if not river_stations:
            st.warning(
                "No real Danube gauge readings are stored yet. In real mode, fetch "
                "current river readings first; the weather connector uses their verified "
                "coordinates."
            )

        stations_with_coordinates = [
            {"station": row["name"], "lat": row["latitude"], "lon": row["longitude"]}
            for row in real_stations
            if row["latitude"] is not None and row["longitude"] is not None
        ]
        if stations_with_coordinates:
            st.markdown("#### Verified gauge and weather-grid locations")
            st.map(pd.DataFrame(stations_with_coordinates), latitude="lat", longitude="lon")

        river_frame: pd.DataFrame | None = None
        if river_stations:
            river_options = {f"{row['name']} ({row['id']})": row for row in river_stations}
            selected_river_label = st.selectbox(
                "Danube gauge station", list(river_options), key="real-river-station"
            )
            selected_river = river_options[selected_river_label]
            river_rows = api(
                f"/v1/real/observations?domain=river&station_id={selected_river['id']}&limit=500"
            )
            if river_rows:
                river_frame = pd.DataFrame(river_rows).sort_values("event_time_utc")
                river_frame["event_time_vienna"] = (
                    pd.to_datetime(river_frame["event_time_utc"], utc=True)
                    .dt.tz_convert("Europe/Vienna")
                    .dt.strftime("%Y-%m-%d %H:%M %z")
                )
                latest = river_frame.iloc[-1]
                left, right = st.columns(2)
                if pd.notna(latest["water_level_m"]):
                    left.metric(
                        "Latest gauge water level",
                        f"{latest['water_level_m']:.2f} m",
                        latest["event_time_vienna"],
                    )
                    st.caption(
                        "Gauge-relative water-level reading; the collection does not expose "
                        "an absolute elevation or the gauge datum."
                    )
                if pd.notna(latest["discharge_m3_s"]):
                    right.metric(
                        "Latest discharge",
                        f"{latest['discharge_m3_s']:.2f} m³/s",
                        latest["event_time_vienna"],
                    )
                st.dataframe(river_frame, use_container_width=True, hide_index=True)
                st.caption("[Datenquelle: ehyd.gv.at](https://ehyd.gv.at/)")
                levels = river_frame.dropna(subset=["water_level_m"])
                discharge = river_frame.dropna(subset=["discharge_m3_s"])
                if len(levels) > 1 or len(discharge) > 1:
                    chart_col_level, chart_col_discharge = st.columns(2)
                    if len(levels) > 1:
                        chart_col_level.markdown("##### Water level (m)")
                        chart_col_level.altair_chart(
                            historical_chart(levels, "water_level_m", "Water level (m)"),
                            use_container_width=True,
                        )
                    if len(discharge) > 1:
                        chart_col_discharge.markdown("##### Discharge (m³/s)")
                        chart_col_discharge.altair_chart(
                            historical_chart(
                                discharge,
                                "discharge_m3_s",
                                "Discharge (m³/s)",
                            ),
                            use_container_width=True,
                        )
                else:
                    st.info(
                        "The official Pegel Aktuell collection contains current readings, "
                        "not an hourly history. This station has one stored snapshot; "
                        "trend charts need repeated snapshots over time."
                    )
            else:
                st.info("No river observations are stored for the selected station.")
        else:
            st.info("River stations will appear after a successful real-data ingestion.")

        if weather_stations:
            weather_options = {f"{row['name']} ({row['id']})": row for row in weather_stations}
            selected_weather_label = st.selectbox(
                "Historical weather grid point",
                list(weather_options),
                key="real-weather-station",
            )
            selected_weather = weather_options[selected_weather_label]
            weather_rows = api(
                f"/v1/real/observations?domain=weather&station_id={selected_weather['id']}&limit=1000"
            )
            if weather_rows:
                weather_frame = pd.DataFrame(weather_rows).sort_values("event_time_utc")
                weather_frame["event_time_vienna"] = (
                    pd.to_datetime(weather_frame["event_time_utc"], utc=True)
                    .dt.tz_convert("Europe/Vienna")
                    .dt.strftime("%Y-%m-%d %H:%M %z")
                )
                chart_fields = [
                    field
                    for field in (
                        "temperature_c",
                        "relative_humidity_percent",
                        "wind_speed_m_s",
                        "precipitation_mm",
                    )
                    if weather_frame[field].notna().any()
                ]
                if chart_fields:
                    st.markdown(
                        "#### Hourly historical weather (Open-Meteo model-based reanalysis)"
                    )
                    chart_columns = st.columns(len(chart_fields))
                    labels = {
                        "temperature_c": "Temperature (°C)",
                        "precipitation_mm": "Hourly precipitation (mm)",
                        "relative_humidity_percent": "Relative humidity (%)",
                        "wind_speed_m_s": "Wind speed (m/s)",
                    }
                    for column, field in zip(chart_columns, chart_fields, strict=True):
                        values = weather_frame.dropna(subset=[field])
                        column.caption(labels[field])
                        column.altair_chart(
                            historical_chart(
                                values,
                                field,
                                labels[field],
                                mark="bar" if field == "precipitation_mm" else "line",
                            ),
                            use_container_width=True,
                        )
                st.dataframe(weather_frame, use_container_width=True, hide_index=True)
                st.caption(
                    f"Reanalysis/model: {weather_frame.iloc[-1]['source_model']}. "
                    "Precipitation is hourly accumulation; null values remain missing, not zero. "
                    "[Weather data by Open-Meteo.com](https://open-meteo.com/) · "
                    "[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)."
                )
            else:
                st.info("No historical weather observations are stored for this grid point.")
        else:
            st.info("Weather grid points will appear after successful weather ingestion.")

        analytics = api("/v1/real/analytics")
        analytics_col_river, analytics_col_weather = st.columns(2)
        with analytics_col_river:
            st.markdown("#### Real river daily coverage")
            if analytics["river_daily"]:
                st.dataframe(
                    pd.DataFrame(analytics["river_daily"]),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info(
                    "No repeated river history is stored yet. Current gauge snapshots "
                    "will accumulate through scheduled/manual ingestion."
                )
        with analytics_col_weather:
            st.markdown("#### Real weather daily analytics")
            if analytics["weather_daily"]:
                st.dataframe(
                    pd.DataFrame(analytics["weather_daily"]),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No real weather history has been ingested.")

        st.markdown("#### Latest source quality and freshness")
        for source_id, latest_run in catalog["latest_runs"].items():
            with st.expander(f"{source_id} · latest run"):
                if latest_run is None:
                    st.info("No ingestion attempt has been recorded.")
                    continue
                st.write(
                    f"Status: **{latest_run['status']}** · received "
                    f"{latest_run['record_count']} · accepted {latest_run['accepted_count']} · "
                    f"rejected {latest_run['rejected_count']}"
                )
                st.caption(
                    f"Retrieved {latest_run['started_at_utc']} UTC · "
                    f"SHA-256 {latest_run['checksum']}"
                )
                if latest_run["quality"]:
                    st.dataframe(
                        pd.DataFrame(latest_run["quality"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                if latest_run["status"] in {"failed", "degraded"}:
                    st.warning(
                        "Provider or data-quality warnings are present. HTTP success alone "
                        "does not make a dataset trusted."
                    )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, KeyError) as error:
        st.error(f"Could not load real Austrian data: {error}")

with tab_pipelines:
    st.subheader("Automated real-data pipelines")
    st.caption(
        "Application-side execution evidence, independent of the optional Airflow service. "
        "Airflow provides scheduler health, retries, task logs and scheduled-run history "
        "at http://localhost:8080. River polling requires operator approval."
    )
    try:
        for pipeline in api("/v1/pipelines"):
            st.markdown(f"### {pipeline['domain'].title()} pipeline")
            latest = pipeline["latest_execution"]
            successful = pipeline["latest_success"]
            if pipeline["stale"]:
                st.warning(
                    f"No verified success in the last {pipeline['stale_after_hours']} hours. "
                    "Airflow may be disabled, paused, delayed or failing."
                )
            else:
                st.success("A recent automated ingestion passed persisted-data verification.")
            st.write(f"Latest event (UTC): {pipeline['latest_event_time_utc'] or 'not available'}")
            st.write(
                "Latest verified execution (UTC): "
                f"{successful['completed_at_utc'] if successful else 'none'}"
            )
            st.write(
                "Latest verified source retrieval (UTC): "
                f"{pipeline['latest_verified_ingestion_at_utc'] or 'none'}"
            )
            if latest:
                st.write(
                    f"Latest attempt: **{latest['status']}** · {latest['orchestrator_run_id']}"
                )
                if latest["status"] in {"running", "ingested"}:
                    st.info(
                        "Execution has not finished verification. Consult Airflow if it remains "
                        "in this state; a client/task timeout is not proof of a source failure."
                    )
            failure = pipeline["latest_failure"]
            if failure:
                st.error(
                    f"Latest failed execution: {failure['started_at_utc']} · {failure['error']}"
                )
            if pipeline["recent_executions"]:
                st.dataframe(
                    pd.DataFrame(pipeline["recent_executions"]),
                    use_container_width=True,
                    hide_index=True,
                )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        st.error(f"Could not load automated pipeline status: {error}")

with tab_energy:
    st.subheader("Austrian Energy Explorer")
    st.caption(
        "Real ENTSO-E data only. Austrian bidding/control-area aggregates are not individual "
        "hydropower plants. Pumping consumption is excluded from generation. "
        "A65 demand forecasts are a separate product, never measured demand."
    )
    product = st.selectbox(
        "Energy data product",
        ["energy", "load_forecast"],
        format_func=lambda value: (
            "Generation and prices (A75/A44)"
            if value == "energy"
            else "Day-ahead demand forecast (A65/A01, not actual load)"
        ),
    )
    try:
        energy = api(f"/v1/energy?product={product}")
        st.caption(energy["attribution"])
        st.caption(energy["product_label"])
        st.info(energy["permission_note"])
        if not energy["access_configured"]:
            st.info(
                "No ENTSO-E API token configured. Obtain approved access or import "
                "operator-verified exports using the documented CLI. No live data is fabricated."
            )
        st.write(f"Latest ingestion: **{energy['latest_run_status']}**")
        st.write(f"Latest event (UTC): {energy['latest_event_time_utc'] or 'not available'}")
        st.write(
            f"Latest retrieval (UTC): {energy['latest_retrieval_time_utc'] or 'not available'}"
        )
        if energy["stale"]:
            st.warning("No recent energy event is available. " + energy["freshness_note"])
        st.caption(energy["note"])
        if energy["quality"]:
            st.dataframe(pd.DataFrame(energy["quality"]), hide_index=True)
        if not energy["observations"]:
            st.warning("No trusted Austrian energy observations are available.")
        else:
            frame = pd.DataFrame(energy["observations"])
            forecasts = frame[frame["metric"] == "load_forecast"]
            if not forecasts.empty:
                st.altair_chart(
                    historical_chart(
                        forecasts.sort_values("event_time"),
                        "load_forecast_mw",
                        "Day-ahead demand forecast (MW, not actual demand)",
                    ),
                    use_container_width=True,
                )
            generation = frame[frame["metric"] == "generation"]
            if not generation.empty:
                types = sorted(generation["production_name"].unique())
                chosen = st.multiselect("Generation production types", types, default=types)
                selected = generation[generation["production_name"].isin(chosen)]
                st.altair_chart(
                    alt.Chart(selected)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("event_time:T", title="UTC interval start"),
                        y=alt.Y("generation_mw:Q", title="Generation (MW)"),
                        color="production_name:N",
                        tooltip=[
                            "event_time_vienna",
                            "production_name",
                            "generation_mw",
                            "duration_hours",
                            "revision",
                        ],
                    ),
                    use_container_width=True,
                )
                hydro = generation[generation["production_type"].isin(["B10", "B11", "B12"])]
                st.markdown("#### Hydropower (pumped storage output, run-of-river, reservoir)")
                st.dataframe(hydro, hide_index=True)
            prices = frame[frame["metric"] == "price"]
            if not prices.empty:
                st.altair_chart(
                    historical_chart(
                        prices.sort_values("event_time"),
                        "price_eur_mwh",
                        "Day-ahead price (EUR/MWh)",
                    ),
                    use_container_width=True,
                )
            st.markdown(
                "#### UTC daily forecast demand and coverage"
                if product == "load_forecast"
                else "#### UTC daily energy and coverage"
            )
            st.dataframe(pd.DataFrame(energy["daily"]), hide_index=True)
        st.markdown("#### Matched environmental comparisons")
        river_station = st.text_input("River station identifier", value="ehyd:207068")
        weather_station = st.text_input("Weather grid identifier", value="open-meteo:207068")
        if st.button("Compare actual overlapping UTC dates"):
            from urllib.parse import urlencode

            comparison = api(
                "/v1/energy/comparisons?"
                + urlencode(
                    {
                        "river_station_id": river_station,
                        "weather_station_id": weather_station,
                        "product": product,
                    }
                )
            )
            st.caption(comparison["note"])
            if comparison["rows"]:
                st.dataframe(pd.DataFrame(comparison["rows"]), hide_index=True)
            else:
                st.info("No actual overlapping dates for these selections; no values inferred.")
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, KeyError) as error:
        st.error(f"Could not load energy data: {error}")
