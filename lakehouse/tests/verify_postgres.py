"""Manual read-only PostgreSQL/Delta integration check on the existing local deployment."""

import json
import math
from pathlib import Path

import psycopg
from lakehouse.job import process, spark_session
from lakehouse.snapshot import METRICS
from psycopg import sql
from psycopg.rows import dict_row


def fingerprint(connection) -> dict:
    result = {}
    for row in connection.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    ).fetchall():
        name = row["tablename"]
        result[name] = dict(
            connection.execute(
                sql.SQL(
                    "SELECT count(*) AS rows, md5(coalesce(string_agg(row_to_json(t)::text, '' "
                    "ORDER BY row_to_json(t)::text), '')) AS checksum FROM {} t"
                ).format(sql.Identifier(name))
            ).fetchone()
        )
    return result


def expected_gold(connection) -> list[dict]:
    result = []
    for domain, metrics in METRICS.items():
        for metric, unit in metrics.items():
            energy = domain == "energy"
            location = "region" if energy else "station_id"
            production = "coalesce(o.production_type, '')" if energy else "''"
            duration = "sum(o.duration_hours)" if energy else "NULL"
            weighted = (
                f"sum(o.{metric} * o.duration_hours)/sum(o.duration_hours)" if energy else "NULL"
            )
            integrated = (
                f"sum(o.{metric} * o.duration_hours)"
                if metric in {"generation_mw", "load_forecast_mw"}
                else "NULL"
            )
            rain = f"sum(o.{metric})" if metric == "precipitation_mm" else "NULL"
            rows = connection.execute(
                f"""
                SELECT s.provenance, o.source_id, %s AS domain, o.{location} AS location,
                       %s AS metric, {production} AS production_type, %s AS unit,
                       CAST(o.event_time AS date) AS event_date,
                       count(*) AS observation_count, avg(o.{metric}) AS sample_average,
                       min(o.{metric}) AS minimum_value, max(o.{metric}) AS maximum_value,
                       {duration} AS covered_hours, {weighted} AS duration_weighted_average,
                       {integrated} AS integrated_mwh, {rain} AS precipitation_total_mm
                FROM {domain}_observations o JOIN data_sources s ON s.id=o.source_id
                WHERE o.quality_status='valid' AND s.provenance='real' AND o.{metric} IS NOT NULL
                GROUP BY 1, 2, 4, 6, 8
                """,
                (domain, metric, unit),
            ).fetchall()
            result.extend(rows)
    return result


def main() -> None:
    with psycopg.connect("", row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        before = fingerprint(connection)
        expected = expected_gold(connection)
    spark = spark_session()
    try:
        first = process(spark, Path("/lakehouse"), "real")
        repeated = process(spark, Path("/lakehouse"), "real")
        assert first["snapshot_id"] == repeated["snapshot_id"]
        assert first["silver_active_rows"] == repeated["silver_active_rows"]
        assert first["tables"]["bronze"]["rows"] == repeated["tables"]["bronze"]["rows"]
        actual = [
            row.asDict()
            for row in spark.read.format("delta").load("/lakehouse/real/gold").collect()
        ]
        keys = (
            "provenance",
            "source_id",
            "domain",
            "location",
            "metric",
            "production_type",
            "unit",
            "event_date",
        )
        indexed = {tuple(row[key] for key in keys): row for row in actual}
        assert len(indexed) == len(expected) == len(actual)
        for row in expected:
            found = indexed[tuple(row[key] for key in keys)]
            for name in set(row) - set(keys):
                if row[name] is None:
                    assert found[name] is None
                else:
                    assert math.isclose(found[name], row[name], rel_tol=1e-12, abs_tol=1e-9), (
                        name,
                        row,
                        found,
                    )
        demo = process(spark, Path("/lakehouse"), "demo")
    finally:
        spark.stop()
    with psycopg.connect("", row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        assert fingerprint(connection) == before, (
            "Application records changed during read-only jobs!"
        )
    print(
        json.dumps(
            {
                "verified": True,
                "postgresql_unchanged": True,
                "repeated_snapshot_id": first["snapshot_id"],
                "real_measurements": repeated["silver_active_rows"],
                "demo_measurements": demo["silver_active_rows"],
                "gold_groups_verified": len(expected),
                "forecast_gold": [row for row in actual if row["metric"] == "load_forecast_mw"],
                "first_seconds": first["duration_seconds"],
                "repeat_seconds": repeated["duration_seconds"],
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
