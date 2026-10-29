"""Manual single-writer local lakehouse jobs. No provider requests or database writes."""

import argparse
import fcntl
import json
import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from lakehouse.snapshot import canonical, digest, export_snapshot

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def spark_session() -> "SparkSession":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("DanubePulse local lakehouse")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.jars", "/opt/delta/delta-spark.jar,/opt/delta/delta-storage.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def schema() -> "StructType":
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    strings = (
        "key",
        "content_hash",
        "provenance",
        "source_id",
        "domain",
        "location",
        "metric",
        "production_type",
        "event_time",
        "interval_end",
        "unit",
        "source_record_id",
        "run_id",
        "source_document_id",
        "source_document_time",
        "ingested_at",
        "processed_at",
        "bronze_path",
        "bronze_checksum",
        "attribution",
        "license",
        "reference_url",
    )
    return StructType(
        [StructField(name, StringType(), True) for name in strings]
        + [
            StructField("value", DoubleType(), False),
            StructField("duration_hours", DoubleType(), True),
            StructField("revision", IntegerType(), True),
        ]
    )


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(f".{uuid4().hex}.tmp")
    with temporary.open("x") as handle:
        json.dump(payload, handle, allow_nan=False, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


@contextmanager
def writer_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".writer.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                "Another lakehouse writer is active; retry after it finishes."
            ) from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def history(spark: "SparkSession", path: Path) -> list[dict[str, Any]]:
    from delta.tables import DeltaTable

    return [
        row.asDict(recursive=True)
        for row in DeltaTable.forPath(spark, str(path)).history(10).collect()
    ]


def table_info(spark: "SparkSession", root: Path) -> dict[str, Any]:
    from delta.tables import DeltaTable

    tables = {}
    for layer in ("bronze", "silver", "gold"):
        path = root / layer
        detail = DeltaTable.forPath(spark, str(path)).detail().first().asDict()
        transactions = history(spark, path)
        tables[layer] = {
            "rows": spark.read.format("delta").load(str(path)).count(),
            "files": detail["numFiles"],
            "size_bytes": detail["sizeInBytes"],
            "version": transactions[0]["version"],
            "history": transactions,
        }
    return tables


def transform(spark: "SparkSession", records: list[dict[str, Any]], root: Path) -> dict[str, Any]:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    expected = schema()
    if any(set(record) != set(expected.fieldNames()) for record in records):
        raise ValueError("Export schema differs from the enforced lakehouse contract.")
    snapshot_id = digest(sorted(records, key=lambda record: record["key"]))
    frame = spark.createDataFrame(records, expected)
    # Refuse ambiguous source observations, rather than choosing an arbitrary Spark row.
    if frame.groupBy("key").count().filter("count > 1").limit(1).count():
        raise ValueError("Trusted export has duplicate semantic observation keys.")
    if (
        frame.filter(
            F.col("value").isNull()
            | F.isnan("value")
            | F.col("key").isNull()
            | ~F.col("provenance").isin("real", "synthetic")
            | F.to_timestamp("event_time").isNull()
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Snapshot contains invalid values, timestamps or provenance.")
    snapshots = root / "snapshots"
    snapshots.mkdir(exist_ok=True)
    snapshot_path = snapshots / f"{snapshot_id}.json"
    if not snapshot_path.exists():
        with snapshot_path.open("x") as handle:
            handle.write(canonical(records))
    bronze_schema = StructType(
        [
            StructField(name, StringType(), False)
            for name in ("snapshot_id", "key", "provenance", "payload")
        ]
    )
    bronze = spark.createDataFrame(
        [(snapshot_id, row["key"], row["provenance"], canonical(row)) for row in records],
        bronze_schema,
    )
    paths = {layer: root / layer for layer in ("bronze", "silver", "gold")}
    if DeltaTable.isDeltaTable(spark, str(paths["bronze"])):
        DeltaTable.forPath(spark, str(paths["bronze"])).alias("t").merge(
            bronze.alias("s"), "t.snapshot_id=s.snapshot_id AND t.key=s.key"
        ).whenNotMatchedInsertAll().execute()
    else:
        bronze.write.format("delta").mode("error").save(str(paths["bronze"]))
    silver = frame.withColumn("active", F.lit(True))
    if DeltaTable.isDeltaTable(spark, str(paths["silver"])):
        DeltaTable.forPath(spark, str(paths["silver"])).alias("t").merge(
            silver.alias("s"), "t.key=s.key"
        ).whenMatchedUpdateAll(
            "t.content_hash <> s.content_hash OR NOT t.active"
        ).whenNotMatchedInsertAll().whenNotMatchedBySourceUpdate(
            condition="t.active", set={"active": "false"}
        ).execute()
    else:
        silver.write.format("delta").mode("error").save(str(paths["silver"]))
    trusted = spark.read.format("delta").load(str(paths["silver"])).filter("active")
    # Verify exact persisted output, not merely the DataFrame that was submitted.
    if trusted.drop("active").exceptAll(frame).limit(1).count() or (
        frame.exceptAll(trusted.drop("active")).limit(1).count()
    ):
        raise RuntimeError("Persisted Delta Silver differs from the trusted source snapshot.")
    groups = [
        "provenance",
        "source_id",
        "domain",
        "location",
        "metric",
        "production_type",
        "unit",
        "event_date",
    ]
    gold = (
        trusted.withColumn("event_date", F.to_date("event_time"))
        .groupBy(*groups)
        .agg(
            F.count("*").alias("observation_count"),
            F.avg("value").alias("sample_average"),
            F.min("value").alias("minimum_value"),
            F.max("value").alias("maximum_value"),
            F.sum("duration_hours").alias("covered_hours"),
            (F.sum(F.col("value") * F.col("duration_hours")) / F.sum("duration_hours")).alias(
                "duration_weighted_average"
            ),
            F.sum(
                F.when(
                    F.col("metric").isin("generation_mw", "load_forecast_mw"),
                    F.col("value") * F.col("duration_hours"),
                )
            ).alias("integrated_mwh"),
            F.sum(F.when(F.col("metric") == "precipitation_mm", F.col("value"))).alias(
                "precipitation_total_mm"
            ),
        )
    )
    if DeltaTable.isDeltaTable(spark, str(paths["gold"])):
        condition = " AND ".join(f"t.{name} <=> s.{name}" for name in groups)
        changed = " OR ".join(
            f"NOT (t.{name} <=> s.{name})" for name in gold.columns if name not in groups
        )
        DeltaTable.forPath(spark, str(paths["gold"])).alias("t").merge(
            gold.alias("s"), condition
        ).whenMatchedUpdateAll(
            changed
        ).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    else:
        gold.write.format("delta").mode("error").save(str(paths["gold"]))
    actual_gold = spark.read.format("delta").load(str(paths["gold"]))
    if actual_gold.exceptAll(gold).limit(1).count() or gold.exceptAll(actual_gold).limit(1).count():
        raise RuntimeError("Persisted Delta Gold failed aggregate verification.")
    return {
        "snapshot_id": snapshot_id,
        "input_measurements": len(records),
        "silver_active_rows": trusted.count(),
        "tables": table_info(spark, root),
        "gold_preview": [row.asDict() for row in actual_gold.orderBy(*groups).limit(100).collect()],
        "sources": [
            row.asDict()
            for row in frame.select(
                "source_id", "provenance", "attribution", "license", "reference_url"
            )
            .distinct()
            .orderBy("source_id")
            .collect()
        ],
    }


@contextmanager
def audited_attempt(root: Path, scope: str, provenance: str) -> Iterator[dict[str, Any]]:
    with writer_lock(root):
        destination = root / scope
        destination.mkdir(exist_ok=True)
        run = {
            "id": str(uuid4()),
            "scope": scope,
            "provenance": provenance,
            "started_at": datetime.now(UTC).isoformat(),
            "status": "running",
        }
        runs = destination / "runs"
        runs.mkdir(exist_ok=True)
        atomic_json(destination / "latest_attempt.json", run)
        started = time.monotonic()
        try:
            run["runtime"] = {
                "spark": version("pyspark"),
                "delta": version("delta-spark"),
                "master": "local[2]",
                "driver_memory": "1g",
            }
            yield run
            run["status"] = "success"
        except Exception as error:
            run.update(status="failed", error=f"{type(error).__name__}: {error}")
            logger.exception("lakehouse_processing_failed")
            raise
        finally:
            run.update(
                finished_at=datetime.now(UTC).isoformat(),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            atomic_json(runs / f"{run['id']}.json", run)
            atomic_json(destination / "latest_attempt.json", run)
        # Publish only a verified generation. The old successful manifest survives failures.
        atomic_json(destination / "status.json", run)


def process(
    spark: "SparkSession", root: Path, scope: str, records: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    if scope not in {"real", "demo"}:
        raise ValueError("Snapshot processing requires real or demo scope.")
    provenance = "real" if scope == "real" else "synthetic"
    with audited_attempt(root, scope, provenance) as run:
        spark.conf.set(
            "spark.databricks.delta.commitInfo.userMetadata",
            canonical({"run_id": run["id"], "scope": scope}),
        )
        if records is None:
            records = export_snapshot(os.environ.get("LAKEHOUSE_DATABASE_URL", ""), provenance)
        if any(record["provenance"] != provenance for record in records):
            raise ValueError("Cross-provenance input rejected.")
        run.update(transform(spark, records, root / scope))
    return run


def benchmark(spark: "SparkSession", root: Path, sizes: list[int]) -> dict[str, Any]:
    from delta.tables import DeltaTable
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F

    if not sizes or any(size < 1000 or size > 1_000_000 for size in sizes):
        raise ValueError("Benchmark sizes must be between 1,000 and 1,000,000.")
    with audited_attempt(root, "synthetic-scale", "synthetic-scale-test") as report:
        spark.conf.set(
            "spark.databricks.delta.commitInfo.userMetadata",
            canonical({"run_id": report["id"], "scope": "synthetic-scale"}),
        )
        destination = root / "synthetic-scale"
        run_id = report["id"]
        results = []
        for size in sizes:
            started = time.monotonic()
            generated = spark.range(size, numPartitions=2).select(
                F.col("id").alias("event_id"),
                (F.col("id") % 100).cast("double").alias("value"),
                F.lit("synthetic-scale-test").alias("provenance"),
            )
            duplicate_input = generated.unionByName(generated)
            unique = duplicate_input.dropDuplicates(["event_id"])
            path = destination / run_id / str(size)
            unique.write.format("delta").mode("error").save(str(path))
            stored = spark.read.format("delta").load(str(path))
            result = stored.agg(F.count("*"), F.sum("value")).first()
            expected_sum = (size // 100) * 4950 + sum(range(size % 100))
            if result[0] != size or result[1] != expected_sum:
                raise RuntimeError("Scale-test persisted count/checksum failed.")
            version = DeltaTable.forPath(spark, str(path)).history(1).first()["version"]
            rejected = False
            try:
                stored.withColumn("unexpected_schema", F.lit("must reject")).write.format(
                    "delta"
                ).mode("append").save(str(path))
            except AnalysisException:
                rejected = True
            if (
                not rejected
                or DeltaTable.forPath(spark, str(path)).history(1).first()["version"] != version
            ):
                raise RuntimeError("Schema enforcement did not preserve the Delta transaction.")
            DeltaTable.forPath(spark, str(path)).update(
                condition="event_id = 0", set={"value": "999.0"}
            )
            old = spark.read.format("delta").option("versionAsOf", version).load(str(path))
            if old.filter("event_id=0").first()["value"] != 0:
                raise RuntimeError("Delta version time travel returned the wrong value.")
            if (
                spark.read.format("delta").load(str(path)).filter("event_id=0").first()["value"]
                != 999
            ):
                raise RuntimeError("Delta update did not persist atomically.")
            elapsed = time.monotonic() - started
            results.append(
                {
                    "unique_rows": size,
                    "input_rows": size * 2,
                    "generator_partitions": generated.rdd.getNumPartitions(),
                    "input_partitions": duplicate_input.rdd.getNumPartitions(),
                    "persisted_scan_partitions": stored.rdd.getNumPartitions(),
                    "persisted_sum_before_update": expected_sum,
                    "seconds": round(elapsed, 3),
                    "input_rows_per_second": round(size * 2 / elapsed, 1),
                    "schema_rejected_without_commit": True,
                    "time_travel_original_value": 0,
                    "current_value": 999,
                    "table_path": str(path.relative_to(root)),
                    "history": history(spark, path),
                }
            )
        report.update(
            results=results,
            note="Artificial scale data only; timings include validation, I/O and Delta "
            "demonstrations. Cold/warm JVM effects prevent a distributed speedup claim.",
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/lakehouse"))
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Read trusted PostgreSQL data; never fetch providers.")
    run.add_argument("--scope", choices=("real", "demo"), required=True)
    scale = subparsers.add_parser(
        "benchmark", help="Separate, clearly labeled synthetic scale test."
    )
    scale.add_argument("--sizes", type=int, nargs="+", default=[10000, 100000])
    inspect = subparsers.add_parser(
        "inspect", help="Read a Delta table, optionally an old version."
    )
    inspect.add_argument("--scope", choices=("real", "demo"), required=True)
    inspect.add_argument("--table", choices=("bronze", "silver", "gold"), required=True)
    inspect.add_argument("--version", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    spark = spark_session()
    try:
        if args.command == "run":
            result = process(spark, args.root, args.scope)
        elif args.command == "benchmark":
            result = benchmark(spark, args.root, args.sizes)
        else:
            path = args.root / args.scope / args.table
            reader = spark.read.format("delta")
            if args.version is not None:
                reader = reader.option("versionAsOf", args.version)
            frame = reader.load(str(path))
            result = {
                "rows": frame.count(),
                "history": history(spark, path),
                "preview": [row.asDict() for row in frame.limit(20).collect()],
            }
        print(json.dumps(result, default=str, allow_nan=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
