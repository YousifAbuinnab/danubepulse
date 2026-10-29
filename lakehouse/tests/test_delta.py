"""Container-only contracts; never access provider endpoints or the application database."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from lakehouse import job
from lakehouse.job import benchmark, process, spark_session, writer_lock
from lakehouse.snapshot import normalize


@pytest.fixture(scope="session")
def spark():
    session = spark_session()
    yield session
    session.stop()


def measurements():
    time = datetime(2026, 10, 19, tzinfo=UTC)
    base = {
        "provenance": "synthetic",
        "source_id": "test-forecast",
        "region": "AT",
        "event_time": time,
        "interval_end": time + timedelta(minutes=15),
        "duration_hours": 0.25,
        "metric": "load_forecast",
        "load_forecast_mw": 100.0,
        "source_record_id": "first",
        "run_id": "fixture-run",
        "revision": 1,
        "source_published_at": time,
        "ingested_at": time,
        "processed_at": time,
        "bronze_path": "synthetic-fixture-only",
        "checksum": "a" * 64,
        "source_name": "SYNTHETIC TEST ONLY",
        "license": "test",
        "reference_url": None,
    }
    second = dict(
        base,
        event_time=time + timedelta(minutes=15),
        interval_end=time + timedelta(minutes=45),
        duration_hours=0.5,
        source_record_id="second",
        load_forecast_mw=200.0,
    )
    return normalize("energy", base, "synthetic") + normalize("energy", second, "synthetic")


def test_incremental_merges_correction_history_failure_and_provenance(spark, tmp_path, monkeypatch):
    records = measurements()
    first = process(spark, tmp_path, "demo", records)
    before_version = first["tables"]["silver"]["history"][0]["version"]
    gold = first["gold_preview"][0]
    assert gold["observation_count"] == 2
    assert gold["integrated_mwh"] == 125
    assert gold["covered_hours"] == 0.75
    assert gold["duration_weighted_average"] == pytest.approx(125 / 0.75)
    repeated = process(spark, tmp_path, "demo", records)
    assert repeated["snapshot_id"] == first["snapshot_id"]
    assert repeated["tables"]["bronze"]["rows"] == 2
    assert repeated["gold_preview"] == first["gold_preview"]
    with pytest.raises(ValueError, match="duplicate semantic"):
        process(spark, tmp_path, "demo", records + records[:1])
    assert json.loads((tmp_path / "demo/status.json").read_text())["id"] == repeated["id"]
    assert json.loads((tmp_path / "demo/latest_attempt.json").read_text())["status"] == "failed"
    with pytest.raises(ValueError, match="Cross-provenance"):
        process(spark, tmp_path, "real", records)
    changed = [dict(records[0], value=300.0, content_hash="changed")]

    def fail_manifest(*_args):
        raise RuntimeError("Injected post-write verification failure")

    with monkeypatch.context() as patch:
        patch.setattr(job, "table_info", fail_manifest)
        with pytest.raises(RuntimeError, match="post-write"):
            process(spark, tmp_path, "demo", changed)
    assert json.loads((tmp_path / "demo/status.json").read_text())["id"] == repeated["id"]
    corrected = process(spark, tmp_path, "demo", changed)
    assert corrected["silver_active_rows"] == 1
    assert corrected["tables"]["silver"]["rows"] == 2  # removed measurement retained inactive
    assert corrected["tables"]["bronze"]["rows"] == 3
    assert corrected["gold_preview"][0]["integrated_mwh"] == 75
    assert corrected["gold_preview"][0]["observation_count"] == 1
    old = (
        spark.read.format("delta")
        .option("versionAsOf", before_version)
        .load(str(tmp_path / "demo/silver"))
    )
    assert sorted(row.value for row in old.collect()) == [100, 200]
    assert len(list((tmp_path / "demo/runs").glob("*.json"))) == 5
    assert json.loads((tmp_path / "real/latest_attempt.json").read_text())["status"] == "failed"
    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another lakehouse writer"):
            with writer_lock(tmp_path):
                pytest.fail("overlapping writer unexpectedly acquired the lock")


def test_scale_dedup_schema_enforcement_transactions_and_time_travel(spark, tmp_path):
    result = benchmark(spark, tmp_path, [1000, 10000])
    assert result["provenance"] == "synthetic-scale-test"
    assert [row["unique_rows"] for row in result["results"]] == [1000, 10000]
    for row in result["results"]:
        assert row["input_rows"] == row["unique_rows"] * 2
        assert row["persisted_sum_before_update"] == row["unique_rows"] * 49.5
        assert row["schema_rejected_without_commit"]
        assert row["time_travel_original_value"] == 0 and row["current_value"] == 999
        assert len(row["history"]) == 2
    assert not (tmp_path / "real").exists() and not (tmp_path / "demo").exists()


def test_domain_gold_semantics_and_authoritative_empty_snapshot(spark, tmp_path):
    time = datetime(2026, 10, 19, tzinfo=UTC)
    common = {
        "provenance": "synthetic",
        "source_id": "synthetic-multi-domain",
        "station_id": "test-station",
        "region": "AT",
        "event_time": time,
        "source_record_id": "first",
        "run_id": "fixture",
        "ingested_at": time,
        "processed_at": time,
        "bronze_path": "test-only",
        "checksum": "b" * 64,
        "source_name": "SYNTHETIC TEST ONLY",
    }
    records = measurements()
    for index in range(2):
        row = dict(
            common,
            event_time=time + timedelta(hours=index),
            source_record_id=f"point-{index}",
        )
        records += normalize(
            "river",
            dict(row, water_level_m=2 + index * 2, discharge_m3_s=100 + index * 200),
            "synthetic",
        )
        records += normalize(
            "weather",
            dict(
                row,
                temperature_c=10 + index * 10,
                precipitation_mm=1 + index,
                relative_humidity_percent=50 + index * 10,
                wind_speed_m_s=2 + index,
            ),
            "synthetic",
        )
        for metric, field, production in (
            ("price", "price_eur_mwh", None),
            ("generation", "generation_mw", "B11"),
        ):
            duration = 0.25 if index == 0 else 0.5
            records += normalize(
                "energy",
                dict(
                    row,
                    metric=metric,
                    production_type=production,
                    duration_hours=duration,
                    interval_end=row["event_time"] + timedelta(hours=duration),
                    **{field: 100 + index * 100},
                ),
                "synthetic",
            )
    result = process(spark, tmp_path, "demo", records)
    by_metric = {row["metric"]: row for row in result["gold_preview"]}
    assert by_metric["water_level_m"]["sample_average"] == 3
    assert by_metric["discharge_m3_s"]["sample_average"] == 200
    assert by_metric["temperature_c"]["sample_average"] == 15
    assert by_metric["precipitation_mm"]["precipitation_total_mm"] == 3
    assert by_metric["price_eur_mwh"]["duration_weighted_average"] == pytest.approx(125 / 0.75)
    assert by_metric["price_eur_mwh"]["integrated_mwh"] is None
    assert by_metric["generation_mw"]["integrated_mwh"] == 125
    assert by_metric["load_forecast_mw"]["integrated_mwh"] == 125
    empty = process(spark, tmp_path, "demo", [])
    assert empty["silver_active_rows"] == 0 and empty["gold_preview"] == []
    assert empty["tables"]["silver"]["rows"] == len(records)
    assert empty["tables"]["bronze"]["rows"] == len(records)
