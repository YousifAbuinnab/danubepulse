"""Run with the pinned Airflow image, not the lightweight application environment."""

import importlib.util
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

from airflow.models import DagBag


class DagContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "pipeline_dags", "/opt/airflow/dags/danubepulse_pipelines.py"
        )
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        demo_spec = importlib.util.spec_from_file_location(
            "interview_demo", "/opt/airflow/dags/danubepulse_interview_demo.py"
        )
        cls.demo_module = importlib.util.module_from_spec(demo_spec)
        demo_spec.loader.exec_module(cls.demo_module)

    def test_real_airflow_parser_and_dependencies(self):
        bag = DagBag("/opt/airflow/dags")
        self.assertEqual(bag.import_errors, {})
        self.assertEqual(
            set(bag.dags),
            {
                "danubepulse_river",
                "danubepulse_weather",
                "danubepulse_energy",
                "danubepulse_interview_demo",
            },
        )
        for dag_id in ("danubepulse_river", "danubepulse_weather", "danubepulse_energy"):
            dag = bag.dags[dag_id]
            self.assertFalse(dag.catchup)
            self.assertEqual(dag.max_active_runs, 1)
            self.assertEqual(dag.max_active_tasks, 1)
            self.assertEqual(set(dag.task_dict), {"ingest", "verify"})
            self.assertEqual(dag.task_dict["ingest"].downstream_task_ids, {"verify"})
            self.assertEqual(dag.task_dict["ingest"].retries, 2)
            self.assertEqual(dag.task_dict["verify"].execution_timeout.total_seconds(), 600)
        self.assertTrue(self.module.river.is_paused_upon_creation)
        self.assertEqual(self.module.river.timetable.summary, "0 * * * *")
        self.assertEqual(self.module.weather.timetable.summary, "0 6 * * *")
        self.assertTrue(self.module.energy.is_paused_upon_creation)
        self.assertEqual(self.module.energy.timetable.summary, "0 7 * * *")
        demo = bag.dags["danubepulse_interview_demo"]
        self.assertIsNone(demo.schedule)
        self.assertTrue(demo.is_paused_upon_creation)
        self.assertEqual(
            set(demo.task_dict), {"validate_fixture", "aggregate_demo", "verify_aggregate"}
        )
        self.assertEqual(demo.task_dict["validate_fixture"].downstream_task_ids, {"aggregate_demo"})
        self.assertEqual(demo.task_dict["aggregate_demo"].downstream_task_ids, {"verify_aggregate"})

    def test_interview_fixture_workflow_returns_verified_synthetic_results(self):
        validated = self.demo_module.load_and_validate_fixture()
        aggregate = self.demo_module.aggregate_records(validated)
        result = self.demo_module.verify_aggregates(aggregate)
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["provenance"], "synthetic")
        self.assertEqual(result["input_count"], 4)
        self.assertEqual(
            [(row["domain"], row["observation_count"], row["average"]) for row in result["groups"]],
            [("river", 2, 2.45), ("weather", 2, 11.5)],
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.demo_module.verify_aggregates({"provenance": "real"})

    def test_weather_target_uses_interval_not_retry_wall_clock(self):
        context = {
            "dag_run": SimpleNamespace(run_id="scheduled:test"),
            "data_interval_end": datetime(2026, 10, 26, 6, tzinfo=UTC),
        }
        with (
            patch.object(self.module, "get_current_context", return_value=context),
            patch.object(
                self.module, "_post", return_value={"id": "attempt", "status": "ingested"}
            ) as post,
        ):
            self.assertEqual(self.module.weather.task_dict["ingest"].python_callable(), "attempt")
            post.assert_called_once_with(
                "/v1/pipelines/weather/ingest",
                {"orchestrator_run_id": "weather:scheduled:test", "target_date": "2026-10-24"},
            )

    def test_verify_rejects_non_success(self):
        with patch.object(self.module, "_post", return_value={"status": "ingested"}):
            with self.assertRaisesRegex(RuntimeError, "did not pass"):
                self.module.river.task_dict["verify"].python_callable("attempt")

    def test_manual_weather_uses_persisted_trigger_time_without_an_interval(self):
        context = {
            "dag_run": SimpleNamespace(
                run_id="manual:test", run_after=datetime(2026, 10, 26, 9, tzinfo=UTC)
            ),
        }
        with (
            patch.object(self.module, "get_current_context", return_value=context),
            patch.object(
                self.module, "_post", return_value={"id": "manual", "status": "ingested"}
            ) as post,
        ):
            self.module.weather.task_dict["ingest"].python_callable()
            self.assertEqual(post.call_args.args[1]["target_date"], "2026-10-24")


if __name__ == "__main__":
    unittest.main(verbosity=2)
