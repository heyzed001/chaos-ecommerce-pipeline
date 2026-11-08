"""Import/render the real DAG in Docker without scheduling or running tasks."""

import json
import runpy
import unittest
import sys

import pendulum
from airflow.timetables.base import TimeRestriction


sys.path.insert(0, "/opt/airflow/dags")
dag = runpy.run_path("/opt/airflow/dags/chaos_ecommerce_daily.py")["dag"]
task_ids = ["validate_configuration", "prepare_dbt", "capture_counts", "generate_data", "load_raw", "check_source_freshness", "snapshot_customers", "build_models", "verify_counts"]


class DagChecks(unittest.TestCase):
    def test_count_checks_are_opt_in(self):
        self.assertFalse(dag.params["check_idempotency"])
        for name in ("capture_counts", "verify_counts"):
            self.assertIn('if [ "$CHECK_IDEMPOTENCY" = "true" ]', dag.get_task(name).bash_command)
            self.assertIn("{{ run_id", dag.get_task(name).env["COUNTS_BASELINE"])

    def test_success_callback_on_dag_only(self):
        callbacks = dag.on_success_callback
        if not isinstance(callbacks, list):
            callbacks = [callbacks]
        self.assertEqual([callback.__name__ for callback in callbacks], ["notify_dag_success"])
        self.assertTrue(all(not task.on_success_callback for task in dag.tasks))

    def test_paused_and_serial(self):
        self.assertEqual(dag.dag_id, "chaos_ecommerce_daily")
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertTrue(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(set(dag.task_ids), set(task_ids))

    def test_dependency_chain_and_retries(self):
        for index, task_id in enumerate(task_ids):
            task = dag.get_task(task_id)
            expected = {task_ids[index + 1]} if index + 1 < len(task_ids) else set()
            self.assertEqual(task.downstream_task_ids, expected)
            self.assertTrue(task.depends_on_past)
            self.assertEqual(task.retries, 2)
            self.assertFalse(task.do_xcom_push)
            self.assertTrue(task.append_env)
            self.assertTrue(task.env["PATH"].startswith("/opt/pipeline-venv/bin:"))
            callbacks = task.on_failure_callback
            if not isinstance(callbacks, list):
                callbacks = [callbacks]
            self.assertEqual([callback.__name__ for callback in callbacks], ["notify_task_failure"])

    def test_first_catchup_interval_follows_manual_history(self):
        info = dag.timetable.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=dag.start_date, latest=None, catchup=True),
        )
        self.assertEqual(info.data_interval.start.in_timezone("Africa/Lagos").to_date_string(), "2026-11-04")
        self.assertEqual(info.data_interval.end.in_timezone("Africa/Lagos").to_date_string(), "2026-11-05")
        self.assertEqual(info.data_interval.start.in_timezone("Africa/Lagos").strftime("%H:%M"), "10:20")
        self.assertEqual(info.run_after.in_timezone("Africa/Lagos").strftime("%H:%M"), "10:20")

    def render(self, batch_date, interval_start):
        task = dag.get_task("snapshot_customers")
        context = {
            "params": {"batch_date": batch_date},
            "data_interval_start": interval_start,
            "run_id": "scheduled__2026-11-05T10:20:00+01:00",
            "task": task,
        }
        return task.render_template(task.env, context, jinja_env=dag.get_template_env())

    def test_scheduled_batch_uses_interval_start_in_lagos(self):
        env = self.render(None, pendulum.datetime(2026, 11, 4, 9, 20, tz="UTC"))
        self.assertEqual(env["PIPELINE_BATCH_DATE"], "2026-11-04")
        self.assertEqual(json.loads(env["DBT_VARS"]), {"customer_snapshot_date": "2026-11-04"})

    def test_manual_date_overrides_interval(self):
        env = self.render("2026-11-05", pendulum.datetime(2026, 11, 6, 10, 20, tz="Africa/Lagos"))
        self.assertEqual(env["PIPELINE_BATCH_DATE"], "2026-11-05")
        self.assertEqual(json.loads(env["DBT_VARS"]), {"customer_snapshot_date": "2026-11-05"})

    def test_manual_run_without_interval_requires_an_override(self):
        self.assertEqual(self.render(None, None)["PIPELINE_BATCH_DATE"], "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
