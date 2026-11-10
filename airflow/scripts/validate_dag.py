"""Import/render the real DAG in Docker without scheduling or running tasks."""

import json
import runpy
import unittest
import sys

import pendulum
from airflow.timetables.base import TimeRestriction


sys.path.insert(0, "/opt/airflow/dags")
namespace = runpy.run_path("/opt/airflow/dags/chaos_ecommerce_daily.py")
dag = namespace["dag"]
build = namespace["build"]
task_ids = ["validate_configuration", "prepare_dbt", "capture_counts", "generate_data", "load_raw", "check_source_freshness", "test_raw_sources", "snapshot_customers", "verify_counts"]


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
        self.assertEqual(set(dag.task_ids) - {t.task_id for t in build}, set(task_ids))
        self.assertEqual(dag.max_active_tasks, 4)

    def test_dependency_chain_and_retries(self):
        chain = task_ids[:-1]
        for index, task_id in enumerate(chain):
            expected = {chain[index + 1]} if index + 1 < len(chain) else {t.task_id for t in build.get_roots()}
            self.assertEqual(dag.get_task(task_id).downstream_task_ids, expected)
        for leaf in build.get_leaves():
            self.assertIn("verify_counts", leaf.downstream_task_ids)
        self.assertFalse(dag.get_task("verify_counts").downstream_task_ids)
        for task in dag.tasks:
            self.assertEqual(task.depends_on_past, task.task_id not in {"capture_counts", "verify_counts", "test_raw_sources"})
            self.assertEqual(task.retries, 2)
            self.assertFalse(task.do_xcom_push)
            if task.task_id.startswith("build_models."):
                self.assertTrue(task.ignore_first_depends_on_past)
            if task.task_id in task_ids:
                self.assertTrue(task.append_env)
                self.assertTrue(task.env["PATH"].startswith("/opt/pipeline-venv/bin:"))
            callbacks = task.on_failure_callback
            if not isinstance(callbacks, list):
                callbacks = [callbacks]
            self.assertEqual([callback.__name__ for callback in callbacks], ["notify_task_failure"])

    def test_cosmos_models_and_singular_tests_preserved(self):
        from pathlib import Path
        manifest = json.loads(Path("/opt/pipeline/manifests/manifest.json").read_text())
        expected_models = {key for key, node in manifest["nodes"].items() if node["resource_type"] == "model"}
        expected_singular_tests = {key for key, node in manifest["nodes"].items()
                                   if node["resource_type"] == "test" and node["original_file_path"].startswith("tests/")}
        filtered = build.dbt_graph.filtered_nodes
        self.assertTrue(expected_models <= filtered.keys())
        self.assertTrue(expected_singular_tests <= filtered.keys())
        self.assertFalse(any(node.resource_type.value == "snapshot" for node in filtered.values()))
        self.assertTrue(expected_singular_tests)
        self.assertIn("build_models.int_customer_snapshot_input.test", dag.task_ids)
        self.assertIn("build_models.revenue_line_items.test", dag.task_ids)

    def test_every_selected_test_has_a_delivery_task(self):
        nodes = build.dbt_graph.filtered_nodes
        selected_tests = [node for node in nodes.values() if node.resource_type.value == "test"]
        test_tasks = [task for task in build if type(task).__name__ == "DbtTestLocalOperator"]
        raw_command = dag.get_task("test_raw_sources").bash_command
        self.assertIn("--select source:raw --indirect-selection cautious", raw_command)
        self.assertTrue(selected_tests)
        for node in selected_tests:
            if all(parent.startswith("source.") for parent in node.depends_on):
                continue  # Explicit RAW test task covers these without freshness duplication.
            # Cosmos normalizes node.name and appends a hash; dbt selects the
            # original test name from the unique ID, without the hash suffix.
            selector = node.unique_id.split(".")[2]
            covered = any(task.select == selector for task in test_tasks)
            if not covered and len(node.depends_on) == 1:
                parent = node.depends_on[0].split(".")[-1]
                covered = any(task.select == parent for task in test_tasks)
            self.assertTrue(covered, node.unique_id)

    def test_cosmos_execution_and_dated_vars(self):
        task = dag.get_task("build_models.fct_order_items.run")
        self.assertEqual(str(task.dbt_executable_path), "/opt/pipeline-venv/bin/dbt")
        self.assertEqual(task.invocation_mode.value, "subprocess")
        self.assertTrue(task.ignore_first_depends_on_past)
        context = {"params": {"batch_date": "2026-11-08"}, "data_interval_start": None,
                   "run_id": "manual_test", "task": task}
        variables = task.render_template(task.vars, context, jinja_env=dag.get_template_env())
        self.assertEqual(variables, {"customer_snapshot_date": "2026-11-08"})
        env = task.render_template(task.env, context, jinja_env=dag.get_template_env())
        # Cosmos reads dbt results from its own per-task temporary project.
        # Overriding these paths would break its artifact/warning processing.
        self.assertNotIn("DBT_TARGET_PATH", env)
        self.assertNotIn("DBT_LOG_PATH", env)

    def test_dag_serializes(self):
        from airflow.serialization.serialized_objects import DagSerialization
        self.assertTrue(DagSerialization.to_dict(dag))

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
