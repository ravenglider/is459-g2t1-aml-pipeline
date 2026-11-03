"""Offline config, runtime injection and packaging tests; never start Spark/AWS."""

from copy import deepcopy
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import glue_sequence_job as job
from package_glue_modules import MODULES, build_package


def config():
    return dict(glue_version="5.0", dataset="sample", destination="development",
        run_id="sample_run_1", attempt_id="attempt_1", start_date="2022-09-01", end_date="2022-09-01",
        source_start_date="2022-09-01", source_end_date="2022-09-03",
        available_dates=["2022-09-01", "2022-09-02", "2022-09-03"], verified_empty_dates=[],
        timezone="UTC", timezone_verified=False, timezone_evidence="",
        coverage_verified=False, coverage_evidence="", source_inventory_fingerprint="a" * 64,
        limits={key: 0 for key in job.LIMIT_NAMES}, **job.OUTPUTS)


def verified():
    cfg = config()
    cfg.update(timezone_verified=True, timezone_evidence="Independent upstream parsing evidence",
               coverage_verified=True, coverage_evidence="Complete extract inventory reviewed")
    evidence = dict(dataset="sample", covered_dates=cfg["available_dates"],
        inventory_before="a" * 64, inventory_after="a" * 64, complete_inventory_verified=True,
        physical_schema_verified=True, required_fields_verified=True, refresh_coordination_confirmed=True,
        verification_reference="Synthetic evidence ONLY, not real source verification",
        identity_check=dict(dataset="sample", identity_check_passed=True,
                            verification_scope="provided_dataframe_only", dataset_wide_verified=False, row_count=10))
    return cfg, evidence


class GlueEntryTests(unittest.TestCase):
    def test_import_safety_without_cloud_or_spark_imports(self):
        script = '''
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'awsglue', 'boto3', 'botocore', 'pyspark'}:
            raise AssertionError('Forbidden import: ' + fullname)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
import glue_sequence_job, glue_sample_preflight, sequence_aws, sequence_s3, package_glue_modules
'''
        result = subprocess.run([sys.executable, "-I", "-c", script, str(ROOT)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_valid_offline_config_and_parser(self):
        cfg = config()
        original = deepcopy(cfg)
        plans = job.validate_config(cfg)
        self.assertEqual(len(plans), 1)
        self.assertEqual(len(plans[0].available_input_dates), 3)
        self.assertEqual(cfg, original)
        self.assertEqual(job.parse_arguments(["--config", "local.json"]).config, "local.json")
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            job.parse_arguments(["--execute"])

    def test_reject_wrong_datasets_destination_runtime_paths_and_ids(self):
        for key, values in {
            "dataset": ["HI-Medium", "LI-Medium", "sample' OR 1=1"],
            "destination": ["production"], "glue_version": ["4.0", "5.1", 5.0],
            "run_id": ["../run", "", "a/b"], "attempt_id": ["a\\b", "x" * 65],
            "timezone": ["Asia/Singapore", ""],
            "staging_root": [job.OUTPUTS["development_root"], "file:///tmp/test"],
            "metadata_root": [job.BUCKET + "bq2_sequences/"],
            "development_root": [job.BUCKET + "bq2_sequences/"],
            "source_inventory_fingerprint": ["", "A" * 64],
            "timezone_verified": ["true", 1],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    job.validate_config(dict(config(), **{key: value}))
        with self.assertRaises(ValueError):
            job.validate_config(dict(config(), extra="not allowed"))

    def test_dates_limits_missing_partitions_and_true_boundaries(self):
        for changes in (
            dict(start_date="20220901"), dict(start_date="2022-02-30"),
            dict(end_date="2022-08-31"), dict(available_dates=[]),
            dict(available_dates=["2022-09-01", "2022-09-03"]),
            dict(available_dates=["2022-09-01"] * 2),
            dict(available_dates=[{}]),
            dict(verified_empty_dates=["2022-09-01"]),
            dict(limits={}), dict(limits=dict(config()["limits"], max_pair_links=-1)),
            dict(limits=dict(config()["limits"], max_pair_links=True)),
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                job.validate_config(dict(config(), **changes))
        cfg = dict(config(), source_end_date="2022-09-01", available_dates=["2022-09-01"])
        plan, = job.validate_config(cfg)
        self.assertEqual(len(plan.unavailable_after_dataset_end), 2)
        cfg = dict(config(), available_dates=["2022-09-01"], verified_empty_dates=["2022-09-02", "2022-09-03"])
        self.assertTrue(job.validate_config(cfg)[0].ready_to_process)

    def test_execution_gates_and_evidence_not_promoted_to_global_certificate(self):
        cfg, evidence = verified()
        original = deepcopy(evidence)
        self.assertEqual(len(job.require_execution_prerequisites(cfg, evidence)), 1)
        self.assertEqual(evidence, original)
        self.assertFalse(evidence["identity_check"]["dataset_wide_verified"])
        for changes in (dict(timezone_verified=False), dict(coverage_verified=False)):
            with self.assertRaises(ValueError):
                job.require_execution_prerequisites(dict(cfg, **changes), evidence)
        for key, value in (list((key, False) for key in (
                "complete_inventory_verified", "physical_schema_verified", "required_fields_verified",
                "refresh_coordination_confirmed")) + [("inventory_after", "b" * 64),
                ("covered_dates", ["2022-09-01"]), ("identity_check", {}),
                ("identity_check", None), ("verification_reference", "")]):
            with self.subTest(key=key), self.assertRaises(ValueError):
                job.require_execution_prerequisites(cfg, dict(evidence, **{key: value}))
        # Invalid config is rejected before any Glue-dependent import.
        with self.assertRaises(ValueError):
            job.initialize_glue(config(), None, "is459-g2t1-bq2-sequence-builder")

    def test_offline_main_never_initializes_runtime(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.json"
            path.write_text(json.dumps(config()), encoding="utf-8")
            with patch.object(job, "initialize_glue", side_effect=AssertionError("must not initialize")), patch("sys.stdout", new_callable=io.StringIO) as output:
                self.assertEqual(job.main(["--config", str(path)]), 0)
                self.assertFalse(json.loads(output.getvalue())["aws_execution_enabled"])
        for path in ("s3://bucket/config.json", "\\\\server\\config.json"):
            with self.assertRaises(ValueError):
                job.main(["--config", path])

    def test_mock_runtime_versions_and_thin_batch_delegation(self):
        cfg, evidence = verified()
        context = MagicMock()
        context.spark_session.version = "3.5.4"
        context.spark_session.sparkContext._jvm.java.lang.System.getProperty.return_value = "17.0.20"
        context_type, job_type, spark_type = MagicMock(return_value=context), MagicMock(), MagicMock()
        modules = {"awsglue": types.ModuleType("awsglue"),
                   "awsglue.context": types.SimpleNamespace(GlueContext=context_type),
                   "awsglue.job": types.SimpleNamespace(Job=job_type),
                   "pyspark.context": types.SimpleNamespace(SparkContext=spark_type)}
        with patch.dict(sys.modules, modules), patch.object(job.sys, "version_info", (3, 11, 9)):
            actual, runtime_job = job.initialize_glue(cfg, evidence, "is459-g2t1-bq2-sequence-builder")
            self.assertIs(actual, context)
            runtime_job.init.assert_called_once()
            context.spark_session.conf.set.assert_called_once_with("spark.sql.session.timeZone", "UTC")
            context.spark_session.version = "3.5.6"
            with self.assertRaises(ValueError):
                job.initialize_glue(cfg, evidence, "is459-g2t1-bq2-sequence-builder")
        plan, = job.validate_config(cfg)
        batch_module = MagicMock()
        batch_module.process_start_date_batch.return_value.__enter__.return_value = "result"
        with patch.dict(sys.modules, {"sequence_batch": batch_module}):
            with job.prepared_batch("frame", cfg, evidence, plan) as result:
                self.assertEqual(result, "result")
            batch_module.process_start_date_batch.assert_called_once_with(
                "frame", "sample", plan, "UTC", batch_module.GrowthLimits.return_value)


class PackagingTests(unittest.TestCase):
    def test_deterministic_allowlist_and_exclusive_creation(self):
        with tempfile.TemporaryDirectory() as folder:
            first, second = (Path(folder) / name for name in ("a.zip", "b.zip"))
            self.assertEqual(build_package(first), build_package(second))
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with zipfile.ZipFile(first) as archive:
                self.assertEqual(archive.namelist(), sorted(MODULES))
                self.assertIn("sequence_s3.py", archive.namelist())
                self.assertIn("glue_sample_preflight.py", archive.namelist())
                self.assertTrue(all("/" not in name and name.endswith(".py") for name in archive.namelist()))
            with self.assertRaises(FileExistsError):
                build_package(first)

    def test_zip_import_in_isolated_interpreter_without_source_on_path(self):
        script = '''
import importlib, importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'awsglue', 'boto3', 'botocore'}:
            raise AssertionError('Forbidden cloud import: ' + fullname)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
for name in sys.argv[2:]:
    module = importlib.import_module(name[:-3])
    assert module.__file__.startswith(sys.argv[1]), module.__file__
from pyspark import SparkContext
assert SparkContext._active_spark_context is None
'''
        with tempfile.TemporaryDirectory() as folder:
            archive = Path(folder) / "modules.zip"
            build_package(archive)
            result = subprocess.run([sys.executable, "-I", "-c", script, str(archive), *MODULES],
                                    cwd=folder, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
