"""Fake read clients plus small genuine Spark fixtures. Never AWS/network writes."""

from copy import deepcopy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import subprocess
import sys
import types
import traceback
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import glue_sample_preflight as preflight
import sequence_aws as adapter
from sequence_source import CLEANED_SCHEMA
from test_sequence_s3 import FakeS3


def config():
    return dict(account_id="123456789012", region="us-east-1", dataset="sample", run_id="preflight_1",
        timezone="UTC", timezone_evidence="Synthetic Amareet UTC parsing confirmation",
        no_refresh_acknowledged=True, no_refresh_evidence="Synthetic coordinated refresh pause",
        builder_fingerprint="a" * 64)


def cli_args():
    return ["--EXPECTED_ACCOUNT_ID", config()["account_id"], "--RUN_ID", "test",
            "--TIMEZONE_EVIDENCE", "synthetic", "--NO_REFRESH_EVIDENCE", "synthetic",
            "--NO_REFRESH_ACKNOWLEDGED", "true", "--BUILDER_FINGERPRINT", "a" * 64]


class FakeCatalog:
    def __init__(self):
        fields = [dict(Name=f.name, Type=f.dataType.simpleString()) for f in CLEANED_SCHEMA]
        self.table = dict(Name=adapter.TABLE,
            StorageDescriptor=dict(Location=adapter.SOURCE_ROOT, Columns=fields[:-2], InputFormat="parquet"),
            PartitionKeys=fields[-2:], Parameters=dict(classification="parquet"))
        self.partitions = [dict(Values=["sample", day], StorageDescriptor=dict(
            self.table["StorageDescriptor"], Location=adapter.SOURCE_ROOT + "dataset=sample/txn_date=" + day + "/"))
            for day in adapter.SAMPLE_DATES]
        self.calls = []
        self.meta = MagicMock(region_name="us-east-1")

    def get_table(self, **kwargs):
        if kwargs != dict(DatabaseName=adapter.DATABASE, Name=adapter.TABLE):
            raise AssertionError("unexpected table")
        self.calls.append(("get_table", kwargs))
        return dict(Table=deepcopy(self.table))

    def get_partitions(self, **kwargs):
        if (kwargs.get("DatabaseName"), kwargs.get("TableName"), kwargs.get("Expression")) != (
                adapter.DATABASE, adapter.TABLE, "dataset = 'sample'"):
            raise AssertionError("not sample-only")
        self.calls.append(("get_partitions", kwargs))
        start = int(kwargs.get("NextToken", 0))
        response = dict(Partitions=deepcopy(self.partitions[start:start + 2]))
        if start + 2 < len(self.partitions):
            response["NextToken"] = str(start + 2)
        return response


def sample_s3():
    client = FakeS3()
    client.meta = MagicMock(region_name="us-east-1")
    for day in adapter.SAMPLE_DATES:
        client.seed("transactions_clean/dataset=sample/txn_date=" + day + "/part.snappy.parquet")
    return client


class PreflightMetadataTests(unittest.TestCase):
    def setUp(self):
        self.catalog, self.s3 = FakeCatalog(), sample_s3()
        self.diagnostics = io.StringIO()
        capture = patch("sys.stdout", self.diagnostics)
        capture.start()
        self.addCleanup(capture.stop)

    def test_import_safe_without_spark_or_cloud_libraries(self):
        script = '''
import sys, importlib.abc
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'boto3','botocore','awsglue','pyspark'}:
            raise AssertionError('Forbidden dependency: ' + fullname)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
import glue_sample_preflight
'''
        result = subprocess.run([sys.executable, "-I", "-c", script, str(Path(__file__).resolve().parents[1])],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_config_account_sample_region_evidence_and_no_refresh_gates(self):
        preflight.validate_config(config())
        for changes in (dict(dataset="HI-Medium"), dict(dataset="LI-Medium"), dict(region="ap-southeast-1"),
                        dict(account_id="wrong"), dict(run_id="../unsafe"), dict(timezone="Asia/Singapore"),
                        dict(timezone_evidence=""), dict(no_refresh_acknowledged=False),
                        dict(no_refresh_evidence=""), dict(builder_fingerprint="")):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                preflight.validate_config(dict(config(), **changes))
        clients = MagicMock()
        with self.assertRaises(ValueError):
            preflight.run_preflight(dict(config(), dataset="HI-Medium"), glue_context=clients,
                catalog_client=clients, s3_client=clients, identity_client=clients)
        self.assertEqual(clients.mock_calls, [])

    def test_exact_sample_catalog_pages_s3_paths_and_fingerprints(self):
        snapshot = adapter.inspect_sample_source(self.catalog, self.s3)
        self.assertEqual(snapshot["dates"], list(adapter.SAMPLE_DATES))
        self.assertEqual(len(snapshot["files"]), 6)
        self.assertEqual(sum(name == "get_partitions" for name, _ in self.catalog.calls), 3)
        self.assertTrue(all(uri.startswith(adapter.SOURCE_ROOT + "dataset=sample/") for uri in snapshot["files"]))
        self.assertTrue(all(name != "put" for name, _ in self.s3.calls))
        reordered = FakeCatalog()
        reordered.partitions.reverse()
        self.assertEqual(snapshot["partitions_fingerprint"], adapter.inspect_sample_source(reordered, self.s3)["partitions_fingerprint"])

    def test_missing_duplicate_unexpected_or_mislocated_partitions_fail(self):
        for kind in ("missing", "duplicate", "extra", "other_dataset", "location"):
            catalog = FakeCatalog()
            if kind == "missing":
                catalog.partitions.pop()
            elif kind == "duplicate":
                catalog.partitions.append(catalog.partitions[0])
            elif kind == "extra":
                extra = deepcopy(catalog.partitions[-1])
                extra["Values"][1] = "2022-09-07"
                extra["StorageDescriptor"]["Location"] = adapter.SOURCE_ROOT + "dataset=sample/txn_date=2022-09-07/"
                catalog.partitions.append(extra)
            elif kind == "other_dataset":
                catalog.partitions[0]["Values"][0] = "HI-Medium"
            else:
                catalog.partitions[0]["StorageDescriptor"]["Location"] = "s3://wrong/"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                adapter.inspect_sample_source(catalog, self.s3)

    def test_missing_or_unexpected_s3_objects_and_catalog_metadata_changes(self):
        snapshot = adapter.inspect_sample_source(self.catalog, self.s3)
        key = next(iter(self.s3.objects))
        del self.s3.objects[key]
        with self.assertRaises(ValueError):
            adapter.inspect_sample_source(self.catalog, self.s3)
        self.s3.seed(key)
        self.assertNotEqual(snapshot["source_fingerprint"], adapter.inspect_sample_source(self.catalog, self.s3)["source_fingerprint"])
        self.s3.seed("transactions_clean/dataset=sample/_temporary/file.parquet")
        with self.assertRaises(ValueError):
            adapter.inspect_sample_source(self.catalog, self.s3)
        self.catalog.table["Parameters"]["version"] = "changed"
        changed = adapter.inspect_sample_source(self.catalog, sample_s3())
        self.assertNotEqual(snapshot["table_fingerprint"], changed["table_fingerprint"])

    def test_physical_file_schema_rejected_without_cast_or_wrong_dataset_reads(self):
        from pyspark.sql.types import StructType, StructField, DoubleType
        spark = MagicMock()
        reader = spark.newSession.return_value.read.option.return_value.parquet.return_value
        reader.schema = StructType(CLEANED_SCHEMA.fields[:-2])
        uri = adapter.SOURCE_ROOT + "dataset=sample/txn_date=2022-09-01/p.parquet"
        adapter.validate_sample_physical_schema(spark, [uri])
        wrong = list(CLEANED_SCHEMA.fields[:-2])
        wrong[0] = StructField("txn_id", DoubleType())
        reader.schema = StructType(wrong)
        with self.assertRaisesRegex(ValueError, "schema"):
            adapter.validate_sample_physical_schema(spark, [uri])
        untouched = MagicMock()
        for files in ([], [uri, uri], [uri.replace("dataset=sample", "dataset=HI-Medium")]):
            with self.assertRaises(ValueError):
                adapter.validate_sample_physical_schema(untouched, files)
        untouched.newSession.assert_not_called()

    def test_offline_cli_has_no_implicit_runtime_and_rejects_unknown_arguments(self):
        args = cli_args()
        with patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(preflight.main(args), 0)
            self.assertFalse(json.loads(output.getvalue())["aws_reads_enabled"])
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            preflight.parse_arguments(args + ["--GENERATE_SEQUENCES", "true"])

    def test_explicit_activation_formats_alias_and_duplicate_rejection(self):
        self.assertEqual(preflight.parse_arguments(cli_args()).ALLOW_AWS_READS, "false")
        for extra in (["--ALLOW_AWS_READS=true"], ["--ALLOW_AWS_READS", "true"],
                      ["--allow-aws-reads", "true"]):
            with self.subTest(extra=extra):
                self.assertEqual(preflight.parse_arguments(cli_args() + extra).ALLOW_AWS_READS, "true")
        bad = [["--ALLOW_AWS_READS"], ["--allow-aws-reads"], ["--ALLOW_AWS_READS="],
               ["--ALLOW_AWS_READS", "TRUE"], ["--ALLOW_AWS_READS", "1"],
               ["--ALLOW_AWS_READS", "yes"], ["--ALLOW_AWS_R=true"],
               ["--ALLOW_AWS_READS=true", "--ALLOW_AWS_READS=false"],
               ["--ALLOW_AWS_READS=true", "--ALLOW_AWS_READS=true"],
               ["--allow-aws-reads", "true", "--ALLOW_AWS_READS", "false"],
               ["--DATASET", "sample", "--DATASET", "HI-Medium"]]
        for extra in bad:
            with self.subTest(extra=extra), patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit) as caught:
                preflight.parse_arguments(cli_args() + extra)
            self.assertEqual(caught.exception.code, 2)

    def test_exact_glue_console_shape_and_allowlisted_engine_arguments(self):
        args = preflight.parse_arguments(cli_args() + [
            "--ALLOW_AWS_READS", "true", "--JOB_NAME", preflight.JOB_NAME, "--JOB_RUN_ID", "jr_fake",
            "--JOB_ID", "job_fake", "--TempDir", "s3://not-accessed/temp/",
            "--extra-py-files", "s3://not-accessed/modules.zip", "--job-bookmark-option", "job-bookmark-disable",
            "--job-language", "python", "--enable-metrics", "--enable-job-insights", "true",
            "--enable-observability-metrics", "false", "--conf", "spark.a=b", "--conf", "spark.c=d"])
        self.assertEqual(args.ALLOW_AWS_READS, "true")
        self.assertEqual(args.JOB_NAME, preflight.JOB_NAME)
        self.assertEqual(args.enable_metrics, "true")
        self.assertEqual(args.conf, ["spark.a=b", "spark.c=d"])
        for extra in (["--ROLE5_UNKNOWN", "value"], ["--DATASE", "sample"],
                      ["--JOB_NAME", preflight.JOB_NAME, "--JOB_NAME", preflight.JOB_NAME]):
            with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
                preflight.parse_arguments(cli_args() + extra)

    def test_documented_glue5_reserved_and_logging_arguments(self):
        argv = cli_args() + ["--JOB_NAME", preflight.JOB_NAME, "--JOB_RUN_ID", "jr_simulated",
            "--ALLOW_AWS_READS=true", "--encryption-type", "sse-s3",
            "--RedshiftTempDir", "s3://not-accessed/temp/", "--continuation-option", "continuation-ignore",
            "--job-bookmark-from", "jr_old", "--job-bookmark-to", "jr_new",
            "--enable-continuous-log-filter", "true", "--enable-glue-datacatalog", "",
            "--enable-s3-parquet-optimized-committer", "true", "--enable-data-lineage", "false",
            "--continuous-log-logGroup", "synthetic-group", "--continuous-log-logStreamPrefix", "synthetic-stream",
            "--continuous-log-conversionPattern", "%m%n", "--custom-logGroup-prefix", "synthetic-prefix",
            "--custom-logStream-prefix", "synthetic-prefix"]
        parsed = preflight.parse_arguments(argv)
        self.assertEqual(parsed.ALLOW_AWS_READS, "true")
        self.assertEqual(parsed.encryption_type, "sse-s3")
        self.assertEqual(parsed.enable_glue_datacatalog, "")
        self.assertEqual(parsed.enable_continuous_log_filter, "true")
        # Arbitrary extras still fail, even in an otherwise valid Glue invocation.
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(preflight.PreflightArgumentError) as caught:
            preflight.parse_arguments(argv + ["--NEW_UNREVIEWED_OPTION", "private-value"])
        self.assertEqual(caught.exception.diagnostic["arguments"], ["--NEW_UNREVIEWED_OPTION"])

    def test_observed_glue5_internal_arguments_consume_exact_six_tokens_opaquely(self):
        observed = ["--glue-di-packages-correlation-ids", "SYNTHETIC_PRIVATE_CORRELATION_VALUE",
                    "--internal-lib-urls", "https://example.invalid/private?token=SYNTHETIC_PRIVATE_URL_VALUE",
                    "--tenant-internal", "SYNTHETIC_PRIVATE_TENANT_VALUE"]
        self.assertEqual(len(observed), 6)
        with patch("sys.stdout", new_callable=io.StringIO) as out, patch("sys.stderr", new_callable=io.StringIO) as err:
            parsed = preflight.parse_arguments(cli_args() + ["--JOB_NAME", preflight.JOB_NAME,
                                                              "--ALLOW_AWS_READS=true"] + observed)
        self.assertEqual(parsed.ALLOW_AWS_READS, "true")
        self.assertEqual(parsed.DATASET, "sample")
        self.assertEqual(parsed.REGION, "us-east-1")
        for name in ("glue_di_packages_correlation_ids", "internal_lib_urls", "tenant_internal"):
            self.assertFalse(hasattr(parsed, name))
        for value in observed[1::2]:
            self.assertNotIn(value, repr(vars(parsed)) + out.getvalue() + err.getvalue())
        self.assertEqual(out.getvalue(), "")
        self.assertEqual(err.getvalue(), "")

    def test_observed_internals_have_bounded_optional_values_and_do_not_activate_reads(self):
        names = ["--glue-di-packages-correlation-ids", "--internal-lib-urls", "--tenant-internal"]
        for runtime in (names, [name + "=" for name in names],
                        [name + '=opaque,{"future":"shape"}' for name in names],
                        [name + "=--ALLOW_AWS_READS=true" for name in names]):
            with self.subTest(runtime=runtime):
                self.assertEqual(preflight.parse_arguments(cli_args() + runtime).ALLOW_AWS_READS, "false")
                enabled = preflight.parse_arguments(cli_args() + runtime + ["--ALLOW_AWS_READS", "true"])
                self.assertEqual(enabled.ALLOW_AWS_READS, "true")
        # An absent opaque value cannot swallow or replace a required safety value.
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(preflight.PreflightArgumentError):
            preflight.parse_arguments(cli_args() + names + ["--ALLOW_AWS_READS"])

    def test_observed_internals_do_not_hide_unknown_options_or_duplicate_activation(self):
        observed = ["--glue-di-packages-correlation-ids", "PRIVATE_CORRELATION",
                    "--internal-lib-urls", "PRIVATE_INTERNAL_URL", "--tenant-internal", "PRIVATE_TENANT"]
        for extra in (["--tenant-internal-unreviewed", "PRIVATE_UNKNOWN_VALUE"],
                      ["--ROLE5_UNKNOWN=PRIVATE_UNKNOWN_VALUE"],
                      ["--ALLOW_AWS_READS=true", "--allow-aws-reads=false"],
                      ["--ALLOW_AWS_READS=true", "--ALLOW_AWS_READS=true"]):
            with self.subTest(extra=extra), patch("sys.stdout", new_callable=io.StringIO) as out, patch(
                    "sys.stderr", new_callable=io.StringIO) as err, self.assertRaises(preflight.PreflightArgumentError) as caught:
                preflight.parse_arguments(cli_args() + observed + extra)
            self.assertEqual(caught.exception.code, 2)
            self.assertFalse(caught.exception.diagnostic["values_logged"])
            self.assertNotIn("PRIVATE_", out.getvalue() + err.getvalue() + str(caught.exception))
            if extra[0] == "--tenant-internal-unreviewed":
                self.assertEqual(caught.exception.diagnostic["arguments"], ["--tenant-internal-unreviewed"])
                self.assertEqual(caught.exception.diagnostic["unparsed_count"], 2)

    def test_argument_failure_diagnostics_never_log_values_or_raw_argparse_context(self):
        secret = "SYNTHETIC_PRIVATE_VALUE_98765"
        for extra, reason, option in (
            (["--ALLOW_AWS_READS=" + secret], "invalid_value", "--ALLOW_AWS_READS"),
            (["--ROLE5_UNKNOWN=" + secret], "unrecognized_arguments", "--ROLE5_UNKNOWN"),
            (["--ALLOW_AWS_READS"], "missing_value", "--ALLOW_AWS_READS"),
            (["--ALLOW_AWS_READS=true", "--allow-aws-reads=false"], "duplicate_argument", "--allow-aws-reads"),
            ([secret], "unrecognized_arguments", None),
        ):
            with self.subTest(extra=extra), patch("sys.stdout", new_callable=io.StringIO) as out, patch(
                    "sys.stderr", new_callable=io.StringIO) as err:
                try:
                    preflight.parse_arguments(cli_args() + extra)
                except preflight.PreflightArgumentError as exc:
                    self.assertEqual(exc.code, 2)
                    self.assertEqual(exc.diagnostic["reason"], reason)
                    if option:
                        self.assertIn(option, exc.diagnostic["arguments"])
                    rendered = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                    for output in (out.getvalue(), err.getvalue(), str(exc), rendered):
                        self.assertNotIn(secret, output)
                        self.assertNotIn(config()["account_id"], output)
                        self.assertNotIn("invalid choice:", output)
                    self.assertIn("PREFLIGHT_ARGUMENTS |", out.getvalue())
                    self.assertIn("ARGUMENT_ERROR", err.getvalue())
                    self.assertFalse(exc.diagnostic["values_logged"])
                else:
                    self.fail("Malformed argument unexpectedly accepted")

    def test_missing_required_values_and_inline_duplicates_have_safe_names(self):
        incomplete = cli_args()
        index = incomplete.index("--EXPECTED_ACCOUNT_ID")
        del incomplete[index:index + 2]
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(preflight.PreflightArgumentError) as caught:
            preflight.parse_arguments(incomplete)
        self.assertEqual(caught.exception.diagnostic["reason"], "missing_required_arguments")
        self.assertIn("--EXPECTED_ACCOUNT_ID", caught.exception.diagnostic["arguments"])
        for extra in (["--RUN_ID=private-duplicate"], ["--ALLOW_AWS_READS=true", "--ALLOW_AWS_READS=true"],
                      ["--ALLOW_AWS_READS", "TRUE"], ["--ALLOW_AWS_READS="]):
            with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(preflight.PreflightArgumentError):
                preflight.parse_arguments(cli_args() + extra)

    def test_missing_or_false_activation_never_reports_success_or_initializes_sdk(self):
        sdk = MagicMock()
        for activation in ([], ["--ALLOW_AWS_READS=false"], ["--allow-aws-reads", "false"]):
            for injected in ([], ["--JOB_NAME", preflight.JOB_NAME], ["--JOB_RUN_ID", "jr_fake"], ["--JOB_ID", "job_fake"]):
                with self.subTest(activation=activation, injected=injected), patch.dict(sys.modules, {"boto3": sdk}), patch(
                        "sys.stdout", new_callable=io.StringIO) as output:
                    self.assertEqual(preflight.main(cli_args() + activation + injected), 2 if injected else 0)
                    report = json.loads(output.getvalue())
                    self.assertEqual(report["status"], "NOT_RUN")
                    self.assertFalse(report["preflight_completed"])
                    self.assertFalse(report["aws_reads_enabled"])
                    self.assertNotIn("PREFLIGHT |", output.getvalue())
        sdk.Session.assert_not_called()

    def test_explicit_true_runtime_is_mocked_and_account_checked_before_spark(self):
        sdk, identity, context_type, context, job_type, runtime_job, spark_type = [MagicMock() for _ in range(7)]
        sdk.Session.return_value.client.side_effect = lambda name: identity if name == "sts" else MagicMock()
        identity.get_caller_identity.return_value = dict(Account=config()["account_id"])
        context_type.return_value = context
        job_type.return_value = runtime_job
        modules = {"boto3": sdk, "awsglue": types.ModuleType("awsglue"),
                   "awsglue.context": types.SimpleNamespace(GlueContext=context_type),
                   "awsglue.job": types.SimpleNamespace(Job=job_type),
                   "pyspark.context": types.SimpleNamespace(SparkContext=spark_type)}
        argv = cli_args() + ["--JOB_NAME", preflight.JOB_NAME, "--ALLOW_AWS_READS=true"]
        with patch.dict(sys.modules, modules), patch.object(preflight, "run_preflight", return_value={"status": "VALIDATED_READ_ONLY"}) as run, patch(
                "sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(preflight.main(argv), 0)
            sdk.Session.assert_called_once_with(region_name="us-east-1")
            self.assertEqual(run.call_args.args[0]["dataset"], "sample")
            self.assertIn("PREFLIGHT |", output.getvalue())
            runtime_job.init.assert_called_once_with(preflight.JOB_NAME, {"JOB_NAME": preflight.JOB_NAME, "DATASET": "sample"})
            runtime_job.commit.assert_called_once()
            context_type.reset_mock()
            identity.get_caller_identity.return_value = dict(Account="999999999999")
            with self.assertRaisesRegex(ValueError, "Wrong AWS account"):
                preflight.main(argv)
            context_type.assert_not_called()


class PreflightOrchestrationTests(unittest.TestCase):
    """Mock analytical work; assert orchestration reads only and cleans own cache."""
    def setUp(self):
        self.catalog, self.s3 = FakeCatalog(), sample_s3()
        self.identity, self.context, self.frame = MagicMock(), MagicMock(), MagicMock()
        self.identity.meta.region_name = "us-east-1"
        self.identity.get_caller_identity.return_value = dict(Account=config()["account_id"])
        self.context.spark_session.version = "3.5.4"
        self.context.spark_session.conf.get.return_value = "UTC"
        self.context.spark_session.sparkContext._jvm.java.lang.System.getProperty.return_value = "17.0.20"
        self.metrics = dict(total_rows=500000, laundering_rows=35, pair_candidates=dict(conservative_upper_bound=123))

    def run_job(self, *, failure=None, caller_cached=False, after_change=False):
        def persist(frame, owned):
            if not caller_cached:
                owned.append(frame)
            return frame
        def profile(frame):
            if failure:
                raise failure
            if after_change:
                self.s3.seed(next(iter(self.s3.objects)), b"changed")
            return self.metrics
        with patch("sequence_aws.validate_sample_physical_schema"), patch(
                "sequence_aws.read_catalog_selected", return_value=self.frame) as read, patch(
                "sequence_batch._persist_owned", side_effect=persist), patch.object(
                preflight, "validate_and_profile_frame", side_effect=profile):
            report = preflight.run_preflight(config(), glue_context=self.context, catalog_client=self.catalog,
                s3_client=self.s3, identity_client=self.identity)
        return report, read

    def test_success_report_exact_pruned_read_owned_cleanup_and_no_writes(self):
        report, read = self.run_job()
        self.assertEqual(report["status"], "VALIDATED_READ_ONLY")
        self.assertEqual(report["metrics"]["total_rows"], 500000)
        self.assertEqual(report["runtime"]["spark"], "3.5.4")
        self.assertFalse(report["report_storage_enabled"])
        self.assertEqual(read.call_args.args[1:], ("sample", adapter.SAMPLE_DATES))
        self.assertEqual(report["coverage_evidence"]["checked_dates"], list(adapter.SAMPLE_DATES))
        self.frame.unpersist.assert_called_once_with(blocking=True)
        self.context.spark_session.stop.assert_not_called()
        self.assertFalse(any(name == "put" for name, _ in self.s3.calls))

    def test_failure_report_and_inventory_change_release_owned_cache(self):
        for kwargs in (dict(failure=ValueError("invalid sample")), dict(after_change=True)):
            self.frame.reset_mock()
            with self.subTest(kwargs=kwargs), self.assertRaises(preflight.PreflightFailure) as caught:
                self.run_job(**kwargs)
            self.assertEqual(caught.exception.report["status"], "FAILED")
            self.assertIn("error", caught.exception.report)
            self.frame.unpersist.assert_called_once_with(blocking=True)

    def test_caller_owned_cache_and_session_remain_intact(self):
        self.run_job(caller_cached=True)
        self.frame.unpersist.assert_not_called()
        self.context.spark_session.stop.assert_not_called()

    def test_wrong_account_runtime_and_client_region_stop_before_scan(self):
        self.identity.get_caller_identity.return_value = dict(Account="999999999999")
        with self.assertRaises(preflight.PreflightFailure):
            self.run_job()
        self.assertEqual(self.catalog.calls, [])
        self.identity.get_caller_identity.return_value = dict(Account=config()["account_id"])
        self.catalog.meta.region_name = "ap-southeast-1"
        with self.assertRaises(preflight.PreflightFailure):
            self.run_job()
        self.catalog.meta.region_name = "us-east-1"
        self.context.spark_session.version = "3.5.6"
        with self.assertRaises(preflight.PreflightFailure):
            self.run_job()
        self.assertEqual(self.catalog.calls, [])

    def test_generation_staging_and_publication_functions_are_never_called(self):
        targets = ["bq2_sequence_builder.build_two_hop_links", "bq2_sequence_builder.build_three_hop_links",
                   "bq2_sequence_builder.build_sequence_output", "sequence_batch.process_start_date_batch",
                   "sequence_staging.stage_sequence_batch_local", "sequence_s3.stage_sequence_batch_s3",
                   "sequence_s3.write_manifest_s3"]
        from contextlib import ExitStack
        with ExitStack() as stack:
            mocks = [stack.enter_context(patch(target, side_effect=AssertionError("forbidden"))) for target in targets]
            self.run_job()
            for mocked in mocks:
                mocked.assert_not_called()


class RealSparkPreflightTests(unittest.TestCase):
    """Actual DataFrame validation/degree estimation; no Parquet or cloud IO."""
    @classmethod
    def setUpClass(cls):
        import os
        from pyspark.sql import SparkSession
        from fixtures import transaction
        previous = os.environ.get("PYSPARK_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        def restore():
            if previous is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = previous
        cls.addClassCleanup(restore)
        cls.spark = (SparkSession.builder.master("local[2]").appName("sample-preflight-tests")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.rows = [transaction(-(2**63) + 1 if i == 1 else 2**63 - i, f"2022-09-{i:02d} 09:00",
            (20, "B") if i == 2 else (100 + i, "sender"),
            (20, "B") if i == 1 else (200 + i, "receiver"), label=int(i == 1)) for i in range(1, 7)]
        # Explicit UTC Python datetimes prevent the Windows host timezone from
        # interpreting naive synthetic timestamps before Spark receives them.
        for row in cls.rows:
            row["txn_ts"] = row["txn_ts"].replace(tzinfo=timezone.utc)

    def frame(self, rows):
        # Tests own these tiny reused fixtures; keep Python-worker setup out of
        # every repeated validation action and release each cache after its test.
        frame = self.spark.createDataFrame(rows, CLEANED_SCHEMA).cache()
        self.addCleanup(frame.unpersist)
        return frame

    def profile(self, rows=None, **changes):
        options = dict(expected_rows=6, expected_laundering=1)
        options.update(changes)
        return preflight.validate_and_profile_frame(self.frame(self.rows if rows is None else rows),
                                                    **options)

    def test_complete_synthetic_sample_metrics_bigints_and_exact_candidate_bound(self):
        metrics = self.profile()
        self.assertEqual(metrics["total_rows"], 6)
        self.assertEqual(metrics["laundering_rows"], 1)
        self.assertEqual(metrics["pair_candidates"]["conservative_upper_bound"], 1)
        self.assertFalse(metrics["pair_candidates"]["pairs_materialized"])
        self.assertFalse(metrics["identity_check"]["dataset_wide_verified"])
        self.assertEqual(metrics["timestamp_coverage"]["first_ts"], "2022-09-01 09:00:00")

    def test_catalog_reader_real_frame_expands_tuple_dates_without_literal_conversion(self):
        context = MagicMock()
        context.spark_session = self.spark
        context.create_dynamic_frame.from_catalog.return_value.toDF.return_value = self.frame(self.rows)
        actual = adapter.read_catalog_selected(context, "sample", adapter.SAMPLE_DATES,
            available_dates=adapter.SAMPLE_DATES, timezone="UTC", timezone_verified=True,
            timezone_evidence="synthetic only", catalog_table=FakeCatalog().table)
        self.assertEqual(actual.count(), 6)
        predicate = context.create_dynamic_frame.from_catalog.call_args.kwargs["push_down_predicate"]
        self.assertEqual(predicate, "dataset = 'sample' AND txn_date IN (" +
                         ", ".join("'" + day + "'" for day in adapter.SAMPLE_DATES) + ")")

    def test_duplicate_ids_on_distant_dates_and_timestamp_partition_inconsistency(self):
        rows = deepcopy(self.rows)
        rows[-1]["txn_id"] = rows[0]["txn_id"]
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.profile(rows)
        rows = deepcopy(self.rows)
        rows[0]["txn_date"] = "2022-09-02"
        with self.assertRaisesRegex(ValueError, "txn_date"):
            self.profile(rows)
        rows = deepcopy(self.rows)
        rows[0]["is_laundering"] = 2
        with self.assertRaisesRegex(ValueError, "is_laundering"):
            self.profile(rows)

    def test_expected_baseline_and_observed_date_discrepancies_stop(self):
        with self.assertRaisesRegex(ValueError, "baseline"):
            preflight.validate_and_profile_frame(self.frame(self.rows))
        with self.assertRaisesRegex(ValueError, "baseline"):
            preflight.validate_and_profile_frame(self.frame(self.rows), expected_rows=6, expected_laundering=35)
        with self.assertRaisesRegex(ValueError, "date coverage"):
            self.profile(self.rows[:-1])


if __name__ == "__main__":
    unittest.main()
