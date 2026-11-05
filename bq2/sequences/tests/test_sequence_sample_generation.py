"""Offline orchestration, local baseline gates and real Spark integration tests."""
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sequence_sample_generation as generation
from test_glue_sample_preflight import FakeCatalog, sample_s3, config as preflight_config
from sequence_aws import inspect_sample_source, SAMPLE_DATES
from sequence_batch import GrowthLimitExceeded


def config(snapshot):
    return dict(preflight_config(), owner_date=generation.OWNER_DATE,
        limits={name: 1000000 for name in generation.LIMIT_NAMES},
        expected_source={key: snapshot[key] for key in generation.FINGERPRINTS})


class GenerationTests(unittest.TestCase):
    def setUp(self):
        self.catalog, self.s3 = FakeCatalog(), sample_s3()
        self.snapshot = inspect_sample_source(self.catalog, self.s3)
        self.cfg = config(self.snapshot)
        self.context, self.identity, self.source, self.output = [MagicMock() for _ in range(4)]
        self.identity.meta.region_name = "us-east-1"
        self.identity.get_caller_identity.return_value = {"Account": self.cfg["account_id"]}
        self.events = []
        self.metrics = dict(starting_pairs=2, actual_triples=3, output_rows=5)
        self.catalog.calls.clear()
        self.s3.calls.clear()

    @contextmanager
    def harness(self, *, batch_error=None, validation_error=None, snapshots=None, profile_error=None):
        @contextmanager
        def batch(source, dataset, plan, timezone, limits):
            self.events.append("batch_enter")
            try:
                if batch_error:
                    raise batch_error
                yield MagicMock(output=self.output, metrics=self.metrics)
            finally:
                self.events.append("batch_cleanup")
        def persist(frame, owned):
            owned.append(frame)
            return frame
        forbidden = ["sequence_s3.stage_sequence_batch_s3", "sequence_s3.write_manifest_s3",
            "sequence_staging.stage_sequence_batch_local", "sequence_publication.plan_development_publication"]
        with ExitStack() as stack:
            self.forbidden = [stack.enter_context(patch(name, side_effect=AssertionError("no writes/publication"))) for name in forbidden]
            self.inspect = stack.enter_context(patch("sequence_aws.inspect_sample_source",
                side_effect=snapshots or [deepcopy(self.snapshot) for _ in range(3)]))
            stack.enter_context(patch("glue_sample_preflight._runtime_versions", return_value={"spark": "3.5.4"}))
            self.physical = stack.enter_context(patch("sequence_aws.validate_sample_physical_schema"))
            self.read = stack.enter_context(patch("sequence_aws.read_catalog_selected", return_value=self.source))
            self.profile = stack.enter_context(patch("glue_sample_preflight.validate_and_profile_frame",
                return_value={"identity_check": {"dataset_wide_verified": False,
                    "verification_scope": "provided_dataframe_only"}}, side_effect=profile_error))
            stack.enter_context(patch("sequence_batch._persist_owned", side_effect=persist))
            self.batch = stack.enter_context(patch("sequence_batch.process_start_date_batch", side_effect=batch))
            self.validate = stack.enter_context(patch("sequence_staging.validate_sequence_output",
                return_value={"counts": {2: 2, 3: 3}}, side_effect=validation_error))
            self.column = MagicMock()
            stack.enter_context(patch("pyspark.sql.functions.col", return_value=self.column))
            yield
        for function in self.forbidden:
            function.assert_not_called()
        self.assertFalse(any(call[0] == "put" for call in self.s3.calls))
        self.assertEqual(self.catalog.calls, [])

    def operation(self, cfg=None):
        return generation.generate_sample_sequences(cfg or self.cfg, glue_context=self.context,
            catalog_client=self.catalog, s3_client=self.s3, identity_client=self.identity)

    def test_success_exact_scope_counts_and_lifetime(self):
        with self.harness():
            with self.operation() as result:
                self.assertIs(result.output, self.output)
                self.assertEqual(result.report["status"], "OUTPUT_VALIDATED_CONTEXT_OPEN")
                self.source.unpersist.assert_not_called()
                self.assertNotIn("batch_cleanup", self.events)
            self.assertEqual(result.report["status"], "GENERATION_COMPLETE")
            self.source.unpersist.assert_called_once_with(blocking=True)
            self.assertEqual(self.events, ["batch_enter", "batch_cleanup"])
            self.assertEqual(self.read.call_args.args[1:], ("sample", SAMPLE_DATES))
            self.column.isin.assert_called_once_with(["2022-09-01", "2022-09-02", "2022-09-03"])
            plan = self.batch.call_args.args[2]
            self.assertEqual(plan.start_date.isoformat(), "2022-09-01")
            self.assertEqual(tuple(d.isoformat() for d in plan.available_input_dates), generation.INPUT_DATES)
            limits = self.batch.call_args.args[4]
            for name, value in self.cfg["limits"].items():
                self.assertEqual(getattr(limits, name), value)
            self.validate.assert_called_once_with(self.output, "sample", "2022-09-01", "UTC", {2: 2, 3: 3})
            self.assertFalse(result.report["sample_validation"]["identity_check"]["dataset_wide_verified"])
            self.assertEqual(self.inspect.call_count, 3)

    def test_invalid_config_rejected_before_clients(self):
        changes = [{"dataset": "HI-Medium"}, {"owner_date": "2022-09-02"}, {"timezone": "Asia/Singapore"},
            {"timezone_evidence": ""}, {"no_refresh_acknowledged": False}, {"no_refresh_evidence": ""},
            {"region": "us-west-2"}, {"run_id": "../bad"}, {"expected_source": {}}, {"extra": True}]
        for key in generation.LIMIT_NAMES:
            for bad in (0, -1, True, "1"):
                changes.append({"limits": dict(self.cfg["limits"], **{key: bad})})
        with self.harness():
            for change in changes:
                with self.subTest(change=change), self.assertRaises(ValueError):
                    with self.operation(dict(self.cfg, **change)):
                        self.fail("unsafe config yielded")
            self.inspect.assert_not_called()
            self.identity.get_caller_identity.assert_not_called()

    def test_stale_each_source_fingerprint_blocks_read(self):
        for key in generation.FINGERPRINTS:
            with self.subTest(key=key), self.harness():
                cfg = deepcopy(self.cfg)
                cfg["expected_source"][key] = "0" * 64
                with self.assertRaisesRegex(ValueError, "Stale"):
                    with self.operation(cfg): pass
                self.read.assert_not_called()

    def test_missing_partition_blocks_read(self):
        snapshot = deepcopy(self.snapshot)
        snapshot["dates"].remove("2022-09-02")
        with self.harness(snapshots=[snapshot]), self.assertRaisesRegex(ValueError, "six"):
            with self.operation(): pass
        self.read.assert_not_called()

    def test_each_growth_limit_failure_propagates_and_cleans(self):
        for name in generation.LIMIT_NAMES:
            failure = GrowthLimitExceeded(name)
            self.source.reset_mock()
            with self.subTest(limit=name), self.harness(batch_error=failure):
                with self.assertRaises(GrowthLimitExceeded) as caught:
                    with self.operation(): pass
                self.assertIs(caught.exception, failure)
                self.source.unpersist.assert_called_once_with(blocking=True)
                self.validate.assert_not_called()

    def test_output_validation_failure_propagates(self):
        with self.harness(validation_error=ValueError("bad output")), self.assertRaisesRegex(ValueError, "bad output"):
            with self.operation(): pass
        self.source.unpersist.assert_called_once_with(blocking=True)
        self.assertIn("batch_cleanup", self.events)

    def test_source_profile_failure_blocks_batch(self):
        with self.harness(profile_error=ValueError("duplicate sample IDs")), self.assertRaisesRegex(ValueError, "duplicate"):
            with self.operation(): pass
        self.batch.assert_not_called()
        self.source.unpersist.assert_called_once_with(blocking=True)

    def test_changed_inventory_before_yield_or_after_consumption(self):
        for key in (*generation.FINGERPRINTS, "objects"):
            for index in (1, 2):
                snapshots = [deepcopy(self.snapshot) for _ in range(3)]
                snapshots[index][key] = [] if key == "objects" else "0" * 64
                self.source.reset_mock()
                with self.subTest(key=key, index=index), self.harness(snapshots=snapshots):
                    with self.assertRaisesRegex(ValueError, "inventory changed"):
                        with self.operation(): pass
                    self.source.unpersist.assert_called_once_with(blocking=True)

    def test_consumer_failure_preserved_with_cleanup_and_source_failure(self):
        snapshots = [deepcopy(self.snapshot) for _ in range(3)]
        snapshots[2]["source_fingerprint"] = "0" * 64
        with self.harness(snapshots=snapshots):
            with self.assertRaisesRegex(RuntimeError, "consumer") as caught:
                with self.operation() as result:
                    raise RuntimeError("consumer")
            self.assertEqual(result.report["status"], "FAILED")
            self.assertTrue(caught.exception.__notes__)
            self.source.unpersist.assert_called_once_with(blocking=True)

    def test_wrong_account_or_region_blocks_source(self):
        with self.harness():
            self.identity.get_caller_identity.return_value = {"Account": "999999999999"}
            with self.assertRaisesRegex(ValueError, "account"):
                with self.operation(): pass
            self.identity.meta.region_name = "us-west-2"
            with self.assertRaisesRegex(ValueError, "region"):
                with self.operation(): pass
            self.inspect.assert_not_called()

    def test_import_safe_in_isolated_interpreter(self):
        script = """import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self, fullname, path=None, target=None):
  if fullname.split('.')[0] in {'pyspark','awsglue','boto3','botocore'}: raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
import sequence_sample_generation
"""
        result = subprocess.run([sys.executable, "-I", "-c", script, str(Path(generation.__file__).parent)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_real_catalog_adapter_uses_exact_sample_date_predicate(self):
        from sequence_aws import read_catalog_selected
        from sequence_source import CLEANED_SCHEMA
        frame = MagicMock()
        frame.columns = CLEANED_SCHEMA.fieldNames()
        frame.select.return_value = frame
        frame.where.return_value.limit.return_value.count.return_value = 0
        self.context.spark_session.conf.get.return_value = "UTC"
        self.context.create_dynamic_frame.from_catalog.return_value.toDF.return_value = frame
        with patch("sequence_source.validate_cleaned_schema"), patch("bq2_sequence_builder.validate_transactions"), patch(
                "pyspark.sql.functions.col", return_value=MagicMock()):
            read_catalog_selected(self.context, "sample", generation.INPUT_DATES,
                available_dates=SAMPLE_DATES, timezone="UTC", timezone_verified=True,
                timezone_evidence="synthetic evidence", catalog_table=self.snapshot["table"])
        self.context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="is459_g2t1_aml", table_name="transactions_clean",
            push_down_predicate="dataset = 'sample' AND txn_date IN ('2022-09-01', '2022-09-02', '2022-09-03')")

    def test_cleanup_failure_prevents_success(self):
        self.source.unpersist.side_effect = RuntimeError("cleanup failure")
        with self.harness():
            with self.assertRaisesRegex(RuntimeError, "cleanup failure"):
                with self.operation() as result: pass
            self.assertEqual(result.report["status"], "FAILED")



class BaselineGateTests(unittest.TestCase):
    def test_local_only_and_defaults_unchanged(self):
        from glue_sample_preflight import validate_and_profile_frame
        self.assertEqual(validate_and_profile_frame.__kwdefaults__,
                         {"expected_rows": 500000, "expected_laundering": 35})
        spark = MagicMock()
        values = {"expected_rows": 10, "expected_laundering": 1}
        for master in ("local", "local[2]", "local[*]", "local[2,4]"):
            spark.sparkContext.master = master
            self.assertEqual(generation._baseline_options(spark, values), values)
        for master in ("yarn", "spark://host:7077", "local-cluster[2,1,512]", "localish", "local[2]/", "k8s://host", ""):
            spark.sparkContext.master = master
            self.assertEqual(generation._baseline_options(spark, None), {})
            with self.subTest(master=master), self.assertRaises(ValueError):
                generation._baseline_options(spark, values)

    def test_invalid_baselines_rejected(self):
        spark = MagicMock()
        spark.sparkContext.master = "local[2]"
        for values in ({}, {"expected_rows": 1}, {"expected_rows": 500000, "expected_laundering": 1},
                       {"expected_rows": True, "expected_laundering": 0},
                       {"expected_rows": 6, "expected_laundering": 7},
                       {"expected_rows": 6, "expected_laundering": -1}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                generation._baseline_options(spark, values)


class RealGenerationTests(unittest.TestCase):
    """Actual Spark orchestrator, source validators, profiler, batch and output checks.

    Only remote Catalog/S3/STS and S3 footer-read boundaries are replaced.
    """
    @classmethod
    def setUpClass(cls):
        import os
        from pyspark.sql import SparkSession
        from datetime import datetime, timezone
        from fixtures import transaction, INPUT_SCHEMA
        previous = os.environ.get("PYSPARK_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        def restore():
            if previous is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = previous
        cls.addClassCleanup(restore)
        cls.spark = (SparkSession.builder.master("local[2]").appName("role5-real-generation")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        specs = [(-9007199254740993, "2022-09-01 09:00", (1,"A"), (3,"C"), "Wire", 1),
                 (9007199254740993, "2022-09-01 09:30", (2,"B"), (3,"C"), "ACH", 0),
                 (103, "2022-09-01 10:00", (3,"C"), (4,"D"), "Wire", 0),
                 (104, "2022-09-01 11:00", (4,"D"), (5,"E"), "Cash", 0),
                 (105, "2022-09-01 12:00", (4,"D"), (6,"F"), "Wire", 0)]
        rows = [transaction(*spec) for spec in specs]
        for day in range(2,7):
            rows.append(transaction(200+day, f"2022-09-{day:02d} 08:00", (100+day,"X"), (200+day,"Y")))
        for row in rows:
            row["txn_ts"] = row["txn_ts"].replace(tzinfo=timezone.utc)
        cls.rows = {r["txn_id"]:r for r in rows}
        cls.frame = cls.spark.createDataFrame(list(reversed(rows)), INPUT_SCHEMA)

    @contextmanager
    def boundaries(self):
        from types import SimpleNamespace
        from sequence_source import CLEANED_SCHEMA
        from pyspark.sql.types import StructType
        catalog, s3 = FakeCatalog(), sample_s3()
        snapshot = inspect_sample_source(catalog, s3)
        cfg = config(snapshot)
        context = SimpleNamespace(spark_session=self.spark, create_dynamic_frame=MagicMock())
        context.create_dynamic_frame.from_catalog.return_value.toDF.return_value = self.frame
        identity = MagicMock()
        identity.meta.region_name = "us-east-1"
        identity.get_caller_identity.return_value = {"Account": cfg["account_id"]}
        # Model the remote Parquet footer read, preserving real schema validation.
        footer_session = MagicMock()
        footer_session.read.option.return_value.parquet.return_value.schema = StructType(CLEANED_SCHEMA.fields[:-2])
        catalog.calls.clear()
        s3.calls.clear()
        with patch.object(self.spark, "newSession", return_value=footer_session):
            yield cfg, context, catalog, s3, identity
        self.assertFalse(any(name == "put" for name, _ in s3.calls))
        self.assertTrue(all(name in ("get_table", "get_partitions") for name, _ in catalog.calls))

    def cache_count(self):
        return self.spark.sparkContext._jsc.sc().getPersistentRDDs().size()

    def test_real_orchestrator_all_columns_ids_counts_inventory_cleanup_and_limits(self):
        import hashlib
        from pyspark.sql import functions as F
        from fixtures import OUTPUT_SCHEMA
        expected_chains = [(-9007199254740993,103), (9007199254740993,103), (103,104), (103,105),
            (-9007199254740993,103,104), (-9007199254740993,103,105),
            (9007199254740993,103,104), (9007199254740993,103,105)]
        def expected(chain):
            members = [self.rows[i] for i in chain]
            times = [r["txn_ts"].replace(tzinfo=None) for r in members]
            labels = [r["is_laundering"] for r in members]
            methods = [r["payment_format"] for r in members]
            currencies = ["US Dollar"]*len(chain)
            gaps = [int((b-a).total_seconds()) for a,b in zip(times,times[1:])]
            return dict(sequence_id=hashlib.sha256(("v1\nsample\n"+",".join(map(str,chain))).encode()).hexdigest(),
                rule_version="v1", sequence_length=len(chain), txn_ids=list(chain),
                txn_timestamps=[t.strftime("%Y-%m-%d %H:%M:%S") for t in times],
                payment_methods=methods, method_sequence=" -> ".join(methods), payment_currencies=currencies,
                receiving_currencies=currencies, payment_currency_sequence=" -> ".join(currencies),
                receiving_currency_sequence=" -> ".join(currencies), txn_laundering_labels=labels,
                laundering_count=sum(labels), any_laundering=int(any(labels)), all_laundering=int(all(labels)),
                final_txn_id=chain[-1], final_is_laundering=labels[-1], gap_seconds=gaps,
                duration_seconds=sum(gaps), dataset="sample", start_date="2022-09-01")
        baseline = self.cache_count()
        with self.boundaries() as (cfg, context, catalog, s3, identity):
            kwargs = dict(glue_context=context, catalog_client=catalog, s3_client=s3, identity_client=identity,
                          _synthetic_baseline={"expected_rows":10,"expected_laundering":1})
            with generation.generate_sample_sequences(cfg, **kwargs) as result:
                self.assertEqual([(f.name,f.dataType.simpleString()) for f in result.output.schema],
                                 [(f.name,f.dataType.simpleString()) for f in OUTPUT_SCHEMA])
                # Check timestamp types above, then render in Spark's verified UTC
                # session; Python collect() otherwise uses the host's local zone.
                rendered = result.output.withColumn("txn_timestamps", F.transform(
                    "txn_timestamps", lambda ts: F.date_format(ts, "yyyy-MM-dd HH:mm:ss")))
                actual = {tuple(r.txn_ids):r.asDict() for r in rendered.collect()}
                self.assertEqual(set(actual), set(expected_chains))
                for chain in expected_chains:
                    for name, value in expected(chain).items():
                        self.assertEqual(actual[chain][name], value, (chain, name))
                metrics = deepcopy(result.report["batch_metrics"])
                self.assertEqual((metrics["starting_pairs"],metrics["actual_triples"]),(4,4))
                self.assertGreater(self.cache_count(),baseline)
            self.assertEqual(self.cache_count(),baseline)
            self.assertEqual(result.report["status"],"GENERATION_COMPLETE")
            self.assertEqual(result.report["source_before"],result.report["source_after"])
            self.assertFalse(result.report["sample_validation"]["identity_check"]["dataset_wide_verified"])
            self.assertEqual(context.create_dynamic_frame.from_catalog.call_args.kwargs["push_down_predicate"],
                "dataset = 'sample' AND txn_date IN ('2022-09-01', '2022-09-02', '2022-09-03', '2022-09-04', '2022-09-05', '2022-09-06')")
            measured = dict(max_pair_candidates=metrics["pair_candidates"], max_pair_links=metrics["pair_links"],
                max_projected_triples=metrics["projected_triples"], max_triples_per_middle=metrics["largest_middle_expansion"],
                max_output_rows=metrics["output_rows"])
            for name,limit in measured.items():
                self.assertGreater(limit,1)
                changed = deepcopy(cfg)
                changed["limits"][name]=limit-1
                with self.subTest(limit=name), self.assertRaises(GrowthLimitExceeded):
                    with generation.generate_sample_sequences(changed, **kwargs):
                        self.fail("Growth limit was not enforced")
                self.assertEqual(self.cache_count(),baseline)

if __name__ == "__main__":
    unittest.main()
