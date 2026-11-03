"""Local staging tests; real Parquet tests run in Ubuntu WSL2 unchanged.

python -m unittest discover -s bq2/sequences/tests -p test_sequence_staging.py -v
"""

import hashlib
import json
import os
import sys
import tempfile
import unittest
from copy import deepcopy
from datetime import timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from fixtures import BRANCHING_TRIPLE_EXPECTED, OUTPUT_SCHEMA, INPUT_SCHEMA, BIGINT_IDS, transaction
from sequence_batch import GrowthLimits, process_start_date_batch
from sequence_processing import plan_start_date_batches
from sequence_staging import (
    SEQUENCE_SCHEMA, _attempt_path, _write_json,
    validate_sequence_schema, validate_sequence_output,
    stage_sequence_batch_local, read_staged_batch_local,
)


class PathAndReceiptTests(unittest.TestCase):
    def test_safe_paths_identifiers_and_overlapping_roots(self):
        with tempfile.TemporaryDirectory(prefix="role5-paths-") as directory:
            root = Path(directory)
            stage, dev, prod = root / "stage", root / "dev", root / "prod"
            result = _attempt_path(stage, dev, prod, "run_1", "attempt-1")
            self.assertEqual(result, stage / "run_1" / "attempt-1")
            for bad in ("..", "a/b", "", "/absolute", "a.b", "a" * 65):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    _attempt_path(stage, dev, prod, bad, "attempt")
            for unsafe in (dev, dev / "nested", root, "s3://not-accessed", Path("relative"), root / "x" / ".." / "stage"):
                with self.subTest(unsafe=unsafe), self.assertRaises(ValueError):
                    _attempt_path(unsafe, dev, prod, "run", "attempt")

    def test_symlink_paths_rejected(self):
        # Ubuntu test: real filesystem symlink coverage, no assertion is skipped.
        with tempfile.TemporaryDirectory(prefix="role5-links-") as directory:
            root = Path(directory)
            target = root / "target"
            target.mkdir()
            (root / "link").symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "Symlinks"):
                _attempt_path(root / "link" / "stage", root / "dev", root / "prod", "run", "attempt")

    def test_atomic_json_receipt_preserves_previous_state_on_interruption(self):
        with tempfile.TemporaryDirectory(prefix="role5-status-") as directory:
            path = Path(directory) / "status.json"
            _write_json(path, {"state": "WRITING"})
            with patch("sequence_staging.os.replace", side_effect=OSError("interrupted")):
                with self.assertRaisesRegex(OSError, "interrupted"):
                    _write_json(path, {"state": "VALIDATED"})
            self.assertEqual(json.loads(path.read_text())["state"], "WRITING")
            self.assertTrue(list(path.parent.glob("*.tmp")))


class LocalStagingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        previous = os.environ.get("PYSPARK_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        def restore():
            if previous is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = previous
        cls.addClassCleanup(restore)
        cls.temporary = tempfile.TemporaryDirectory(prefix="role5-staging-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.spark = (SparkSession.builder.master("local[2]").appName("role5-staging-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        triple = deepcopy(BRANCHING_TRIPLE_EXPECTED)
        triple["txn_timestamps"] = [ts.replace(tzinfo=timezone.utc) for ts in triple["txn_timestamps"]]
        large = deepcopy(triple)
        large["txn_ids"] = list(BIGINT_IDS)
        large["final_txn_id"] = BIGINT_IDS[-1]
        large["sequence_id"] = hashlib.sha256(("v1\nsample\n" + ",".join(map(str, BIGINT_IDS))).encode()).hexdigest()
        pair = deepcopy(triple)
        pair.update(sequence_length=2, txn_ids=[7, 8], final_txn_id=8,
                    final_is_laundering=0, duration_seconds=10800)
        for name in ("txn_timestamps", "payment_methods", "payment_currencies", "receiving_currencies", "txn_laundering_labels"):
            pair[name] = pair[name][:2]
        pair["gap_seconds"] = [10800]
        for name, array in (("method_sequence", "payment_methods"),
                            ("payment_currency_sequence", "payment_currencies"),
                            ("receiving_currency_sequence", "receiving_currencies")):
            pair[name] = " -> ".join(pair[array])
        pair["sequence_id"] = hashlib.sha256(b"v1\nsample\n7,8").hexdigest()
        cls.rows = [triple, large, pair]
        cls.output = cls.spark.createDataFrame(cls.rows, OUTPUT_SCHEMA).cache()
        cls.addClassCleanup(cls.output.unpersist)
        cls.expected = {2: 1, 3: 2}

    def settings(self):
        return dict(dataset="sample", start_date="2022-09-01", timezone="UTC", expected_counts=self.expected,
                    max_records_per_file=1, development_root=self.root / "dev", production_root=self.root / "prod")

    def stage(self, name, output=None, **changes):
        settings = self.settings()
        settings.update(changes)
        return stage_sequence_batch_local(self.output if output is None else output,
                   self.root / "stage", "run", name, **settings)

    def read(self, attempt, **changes):
        settings = dict(dataset="sample", start_date="2022-09-01", timezone="UTC", expected_counts=self.expected)
        settings.update(changes)
        return read_staged_batch_local(self.spark, attempt, **settings)

    def test_distributed_validation_schema_values_and_invalid_rows(self):
        self.assertEqual(SEQUENCE_SCHEMA, OUTPUT_SCHEMA)
        validate_sequence_schema(self.output)
        self.assertEqual(validate_sequence_output(self.output, "sample", "2022-09-01", "UTC", self.expected)["row_count"], 3)
        invalid = [
            self.output.withColumn("sequence_id", F.lit("wrong")),
            self.output.unionByName(self.output),
            self.output.withColumn("any_laundering", F.lit(0)),
            self.output.withColumn("txn_ids", F.lit(None).cast("array<bigint>")),
            self.output.withColumn("txn_laundering_labels", F.array(F.lit(2))),
            self.output.withColumn("gap_seconds", F.array(F.lit(0).cast("bigint"))),
            self.output.withColumn("duration_seconds", F.lit(999).cast("bigint")),
            self.output.withColumn("dataset", F.lit("HI-Medium")),
            self.output.withColumn("start_date", F.lit("2022-09-02")),
            self.output.withColumn("txn_timestamps", F.array(F.lit(None).cast("timestamp"))),
        ]
        for index, frame in enumerate(invalid):
            with self.subTest(index=index), self.assertRaises(ValueError):
                validate_sequence_output(frame, "sample", "2022-09-01", "UTC", self.expected)
        with self.assertRaisesRegex(ValueError, "schema"):
            validate_sequence_schema(self.output.withColumn("final_txn_id", F.col("final_txn_id").cast("double")))
        with self.assertRaisesRegex(ValueError, "counts"):
            validate_sequence_output(self.output, "sample", "2022-09-01", "UTC", {2: 0, 3: 0})

    def test_round_trip_all_columns_bigints_layout_snappy_and_file_sizing(self):
        attempt = self.stage("roundtrip")
        back = self.read(attempt)
        self.assertEqual(back.schema.simpleString(), OUTPUT_SCHEMA.simpleString())
        canonical = lambda frame: sorted(json.dumps(r.asDict(), sort_keys=True, default=str) for r in frame.collect())
        self.assertEqual(canonical(back), canonical(self.output))
        files = list((attempt / "data" / "dataset=sample" / "start_date=2022-09-01").glob("*.parquet"))
        self.assertGreaterEqual(len(files), 3)
        # Read-back itself verifies every column-chunk codec is SNAPPY.
        for file in files:
            count = self.spark.read.option("recursiveFileLookup", "true").parquet(file.as_posix()).count()
            self.assertLessEqual(count, 1)
        self.assertTrue(self.output.is_cached)
        self.assertEqual(self.spark.range(1).count(), 1)
        self.assertEqual(json.loads((attempt / "metadata" / "status.json").read_text())["state"], "VALIDATED")

    def test_empty_rerun_uses_fresh_attempt_without_stale_files(self):
        nonempty = self.stage("before-empty")
        empty = self.spark.createDataFrame([], OUTPUT_SCHEMA)
        attempt = self.stage("after-empty", empty, expected_counts={2: 0, 3: 0})
        self.assertEqual(self.read(attempt, expected_counts={2: 0, 3: 0}).collect(), [])
        self.assertEqual(list((attempt / "data").rglob("*.parquet")), [])
        self.assertEqual(self.read(nonempty).count(), 3)

    def test_existing_attempt_and_bad_settings_rejected(self):
        attempt = self.stage("existing")
        before = (attempt / "metadata" / "status.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.stage("existing")
        self.assertEqual((attempt / "metadata" / "status.json").read_bytes(), before)
        for value in (0, -1, True, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.stage("invalid", max_records_per_file=value)

    def test_write_failure_retained_and_fresh_retry_succeeds(self):
        with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", side_effect=OSError("injected write")):
            with self.assertRaisesRegex(OSError, "injected write"):
                self.stage("write-failed")
        failed = self.root / "stage" / "run" / "write-failed"
        self.assertEqual(json.loads((failed / "metadata" / "status.json").read_text())["state"], "FAILED")
        with self.assertRaisesRegex(ValueError, "not VALIDATED"):
            self.read(failed)
        with self.assertRaises(FileExistsError):
            self.stage("write-failed")
        self.assertEqual(self.read(self.stage("write-retry")).count(), 3)

    def test_partial_write_keeps_failed_artifacts(self):
        def partial_write(path):
            temporary = Path(path) / "_temporary"
            temporary.mkdir(parents=True)
            (temporary / "partial.tmp").write_text("incomplete", encoding="utf-8")
            raise OSError("injected partial write")
        with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", side_effect=partial_write):
            with self.assertRaisesRegex(OSError, "partial write"):
                self.stage("partial-failed")
        failed = self.root / "stage" / "run" / "partial-failed"
        self.assertTrue((failed / "data" / "_temporary" / "partial.tmp").is_file())
        self.assertEqual(json.loads((failed / "metadata" / "status.json").read_text())["state"], "FAILED")
        with self.assertRaises(ValueError):
            self.read(failed)

    def test_stage_inside_batch_context_read_after_cleanup(self):
        rows = []
        for i, (clock, sender, receiver) in enumerate((
            ("2022-09-01 09:00", (10, "A"), (20, "B")),
            ("2022-09-01 12:00", (20, "B"), (30, "C")),
            ("2022-09-02 12:00", (30, "C"), (40, "D")),
        )):
            row = transaction(i + 1, clock, sender, receiver)
            row["txn_ts"] = row["txn_ts"].replace(tzinfo=timezone.utc)
            rows.append(row)
        source = self.spark.createDataFrame(rows, INPUT_SCHEMA)
        plan, = plan_start_date_batches("2022-09-01", "2022-09-01",
            dataset_start_date="2022-09-01", dataset_end_date="2022-09-02",
            available_partition_dates=["2022-09-01", "2022-09-02"])
        with process_start_date_batch(source, "sample", plan, "UTC", GrowthLimits(100, 100, 100, 100, 100)) as batch:
            counts = {2: batch.metrics["starting_pairs"], 3: batch.metrics["actual_triples"]}
            expected = sorted(r.sequence_id for r in batch.output.select("sequence_id").collect())
            attempt = self.stage("batch-context", batch.output, expected_counts=counts)
        back = self.read(attempt, expected_counts=counts)
        self.assertEqual(sorted(r.sequence_id for r in back.select("sequence_id").collect()), expected)

    def test_validation_failure_retains_data_and_rejects_read(self):
        bad = self.output.withColumn("sequence_id", F.lit("invalid"))
        with self.assertRaisesRegex(ValueError, "Invalid sequence"):
            self.stage("validation-failed", bad)
        failed = self.root / "stage" / "run" / "validation-failed"
        self.assertTrue(list((failed / "data").rglob("*.parquet")))
        self.assertEqual(json.loads((failed / "metadata" / "status.json").read_text())["state"], "FAILED")
        with self.assertRaises(ValueError):
            self.read(failed)

    def test_missing_incomplete_receipts_and_temporary_files(self):
        attempt = self.root / "stage" / "run" / "no-receipt"
        attempt.mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, "receipt"):
            self.read(attempt)
        metadata = attempt / "metadata"
        metadata.mkdir()
        for state in ("WRITING", "VALIDATING", "FAILED"):
            _write_json(metadata / "status.json", {"format_version": 1, "state": state})
            with self.subTest(state=state), self.assertRaisesRegex(ValueError, "not VALIDATED"):
                self.read(attempt)
        valid = self.stage("leftover")
        (valid / "data" / "_temporary").mkdir()
        with self.assertRaisesRegex(ValueError, "temporary"):
            self.read(valid)

    def test_incompatible_physical_schema_is_not_cast(self):
        valid = self.stage("physical")
        data = valid / "data"
        # Deliberately tamper with this isolated fixture, never production data.
        bad = self.output.withColumn("final_txn_id", F.col("final_txn_id").cast("double"))
        (bad.write.mode("overwrite").option("compression", "snappy")
         .partitionBy("dataset", "start_date").parquet(data.as_posix()))
        with self.assertRaisesRegex(ValueError, "physical Parquet schema"):
            self.read(valid)
        (self.output.write.mode("overwrite").option("compression", "gzip")
         .partitionBy("dataset", "start_date").parquet(data.as_posix()))
        with self.assertRaisesRegex(ValueError, "Snappy"):
            self.read(valid)


if __name__ == "__main__":
    unittest.main()
