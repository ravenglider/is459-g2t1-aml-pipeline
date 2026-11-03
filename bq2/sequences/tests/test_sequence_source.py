"""Source tests only: pure metadata tests and tiny local Spark/Parquet fixtures."""

import os
import sys
import tempfile
import unittest
from uuid import uuid4
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from fixtures import INPUT_SCHEMA, BIGINT_IDS, transaction
from sequence_processing import plan_start_date_batches
from sequence_source import (
    CLEANED_SCHEMA, build_partition_predicate, validate_cleaned_schema,
    read_cleaned_local, validate_dataset_identity,
    fingerprint_source_inventory, assert_source_unchanged,
)


class SourceMetadataTests(unittest.TestCase):
    def test_predicate_exact_dates_and_empty_selection(self):
        self.assertEqual(build_partition_predicate("sample", ["2022-09-02", "2022-09-01", "2022-09-02"]),
                         "dataset = 'sample' AND txn_date IN ('2022-09-01', '2022-09-02')")
        self.assertEqual(build_partition_predicate("LI-Medium", []), "1 = 0")

    def test_invalid_selection(self):
        for dataset, dates in (("HI", []), ("sample", ["20220901"]),
                               ("sample", ["2022-02-30"]), ("sample", "2022-09-01")):
            with self.subTest(dataset=dataset, dates=dates), self.assertRaises(ValueError):
                build_partition_predicate(dataset, dates)

    def test_inventory_order_stability_and_change_detection(self):
        a = dict(path="a.parquet", size=10, modification_time="2022-09-01T00:00:00.000000Z")
        b = dict(a, path="b.parquet")
        self.assertEqual(fingerprint_source_inventory([a, b]), fingerprint_source_inventory([b, a]))
        assert_source_unchanged([a, b], [b, a])
        for changed in ([a], [a, b, dict(a, path="c.parquet")],
                        [dict(a, size=11), b], [dict(a, version_id="new"), b],
                        [dict(a, etag="new"), b],
                        [dict(a, modification_time="2022-09-02T00:00:00.000000Z"), b]):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, "changed"):
                assert_source_unchanged([a, b], changed)
        self.assertEqual(len(fingerprint_source_inventory([])), 64)

    def test_invalid_inventory(self):
        a = dict(path="a", size=0, modification_time="2022-09-01T00:00:00.000000Z")
        for records in ([a, a], [dict(a, size=True)], [dict(a, path="")],
                        [dict(a, modification_time="yesterday")], [dict(a, size=-1)],
                        [dict(a, etag=4)]):
            with self.subTest(records=records), self.assertRaises(ValueError):
                fingerprint_source_inventory(records)


class LocalSourceTests(unittest.TestCase):
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
        cls.spark = (SparkSession.builder.master("local[2]").appName("role5-source-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.temp = tempfile.TemporaryDirectory(prefix="role5-source-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name) / "cleaned"
        cls.rows = []
        for index, (dataset, day, hour, minute) in enumerate((
            ("sample", "2022-09-01", 23, 59), ("sample", "2022-09-02", 0, 1),
            ("sample", "2022-09-03", 0, 0), ("sample", "2022-09-04", 12, 0),
            ("HI-Medium", "2022-09-01", 12, 0), ("LI-Medium", "2022-09-01", 12, 0),
        )):
            ts = datetime.fromisoformat(day).replace(hour=hour, minute=minute, tzinfo=timezone.utc)
            row = transaction(BIGINT_IDS[index] if index < 3 else index,
                              f"{day} 12:00", (10, "00123"), (20, "A"), dataset=dataset)
            row["txn_ts"] = ts
            cls.rows.append(row)
        cls.frame = cls.spark.createDataFrame(cls.rows, INPUT_SCHEMA)

    @classmethod
    def ensure_parquet(cls):
        # Lazy write keeps identity/schema tests usable if Windows filesystem IO fails.
        if not getattr(cls, "parquet_ready", False):
            # '=' in the root also exercises the reader's basePath conversion.
            candidate = Path(cls.temp.name) / f"cleaned={uuid4().hex}"
            cls.frame.write.mode("error").partitionBy("dataset", "txn_date").parquet(candidate.as_posix())
            cls.root = candidate
            cls.parquet_ready = True

    def plan(self, dataset_end="2022-09-04", available=("2022-09-01", "2022-09-02", "2022-09-03"), empty=()):
        return plan_start_date_batches("2022-09-01", "2022-09-01",
            dataset_start_date="2022-09-01", dataset_end_date=dataset_end,
            available_partition_dates=available, verified_empty_dates=empty)[0]

    def test_schema_matches_role2_and_rejects_wrong_types_order(self):
        self.assertEqual(CLEANED_SCHEMA, INPUT_SCHEMA)
        validate_cleaned_schema(self.frame)
        for bad in (self.frame.withColumn("txn_id", F.col("txn_id").cast("double")),
                    self.frame.select(*reversed(self.frame.columns)), self.frame.drop("attempt_id")):
            with self.assertRaises(ValueError):
                validate_cleaned_schema(bad)

    def test_selected_dates_dataset_bigint_and_midnight(self):
        self.ensure_parquet()
        partition = self.root / "dataset=sample" / "txn_date=2022-09-01"
        self.assertTrue(partition.is_dir(), f"Physical partition missing: {partition}")
        files = sorted(partition.glob("*.parquet"))
        self.assertTrue(files, f"No physical Parquet fixtures in {partition}")
        self.assertTrue(all(path.is_file() for path in files))
        self.assertIn("dataset=sample/txn_date=2022-09-01", files[0].as_posix())
        self.assertIn("dataset%3Dsample/txn_date%3D2022-09-01", files[0].as_uri())
        # A successful read below must use real '=' directories, not URI-escaped ones.
        original_inference = self.spark.conf.get("spark.sql.sources.partitionColumnTypeInference.enabled")
        data = read_cleaned_local(self.spark, self.root, "sample", self.plan(), "UTC")
        validate_cleaned_schema(data)
        rows = data.select("txn_id", "dataset", "txn_date",
                           F.date_format("txn_ts", "yyyy-MM-dd HH:mm").alias("clock")).collect()
        self.assertEqual({r.txn_id for r in rows}, set(BIGINT_IDS))
        self.assertEqual({r.dataset for r in rows}, {"sample"})
        self.assertEqual({r.clock for r in rows}, {"2022-09-01 23:59", "2022-09-02 00:01", "2022-09-03 00:00"})
        self.assertTrue(all(r.clock.startswith(r.txn_date) for r in rows))
        self.assertEqual(self.spark.conf.get("spark.sql.sources.partitionColumnTypeInference.enabled"), original_inference)
        one_day = self.plan(dataset_end="2022-09-01", available=("2022-09-01",))
        for dataset in ("HI-Medium", "LI-Medium"):
            selected = read_cleaned_local(self.spark, self.root, dataset, one_day, "UTC").collect()
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0].dataset, dataset)
        empty_plan = self.plan(available=(), empty=("2022-09-01", "2022-09-02", "2022-09-03"))
        with self.assertRaisesRegex(ValueError, "Verified-empty"):
            read_cleaned_local(self.spark, self.root, "sample", empty_plan, "UTC")

    def test_missing_and_blocked_plans(self):
        empty_root = Path(self.temp.name) / "missing"
        empty_root.mkdir(exist_ok=True)
        with self.assertRaisesRegex(ValueError, "Missing required"):
            read_cleaned_local(self.spark, empty_root, "LI-Medium", self.plan(), "UTC")
        with self.assertRaisesRegex(ValueError, "missing required"):
            read_cleaned_local(self.spark, empty_root, "sample", self.plan(available=()), "UTC")

    def test_verified_empty_has_explicit_schema_without_parquet_write(self):
        empty_root = Path(self.temp.name) / "empty"
        empty_root.mkdir(exist_ok=True)
        empty_plan = self.plan(available=(), empty=("2022-09-01", "2022-09-02", "2022-09-03"))
        data = read_cleaned_local(self.spark, empty_root, "sample", empty_plan, "UTC")
        validate_cleaned_schema(data)
        self.assertEqual(data.collect(), [])

    def test_timezone_dataset_and_root_restrictions(self):
        for dataset, zone in (("HI", "UTC"), ("sample", ""), ("sample", "Asia/Singapore")):
            with self.subTest(dataset=dataset, zone=zone), self.assertRaises(ValueError):
                read_cleaned_local(self.spark, self.temp.name, dataset, self.plan(), zone)
        with self.assertRaisesRegex(ValueError, "local paths"):
            read_cleaned_local(self.spark, "s3://not-accessed", "sample", self.plan(), "UTC")

    def test_dataset_identity_duplicates_on_distant_dates(self):
        sample = self.frame.where(F.col("dataset") == "sample")
        report = validate_dataset_identity(sample.select("dataset", "txn_id"), "sample")
        self.assertEqual(report["row_count"], 4)
        self.assertFalse(report["dataset_wide_verified"])
        self.assertEqual(report["verification_scope"], "provided_dataframe_only")
        # A partial batch cannot claim global certification either.
        self.assertFalse(validate_dataset_identity(sample.limit(1), "sample")["dataset_wide_verified"])
        duplicate = dict(self.rows[0], txn_ts=datetime(2022, 9, 20, tzinfo=timezone.utc), txn_date="2022-09-20")
        for addition in (duplicate, self.rows[0], dict(duplicate, txn_id=None)):
            with self.assertRaises(ValueError):
                validate_dataset_identity(sample.unionByName(self.spark.createDataFrame([addition], INPUT_SCHEMA)), "sample")
        with self.assertRaisesRegex(ValueError, "population"):
            validate_dataset_identity(self.frame, "sample")
        # Identical ID in a different dataset does not invalidate either population.
        for dataset in ("HI-Medium", "LI-Medium"):
            one = self.frame.where(F.col("dataset") == dataset).withColumn("txn_id", F.lit(123).cast("bigint"))
            self.assertTrue(validate_dataset_identity(one, dataset)["identity_check_passed"])

    def test_incompatible_physical_parquet_schema(self):
        bad_root = Path(self.temp.name) / "bad=physical"
        (self.frame.where(F.col("dataset") == "sample").withColumn("txn_id", F.col("txn_id").cast("double"))
         .write.mode("error").partitionBy("dataset", "txn_date").parquet(bad_root.as_posix()))
        with self.assertRaisesRegex(ValueError, "schema"):
            read_cleaned_local(self.spark, bad_root, "sample", self.plan(), "UTC")


if __name__ == "__main__":
    unittest.main()
