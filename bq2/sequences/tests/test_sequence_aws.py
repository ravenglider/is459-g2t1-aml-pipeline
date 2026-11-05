"""Injected Catalog mocks only: no credentials, SDK clients, Spark JVM or AWS."""

from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql.types import StructType, StructField, DoubleType
from sequence_source import CLEANED_SCHEMA
import sequence_aws as adapter


def table():
    fields = [dict(Name=f.name, Type=f.dataType.simpleString()) for f in CLEANED_SCHEMA]
    return dict(Name=adapter.TABLE, StorageDescriptor=dict(Location=adapter.SOURCE_ROOT, Columns=fields[:-2]),
                PartitionKeys=fields[-2:])


class CatalogAdapterTests(unittest.TestCase):
    def setUp(self):
        self.context = MagicMock()
        self.context.spark_session.conf.get.return_value = "UTC"
        self.frame = MagicMock()
        self.frame.columns = CLEANED_SCHEMA.fieldNames()
        self.frame.schema = CLEANED_SCHEMA
        self.frame.select.return_value = self.frame
        self.frame.where.return_value.limit.return_value.count.return_value = 0
        self.context.create_dynamic_frame.from_catalog.return_value.toDF.return_value = self.frame
        self.kwargs = dict(available_dates=["2022-09-01", "2022-09-02", "2022-09-03"], timezone="UTC",
                           timezone_verified=True, timezone_evidence="Synthetic UTC verification", catalog_table=table())

    def read(self, dates=("2022-09-02", "2022-09-01"), dataset="sample", **changes):
        # Mock expressions avoid requiring a live SparkContext. Schema validators
        # are real; existing Spark suites retain value-level chaining coverage.
        with patch("pyspark.sql.functions.col", return_value=MagicMock()), patch("bq2_sequence_builder.validate_transactions") as validate:
            result = adapter.read_catalog_selected(self.context, dataset, dates, **dict(self.kwargs, **changes))
        return result, validate

    def test_exact_pushdown_and_no_type_casts(self):
        result, validate = self.read()
        self.assertIs(result, self.frame)
        self.context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="is459_g2t1_aml", table_name="transactions_clean",
            push_down_predicate="dataset = 'sample' AND txn_date IN ('2022-09-01', '2022-09-02')")
        self.frame.select.assert_called_once_with(*CLEANED_SCHEMA.fieldNames())
        validate.assert_called_once_with(self.frame)
        self.frame.cast.assert_not_called()

    def test_empty_invalid_missing_and_non_sample_selections_never_read(self):
        for dataset, dates in (("sample", []), ("sample", "2022-09-01"), ("sample", ["20220901"]),
                               ("sample", ["2022-02-30"]), ("sample", ["2022-09-04"]),
                               ("HI-Medium", ["2022-09-01"]), ("LI-Medium", ["2022-09-01"])):
            with self.subTest(dataset=dataset, dates=dates), self.assertRaises(ValueError):
                self.read(dates, dataset)
        self.context.create_dynamic_frame.from_catalog.assert_not_called()

    def test_timezone_unverified_or_mismatched_blocks_reads(self):
        for changes in (dict(timezone_verified=False), dict(timezone_evidence=""), dict(timezone="Asia/Singapore")):
            with self.assertRaises(ValueError):
                self.read(**changes)
        self.context.spark_session.conf.get.return_value = "Asia/Singapore"
        with self.assertRaises(ValueError):
            self.read()
        self.context.create_dynamic_frame.from_catalog.assert_not_called()

    def test_catalog_schema_location_partition_order_and_id_type(self):
        metadata = table()
        original = deepcopy(metadata)
        adapter.validate_catalog_table(metadata)
        self.assertEqual(metadata, original)
        for key in ("id_type", "location", "partitions", "name"):
            bad = table()
            if key == "id_type":
                bad["StorageDescriptor"]["Columns"][0]["Type"] = "double"
            elif key == "location":
                bad["StorageDescriptor"]["Location"] = "s3://wrong/"
            elif key == "partitions":
                bad["PartitionKeys"].reverse()
            else:
                bad["Name"] = "raw_sample"
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.read(catalog_table=bad)
        self.context.create_dynamic_frame.from_catalog.assert_not_called()

    def test_result_reordered_without_cast_and_bad_schema_rejected(self):
        self.frame.columns = list(reversed(CLEANED_SCHEMA.fieldNames()))
        self.read()
        self.frame.select.assert_called_once_with(*CLEANED_SCHEMA.fieldNames())
        fields = list(CLEANED_SCHEMA.fields)
        fields[0] = StructField("txn_id", DoubleType())
        self.frame.schema = StructType(fields)
        with self.assertRaises(ValueError):
            self.read()
        self.frame.columns = CLEANED_SCHEMA.fieldNames() + ["unexpected"]
        with self.assertRaises(ValueError):
            self.read()

    def test_leaked_partition_rows_and_dependency_failures_propagate(self):
        self.frame.where.return_value.limit.return_value.count.return_value = 1
        with self.assertRaisesRegex(ValueError, "outside"):
            self.read()
        self.context.create_dynamic_frame.from_catalog.side_effect = RuntimeError("mocked denied")
        with self.assertRaisesRegex(RuntimeError, "mocked denied"):
            self.read()
        self.context.create_dynamic_frame.from_catalog.side_effect = None
        self.frame.where.return_value.limit.return_value.count.return_value = 0
        with patch("pyspark.sql.functions.col", return_value=MagicMock()), patch(
                "bq2_sequence_builder.validate_transactions", side_effect=ValueError("mocked invalid values")):
            with self.assertRaisesRegex(ValueError, "invalid values"):
                adapter.read_catalog_selected(self.context, "sample", ["2022-09-01"], **self.kwargs)


if __name__ == "__main__":
    unittest.main()
