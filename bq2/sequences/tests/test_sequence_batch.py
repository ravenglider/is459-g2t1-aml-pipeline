"""Anchored processing tests. Synthetic UTC only; no Parquet, AWS or publication.

Run: python -m unittest discover -s bq2/sequences/tests -p test_sequence_batch.py -v
"""

import json
import os
import sys
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from fixtures import INPUT_SCHEMA, OUTPUT_SCHEMA, BIGINT_IDS, transaction
from bq2_sequence_builder import build_two_hop_links, build_three_hop_links, build_sequence_output
from sequence_processing import plan_start_date_batches
from sequence_batch import (
    GrowthLimits, GrowthLimitExceeded, process_start_date_batch,
    estimate_pair_candidates, estimate_three_hop_growth, _sum_products, _enforce_limit,
)


class LimitTests(unittest.TestCase):
    def test_all_limits_require_explicit_nonnegative_integers(self):
        with self.assertRaises(TypeError):
            GrowthLimits()
        for name in GrowthLimits.__dataclass_fields__:
            for bad in (-1, True, None, 1.5):
                values = dict.fromkeys(GrowthLimits.__dataclass_fields__, 10)
                values[name] = bad
                with self.subTest(name=name, bad=bad), self.assertRaises(ValueError):
                    GrowthLimits(**values)

    def test_all_limit_comparisons_are_inclusive(self):
        for name in GrowthLimits.__dataclass_fields__:
            _enforce_limit(name, 10, 10)
            with self.subTest(name=name), self.assertRaises(GrowthLimitExceeded):
                _enforce_limit(name, 11, 10)


def canonical(rows):
    # Compare every field, including arrays/timestamps, without hashing mutable lists.
    return sorted(json.dumps(r.asDict(), sort_keys=True, default=str) for r in rows)


class AnchoredBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        old = os.environ.get("PYSPARK_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        def restore():
            if old is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = old
        cls.addClassCleanup(restore)
        cls.spark = (SparkSession.builder.master("local[2]").appName("role5-anchored-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        rows = []
        specs = [
            (1, "2022-09-01 09:00", (10, "A"), (20, "B"), 1),
            (2, "2022-09-01 09:30", (11, "X"), (20, "B"), 0),
            (3, "2022-09-01 10:00", (20, "B"), (20, "C"), 0),
            (4, "2022-09-01 11:00", (20, "C"), (30, "D"), 1),
            (5, "2022-09-01 12:00", (20, "C"), (40, "E"), 0),
            (6, "2022-09-01 23:59", (50, "F"), (60, "G"), 0),
            (7, "2022-09-02 00:00", (60, "G"), (70, "H"), 1),
            (8, "2022-09-03 00:00", (70, "H"), (80, "I"), 0),
            (BIGINT_IDS[0], "2022-09-01 23:59", (90, "J"), (91, "K"), 1),
            (BIGINT_IDS[1], "2022-09-02 23:59", (91, "K"), (92, "L"), 0),
            (BIGINT_IDS[2], "2022-09-03 23:59", (92, "L"), (93, "M"), 0),
        ]
        for ident, clock, sender, receiver, label in specs:
            row = transaction(ident, clock, sender, receiver, "Wire", label)
            row["txn_ts"] = datetime.fromisoformat(clock).replace(tzinfo=timezone.utc)
            rows.append(row)
        cls.source = cls.spark.createDataFrame(list(reversed(rows)), INPUT_SCHEMA).cache()
        cls.addClassCleanup(cls.source.unpersist)
        cls.generous = GrowthLimits(100000, 100000, 100000, 100000, 100000)  # test-only bounds
        cls.plans = plan_start_date_batches("2022-09-01", "2022-09-03",
            dataset_start_date="2022-09-01", dataset_end_date="2022-09-03",
            available_partition_dates=["2022-09-01", "2022-09-02", "2022-09-03"])
        pairs = build_two_hop_links(cls.source).cache()
        cls.addClassCleanup(pairs.unpersist)
        cls.pairs = pairs
        cls.reference = build_sequence_output(cls.source, pairs, build_three_hop_links(pairs)).collect()

    def cache_count(self):
        return self.spark.sparkContext._jsc.sc().getPersistentRDDs().size()

    def slice(self, plan):
        return self.source.where(F.col("txn_date").isin([d.isoformat() for d in plan.available_input_dates]))

    def test_batched_equivalence_all_columns_boundaries_and_unique_ownership(self):
        actual, metrics = [], None
        for plan in self.plans:
            baseline = self.cache_count()
            with process_start_date_batch(self.slice(plan), "sample", plan, "UTC", self.generous) as result:
                self.assertEqual(result.output.schema.simpleString(), OUTPUT_SCHEMA.simpleString())
                rows = result.output.collect()
                self.assertTrue(all(r.start_date == plan.start_date.isoformat() for r in rows))
                self.assertEqual(len(rows), result.metrics["output_rows"])
                actual.extend(rows)
                if plan == self.plans[0]:
                    metrics = result.metrics
            self.assertEqual(self.cache_count(), baseline)
        self.assertEqual(canonical(actual), canonical(self.reference))
        self.assertEqual(len({(r.dataset, tuple(r.txn_ids)) for r in actual}), len(actual))
        large = next(r for r in actual if tuple(r.txn_ids) == BIGINT_IDS)
        self.assertEqual(large.gap_seconds, [86400, 86400])
        self.assertEqual(large.duration_seconds, 172800)
        self.assertIn([1, 3, 4], [r.txn_ids for r in actual])
        self.assertIn([2, 3, 5], [r.txn_ids for r in actual])
        # Verify exact limit equality with an actual batch, not just the comparator.
        limits = GrowthLimits(metrics["pair_candidates"], metrics["pair_links"],
                              metrics["projected_triples"], metrics["largest_middle_expansion"], metrics["output_rows"])
        with process_start_date_batch(self.slice(self.plans[0]), "sample", self.plans[0], "UTC", limits) as result:
            self.assertEqual(result.metrics, metrics)

    def test_pair_upper_bound_and_exact_triple_projection(self):
        dates = ["2022-09-01", "2022-09-02"]
        pairs = build_two_hop_links(self.source, first_dates=dates)
        self.assertGreaterEqual(estimate_pair_candidates(self.source, dates), pairs.count())
        first = self.source.where(F.col("txn_date") == dates[0]).select("dataset", F.col("txn_id").alias("first_txn_id"))
        starting = pairs.join(first, ["dataset", "first_txn_id"], "left_semi")
        projection, maximum = estimate_three_hop_growth(starting, pairs)
        triples = build_three_hop_links(pairs, starting_links=starting).collect()
        self.assertEqual(projection, len(triples))
        self.assertEqual(maximum, 4)
        self.assertTrue(all(r.first_txn_id in (1, 2, 6, BIGINT_IDS[0]) for r in triples))
        # A synthetic degree product above signed bigint must stay exact.
        degrees = self.spark.createDataFrame([(4000000000, 4000000000)], "predecessors bigint, successors bigint")
        self.assertEqual(_sum_products(degrees), (16000000000000000000, 16000000000000000000))

    def test_every_growth_failure_cleans_up_and_blocks_triple_expansion(self):
        plan = self.plans[0]
        with process_start_date_batch(self.slice(plan), "sample", plan, "UTC", self.generous) as result:
            metrics = result.metrics
        mapping = dict(max_pair_candidates="pair_candidates", max_pair_links="pair_links",
                       max_projected_triples="projected_triples", max_triples_per_middle="largest_middle_expansion",
                       max_output_rows="output_rows")
        for setting, metric in mapping.items():
            baseline = self.cache_count()
            limits = replace(self.generous, **{setting: metrics[metric] - 1})
            with self.subTest(setting=setting), patch("sequence_batch.build_three_hop_links") as expansion:
                with self.assertRaises(GrowthLimitExceeded):
                    with process_start_date_batch(self.slice(plan), "sample", plan, "UTC", limits):
                        self.fail("Limit failure must not yield output")
                expansion.assert_not_called()
            self.assertEqual(self.cache_count(), baseline)

    def test_cleanup_after_injected_failure_preserves_caller_cache_and_session(self):
        baseline = self.cache_count()
        with patch("sequence_batch.build_sequence_output", side_effect=RuntimeError("injected")):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                with process_start_date_batch(self.source, "sample", self.plans[0], "UTC", self.generous):
                    self.fail("Injected failure must not yield")
        self.assertTrue(self.source.is_cached)
        self.assertEqual(self.cache_count(), baseline)
        self.assertEqual(self.spark.range(1).count(), 1)

    def test_empty_true_end_boundary_and_invalid_populations(self):
        end = self.plans[-1]
        empty_plan = plan_start_date_batches("2022-09-03", "2022-09-03",
            dataset_start_date="2022-09-01", dataset_end_date="2022-09-03",
            available_partition_dates=[], verified_empty_dates=["2022-09-03"])[0]
        with process_start_date_batch(self.spark.createDataFrame([], INPUT_SCHEMA), "sample", empty_plan,
                                     "UTC", GrowthLimits(0, 0, 0, 0, 0)) as result:
            self.assertEqual(result.output.collect(), [])
        self.assertTrue(end.unavailable_after_dataset_end)
        for source, dataset, plan, zone in (
            (self.source, "LI-Medium", self.plans[0], "UTC"),
            (self.source, "sample", end, "UTC"),
            (self.source, "sample", self.plans[0], "Asia/Singapore"),
            (self.source, "sample", replace(self.plans[0], unexpected_missing_input_dates=(end.start_date,)), "UTC"),
            (self.source, "sample", replace(self.plans[0], output_end_exclusive=end.start_date), "UTC"),
        ):
            with self.subTest(dataset=dataset, zone=zone, plan=plan), self.assertRaises(ValueError):
                with process_start_date_batch(source, dataset, plan, zone, self.generous):
                    self.fail("Invalid batch must not yield")

    def test_builder_optional_arguments_preserve_default_behavior(self):
        pairs = build_two_hop_links(self.source, first_dates=None)
        self.assertEqual(set(pairs.collect()), set(self.pairs.collect()))
        triples = build_three_hop_links(self.pairs, starting_links=None)
        self.assertEqual(set(triples.collect()), set(build_three_hop_links(self.pairs).collect()))
        with self.assertRaises(ValueError):
            build_two_hop_links(self.source, first_dates="2022-09-01")


if __name__ == "__main__":
    unittest.main()
