"""Fixture checks and local Spark pair/triple tests; later features remain deferred.

Run from repository root:
    python -m unittest discover -s bq2/sequences/tests -v

TwoHopTests starts local Spark with synthetic UTC timestamps and stops it after
testing. No external transaction files or AWS services are used. FixtureTests
alone does not start Spark. Passing these tests does not certify a Glue job.
"""

import unittest
import os
import sys
import hashlib
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, LongType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bq2_sequence_builder import (
    build_two_hop_links, build_three_hop_links, validate_transaction_ids,
    validate_transactions, build_sequence_output, _sequence_id,
)

from fixtures import (
    BIGINT_IDS, BRANCHING_PAIRS, BRANCHING_ROWS, BRANCHING_TRIPLES,
    BRANCHING_TRIPLE_EXPECTED, DUPLICATE_CANDIDATE_CHAINS, INPUT_SCHEMA,
    INVALID_INPUTS, ISOLATION_EXPECTED, ISOLATION_ROWS, LABEL_CASES, LABEL_ROWS,
    OUTPUT_SCHEMA, REPEATED_ID_CANDIDATE, SCENARIOS,
    SEQUENCE_ID_TEST_REQUIREMENTS, PAIR_SCHEMA, PAIR_SCENARIOS,
)


class FixtureTests(unittest.TestCase):
    def test_input_schema_matches_role2_contract(self):
        expected = [
            ("txn_id", "bigint"), ("txn_ts", "timestamp"),
            ("from_bank", "bigint"), ("from_account", "string"),
            ("to_bank", "bigint"), ("to_account", "string"),
            ("amount_paid", "double"), ("payment_currency", "string"),
            ("amount_paid_usd", "double"), ("amount_received", "double"),
            ("receiving_currency", "string"), ("amount_received_usd", "double"),
            ("payment_format", "string"), ("cross_bank", "int"),
            ("is_laundering", "int"), ("attempt_id", "int"),
            ("pattern_type", "string"), ("usd_missing", "int"),
            ("dataset", "string"), ("txn_date", "string"),
        ]
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in INPUT_SCHEMA], expected)

    def test_fixture_rows_have_all_20_columns_and_python_types(self):
        rows = [r for scenario in SCENARIOS.values() for r in scenario[0]]
        rows += ISOLATION_ROWS
        rows += [r for scenario in LABEL_ROWS.values() for r in scenario]
        rows += [r for scenario in INVALID_INPUTS.values() for r in scenario]
        python_types = {"bigint": int, "int": int, "double": float,
                        "string": str, "timestamp": datetime}
        for row in rows:
            self.assertEqual(set(row), set(INPUT_SCHEMA.fieldNames()))
            for field in INPUT_SCHEMA:
                value = row[field.name]
                if value is not None:
                    self.assertIs(type(value), python_types[field.dataType.simpleString()])

    def test_output_contract_has_exact_21_names_and_types(self):
        expected = [
            ("sequence_id", "string"), ("rule_version", "string"),
            ("sequence_length", "int"), ("txn_ids", "array<bigint>"),
            ("txn_timestamps", "array<timestamp>"),
            ("payment_methods", "array<string>"), ("method_sequence", "string"),
            ("payment_currencies", "array<string>"),
            ("receiving_currencies", "array<string>"),
            ("payment_currency_sequence", "string"),
            ("receiving_currency_sequence", "string"),
            ("txn_laundering_labels", "array<int>"), ("laundering_count", "int"),
            ("any_laundering", "int"), ("all_laundering", "int"),
            ("final_txn_id", "bigint"), ("final_is_laundering", "int"),
            ("gap_seconds", "array<bigint>"), ("duration_seconds", "bigint"),
            ("dataset", "string"), ("start_date", "string"),
        ]
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in OUTPUT_SCHEMA], expected)

    def test_manual_branching_expectations(self):
        self.assertEqual(BRANCHING_PAIRS, {(101, 102), (101, 104), (102, 103)})
        self.assertEqual(BRANCHING_TRIPLES, {(101, 102, 103)})
        expected = BRANCHING_TRIPLE_EXPECTED
        source = {r["txn_id"]: r for r in BRANCHING_ROWS}
        members = [source[i] for i in expected["txn_ids"]]
        for output, original in (
            ("txn_timestamps", "txn_ts"), ("payment_methods", "payment_format"),
            ("payment_currencies", "payment_currency"),
            ("receiving_currencies", "receiving_currency"),
            ("txn_laundering_labels", "is_laundering"),
        ):
            self.assertEqual(expected[output], [r[original] for r in members])
        self.assertEqual(set(expected), set(OUTPUT_SCHEMA.fieldNames()))
        self.assertEqual(expected["gap_seconds"], [10800, 86400])
        self.assertEqual(expected["duration_seconds"], 27 * 3600)
        self.assertEqual(expected["any_laundering"], 1)
        self.assertEqual(expected["final_is_laundering"], 0)

    def test_expected_links_obey_account_and_time_rules(self):
        # Validate handwritten positive examples; this does not enumerate missing links.
        for name, (rows, pairs, triples) in SCENARIOS.items():
            source = {r["txn_id"]: r for r in rows}
            for chain in pairs | triples:
                with self.subTest(scenario=name, chain=chain):
                    self.assertEqual(len(chain), len(set(chain)))
                    for first, second in zip(chain, chain[1:]):
                        a, b = source[first], source[second]
                        self.assertEqual(a["dataset"], b["dataset"])
                        self.assertEqual((a["to_bank"], a["to_account"]),
                                         (b["from_bank"], b["from_account"]))
                        gap = (b["txn_ts"] - a["txn_ts"]).total_seconds()
                        self.assertGreater(gap, 0)
                        self.assertLessEqual(gap, 86400)

    def test_label_expectations_preserve_final_label(self):
        for labels, count, any_label, all_label, final in LABEL_CASES:
            with self.subTest(labels=labels):
                self.assertEqual(tuple(r["is_laundering"] for r in LABEL_ROWS[labels]), labels)
                self.assertEqual(sum(labels), count)
                self.assertEqual(int(any(labels)), any_label)
                self.assertEqual(int(all(labels)), all_label)
                self.assertEqual(labels[-1], final)

    def test_bigint_fixture_preserves_exact_integer_values(self):
        rows, _, triples = SCENARIOS["signed_bigint"]
        self.assertEqual({r["txn_id"] for r in rows}, set(BIGINT_IDS))
        self.assertEqual(triples, {BIGINT_IDS})
        self.assertTrue(all(type(i) is int and -(2**63) <= i < 2**63 for i in BIGINT_IDS))
        self.assertNotEqual(int(float(BIGINT_IDS[1])), BIGINT_IDS[1])
        self.assertIsInstance(INPUT_SCHEMA["txn_id"].dataType, LongType)
        self.assertEqual(OUTPUT_SCHEMA["txn_ids"].dataType, ArrayType(LongType()))
        self.assertIsInstance(OUTPUT_SCHEMA["final_txn_id"].dataType, LongType)

    def test_isolation_fixture_has_reused_ids_and_no_li_attempt_metadata(self):
        self.assertEqual(ISOLATION_EXPECTED,
                         {("HI-Medium", (1, 2)), ("LI-Medium", (1, 2))})
        self.assertEqual(len({r["txn_id"] for r in ISOLATION_ROWS}), 3)
        self.assertEqual(len({(r["dataset"], r["txn_id"]) for r in ISOLATION_ROWS}), 5)
        self.assertTrue(all(r["attempt_id"] is None for r in ISOLATION_ROWS))

    def test_bad_input_and_candidate_fixtures_are_explicit(self):
        repeated = INVALID_INPUTS["identical_repeated_source"]
        self.assertEqual(repeated[0], repeated[1])
        conflict = INVALID_INPUTS["conflicting_id_label"]
        self.assertEqual(conflict[0]["txn_id"], conflict[1]["txn_id"])
        self.assertNotEqual(conflict[0]["is_laundering"], conflict[1]["is_laundering"])
        self.assertEqual(DUPLICATE_CANDIDATE_CHAINS[0], DUPLICATE_CANDIDATE_CHAINS[1])
        self.assertNotEqual(len(REPEATED_ID_CANDIDATE), len(set(REPEATED_ID_CANDIDATE)))

    def test_sequence_id_approved_fixed_vector(self):
        self.assertEqual(hashlib.sha256(b"v1\nsample\n101,102,103").hexdigest(),
                         BRANCHING_TRIPLE_EXPECTED["sequence_id"])
        self.assertEqual(len(SEQUENCE_ID_TEST_REQUIREMENTS), 5)


class TwoHopTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Settings are scoped to this test process. Pin Python workers on Windows.
        cls.previous_python = os.environ.get("PYSPARK_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("role5-two-hop-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        # Batch independent scenarios into one job. Prefix accounts and remap
        # ordinary IDs, preserving production dataset values and bigint extremes.
        cls.scenario_rows = []
        cls.expected_pairs = set()
        cls.expected_triples = set()
        cases = list(SCENARIOS.items())
        cases += [(f"labels_{labels}", (rows,
                   {(i, i + 1) for i in range(1, len(labels))},
                   {(1, 2, 3)} if len(labels) == 3 else set()))
                  for labels, rows in LABEL_ROWS.items()]
        for index, (name, (rows, pairs, triples)) in enumerate(cases, 1):
            mapping = {r["txn_id"]: (r["txn_id"] if name == "signed_bigint"
                                    else index * 10000 + r["txn_id"]) for r in rows}
            for row in rows:
                cls.scenario_rows.append(dict(
                    row, txn_id=mapping[row["txn_id"]],
                    from_account=f"{name}:{row['from_account']}",
                    to_account=f"{name}:{row['to_account']}"))
            dataset = rows[0]["dataset"] if rows else "sample"
            cls.expected_pairs.update((dataset, tuple(mapping[i] for i in p)) for p in pairs)
            cls.expected_triples.update((dataset, tuple(mapping[i] for i in t)) for t in triples)
        cls.pairs = build_two_hop_links(
            cls.spark.createDataFrame(cls.scenario_rows, INPUT_SCHEMA)).persist()
        cls.actual_pairs = cls.pairs.collect()
        cls.triples = build_three_hop_links(cls.pairs)
        cls.actual_triples = cls.triples.collect()
        cls.source = cls.spark.createDataFrame(cls.scenario_rows, INPUT_SCHEMA)
        cls.output = build_sequence_output(cls.source, cls.pairs, cls.triples)
        cls.actual_output = cls.output.collect()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.pairs.unpersist()
            cls.spark.stop()
        finally:
            if cls.previous_python is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = cls.previous_python

    def frame(self, rows):
        return self.spark.createDataFrame(rows, INPUT_SCHEMA)

    def assert_links(self, rows, expected):
        links = build_two_hop_links(self.frame(rows))
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in links.schema],
                         [("dataset", "string"), ("first_txn_id", "bigint"),
                          ("second_txn_id", "bigint")])
        actual = [(r.dataset, (r.first_txn_id, r.second_txn_id)) for r in links.collect()]
        self.assertEqual(len(actual), len(set(actual)), "Duplicate ordered output pairs")
        self.assertEqual(set(actual), expected)

    def test_exact_pairs_for_all_scenarios(self):
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in self.pairs.schema],
                         [("dataset", "string"), ("first_txn_id", "bigint"),
                          ("second_txn_id", "bigint")])
        actual = [(r.dataset, (r.first_txn_id, r.second_txn_id)) for r in self.actual_pairs]
        self.assertEqual(len(actual), len(set(actual)))
        self.assertEqual(set(actual), self.expected_pairs)
        # Empty input needs its own execution, not just inclusion in a batch.
        self.assert_links([], set())

    def test_input_reordering_preserves_branches(self):
        self.assert_links(list(reversed(BRANCHING_ROWS)),
                          {("sample", pair) for pair in BRANCHING_PAIRS})

    def test_dataset_isolation_allows_same_id_in_different_datasets(self):
        validate_transaction_ids(self.frame(ISOLATION_ROWS))
        self.assert_links(ISOLATION_ROWS, ISOLATION_EXPECTED)

    def test_reject_null_and_duplicate_source_ids(self):
        for name in ("null_id", "identical_repeated_source",
                     "conflicting_id_label", "conflicting_id_content"):
            with self.subTest(scenario=name):
                message = "Null transaction IDs" if name == "null_id" else "Duplicate transaction IDs"
                with self.assertRaisesRegex(ValueError, message):
                    validate_transaction_ids(self.frame(INVALID_INPUTS[name]))
                with self.assertRaisesRegex(ValueError, message):
                    build_two_hop_links(self.frame(INVALID_INPUTS[name]))

    def test_reject_non_bigint_ids_instead_of_casting(self):
        rows = self.frame(SCENARIOS["valid_pair"][0])
        rows = rows.withColumn("txn_id", rows["txn_id"].cast("double"))
        with self.assertRaisesRegex(ValueError, "bigint"):
            validate_transaction_ids(rows)

    def test_original_labels_do_not_filter_links(self):
        actual = {(r.dataset, (r.first_txn_id, r.second_txn_id)) for r in self.actual_pairs}
        self.assertEqual({p for p in actual if p[0] == "LI-Medium"},
                         {p for p in self.expected_pairs if p[0] == "LI-Medium"})

    def test_exact_triples_for_all_transaction_scenarios(self):
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in self.triples.schema],
                         [("dataset", "string"), ("first_txn_id", "bigint"),
                          ("second_txn_id", "bigint"), ("third_txn_id", "bigint")])
        actual = [(r.dataset, (r.first_txn_id, r.second_txn_id, r.third_txn_id))
                  for r in self.actual_triples]
        self.assertEqual(len(actual), len(set(actual)))
        self.assertEqual(set(actual), self.expected_triples)
        self.assertIn(("sample", BIGINT_IDS), actual)

    def test_triple_candidate_edge_cases(self):
        # Independent datasets avoid accidental connections between pair cases.
        rows, expected = [], set()
        for name, (pairs, triples) in PAIR_SCENARIOS.items():
            rows.extend((f"{name}:{d}", a, b) for d, a, b in pairs)
            expected.update((f"{name}:{d}", ids) for d, ids in triples)
        output = build_three_hop_links(self.spark.createDataFrame(rows, PAIR_SCHEMA)).collect()
        actual = [(r.dataset, (r.first_txn_id, r.second_txn_id, r.third_txn_id)) for r in output]
        self.assertEqual(len(actual), len(set(actual)))
        self.assertEqual(set(actual), expected)

    def test_triples_empty_input_and_shuffled_pairs(self):
        empty = build_three_hop_links(self.spark.createDataFrame([], PAIR_SCHEMA))
        self.assertEqual(empty.schema.simpleString(), self.triples.schema.simpleString())
        self.assertEqual(empty.collect(), [])
        shuffled = self.spark.createDataFrame(list(reversed(self.actual_pairs)), PAIR_SCHEMA)
        actual = build_three_hop_links(shuffled).collect()
        self.assertEqual(set(actual), set(self.actual_triples))

    def test_triple_input_schema_validation(self):
        pairs = self.spark.createDataFrame([], PAIR_SCHEMA)
        for name in PAIR_SCHEMA.fieldNames():
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError, name):
                    build_three_hop_links(pairs.drop(name))
            wrong_type = "int" if name == "dataset" else "double"
            with self.subTest(wrong_type=name):
                bad = pairs.withColumn(name, pairs[name].cast(wrong_type))
                with self.assertRaisesRegex(ValueError, name):
                    build_three_hop_links(bad)
        with self.assertRaisesRegex(ValueError, "first_txn_id"):
            build_three_hop_links(pairs.select("*", "first_txn_id"))

    def test_synthetic_session_uses_utc(self):
        self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")


    def test_all_21_output_columns_and_ordered_member_arrays(self):
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in self.output.schema],
                         [(f.name, f.dataType.simpleString()) for f in OUTPUT_SCHEMA])
        source = {(r["dataset"], r["txn_id"]): r for r in self.scenario_rows}
        actual_keys = [(r.dataset, tuple(r.txn_ids)) for r in self.actual_output]
        self.assertEqual(len(actual_keys), len(set(actual_keys)))
        self.assertEqual(set(actual_keys), self.expected_pairs | self.expected_triples)
        for row in self.actual_output:
            with self.subTest(ids=row.txn_ids):
                self.assertTrue(all(v is not None for v in row))
                members = [source[row.dataset, i] for i in row.txn_ids]
                self.assertEqual(row.sequence_length, len(members))
                for name, field in (("txn_timestamps", "txn_ts"), ("payment_methods", "payment_format"),
                                    ("payment_currencies", "payment_currency"),
                                    ("receiving_currencies", "receiving_currency"),
                                    ("txn_laundering_labels", "is_laundering")):
                    self.assertEqual(row[name], [m[field] for m in members])
                    self.assertNotIn(None, row[name])
                self.assertEqual(row.method_sequence, " -> ".join(row.payment_methods))
                self.assertEqual(row.payment_currency_sequence, " -> ".join(row.payment_currencies))
                self.assertEqual(row.receiving_currency_sequence, " -> ".join(row.receiving_currencies))
                labels = row.txn_laundering_labels
                self.assertEqual((row.laundering_count, row.any_laundering, row.all_laundering),
                                 (sum(labels), int(any(labels)), int(all(labels))))
                self.assertEqual(row.final_is_laundering, labels[-1])
                self.assertEqual(row.final_txn_id, row.txn_ids[-1])
                gaps = [int((b["txn_ts"] - a["txn_ts"]).total_seconds())
                        for a, b in zip(members, members[1:])]
                self.assertEqual(row.gap_seconds, gaps)
                self.assertEqual(row.duration_seconds, sum(gaps))
                self.assertTrue(all(0 < gap <= 86400 for gap in gaps))
                payload = f"v1\n{row.dataset}\n" + ",".join(str(i) for i in row.txn_ids)
                self.assertEqual(row.sequence_id, hashlib.sha256(payload.encode("utf-8")).hexdigest())
        big = next(r for r in self.actual_output if tuple(r.txn_ids) == BIGINT_IDS)
        self.assertEqual(big.final_txn_id, BIGINT_IDS[-1])
        self.assertTrue(any(r.duration_seconds == 172800 for r in self.actual_output))

    def test_manual_output_example_and_rerun(self):
        pairs = self.spark.createDataFrame([("sample", *p) for p in BRANCHING_PAIRS], PAIR_SCHEMA)
        triples = self.spark.createDataFrame(
            [("sample", 101, 102, 103)], self.triples.schema)
        rows = list(reversed(BRANCHING_ROWS))
        # Duplicate links must not duplicate output; no source duplication is allowed.
        output = build_sequence_output(self.frame(rows), pairs.unionByName(pairs), triples).collect()
        triple = next(r for r in output if r.sequence_length == 3)
        self.assertEqual(triple.asDict(), BRANCHING_TRIPLE_EXPECTED)
        rerun = build_sequence_output(self.frame(BRANCHING_ROWS), pairs, triples).collect()
        self.assertEqual({r.sequence_id: r.asDict() for r in output},
                         {r.sequence_id: r.asDict() for r in rerun})

    def test_source_validation_and_generated_chain_deduplication(self):
        # Batch bad-field cases with unique identities to test one aggregate scan.
        rows = []
        for i, (name, bad) in enumerate(INVALID_INPUTS.items(), 1):
            if "source" in name or "conflicting" in name or name == "null_id":
                continue
            rows.append(dict(bad[-1], txn_id=i))
        with self.assertRaisesRegex(ValueError, "Invalid required transaction fields"):
            validate_transactions(self.frame(rows))
        # Check each validation rule independently without repeating the full joins.
        for name, bad in INVALID_INPUTS.items():
            with self.subTest(scenario=name):
                with self.assertRaises(ValueError):
                    validate_transactions(self.frame(bad))

    def test_required_source_schema_validation(self):
        rows = self.frame(BRANCHING_ROWS)
        for name in ("txn_ts", "from_bank", "from_account", "is_laundering", "dataset", "txn_date"):
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError, name):
                    validate_transactions(rows.drop(name))
            with self.subTest(type=name):
                with self.assertRaisesRegex(ValueError, name):
                    validate_transactions(rows.withColumn(name, F.lit(1.0)))
        with self.assertRaisesRegex(ValueError, "txn_ts"):
            validate_transactions(rows.select("*", "txn_ts"))

    def test_sequence_ids_match_reviewed_serialization_and_fixed_hash(self):
        ids = self.spark.createDataFrame([
            ("sample", [101, 102, 103]), ("HI-Medium", [101, 102, 103]),
            ("sample", [103, 102, 101]), ("sample", list(BIGINT_IDS)),
        ], "dataset string, txn_ids array<bigint>")
        hashes = ids.select(_sequence_id(F.col("dataset"), F.col("txn_ids")).alias("v1"),
                            _sequence_id(F.col("dataset"), F.col("txn_ids"), "v2").alias("v2")).collect()
        self.assertEqual(hashes[0].v1, BRANCHING_TRIPLE_EXPECTED["sequence_id"])
        self.assertEqual(len({r.v1 for r in hashes}), 4)
        self.assertTrue(all(r.v1 != r.v2 for r in hashes))
        # Change all labels while keeping transaction identities unchanged.
        changed = self.source.withColumn("is_laundering", 1 - F.col("is_laundering"))
        output = build_sequence_output(changed, self.pairs, self.triples).collect()
        self.assertEqual({(r.dataset, tuple(r.txn_ids), r.sequence_id) for r in output},
                         {(r.dataset, tuple(r.txn_ids), r.sequence_id) for r in self.actual_output})

    def test_empty_final_output(self):
        empty = build_sequence_output(self.frame([]),
                    self.spark.createDataFrame([], PAIR_SCHEMA),
                    self.spark.createDataFrame([], self.triples.schema))
        self.assertEqual(empty.schema.simpleString(), OUTPUT_SCHEMA.simpleString())
        self.assertEqual(empty.collect(), [])

    def test_missing_repeated_and_mismatched_members_fail(self):
        empty_triples = self.spark.createDataFrame([], self.triples.schema)
        for pair in ((101, 999), (101, 101), (101, 103), (102, 101)):
            with self.subTest(pair=pair):
                links = self.spark.createDataFrame([("sample", *pair)], PAIR_SCHEMA)
                with self.assertRaisesRegex(ValueError, "sequence members"):
                    build_sequence_output(self.frame(BRANCHING_ROWS), links, empty_triples)
        wrong_dataset = self.spark.createDataFrame([("LI-Medium", 101, 102)], PAIR_SCHEMA)
        with self.assertRaisesRegex(ValueError, "sequence members"):
            build_sequence_output(self.frame(BRANCHING_ROWS), wrong_dataset, empty_triples)
        empty_pairs = self.spark.createDataFrame([], PAIR_SCHEMA)
        for triple in ((101, 102, 999), (101, 102, 101), (101, 104, 103)):
            with self.subTest(triple=triple):
                links = self.spark.createDataFrame([("sample", *triple)], self.triples.schema)
                with self.assertRaisesRegex(ValueError, "sequence members"):
                    build_sequence_output(self.frame(BRANCHING_ROWS), empty_pairs, links)


if __name__ == "__main__":
    unittest.main()
