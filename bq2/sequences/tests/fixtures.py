"""Fictional cleaned transactions and handwritten expectations, never AWS data.

No sequence-builder logic or sequence-ID serialization is implemented here.
All scenarios are independent; combining them could create unintended links.
"""

from datetime import datetime

from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)


# Exact order and data types of Role 2's final select, including partition columns.
# Nullable fields allow deliberate malformed-input fixtures; validation comes later.
INPUT_SCHEMA = StructType([
    StructField("txn_id", LongType(), True),
    StructField("txn_ts", TimestampType(), True),
    StructField("from_bank", LongType(), True),
    StructField("from_account", StringType(), True),
    StructField("to_bank", LongType(), True),
    StructField("to_account", StringType(), True),
    StructField("amount_paid", DoubleType(), True),
    StructField("payment_currency", StringType(), True),
    StructField("amount_paid_usd", DoubleType(), True),
    StructField("amount_received", DoubleType(), True),
    StructField("receiving_currency", StringType(), True),
    StructField("amount_received_usd", DoubleType(), True),
    StructField("payment_format", StringType(), True),
    StructField("cross_bank", IntegerType(), True),
    StructField("is_laundering", IntegerType(), True),
    StructField("attempt_id", IntegerType(), True),
    StructField("pattern_type", StringType(), True),
    StructField("usd_missing", IntegerType(), True),
    StructField("dataset", StringType(), True),
    StructField("txn_date", StringType(), True),
])


# Future output checks compare names/types, with value checks enforcing non-nullness.
OUTPUT_SCHEMA = StructType([
    StructField("sequence_id", StringType()),
    StructField("rule_version", StringType()),
    StructField("sequence_length", IntegerType()),
    StructField("txn_ids", ArrayType(LongType())),
    StructField("txn_timestamps", ArrayType(TimestampType())),
    StructField("payment_methods", ArrayType(StringType())),
    StructField("method_sequence", StringType()),
    StructField("payment_currencies", ArrayType(StringType())),
    StructField("receiving_currencies", ArrayType(StringType())),
    StructField("payment_currency_sequence", StringType()),
    StructField("receiving_currency_sequence", StringType()),
    StructField("txn_laundering_labels", ArrayType(IntegerType())),
    StructField("laundering_count", IntegerType()),
    StructField("any_laundering", IntegerType()),
    StructField("all_laundering", IntegerType()),
    StructField("final_txn_id", LongType()),
    StructField("final_is_laundering", IntegerType()),
    StructField("gap_seconds", ArrayType(LongType())),
    StructField("duration_seconds", LongType()),
    StructField("dataset", StringType()),
    StructField("start_date", StringType()),
])


def transaction(txn_id, time, sender, receiver, method="Wire", label=0, **changes):
    """Construct one complete synthetic input row; overrides support bad inputs."""
    timestamp = datetime.fromisoformat(time)
    row = dict(
        txn_id=txn_id, txn_ts=timestamp,
        from_bank=sender[0], from_account=sender[1],
        to_bank=receiver[0], to_account=receiver[1],
        amount_paid=100.0, payment_currency="US Dollar", amount_paid_usd=100.0,
        amount_received=100.0, receiving_currency="US Dollar",
        amount_received_usd=100.0, payment_format=method,
        cross_bank=int(sender[0] != receiver[0]), is_laundering=label,
        attempt_id=None, pattern_type=None, usd_missing=0,
        dataset="sample", txn_date=timestamp.strftime("%Y-%m-%d"),
    )
    row.update(changes)
    return row


def base_pair(**second_changes):
    rows = [
        transaction(1, "2022-09-01 09:00", (10, "A"), (20, "B")),
        transaction(2, "2022-09-01 10:00", (20, "B"), (30, "C")),
    ]
    rows[1].update(second_changes)
    return rows


# Keep input deliberately out of chronological and ID order.
BRANCHING_ROWS = [
    transaction(103, "2022-09-02 12:00", (30, "C300"), (40, "D400"),
                "Cheque", 0, payment_currency="Euro", receiving_currency="UK Pound"),
    transaction(101, "2022-09-01 09:00", (10, "A100"), (20, "B200"),
                "Wire", 1, receiving_currency="Euro"),
    transaction(104, "2022-09-01 13:00", (20, "B200"), (50, "E500"), "Cash"),
    transaction(102, "2022-09-01 12:00", (20, "B200"), (30, "C300"),
                "ACH", 0, payment_currency="Euro", receiving_currency="Euro"),
]
BRANCHING_PAIRS = {(101, 102), (101, 104), (102, 103)}
BRANCHING_TRIPLES = {(101, 102, 103)}
BRANCHING_TRIPLE_EXPECTED = {
    "rule_version": "v1", "dataset": "sample", "sequence_length": 3,
    "txn_ids": [101, 102, 103],
    "txn_timestamps": [datetime(2022, 9, 1, 9), datetime(2022, 9, 1, 12),
                       datetime(2022, 9, 2, 12)],
    "payment_methods": ["Wire", "ACH", "Cheque"],
    "method_sequence": "Wire -> ACH -> Cheque",
    "payment_currencies": ["US Dollar", "Euro", "Euro"],
    "receiving_currencies": ["Euro", "Euro", "UK Pound"],
    "payment_currency_sequence": "US Dollar -> Euro -> Euro",
    "receiving_currency_sequence": "Euro -> Euro -> UK Pound",
    "txn_laundering_labels": [1, 0, 0], "laundering_count": 1,
    "any_laundering": 1, "all_laundering": 0,
    "final_txn_id": 103, "final_is_laundering": 0,
    "gap_seconds": [10800, 86400], "duration_seconds": 97200,
    "start_date": "2022-09-01",
    "sequence_id": "d7ccc2d78595f228b8c0957d1b22ebf1cac5d78e4b294f4c09a809c9861c7a30",
}


# Expectations are handwritten, not calculated by the builder under test.
SCENARIOS = {
    "valid_pair": (base_pair(), {(1, 2)}, set()),
    "bank_mismatch": (base_pair(from_bank=99), set(), set()),
    "account_mismatch": (base_pair(from_account="OTHER"), set(), set()),
    "equal_time": (base_pair(txn_ts=datetime(2022, 9, 1, 9)), set(), set()),
    "earlier_time": (base_pair(txn_ts=datetime(2022, 9, 1, 8)), set(), set()),
    "exact_24h": (base_pair(txn_ts=datetime(2022, 9, 2, 9), txn_date="2022-09-02"),
                  {(1, 2)}, set()),
    "over_24h": (base_pair(txn_ts=datetime(2022, 9, 2, 9, 1), txn_date="2022-09-02"),
                 set(), set()),
    "same_bank": (base_pair(to_bank=20), {(1, 2)}, set()),
    "amount_currency_mismatch": (
        base_pair(amount_paid=300.0, payment_currency="Yen"), {(1, 2)}, set()),
    "branching_midnight": (BRANCHING_ROWS, BRANCHING_PAIRS, BRANCHING_TRIPLES),
    "empty": ([], set(), set()),
}
# Override derived cross_bank where a test changes a bank field.
SCENARIOS["same_bank"][0][1]["cross_bank"] = 0

SCENARIOS["leading_zero_account"] = ([
    transaction(1, "2022-09-01 09:00", (10, "A"), (20, "00123")),
    transaction(2, "2022-09-01 10:00", (20, "123"), (30, "C")),
], set(), set())
SCENARIOS["48h_triple"] = ([
    transaction(1, "2022-09-01 09:00", (10, "A"), (20, "B")),
    transaction(2, "2022-09-02 09:00", (20, "B"), (30, "C")),
    transaction(3, "2022-09-03 09:00", (30, "C"), (40, "D")),
], {(1, 2), (2, 3)}, {(1, 2, 3)})
SCENARIOS["cycle"] = ([
    transaction(1, "2022-09-01 09:00", (10, "A"), (20, "B")),
    transaction(2, "2022-09-01 10:00", (20, "B"), (10, "A")),
    transaction(3, "2022-09-01 11:00", (10, "A"), (30, "C")),
], {(1, 2), (2, 3)}, {(1, 2, 3)})
SCENARIOS["self_transfer"] = ([
    transaction(1, "2022-09-01 09:00", (20, "B"), (20, "B")),
    transaction(2, "2022-09-01 10:00", (20, "B"), (30, "C")),
], {(1, 2)}, set())
SCENARIOS["same_method_distinct_chains"] = ([
    *base_pair(),
    transaction(3, "2022-09-01 11:00", (20, "B"), (40, "D")),
], {(1, 2), (1, 3)}, set())

BIGINT_IDS = (-9223372036854775808, 9007199254740993, 9223372036854775807)
SCENARIOS["signed_bigint"] = ([
    transaction(BIGINT_IDS[2], "2022-09-01 11:00", (30, "C"), (40, "D")),
    transaction(BIGINT_IDS[0], "2022-09-01 09:00", (10, "A"), (20, "B")),
    transaction(BIGINT_IDS[1], "2022-09-01 10:00", (20, "B"), (30, "C")),
], {(BIGINT_IDS[0], BIGINT_IDS[1]), (BIGINT_IDS[1], BIGINT_IDS[2])}, {BIGINT_IDS})

# Same IDs are valid in different datasets. ID 3 would cross-link without dataset equality.
ISOLATION_ROWS = []
for dataset in ("HI-Medium", "LI-Medium"):
    ISOLATION_ROWS.extend([dict(row, dataset=dataset) for row in base_pair()])
ISOLATION_ROWS.append(transaction(3, "2022-09-01 11:00", (30, "C"), (40, "D"),
                                  dataset="sample"))
ISOLATION_EXPECTED = {
    ("HI-Medium", (1, 2)), ("LI-Medium", (1, 2)),
}

# labels, laundering_count, any_laundering, all_laundering, final_is_laundering
LABEL_CASES = [
    ((0, 0), 0, 0, 0, 0), ((1, 1), 2, 1, 1, 1),
    ((1, 0), 1, 1, 0, 0), ((0, 1), 1, 1, 0, 1),
    ((1, 0, 0), 1, 1, 0, 0), ((1, 1, 1), 3, 1, 1, 1),
]
LABEL_ROWS = {}
for labels, *_ in LABEL_CASES:
    LABEL_ROWS[labels] = [
        transaction(i + 1, f"2022-09-01 {9 + i:02d}:00",
                    (10 * (i + 1), f"A{i}"), (10 * (i + 2), f"A{i + 1}"),
                    label=label, dataset="LI-Medium")
        for i, label in enumerate(labels)
    ]

_pair = base_pair()
INVALID_INPUTS = {
    "identical_repeated_source": [_pair[0], dict(_pair[0]), _pair[1]],
    "conflicting_id_label": [_pair[0], dict(_pair[0], is_laundering=1), _pair[1]],
    "conflicting_id_content": [_pair[0], dict(_pair[0], amount_paid=200.0), _pair[1]],
    "null_id": base_pair(txn_id=None),
    "null_timestamp": base_pair(txn_ts=None),
    "null_bank": base_pair(from_bank=None),
    "null_account": base_pair(from_account=None),
    "null_label": base_pair(is_laundering=None),
    "nonbinary_label": base_pair(is_laundering=2),
    "null_method": base_pair(payment_format=None),
    "empty_currency": base_pair(payment_currency=""),
    "method_delimiter": base_pair(payment_format="Wire -> ACH"),
    "currency_delimiter": base_pair(receiving_currency="Euro -> Yen"),
    "null_dataset": base_pair(dataset=None),
    "unknown_dataset": base_pair(dataset="unknown"),
    "wrong_date": base_pair(txn_date="2022-09-02"),
    "subminute_timestamp": base_pair(txn_ts=datetime(2022, 9, 1, 10, 0, 1)),
    "blank_account": base_pair(from_account="   "),
}

# These test future output normalization/validation, not source-ID acceptance.
DUPLICATE_CANDIDATE_CHAINS = [(101, 102), (101, 102)]
REPEATED_ID_CANDIDATE = (101, 102, 101)

# Narrow, synthetic pair inputs test triple mechanics independently of source joins.
PAIR_SCHEMA = StructType([
    StructField("dataset", StringType()),
    StructField("first_txn_id", LongType()),
    StructField("second_txn_id", LongType()),
])
PAIR_SCENARIOS = {
    "multiple_branches": (
        [("sample", 1, 3), ("sample", 2, 3), ("sample", 3, 4), ("sample", 3, 5)],
        {("sample", (1, 3, 4)), ("sample", (1, 3, 5)),
         ("sample", (2, 3, 4)), ("sample", (2, 3, 5))}),
    "duplicates": (
        [("sample", 1, 2), ("sample", 1, 2), ("sample", 2, 3), ("sample", 2, 3)],
        {("sample", (1, 2, 3))}),
    "repeated_ids": (
        [("sample", 1, 1), ("sample", 1, 2), ("sample", 2, 1),
         ("sample", 2, 2)], set()),
    "dataset_isolation": (
        [("HI-Medium", 1, 2), ("LI-Medium", 2, 3)], set()),
    "no_shared_middle": ([("sample", 1, 2), ("sample", 3, 4)], set()),
    "empty": ([], set()),
}

# Approved identity regression requirements; fixed example is in the triple above.
SEQUENCE_ID_TEST_REQUIREMENTS = (
    "same identity survives input reorder and reruns",
    "dataset, rule_version and ordered IDs participate in identity",
    "labels and runtime values do not participate in identity",
    "negative and large bigint IDs serialize exactly",
    "fixed LF-only UTF-8 byte-string/hash example matches approved vector",
)
