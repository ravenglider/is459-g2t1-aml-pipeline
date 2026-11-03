"""Local, importable sequence transformations. Importing starts no Spark session.

This is not a runnable Glue job. The production caller must supply a consistent
source snapshot, configure its verified timezone, and coordinate publication.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, TimestampType


def _require_columns(frame, required):
    for name, expected_type in required.items():
        if frame.columns.count(name) != 1:
            raise ValueError(f"Expected exactly one input column named {name}")
        if frame.schema[name].dataType != expected_type:
            raise ValueError(f"{name} must have Spark {expected_type.simpleString()} type")


def validate_transaction_ids(transactions: DataFrame) -> None:
    """Raise ValueError for null IDs or duplicate IDs within a dataset.

    Require bigint IDs without casting: floating-point IDs may already have lost
    precision. Identical repeated source rows also fail; silently dropping them
    would disagree with Role 6's full-source denominators.
    """
    _require_columns(transactions, {"txn_id": LongType(), "dataset": StringType()})
    if transactions.where(F.col("txn_id").isNull()).limit(1).count():
        raise ValueError("Null transaction IDs are not allowed")
    duplicates = (transactions.groupBy("dataset", "txn_id")
                  .count().where(F.col("count") > 1))
    if duplicates.limit(1).count():
        raise ValueError("Duplicate transaction IDs within a dataset are not allowed")


def validate_transactions(transactions: DataFrame) -> None:
    """Reject invalid required source fields without dropping or repairing rows.

    Date consistency uses the caller's Spark session timezone. Minute precision
    matches Role 2's parser and permits exact integer-second duration arithmetic.
    """
    required = {"txn_id": LongType(), "dataset": StringType(),
                "txn_ts": TimestampType(), "from_bank": LongType(),
                "to_bank": LongType(), "is_laundering": IntegerType(),
                **{name: StringType() for name in (
                    "from_account", "to_account", "payment_format",
                    "payment_currency", "receiving_currency", "txn_date")}}
    _require_columns(transactions, required)
    validate_transaction_ids(transactions)
    checks = {name: F.col(name).isNull() for name in required}
    checks["dataset"] = checks["dataset"] | ~F.col("dataset").isin("sample", "HI-Medium", "LI-Medium")
    checks["is_laundering"] = checks["is_laundering"] | ~F.col("is_laundering").isin(0, 1)
    checks["txn_ts"] = checks["txn_ts"] | (F.col("txn_ts") != F.date_trunc("minute", "txn_ts"))
    checks["txn_date"] = checks["txn_date"] | (F.col("txn_date") != F.date_format("txn_ts", "yyyy-MM-dd"))
    for name in ("from_account", "to_account", "payment_format", "payment_currency", "receiving_currency"):
        checks[name] = checks[name] | (F.length(F.trim(F.col(name))) == 0)
    for name in ("payment_format", "payment_currency", "receiving_currency"):
        checks[name] = checks[name] | F.col(name).contains(" -> ")
    result = transactions.agg(*[
        F.max(F.when(invalid, 1).otherwise(0)).alias(name)
        for name, invalid in checks.items()
    ]).first().asDict()
    failed = [name for name, invalid in result.items() if invalid == 1]
    if failed:
        raise ValueError("Invalid required transaction fields: " + ", ".join(failed))


def _timestamp_bucket(column):
    """Fixed elapsed 24-hour bucket; decimal division avoids floating point."""
    return F.floor(column.cast("bigint").cast("decimal(20,0)") / F.lit(86400)).cast("bigint")


def build_two_hop_links(transactions: DataFrame, *, first_dates=None) -> DataFrame:
    """Return distinct dataset/first_txn_id/second_txn_id links.

    Validates required source fields before joining. This function
    neither starts Spark nor reads or writes any external data.
    first_dates optionally restricts preceding transactions only; later receivers
    remain available. Omission preserves the original unrestricted behavior.
    """
    validate_transactions(transactions)
    columns = ["dataset", "txn_id", "txn_ts", "from_bank", "from_account",
               "to_bank", "to_account"]
    source = transactions.select(*columns).withColumn("_bucket", _timestamp_bucket(F.col("txn_ts")))
    preceding = source
    if first_dates is not None:
        from sequence_processing import _iso_date
        if isinstance(first_dates, (str, bytes)):
            raise ValueError("first_dates must be an iterable of date strings")
        dates = [_iso_date(d, "first_dates").isoformat() for d in first_dates]
        preceding = source.where(F.date_format("txn_ts", "yyyy-MM-dd").isin(dates))
    a = preceding.alias("a")
    b = source.alias("b")
    condition = (
        (F.col("a.dataset") == F.col("b.dataset"))
        & (F.col("a.to_bank") == F.col("b.from_bank"))
        & (F.col("a.to_account") == F.col("b.from_account"))
        & (F.col("a.txn_id") != F.col("b.txn_id"))
        & ((F.col("b._bucket") == F.col("a._bucket"))
           | (F.col("b._bucket") == F.col("a._bucket") + 1))
        & (F.col("b.txn_ts") > F.col("a.txn_ts"))
        & (F.col("b.txn_ts") <= F.col("a.txn_ts") + F.expr("INTERVAL 24 HOURS"))
    )
    return (a.join(b, condition, "inner")
            .select(F.col("a.dataset").alias("dataset"),
                    F.col("a.txn_id").alias("first_txn_id"),
                    F.col("b.txn_id").alias("second_txn_id"))
            .dropDuplicates(["dataset", "first_txn_id", "second_txn_id"]))


def build_three_hop_links(two_hop_links: DataFrame, *, starting_links=None) -> DataFrame:
    """Extend validated A->B and B->C links into distinct A->B->C triples.

    Input must come from the same validated transaction population. ID pairs
    alone cannot establish account/time validity; those checks belong upstream.
    This transformation performs no actions, source validation or caching.
    starting_links optionally restricts the left-hand pairs before expansion;
    it must be a subset of the same unique, validated chronological input links.
    """
    required = {"dataset": StringType(), "first_txn_id": LongType(),
                "second_txn_id": LongType()}
    _require_columns(two_hop_links, required)

    if starting_links is not None:
        _require_columns(starting_links, required)
    left = (two_hop_links if starting_links is None else starting_links).select(*required).alias("left")
    right = two_hop_links.select(*required).alias("right")
    condition = (
        (F.col("left.dataset") == F.col("right.dataset"))
        & (F.col("left.second_txn_id") == F.col("right.first_txn_id"))
        & (F.col("left.first_txn_id") != F.col("left.second_txn_id"))
        & (F.col("right.first_txn_id") != F.col("right.second_txn_id"))
        & (F.col("left.first_txn_id") != F.col("right.second_txn_id"))
    )
    return (left.join(right, condition, "inner")
            .select(F.col("left.dataset").alias("dataset"),
                    F.col("left.first_txn_id").alias("first_txn_id"),
                    F.col("left.second_txn_id").alias("second_txn_id"),
                    F.col("right.second_txn_id").alias("third_txn_id"))
            .dropDuplicates(["dataset", "first_txn_id", "second_txn_id", "third_txn_id"]))


def _sequence_id(dataset, txn_ids, rule_version="v1"):
    """SHA-256 of UTF-8 version LF dataset LF comma-joined signed decimal IDs.

    No CR, trailing newline, padding, labels or floating-point ID conversions.
    Public output is fixed to v1; the argument supports identity regression tests.
    """
    decimal_ids = F.transform(txn_ids, lambda value: value.cast("string"))
    payload = F.concat(F.lit(rule_version + "\n"), dataset, F.lit("\n"),
                       F.concat_ws(",", decimal_ids))
    return F.sha2(F.encode(payload, "UTF-8"), 256)


def build_sequence_output(transactions: DataFrame, two_hop_links: DataFrame,
                          three_hop_links: DataFrame) -> DataFrame:
    """Enrich links into the approved 21 columns, failing on invalid members.

    All joins use dataset and ID. Link predicates are checked against recovered
    members so independently supplied/mismatched links cannot be published.
    Matching values cannot prove historical snapshot identity: the later job
    must enforce one source snapshot for link construction and enrichment.
    No Spark startup, file IO, publication or automatic caching occurs here.
    """
    validate_transactions(transactions)
    pair_types = {"dataset": StringType(), "first_txn_id": LongType(), "second_txn_id": LongType()}
    _require_columns(two_hop_links, pair_types)
    _require_columns(three_hop_links, {**pair_types, "third_txn_id": LongType()})
    names = list(pair_types)
    pairs = (two_hop_links.select(*names).withColumn("sequence_length", F.lit(2))
             .withColumn("third_txn_id", F.lit(None).cast("bigint")))
    triples = three_hop_links.select(*names, "third_txn_id").withColumn("sequence_length", F.lit(3))
    links = pairs.unionByName(triples).dropDuplicates(
        ["dataset", "first_txn_id", "second_txn_id", "third_txn_id", "sequence_length"])
    fields = ["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account",
              "payment_format", "payment_currency", "receiving_currency", "is_laundering"]
    joined = links
    for index, id_name in enumerate(("first_txn_id", "second_txn_id", "third_txn_id"), 1):
        alias = f"t{index}"
        lookup = transactions.select(F.col("dataset").alias(f"d{index}"),
                                     F.struct(*fields).alias(alias))
        joined = (joined.join(lookup,
                  (F.col("dataset") == F.col(f"d{index}"))
                  & (F.col(id_name) == F.col(f"{alias}.txn_id")), "left")
                  .drop(f"d{index}"))

    def valid_link(first, second):
        return ((F.col(f"{first}.to_bank") == F.col(f"{second}.from_bank"))
                & (F.col(f"{first}.to_account") == F.col(f"{second}.from_account"))
                & (F.col(f"{second}.txn_ts") > F.col(f"{first}.txn_ts"))
                & (F.col(f"{second}.txn_ts") <= F.col(f"{first}.txn_ts") + F.expr("INTERVAL 24 HOURS")))

    valid = (F.col("dataset").isNotNull() & F.col("t1.txn_id").isNotNull()
             & F.col("t2.txn_id").isNotNull()
             & (F.col("first_txn_id") != F.col("second_txn_id")) & valid_link("t1", "t2")
             & ((F.col("sequence_length") == 2)
                | (F.col("t3.txn_id").isNotNull()
                   & (F.col("first_txn_id") != F.col("third_txn_id"))
                   & (F.col("second_txn_id") != F.col("third_txn_id"))
                   & valid_link("t2", "t3"))))
    if joined.where(~F.coalesce(valid, F.lit(False))).limit(1).count():
        raise ValueError("Missing, repeated or mismatched sequence members in dataset/source population")

    def member_array(field):
        return F.when(F.col("sequence_length") == 2,
                      F.array(F.col(f"t1.{field}"), F.col(f"t2.{field}"))).otherwise(
            F.array(F.col(f"t1.{field}"), F.col(f"t2.{field}"), F.col(f"t3.{field}")))

    output = joined.select("dataset", "sequence_length", *[
        member_array(field).alias(name) for name, field in (
            ("txn_ids", "txn_id"), ("txn_timestamps", "txn_ts"),
            ("payment_methods", "payment_format"), ("payment_currencies", "payment_currency"),
            ("receiving_currencies", "receiving_currency"),
            ("txn_laundering_labels", "is_laundering"))])
    output = (output.withColumn("rule_version", F.lit("v1"))
              .withColumn("sequence_id", _sequence_id(F.col("dataset"), F.col("txn_ids")))
              .withColumn("method_sequence", F.concat_ws(" -> ", "payment_methods"))
              .withColumn("payment_currency_sequence", F.concat_ws(" -> ", "payment_currencies"))
              .withColumn("receiving_currency_sequence", F.concat_ws(" -> ", "receiving_currencies"))
              .withColumn("laundering_count", F.aggregate("txn_laundering_labels", F.lit(0), lambda a, b: a + b))
              .withColumn("any_laundering", (F.col("laundering_count") > 0).cast("int"))
              .withColumn("all_laundering", (F.col("laundering_count") == F.col("sequence_length")).cast("int"))
              .withColumn("final_txn_id", F.element_at("txn_ids", -1))
              .withColumn("final_is_laundering", F.element_at("txn_laundering_labels", -1))
              .withColumn("gap_seconds", F.transform(F.sequence(F.lit(2), F.col("sequence_length")),
                          lambda i: F.element_at("txn_timestamps", i).cast("bigint")
                          - F.element_at("txn_timestamps", i - 1).cast("bigint")))
              .withColumn("duration_seconds", F.element_at("txn_timestamps", -1).cast("bigint")
                          - F.element_at("txn_timestamps", 1).cast("bigint"))
              .withColumn("start_date", F.date_format(F.element_at("txn_timestamps", 1), "yyyy-MM-dd")))
    return output.select(
        "sequence_id", "rule_version", "sequence_length", "txn_ids", "txn_timestamps",
        "payment_methods", "method_sequence", "payment_currencies", "receiving_currencies",
        "payment_currency_sequence", "receiving_currency_sequence", "txn_laundering_labels",
        "laundering_count", "any_laundering", "all_laundering", "final_txn_id", "final_is_laundering",
        "gap_seconds", "duration_seconds", "dataset", "start_date").dropDuplicates(["dataset", "txn_ids"])
