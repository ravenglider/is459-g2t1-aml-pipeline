"""Guarded local transformations for one start-date owner; no IO or Spark startup.

UTC is accepted for synthetic testing. Production must VERIFY that the original
source uses that same interpretation; passing 'UTC' is not evidence of it. Other
zones are rejected until calendar-window completeness across DST is implemented.
Global source identity/coverage and a stable snapshot remain caller prerequisites.
"""

from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import timedelta

from pyspark import StorageLevel
from pyspark.sql import functions as F

from bq2_sequence_builder import (
    _timestamp_bucket, validate_transactions, build_two_hop_links,
    build_three_hop_links, build_sequence_output,
)
from sequence_processing import DateBatchPlan, plan_start_date_batches


class GrowthLimitExceeded(ValueError):
    """An explicit operational limit stopped the batch without truncating rows."""


@dataclass(frozen=True)
class GrowthLimits:
    max_pair_candidates: int
    max_pair_links: int
    max_projected_triples: int
    max_triples_per_middle: int
    max_output_rows: int

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{field.name} must be an explicit nonnegative integer")


@dataclass(frozen=True)
class BatchResult:
    output: object
    metrics: dict


def _enforce_limit(name, actual, limit):
    if actual > limit:
        raise GrowthLimitExceeded(f"{name}={actual} exceeds configured limit {limit}")


def _sum_products(grouped):
    # Counts are bigint, but their products/sums must not silently overflow bigint.
    product = F.col("predecessors").cast("decimal(38,0)") * F.col("successors").cast("decimal(38,0)")
    values = grouped.select(product.alias("product"))
    row = values.agg(F.count("*").alias("groups"), F.count("product").alias("valid"),
                     F.sum("product").alias("total"), F.max("product").alias("maximum")).first()
    if row.groups != row.valid or (row.groups and row.total is None):
        raise OverflowError("Growth estimate exceeds decimal arithmetic capacity")
    return int(row.total or 0), int(row.maximum or 0)


def estimate_pair_candidates(transactions, first_dates):
    """Upper bound before transaction self-join, including time-invalid candidates.

    Preconditions: validated source and explicit eligible preceding dates. Group
    counts by bank/account and fixed elapsed-day buckets; match same/next bucket.
    No IDs are cast and no transaction-level pairs are enumerated.
    """
    source = transactions.withColumn("_bucket", _timestamp_bucket(F.col("txn_ts")))
    incoming = (source.where(F.col("txn_date").isin(first_dates))
                .groupBy("dataset", "to_bank", "to_account", "_bucket").count()
                .select("dataset", F.col("to_bank").alias("bank"), F.col("to_account").alias("account"),
                        F.col("_bucket").alias("preceding_bucket"), F.col("count").alias("predecessors")))
    outgoing = (source.groupBy("dataset", "from_bank", "from_account", "_bucket").count()
                .select("dataset", F.col("from_bank").alias("bank"), F.col("from_account").alias("account"),
                        F.col("_bucket").alias("following_bucket"), F.col("count").alias("successors")))
    candidates = incoming.join(outgoing, ["dataset", "bank", "account"]).where(
        (F.col("following_bucket") == F.col("preceding_bucket"))
        | (F.col("following_bucket") == F.col("preceding_bucket") + 1))
    return _sum_products(candidates)[0]


def estimate_three_hop_growth(starting_links, continuation_links):
    """Exact (total, largest-middle expansion) for UNIQUE VALID chronological links.

    Arbitrary cyclic/malformed/duplicated ID pairs do not satisfy this precondition.
    Counts do not enumerate triples. Dataset is part of every middle-ID key.
    """
    before = (starting_links.groupBy("dataset", "second_txn_id").count()
              .select("dataset", F.col("second_txn_id").alias("middle"),
                      F.col("count").alias("predecessors")))
    after = (continuation_links.groupBy("dataset", "first_txn_id").count()
             .select("dataset", F.col("first_txn_id").alias("middle"),
                     F.col("count").alias("successors")))
    return _sum_products(before.join(after, ["dataset", "middle"]))


def _persist_owned(frame, owned):
    # Spark caches logical plans, not Python object identities. Detect caller-owned
    # cached equivalents before persisting; never unpersist those on exit.
    manager = frame.sparkSession._jsparkSession.sharedState().cacheManager()
    if not manager.lookupCachedData(frame._jdf).nonEmpty():
        frame.persist(StorageLevel.MEMORY_AND_DISK)
        owned.append(frame)
    return frame


@contextmanager
def process_start_date_batch(transactions, dataset, plan, timezone, limits):
    """Yield verified output and metrics; consume output INSIDE the with block.

    Cleans only batch-owned persistence on success/failure and does not stop Spark.
    No read/write/publication or snapshot certification occurs. Limits are required
    and have no production defaults. Missing inventory evidence is NOT repaired.
    """
    if dataset not in ("sample", "HI-Medium", "LI-Medium"):
        raise ValueError("Invalid dataset")
    if timezone != "UTC" or transactions.sparkSession.conf.get("spark.sql.session.timeZone") != timezone:
        raise ValueError("This calendar-window runner requires explicit matching UTC; production source semantics must be verified")
    if not isinstance(limits, GrowthLimits):
        raise ValueError("Explicit GrowthLimits are required")
    if not isinstance(plan, DateBatchPlan):
        raise ValueError("A ready DateBatchPlan is required")
    expected, = plan_start_date_batches(
        plan.start_date.isoformat(), plan.start_date.isoformat(),
        dataset_start_date=plan.dataset_start_date.isoformat(), dataset_end_date=plan.dataset_end_date.isoformat(),
        available_partition_dates=[d.isoformat() for d in plan.available_input_dates],
        verified_empty_dates=[d.isoformat() for d in plan.verified_empty_input_dates])
    if expected != plan or not plan.ready_to_process:
        raise ValueError("Blocked or inconsistent batch plan")

    owned = []
    try:
        source = _persist_owned(transactions, owned)
        validate_transactions(source)
        allowed = [d.isoformat() for d in plan.available_input_dates]
        if source.where((F.col("dataset") != dataset) | ~F.col("txn_date").isin(allowed)).limit(1).count():
            raise ValueError("Source rows outside batch dataset/date coverage")
        first_dates = [plan.start_date.isoformat(), (plan.start_date + timedelta(days=1)).isoformat()]
        candidates = estimate_pair_candidates(source, first_dates)
        _enforce_limit("pair_candidates", candidates, limits.max_pair_candidates)
        pairs = _persist_owned(build_two_hop_links(source, first_dates=first_dates), owned)
        pair_count = pairs.count()
        _enforce_limit("pair_links", pair_count, limits.max_pair_links)
        first_ids = source.where(F.col("txn_date") == plan.start_date.isoformat()).select(
            "dataset", F.col("txn_id").alias("first_txn_id"))
        starting = pairs.join(first_ids, ["dataset", "first_txn_id"], "left_semi")
        start_count = starting.count()
        projected, largest = estimate_three_hop_growth(starting, pairs)
        _enforce_limit("projected_triples", projected, limits.max_projected_triples)
        _enforce_limit("triples_per_middle", largest, limits.max_triples_per_middle)
        _enforce_limit("output_rows", start_count + projected, limits.max_output_rows)
        triples = _persist_owned(build_three_hop_links(pairs, starting_links=starting), owned)
        actual_triples = triples.count()
        if actual_triples != projected:
            raise ValueError("Actual triple count differs from exact projection")
        output = _persist_owned(build_sequence_output(source, starting, triples), owned)
        counts = {r.sequence_length: r["count"] for r in output.groupBy("sequence_length").count().collect()}
        expected_counts = {length: count for length, count in ((2, start_count), (3, actual_triples)) if count}
        if counts != expected_counts:
            raise ValueError("Actual output counts differ from verified link counts")
        if output.where((F.col("dataset") != dataset)
                        | (F.col("start_date") != plan.start_date.isoformat())).limit(1).count():
            raise ValueError("Output dataset/start-date ownership mismatch")
        yield BatchResult(output, dict(pair_candidates=candidates, pair_links=pair_count,
                          starting_pairs=start_count, projected_triples=projected,
                          largest_middle_expansion=largest, actual_triples=actual_triples,
                          output_rows=start_count + actual_triples))
    finally:
        # Attempt every cleanup even if one unpersist fails; preserve active errors.
        import sys
        active_error = sys.exc_info()[0] is not None
        cleanup_error = None
        for frame in reversed(owned):
            try:
                frame.unpersist(blocking=True)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
        if cleanup_error is not None and not active_error:
            raise cleanup_error
