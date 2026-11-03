"""Local-only source checks. Importing starts no Spark session or cloud access.

Inventory equality is change evidence, not a transactional snapshot or a lock.
The production runner must prevent upstream refreshes and attest full dataset
coverage before interpreting an ID check as dataset-wide verification.
"""

import hashlib
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from bq2_sequence_builder import validate_transaction_ids, validate_transactions
from sequence_processing import DateBatchPlan, _iso_date, plan_start_date_batches


CLEANED_SCHEMA = StructType([
    StructField(name, kind(), True) for name, kind in (
        ("txn_id", LongType), ("txn_ts", TimestampType),
        ("from_bank", LongType), ("from_account", StringType),
        ("to_bank", LongType), ("to_account", StringType),
        ("amount_paid", DoubleType), ("payment_currency", StringType),
        ("amount_paid_usd", DoubleType), ("amount_received", DoubleType),
        ("receiving_currency", StringType), ("amount_received_usd", DoubleType),
        ("payment_format", StringType), ("cross_bank", IntegerType),
        ("is_laundering", IntegerType), ("attempt_id", IntegerType),
        ("pattern_type", StringType), ("usd_missing", IntegerType),
        ("dataset", StringType), ("txn_date", StringType),
    )
])


def _dataset(dataset):
    if dataset not in ("sample", "HI-Medium", "LI-Medium"):
        raise ValueError("Invalid dataset: choose sample, HI-Medium or LI-Medium")


def build_partition_predicate(dataset, dates):
    """Exact catalog predicate; an empty date set is FALSE, never an unfiltered read."""
    _dataset(dataset)
    if isinstance(dates, (str, bytes)):
        raise ValueError("dates must be an iterable of YYYY-MM-DD strings")
    try:
        selected = sorted({_iso_date(d, "dates").isoformat() for d in dates})
    except TypeError as exc:
        raise ValueError("dates must be an iterable of date strings") from exc
    if not selected:
        return "1 = 0"
    values = ", ".join(f"'{d}'" for d in selected)
    return f"dataset = '{dataset}' AND txn_date IN ({values})"


def _check_schema(actual, expected):
    # Ignore nullability: Parquet readers legitimately relax it.
    a = [(field.name, field.dataType) for field in actual]
    b = [(field.name, field.dataType) for field in expected]
    if a != b:
        raise ValueError("Incompatible transactions_clean schema: names, order and types must match")


def validate_cleaned_schema(transactions):
    """Require all 20 source columns in order, without any implicit type repair."""
    _check_schema(transactions.schema, CLEANED_SCHEMA)


def read_cleaned_local(spark, root, dataset, plan, timezone):
    """Read only explicit local partition files from a ready, internally consistent plan.

    Caller supplies independently confirmed bounds/empty-date evidence. This
    function cannot establish those facts itself. It checks every selected file's
    physical schema before reading, then validates required values. It uses a
    separate SQL session to disable partition type inference without changing the
    caller's settings. Both sessions share the caller-owned SparkContext.
    """
    _dataset(dataset)
    if not isinstance(timezone, str) or not timezone.strip():
        raise ValueError("An explicit verified timezone is required")
    if spark.conf.get("spark.sql.session.timeZone") != timezone:
        raise ValueError("Spark session timezone does not match the explicit timezone")
    if not isinstance(plan, DateBatchPlan):
        raise ValueError("A DateBatchPlan is required")
    reconstructed, = plan_start_date_batches(
        plan.start_date.isoformat(), plan.start_date.isoformat(),
        dataset_start_date=plan.dataset_start_date.isoformat(),
        dataset_end_date=plan.dataset_end_date.isoformat(),
        available_partition_dates=[d.isoformat() for d in plan.available_input_dates],
        verified_empty_dates=[d.isoformat() for d in plan.verified_empty_input_dates],
    )
    if reconstructed != plan or not plan.ready_to_process:
        raise ValueError("Blocked or inconsistent date plan: missing required partitions")
    if "://" in str(root):
        raise ValueError("This reader accepts local paths only; no cloud/URI roots")
    base = Path(root).resolve()
    if not base.is_dir():
        raise ValueError("Local source root does not exist")
    files = []
    for day in plan.available_input_dates + plan.verified_empty_input_dates:
        partition = base / f"dataset={dataset}" / f"txn_date={day.isoformat()}"
        if not partition.resolve().is_relative_to(base):
            raise ValueError("Partition escapes the local source root")
        found = sorted(partition.rglob("*.parquet")) if partition.is_dir() else []
        if any(not path.resolve().is_relative_to(partition.resolve()) for path in found):
            raise ValueError("Parquet file escapes its partition")
        if day in plan.verified_empty_input_dates:
            if found:
                raise ValueError("Verified-empty date unexpectedly contains Parquet files")
        elif not found:
            raise ValueError(f"Missing required Parquet files for {day}")
        else:
            files.extend(found)
    if not files:
        # A ready plan must account for every in-bounds day as independently empty.
        return spark.createDataFrame([], CLEANED_SCHEMA)

    reader_session = spark.newSession()
    reader_session.conf.set("spark.sql.session.timeZone", timezone)
    reader_session.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    physical = StructType(CLEANED_SCHEMA.fields[:-2])
    # Pass native absolute paths with forward slashes, not pre-escaped file URIs.
    # Path.as_uri() encodes Hive directory '=' as '%3D'; Hadoop's string-path
    # handling can then look for literal percent-encoded directory names.
    for path in files:
        _check_schema(reader_session.read.parquet(path.resolve().as_posix()).schema, physical)
    frame = (reader_session.read.option("basePath", base.as_posix())
             .parquet(*[path.resolve().as_posix() for path in files]))
    frame = frame.select(*CLEANED_SCHEMA.fieldNames())
    validate_cleaned_schema(frame)
    if frame.where((F.col("dataset") != dataset) | ~F.col("txn_date").isin(
            [d.isoformat() for d in plan.available_input_dates])).limit(1).count():
        raise ValueError("Rows do not match selected dataset/date population")
    validate_transactions(frame)
    return frame


def validate_dataset_identity(transactions, dataset):
    """Check IDs across the FULL selected dataset, including distant dates.

    PRECONDITION for a global interpretation: caller reads every partition from
    one confirmed source inventory, not a DateBatchPlan slice. A DataFrame has no
    trustworthy completeness provenance: this function ALWAYS reports
    dataset_wide_verified=False and provided_dataframe_only scope. The later
    production runner must separately attest inventory coverage/snapshot evidence.
    This prevents a successful partial check from claiming global certification.
    Only dataset/txn_id are required, permitting a narrow full-dataset scan.
    """
    _dataset(dataset)
    validate_transaction_ids(transactions)
    if transactions.where(F.col("dataset").isNull() | (F.col("dataset") != dataset)).limit(1).count():
        raise ValueError("Rows outside the selected dataset population")
    return {"dataset": dataset, "row_count": transactions.count(),
            "identity_check_passed": True, "verification_scope": "provided_dataframe_only",
            "dataset_wide_verified": False}


def fingerprint_source_inventory(records):
    """Hash supplied file metadata, sorted by path; no file/cloud IO.

    Each record: path (nonempty str), size (nonnegative int), modification_time
    (canonical UTC YYYY-MM-DDTHH:MM:SS.ffffffZ string), optional version_id/etag.
    ETags are change evidence, not universally content hashes. Callers must list
    the complete intended source inventory; equality cannot prevent refreshes,
    prove completeness or detect a transient change restored between inventories.
    """
    from datetime import datetime

    normalized, seen = [], set()
    for record in records:
        if not isinstance(record, dict) or set(record) - {"path", "size", "modification_time", "version_id", "etag"}:
            raise ValueError("Invalid source inventory record")
        path, size, modified = (record.get(k) for k in ("path", "size", "modification_time"))
        if not isinstance(path, str) or not path.strip() or path in seen:
            raise ValueError("Inventory paths must be nonempty and unique")
        if type(size) is not int or size < 0:
            raise ValueError("Inventory size must be a nonnegative integer")
        try:
            parsed = datetime.strptime(modified, "%Y-%m-%dT%H:%M:%S.%fZ")
            if parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != modified:
                raise ValueError()
        except (TypeError, ValueError) as exc:
            raise ValueError("Inventory modification_time must be canonical UTC with six fractional digits") from exc
        item = {"path": path, "size": size, "modification_time": modified}
        for key in ("version_id", "etag"):
            value = record.get(key)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"Inventory {key} must be a string or null")
            item[key] = value
        seen.add(path)
        normalized.append(item)
    payload = json.dumps(sorted(normalized, key=lambda r: r["path"]),
                         sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def assert_source_unchanged(before, after):
    """Compare supplied inventory records; raise on additions/removals/metadata changes."""
    if fingerprint_source_inventory(before) != fingerprint_source_inventory(after):
        raise ValueError("Source inventory changed; abort processing/publication")
