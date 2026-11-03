"""Immutable LOCAL staging attempts, not publication or catalog registration.

Call stage_sequence_batch_local inside the batch context while its caches live.
No Spark startup/cache ownership changes. Validation cannot independently prove
bank/account chaining without source transactions, source snapshot consistency,
or production timezone semantics. UTC in tests is not production evidence.
"""

import json
import os
import re
import stat
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)
from bq2_sequence_builder import _sequence_id
from sequence_processing import _iso_date


SEQUENCE_SCHEMA = StructType([StructField(name, kind, True) for name, kind in (
    ("sequence_id", StringType()), ("rule_version", StringType()), ("sequence_length", IntegerType()),
    ("txn_ids", ArrayType(LongType())), ("txn_timestamps", ArrayType(TimestampType())),
    ("payment_methods", ArrayType(StringType())), ("method_sequence", StringType()),
    ("payment_currencies", ArrayType(StringType())), ("receiving_currencies", ArrayType(StringType())),
    ("payment_currency_sequence", StringType()), ("receiving_currency_sequence", StringType()),
    ("txn_laundering_labels", ArrayType(IntegerType())), ("laundering_count", IntegerType()),
    ("any_laundering", IntegerType()), ("all_laundering", IntegerType()),
    ("final_txn_id", LongType()), ("final_is_laundering", IntegerType()),
    ("gap_seconds", ArrayType(LongType())), ("duration_seconds", LongType()),
    ("dataset", StringType()), ("start_date", StringType()),
)])


def _signature(schema):
    return [(field.name, field.dataType.simpleString()) for field in schema]


def validate_sequence_schema(transactions):
    """Check all 21 ordered names/types, ignoring nullable metadata at every level."""
    if _signature(transactions.schema) != _signature(SEQUENCE_SCHEMA):
        raise ValueError("Sequence schema must match all 21 approved names, order and types")


def _counts(values):
    if not isinstance(values, dict) or set(values) not in ({2, 3}, {"2", "3"}):
        raise ValueError("expected_counts must explicitly contain lengths 2 and 3")
    result = {int(key): value for key, value in values.items()}
    if any(type(value) is not int or value < 0 for value in result.values()):
        raise ValueError("Expected counts must be nonnegative integers")
    return result


def _ownership(spark, dataset, start_date, timezone):
    if dataset not in ("sample", "HI-Medium", "LI-Medium"):
        raise ValueError("Invalid dataset")
    _iso_date(start_date, "start_date")
    if not isinstance(timezone, str) or not timezone.strip() or spark.conf.get("spark.sql.session.timeZone") != timezone:
        raise ValueError("Explicit timezone must match the Spark session; source semantics require independent verification")


def validate_sequence_output(output, dataset, start_date, timezone, expected_counts):
    """Distributed value/count checks; collect only two length-count records.

    Reuses the exact approved v1 hash expression. No source bank/account fields
    exist in this schema; those link predicates must be verified upstream.
    """
    validate_sequence_schema(output)
    _ownership(output.sparkSession, dataset, start_date, timezone)
    expected = _counts(expected_counts)
    valid = F.lit(True)
    for field in SEQUENCE_SCHEMA:
        valid = valid & F.col(field.name).isNotNull()
        if isinstance(field.dataType, ArrayType):
            valid = valid & ~F.exists(F.col(field.name), lambda value: value.isNull())
    valid = (valid & (F.col("dataset") == dataset) & (F.col("start_date") == start_date)
             & (F.col("rule_version") == "v1") & F.col("sequence_length").isin(2, 3))
    for name in ("txn_ids", "txn_timestamps", "payment_methods", "payment_currencies",
                 "receiving_currencies", "txn_laundering_labels"):
        valid = valid & (F.size(name) == F.col("sequence_length"))
    valid = (valid & (F.size("gap_seconds") == F.col("sequence_length") - 1)
             & (F.size(F.array_distinct("txn_ids")) == F.col("sequence_length"))
             & ~F.exists("txn_laundering_labels", lambda value: ~value.isin(0, 1)))
    for name, readable in (("payment_methods", "method_sequence"),
                           ("payment_currencies", "payment_currency_sequence"),
                           ("receiving_currencies", "receiving_currency_sequence")):
        valid = (valid & ~F.exists(name, lambda value: (F.length(F.trim(value)) == 0) | value.contains(" -> "))
                 & (F.col(readable) == F.concat_ws(" -> ", name)))
    labels = F.aggregate("txn_laundering_labels", F.lit(0), lambda a, b: a + b)
    gaps = F.transform(F.sequence(F.lit(1), F.col("sequence_length") - 1),
                       lambda i: F.get("txn_timestamps", i).cast("bigint")
                       - F.get("txn_timestamps", i - 1).cast("bigint"))
    valid = (valid & (F.col("laundering_count") == labels)
             & (F.col("any_laundering") == (labels > 0).cast("int"))
             & (F.col("all_laundering") == (labels == F.col("sequence_length")).cast("int"))
             & (F.col("final_txn_id") == F.get("txn_ids", F.col("sequence_length") - 1))
             & (F.col("final_is_laundering") == F.get("txn_laundering_labels", F.col("sequence_length") - 1))
             & ~F.exists("txn_timestamps", lambda ts: ts != F.date_trunc("minute", ts))
             & (F.col("gap_seconds") == gaps)
             & ~F.exists(gaps, lambda gap: (gap <= 0) | (gap > 86400))
             & (F.col("duration_seconds") == F.aggregate(gaps, F.lit(0).cast("bigint"), lambda a, b: a + b))
             & (F.col("start_date") == F.date_format(F.get("txn_timestamps", 0), "yyyy-MM-dd"))
             & (F.col("sequence_id") == _sequence_id(F.col("dataset"), F.col("txn_ids"))))
    if output.where(~F.coalesce(valid, F.lit(False))).limit(1).count():
        raise ValueError("Invalid sequence values, identity, timing, labels or ownership")
    for keys in (("dataset", "sequence_id"), ("dataset", "txn_ids")):
        if output.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
            raise ValueError("Duplicate sequence identity or ordered chain")
    actual = {2: 0, 3: 0}
    actual.update({r.sequence_length: r["count"] for r in output.groupBy("sequence_length").count().collect()})
    if actual != expected:
        raise ValueError(f"Sequence row counts differ: expected {expected}, actual {actual}")
    return {"counts": actual, "row_count": sum(actual.values()), "schema_valid": True, "values_valid": True}


def _is_link(path):
    if path.is_symlink():
        return True
    try:
        # Python 3.11 lacks Path.is_junction(); reject Windows reparse points too.
        return bool(getattr(path.lstat(), "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    except FileNotFoundError:
        return False


def _local_path(value):
    if not isinstance(value, (str, os.PathLike)):
        raise ValueError("A local filesystem path is required")
    text = str(value)
    if "://" in text or text.startswith(("//", "\\\\")):
        raise ValueError("Only absolute local filesystem paths are allowed")
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("An absolute local path without '..' is required")
    for ancestor in (path, *path.parents):
        if _is_link(ancestor):
            raise ValueError("Symlinks/junctions are not allowed in staging paths")
        if ancestor.exists() and not ancestor.is_dir():
            raise ValueError("Staging path ancestors must be directories")
    return path.resolve()


def _attempt_path(staging_root, development_root, production_root, run_id, attempt_id):
    root = _local_path(staging_root)
    for table_root in (development_root, production_root):
        other = _local_path(table_root)
        if root == other or root in other.parents or other in root.parents:
            raise ValueError("Staging and table roots must not overlap")
    for value in (run_id, attempt_id):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
            raise ValueError("Unsafe run/attempt identifier")
    return _local_path(root / run_id / attempt_id)


def _write_json(path, record):
    """Atomic same-directory replacement; interrupted temporary files stay visible.

    Local filesystem semantics only, not a distributed lock or S3 guarantee.
    """
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(record, handle, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _inspect_tree(attempt):
    for path in attempt.rglob("*"):
        if _is_link(path):
            raise ValueError("Symlinks/junctions are not allowed in attempts")
        if path.name.startswith("_temporary") or path.name.endswith(".tmp"):
            raise ValueError("Incomplete attempt contains leftover temporary files")


def _read_data(spark, attempt, dataset, start_date, timezone, expected):
    _inspect_tree(attempt)
    data = attempt / "data"
    if not data.is_dir():
        raise ValueError("Staged data directory is missing")
    files = sorted(data.rglob("*.parquet"))
    if not files:
        if sum(expected.values()):
            raise ValueError("Expected nonempty staged Parquet files")
        # Only Spark completion/checksum artifacts may accompany an empty result.
        if any(p.is_file() and p.name != "_SUCCESS" and not p.name.endswith(".crc") for p in data.rglob("*")):
            raise ValueError("Unexpected files in empty staged output")
        return spark.createDataFrame([], SEQUENCE_SCHEMA)
    if not sum(expected.values()):
        raise ValueError("Empty receipt contradicts staged Parquet files")
    expected_parent = data / f"dataset={dataset}" / f"start_date={start_date}"
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", timezone)
    session.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    physical = StructType(SEQUENCE_SCHEMA.fields[:-2])
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for path in files:
        if path.parent != expected_parent:
            raise ValueError("Unexpected dataset/start-date partition layout")
        schema = session.read.option("recursiveFileLookup", "true").parquet(path.as_posix()).schema
        if _signature(schema) != _signature(physical):
            raise ValueError("Incompatible physical Parquet schema")
        reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(
            jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(jvm.org.apache.hadoop.fs.Path(path.as_posix()), conf))
        try:
            blocks = reader.getFooter().getBlocks()
            for i in range(blocks.size()):
                columns = blocks.get(i).getColumns()
                for k in range(columns.size()):
                    if columns.get(k).getCodec().name() != "SNAPPY":
                        raise ValueError("Staged Parquet is not Snappy-compressed")
        finally:
            reader.close()
    output = session.read.option("basePath", data.as_posix()).parquet(*[p.as_posix() for p in files])
    return output.select(*SEQUENCE_SCHEMA.fieldNames())


def read_staged_batch_local(spark, attempt_path, *, dataset, start_date, timezone, expected_counts=None):
    """Read and revalidate a VALIDATED local attempt; never trust _SUCCESS alone."""
    _ownership(spark, dataset, start_date, timezone)
    attempt = _local_path(attempt_path)
    _inspect_tree(attempt)
    try:
        receipt = json.loads((attempt / "metadata" / "status.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("Missing or invalid staging status receipt") from exc
    if not isinstance(receipt, dict) or receipt.get("format_version") != 1 or receipt.get("state") != "VALIDATED":
        raise ValueError("Attempt is not VALIDATED")
    if (receipt.get("dataset"), receipt.get("start_date"), receipt.get("timezone")) != (dataset, start_date, timezone):
        raise ValueError("Receipt ownership/timezone mismatch")
    expected = _counts(receipt.get("expected_counts"))
    if expected_counts is not None and _counts(expected_counts) != expected:
        raise ValueError("Expected counts disagree with receipt")
    output = _read_data(spark, attempt, dataset, start_date, timezone, expected)
    report = validate_sequence_output(output, dataset, start_date, timezone, expected)
    try:
        saved = json.loads((attempt / "metadata" / "validation.json").read_text(encoding="utf-8"))
        if (not isinstance(saved, dict) or _counts(saved.get("counts")) != expected
                or saved.get("row_count") != report["row_count"]
                or saved.get("schema_valid") is not True or saved.get("values_valid") is not True):
            raise ValueError("Validation receipt disagrees with staged output")
    except (OSError, ValueError) as exc:
        raise ValueError("Missing or invalid staging validation receipt") from exc
    return output


def stage_sequence_batch_local(output, staging_root, run_id, attempt_id, *, dataset,
                               start_date, timezone, expected_counts, max_records_per_file,
                               development_root, production_root):
    """Write a fresh immutable attempt and return its path after read-back validation.

    Use inside process_start_date_batch's with block. No append/overwrite, cleanup
    of old attempts, cache changes, publication or catalog writes. Local exclusive
    mkdir prevents cooperative duplicate writers; hostile concurrent symlink
    replacement is outside this local trust model. Failed artifacts are retained.
    """
    validate_sequence_schema(output)
    _ownership(output.sparkSession, dataset, start_date, timezone)
    expected = _counts(expected_counts)
    if type(max_records_per_file) is not int or max_records_per_file <= 0:
        raise ValueError("max_records_per_file must be a positive integer")
    attempt = _attempt_path(staging_root, development_root, production_root, run_id, attempt_id)
    attempt.parent.mkdir(parents=True, exist_ok=True)
    _local_path(attempt)  # Recheck after parent creation, before exclusive reservation.
    attempt.mkdir(exist_ok=False)
    metadata = attempt / "metadata"
    metadata.mkdir()
    record = dict(format_version=1, dataset=dataset, start_date=start_date,
                  timezone=timezone, expected_counts=expected, state="WRITING")
    status = metadata / "status.json"
    try:
        _write_json(status, record)
        if sum(expected.values()):
            (output.write.mode("error").option("compression", "snappy")
             .option("maxRecordsPerFile", max_records_per_file)
             .partitionBy("dataset", "start_date").parquet((attempt / "data").as_posix()))
        else:
            # Validate actual emptiness, not merely a caller's zero expectation.
            validate_sequence_output(output, dataset, start_date, timezone, expected)
            (attempt / "data").mkdir()
        record["state"] = "VALIDATING"
        _write_json(status, record)
        read_back = _read_data(output.sparkSession, attempt, dataset, start_date, timezone, expected)
        report = validate_sequence_output(read_back, dataset, start_date, timezone, expected)
        _write_json(metadata / "validation.json", report)
        record["state"] = "VALIDATED"
        _write_json(status, record)
        return attempt
    except Exception as exc:
        record.update(state="FAILED", error_type=type(exc).__name__, error=str(exc))
        try:
            _write_json(status, record)
        except Exception as receipt_error:
            exc.add_note(f"Could not record failure receipt: {receipt_error}")
        raise
