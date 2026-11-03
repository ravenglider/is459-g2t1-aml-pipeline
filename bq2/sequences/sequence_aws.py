"""Injected Glue Catalog reader, not an S3 writer or snapshot certificate.

Importing this module imports no AWS SDK, Glue runtime or Spark runtime. The
caller supplies an initialized GlueContext and independently verified metadata.
Catalog/logical schema checks do not inspect physical Parquet footer types;
that additional check remains a prerequisite for the later real-data runner.
"""

from sequence_processing import _iso_date

DATABASE = "is459_g2t1_aml"
TABLE = "transactions_clean"
SOURCE_ROOT = "s3://is459-g2t1-aml-processed-bucket/transactions_clean/"


def selected_dates(dataset, dates):
    """Reject empty selections rather than permitting an unrestricted read."""
    if dataset != "sample":
        raise ValueError("This integration supports sample only")
    if isinstance(dates, (str, bytes)):
        raise ValueError("dates must be a nonempty collection")
    try:
        result = tuple(sorted({_iso_date(day, "dates").isoformat() for day in dates}))
    except TypeError as exc:
        raise ValueError("dates must be a nonempty collection") from exc
    if not result:
        raise ValueError("Empty date selection is forbidden")
    return result


def require_verified_timezone(timezone, verified, evidence):
    if timezone != "UTC" or verified is not True or not isinstance(evidence, str) or not evidence.strip():
        raise ValueError("Verified upstream UTC semantics and independent evidence are required")


def validate_catalog_table(table):
    """Check a supplied GetTable Table object; never fetch metadata implicitly."""
    from sequence_source import CLEANED_SCHEMA

    if not isinstance(table, dict):
        raise ValueError("Catalog Table metadata is required")
    storage = table.get("StorageDescriptor", {})
    columns = [(c.get("Name"), c.get("Type")) for c in storage.get("Columns", [])]
    partitions = [(c.get("Name"), c.get("Type")) for c in table.get("PartitionKeys", [])]
    expected = [(f.name, f.dataType.simpleString()) for f in CLEANED_SCHEMA]
    if (table.get("Name") != TABLE or storage.get("Location", "").rstrip("/") + "/" != SOURCE_ROOT
            or columns != expected[:-2] or partitions != expected[-2:]):
        raise ValueError("Catalog names, types, partition order or source location mismatch")


def read_catalog_selected(glue_context, dataset, dates, *, available_dates,
                          timezone, timezone_verified, timezone_evidence, catalog_table):
    """Read only explicit available dates using Catalog pushdown BEFORE data IO.

    available_dates must come from a separately checked Catalog/S3 inventory.
    It is supplied evidence, not proof obtained by this adapter. A daily result
    is never dataset-wide identity/completeness verification. Verified-empty
    dates are handled by the planner, not by falling back to an unrestricted read.
    """
    dates = selected_dates(dataset, dates)
    available = selected_dates(dataset, available_dates)
    if not set(dates) <= set(available):
        raise ValueError("Missing required input partitions")
    require_verified_timezone(timezone, timezone_verified, timezone_evidence)
    validate_catalog_table(catalog_table)
    if glue_context.spark_session.conf.get("spark.sql.session.timeZone") != timezone:
        raise ValueError("Spark session timezone mismatch")

    from pyspark.sql import functions as F
    from sequence_source import CLEANED_SCHEMA, build_partition_predicate, validate_cleaned_schema
    from bq2_sequence_builder import validate_transactions

    frame = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE, table_name=TABLE,
        push_down_predicate=build_partition_predicate(dataset, dates),
    ).toDF()
    names = CLEANED_SCHEMA.fieldNames()
    if len(frame.columns) != len(names) or set(frame.columns) != set(names):
        raise ValueError("Catalog result must contain exactly the 20 cleaned columns")
    # Catalog readers can return a different column order. Reorder, never cast.
    frame = frame.select(*names)
    validate_cleaned_schema(frame)
    if frame.where(F.col("dataset").isNull() | F.col("txn_date").isNull()
                   | (F.col("dataset") != dataset) | ~F.col("txn_date").isin(dates)).limit(1).count():
        raise ValueError("Catalog result contains rows outside selected partitions")
    validate_transactions(frame)
    return frame
