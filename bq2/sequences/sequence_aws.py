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
SAMPLE_DATES = tuple(f"2022-09-{day:02d}" for day in range(1, 7))


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
                   | (F.col("dataset") != dataset) | ~F.col("txn_date").isin(list(dates))).limit(1).count():
        raise ValueError("Catalog result contains rows outside selected partitions")
    validate_transactions(frame)
    return frame


def inspect_sample_source(catalog_client, s3_client):
    """READ ONLY complete sample metadata snapshot; injected clients only.

    No transaction scan. Pagination, exact partition locations and every sample
    file are checked; metadata fingerprints detect changes, not snapshot locks.
    """
    from datetime import datetime, timezone
    import hashlib
    import json
    import re
    from sequence_manifest import _digest
    from sequence_s3 import inventory_s3_objects, SOURCE
    from sequence_source import fingerprint_source_inventory

    table = catalog_client.get_table(DatabaseName=DATABASE, Name=TABLE)["Table"]
    validate_catalog_table(table)
    if "parquet" not in table.get("StorageDescriptor", {}).get("InputFormat", "").lower():
        raise ValueError("transactions_clean must use a Parquet input format")
    partitions, tokens, token = [], set(), None
    while True:
        args = dict(DatabaseName=DATABASE, TableName=TABLE, Expression="dataset = 'sample'")
        if token is not None:
            args["NextToken"] = token
        response = catalog_client.get_partitions(**args)
        partitions.extend(response.get("Partitions", []))
        token = response.get("NextToken")
        if token is None:
            break
        if not isinstance(token, str) or not token or token in tokens:
            raise ValueError("Invalid sample Catalog pagination")
        tokens.add(token)
    days = []
    for partition in partitions:
        values = partition.get("Values")
        if not isinstance(values, list) or len(values) != 2 or values[0] != "sample":
            raise ValueError("Catalog returned a non-sample/malformed partition")
        day = _iso_date(values[1], "partition date").isoformat()
        expected = SOURCE + "txn_date=" + day + "/"
        storage = partition.get("StorageDescriptor", {})
        actual = storage.get("Location")
        # Glue's API may omit the final separator. Accept exactly these two
        # spellings; do not trim whitespace, repeated slashes or other segments.
        if actual not in (expected, expected[:-1]):
            details = dict(dataset="sample", txn_date=day, expected_location=expected,
                           actual_location_type=type(actual).__name__)
            if isinstance(actual, str):
                details["actual_location_length"] = len(actual)
                details["actual_location_sha256"] = hashlib.sha256(actual.encode("utf-8")).hexdigest()
                # Show only recognizable group transaction-partition paths.
                # JSON escapes whitespace; query strings, credentials and other
                # arbitrary URI content are redacted. Never normalize for equality.
                safe_path = (r"[ \t\r\n]*" + re.escape(SOURCE_ROOT)
                    + r"dataset=(?:sample|HI-Medium|LI-Medium)/txn_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/?[ \t\r\n]*")
                details["actual_location"] = actual if re.fullmatch(safe_path, actual) else "<redacted unexpected URI>"
            else:
                details["actual_location"] = "<missing or non-string location>"
            raise ValueError("Sample Catalog/S3 partition locations disagree: " + json.dumps(details, sort_keys=True))
        validate_catalog_table(dict(table, StorageDescriptor=dict(storage, Location=SOURCE_ROOT)))
        if "parquet" not in storage.get("InputFormat", "").lower():
            raise ValueError("Sample partition is not Parquet")
        days.append(day)
    if sorted(days) != list(SAMPLE_DATES):
        raise ValueError("Missing, duplicate or unexpected sample partitions")
    objects = inventory_s3_objects(s3_client, SOURCE)
    files, found = [], set()
    for record in objects:
        tail = record["path"][len(SOURCE):]
        if tail == "_SUCCESS":
            continue
        pieces = tail.split("/")
        if len(pieces) != 2 or not pieces[0].startswith("txn_date="):
            raise ValueError("Unexpected sample S3 object layout")
        day = pieces[0][len("txn_date="):]
        if day not in SAMPLE_DATES or (not pieces[1].endswith(".parquet") and pieces[1] != "_SUCCESS"):
            raise ValueError("Unexpected sample date/file artifact")
        if pieces[1].endswith(".parquet"):
            if record["size"] == 0:
                raise ValueError("Empty sample Parquet object")
            files.append(record["path"])
            found.add(day)
    if found != set(SAMPLE_DATES):
        raise ValueError("Missing sample Parquet objects")

    def serial(value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("Catalog metadata datetime must be timezone-aware")
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, dict):
            return {key: serial(item) for key, item in value.items()}
        if isinstance(value, list):
            return [serial(item) for item in value]
        return value
    # Stable relevant metadata; do not fingerprint access-time counters.
    table_record = {key: table[key] for key in ("Name", "StorageDescriptor", "PartitionKeys", "Parameters", "VersionId", "UpdateTime") if key in table}
    partition_records = [{key: p[key] for key in ("Values", "StorageDescriptor", "Parameters", "CreationTime") if key in p}
                         for p in sorted(partitions, key=lambda p: p["Values"])]
    return dict(table=table, dates=list(SAMPLE_DATES), files=sorted(files), objects=objects,
                source_fingerprint=fingerprint_source_inventory(objects),
                table_fingerprint=_digest(serial(table_record)),
                partitions_fingerprint=_digest(serial(partition_records)))


def validate_sample_physical_schema(spark, files):
    """Check each inventoried file's 18 physical names/types, without casting."""
    from sequence_source import CLEANED_SCHEMA, _check_schema
    from pyspark.sql.types import StructType
    if not files or len(files) != len(set(files)):
        raise ValueError("Explicit unique sample Parquet files required")
    prefix = SOURCE_ROOT + "dataset=sample/"
    for uri in files:
        if not isinstance(uri, str) or not uri.startswith(prefix):
            raise ValueError("Physical schema scan must be sample-only")
        tail = uri[len(prefix):].split("/")
        if (len(tail) != 2 or tail[0] not in ["txn_date=" + d for d in SAMPLE_DATES]
                or not tail[1].endswith(".parquet") or any(c in uri for c in ("%", "\\", "?", "#"))):
            raise ValueError("Unsafe sample file path")
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    physical = StructType(CLEANED_SCHEMA.fields[:-2])
    for uri in files:
        schema = session.read.option("recursiveFileLookup", "true").parquet(uri).schema
        _check_schema(schema, physical)
