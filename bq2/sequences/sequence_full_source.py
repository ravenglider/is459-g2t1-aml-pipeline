"""Full HI/LI metadata and pruned reads; injected clients only, no publication.

Frozen coverage/baselines come from the approved Role 2 handoff. Missing dates
are never inferred to be empty. Inventory equality is not a refresh lock.
"""
from datetime import datetime, timezone
import re

from sequence_aws import DATABASE, TABLE, SOURCE_ROOT, validate_catalog_table, require_verified_timezone
from sequence_manifest import _digest
from sequence_processing import plan_start_date_batches

DATASETS = {
    "HI-Medium": dict(first="2022-09-01", last="2022-09-28", rows=31898218, laundering=35230),
    "LI-Medium": dict(first="2022-09-01", last="2022-09-27", rows=31251469, laundering=16041),
}
BUCKET = "is459-g2t1-aml-processed-bucket"


def dataset_spec(dataset):
    if not isinstance(dataset, str) or dataset not in DATASETS:
        raise ValueError("DATASET must be exactly HI-Medium or LI-Medium")
    return dict(DATASETS[dataset])


def owner_dates(dataset):
    spec = dataset_spec(dataset)
    from datetime import date, timedelta
    first, last = date.fromisoformat(spec["first"]), date.fromisoformat(spec["last"])
    return tuple((first + timedelta(days=i)).isoformat() for i in range((last-first).days+1))


def inventory_objects(client, prefix):
    """Complete metadata listing within one allowlisted source/staging prefix."""
    roots = tuple(SOURCE_ROOT + "dataset=" + d + "/" for d in DATASETS) + (
        f"s3://{BUCKET}/bq2_sequences_staging/full/",)
    if (not isinstance(prefix, str) or not prefix.startswith(roots) or not prefix.endswith("/")
            or any(c in prefix for c in ("%", "\\", "?", "#"))
            or any(p in ("", ".", "..") for p in prefix.split("/", 3)[3].rstrip("/").split("/"))):
        raise ValueError("Unsafe full-generation inventory prefix")
    root = f"s3://{BUCKET}/"
    records, seen, tokens, token = [], set(), set(), None
    while True:
        args = dict(Bucket=BUCKET, Prefix=prefix[len(root):])
        if token is not None:
            args["ContinuationToken"] = token
        response = client.list_objects_v2(**args)
        for obj in response.get("Contents", []):
            key, size, stamp, etag = (obj.get(k) for k in ("Key", "Size", "LastModified", "ETag"))
            if (not isinstance(key, str) or not key.startswith(args["Prefix"]) or key in seen
                    or type(size) is not int or size < 0 or not isinstance(stamp, datetime)
                    or stamp.tzinfo is None or not isinstance(etag, str) or not etag):
                raise ValueError("Malformed/duplicate inventory metadata")
            seen.add(key)
            records.append(dict(path=root+key, size=size, etag=etag,
                modification_time=stamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))
        if response.get("IsTruncated") is not True:
            break
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or not token or token in tokens:
            raise ValueError("Invalid S3 pagination")
        tokens.add(token)
    return sorted(records, key=lambda r: r["path"])


def _serial(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Catalog datetimes must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {k: _serial(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serial(v) for v in value]
    return value


def inspect_full_source(catalog_client, s3_client, dataset, *, verified_empty_dates=None):
    """Inspect all selected-dataset dates; empty-date evidence is explicit text.

    A missing Catalog partition or absent files INSIDE coverage is a blocker
    unless independently attested empty. Boundaries remain the approved dates.
    """
    from sequence_source import fingerprint_source_inventory
    spec, days = dataset_spec(dataset), owner_dates(dataset)
    empty = {} if verified_empty_dates is None else dict(verified_empty_dates)
    if (not set(empty) <= set(days) or any(not isinstance(v, str) or not v.strip() for v in empty.values())):
        raise ValueError("Verified empty dates require in-range independent evidence")
    table = catalog_client.get_table(DatabaseName=DATABASE, Name=TABLE)["Table"]
    validate_catalog_table(table)
    if "parquet" not in table["StorageDescriptor"].get("InputFormat", "").lower():
        raise ValueError("Source table must be Parquet")
    partitions, seen, tokens, token = [], set(), set(), None
    prefix = SOURCE_ROOT + "dataset=" + dataset + "/"
    while True:
        args = dict(DatabaseName=DATABASE, TableName=TABLE, Expression=f"dataset = '{dataset}'")
        if token is not None:
            args["NextToken"] = token
        page = catalog_client.get_partitions(**args)
        for partition in page.get("Partitions", []):
            values = partition.get("Values")
            if (not isinstance(values, list) or len(values) != 2 or values[0] != dataset
                    or values[1] not in days or values[1] in seen):
                raise ValueError("Unexpected/duplicate dataset partition")
            day = values[1]
            expected = prefix + "txn_date=" + day + "/"
            storage = partition.get("StorageDescriptor", {})
            if storage.get("Location") not in (expected, expected[:-1]):
                raise ValueError(f"Source partition location mismatch: {dataset}/{day}")
            validate_catalog_table(dict(table, StorageDescriptor=dict(storage, Location=SOURCE_ROOT)))
            if "parquet" not in storage.get("InputFormat", "").lower():
                raise ValueError("Source partition must be Parquet")
            seen.add(day)
            partitions.append(partition)
        token = page.get("NextToken")
        if token is None:
            break
        if not isinstance(token, str) or not token or token in tokens:
            raise ValueError("Invalid Catalog pagination")
        tokens.add(token)
    if set(days) - seen - set(empty):
        raise ValueError("Missing required Catalog partitions")
    objects = inventory_objects(s3_client, prefix)
    by_date = {d: [] for d in days}
    for record in objects:
        tail = record["path"][len(prefix):]
        if tail == "_SUCCESS":
            continue
        match = re.fullmatch(r"txn_date=([0-9-]{10})/(_SUCCESS|[A-Za-z0-9][A-Za-z0-9._-]*)", tail)
        if not match or match[1] not in seen or (not match[2].endswith(".parquet") and match[2] != "_SUCCESS"):
            raise ValueError("Unexpected source object/partition")
        if match[2].endswith(".parquet"):
            if record["size"] <= 0:
                raise ValueError("Zero-byte Parquet file")
            by_date[match[1]].append(record["path"])
    if any((not by_date[d] and d not in empty) or (by_date[d] and d in empty) for d in days):
        raise ValueError("Missing files or conflicting verified-empty evidence")
    plans = plan_start_date_batches(spec["first"], spec["last"], dataset_start_date=spec["first"],
        dataset_end_date=spec["last"], available_partition_dates=[d for d in days if by_date[d]],
        verified_empty_dates=empty)
    table_record = {k: table[k] for k in ("Name", "StorageDescriptor", "PartitionKeys", "Parameters", "VersionId", "UpdateTime") if k in table}
    partition_records = [{k: p[k] for k in ("Values", "StorageDescriptor", "Parameters", "CreationTime") if k in p}
                         for p in sorted(partitions, key=lambda p: p["Values"])]
    return dict(dataset=dataset, table=table, owner_dates=list(days), plans=plans, files_by_date=by_date,
        available_dates=[d for d in days if by_date[d]], verified_empty_dates=empty, objects=objects,
        source_fingerprint=fingerprint_source_inventory(objects), table_fingerprint=_digest(_serial(table_record)),
        partitions_fingerprint=_digest(_serial(partition_records)))


def snapshot_identity(snapshot):
    return {k: snapshot[k] for k in ("source_fingerprint", "table_fingerprint", "partitions_fingerprint")}


def assert_full_source_unchanged(before, after):
    if (snapshot_identity(before) != snapshot_identity(after)
            or before["dataset"] != after["dataset"] or before["objects"] != after["objects"]
            or before["owner_dates"] != after["owner_dates"] or before["verified_empty_dates"] != after["verified_empty_dates"]):
        raise ValueError("Full source/Catalog snapshot changed")


def read_full_catalog(glue_context, dataset, dates, *, snapshot, timezone_evidence):
    """Explicit partition pushdown, exact logical/physical types, no casts.

    Checks all inventoried Parquet footers once; full row/ID validation is owned
    by the orchestrator once per selected snapshot, before batch filtering.
    """
    from sequence_source import CLEANED_SCHEMA, build_partition_predicate, validate_cleaned_schema, _check_schema
    from pyspark.sql.types import StructType
    dataset_spec(dataset)
    require_verified_timezone("UTC", True, timezone_evidence)
    dates = list(dates)
    if (not dates or len(set(dates)) != len(dates) or snapshot["dataset"] != dataset
            or set(dates) != set(snapshot["available_dates"])):
        raise ValueError("Reader must cover exactly the selected snapshot's available dates")
    spark = glue_context.spark_session
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Explicit matching UTC session required")
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    for day in dates:
        for uri in snapshot["files_by_date"][day]:
            _check_schema(session.read.option("recursiveFileLookup", "true").parquet(uri).schema,
                          StructType(CLEANED_SCHEMA.fields[:-2]))
    frame = glue_context.create_dynamic_frame.from_catalog(database=DATABASE, table_name=TABLE,
        push_down_predicate=build_partition_predicate(dataset, dates)).toDF()
    if len(frame.columns) != 20 or set(frame.columns) != set(CLEANED_SCHEMA.fieldNames()):
        raise ValueError("Source must contain exactly the 20 cleaned columns")
    frame = frame.select(*CLEANED_SCHEMA.fieldNames())
    validate_cleaned_schema(frame)
    # Row ownership and required-field checks run once AFTER persistence in
    # profile_full_source; no extra uncached full-dataset scan here.
    return frame
