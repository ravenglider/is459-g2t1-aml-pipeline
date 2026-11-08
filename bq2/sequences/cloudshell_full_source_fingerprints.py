"""Standalone READ-ONLY HI/LI Catalog/S3 metadata fingerprints.

Parity-tested against sequence_full_source.py at publication commit 486f57a.
No Spark, object-content reads, writes or job APIs. Equality is change evidence, not a
transactional snapshot, refresh lock, row count or identity certification.
Only STS GetCallerIdentity, Glue GetTable/GetPartitions and S3 ListObjectsV2
are used. boto3 is imported only by main after explicit argument validation.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re

REGION = "us-east-1"
DATABASE = "is459_g2t1_aml"
TABLE = "transactions_clean"
BUCKET = "is459-g2t1-aml-processed-bucket"
SOURCE_ROOT = f"s3://{BUCKET}/transactions_clean/"
CLEANED_COLUMNS = (
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
)



DATASETS = {
    "HI-Medium": dict(first="2022-09-01", last="2022-09-28", rows=31898218, laundering=35230),
    "LI-Medium": dict(first="2022-09-01", last="2022-09-27", rows=31251469, laundering=16041),
}

def validate_catalog_table(table):
    """Check a supplied GetTable Table object; never fetch metadata implicitly."""

    if not isinstance(table, dict):
        raise ValueError("Catalog Table metadata is required")
    storage = table.get("StorageDescriptor", {})
    columns = [(c.get("Name"), c.get("Type")) for c in storage.get("Columns", [])]
    partitions = [(c.get("Name"), c.get("Type")) for c in table.get("PartitionKeys", [])]
    expected = list(CLEANED_COLUMNS)
    if (table.get("Name") != TABLE or storage.get("Location", "").rstrip("/") + "/" != SOURCE_ROOT
            or columns != expected[:-2] or partitions != expected[-2:]):
        raise ValueError("Catalog names, types, partition order or source location mismatch")

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

def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()

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

def inspect_full_source(catalog_client, s3_client, dataset, *, verified_empty_dates=None):
    """Inspect all selected-dataset dates; empty-date evidence is explicit text.

    A missing Catalog partition or absent files INSIDE coverage is a blocker
    unless independently attested empty. Boundaries remain the approved dates.
    """
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
    table_record = {k: table[k] for k in ("Name", "StorageDescriptor", "PartitionKeys", "Parameters", "VersionId", "UpdateTime") if k in table}
    partition_records = [{k: p[k] for k in ("Values", "StorageDescriptor", "Parameters", "CreationTime") if k in p}
                         for p in sorted(partitions, key=lambda p: p["Values"])]
    return dict(dataset=dataset, table=table, owner_dates=list(days), files_by_date=by_date,
        available_dates=[d for d in days if by_date[d]], verified_empty_dates=empty, objects=objects,
        source_fingerprint=fingerprint_source_inventory(objects), table_fingerprint=_digest(_serial(table_record)),
        partitions_fingerprint=_digest(_serial(partition_records)))

def inspect_twice(catalog_client, s3_client, dataset):
    """Accept only two identical complete observations, including inventories."""
    before = inspect_full_source(catalog_client, s3_client, dataset)
    after = inspect_full_source(catalog_client, s3_client, dataset)
    keys = ("source_fingerprint", "table_fingerprint", "partitions_fingerprint")
    if (any(before[k] != after[k] for k in keys)
            or before["objects"] != after["objects"]
            or before["owner_dates"] != after["owner_dates"]
            or before["files_by_date"] != after["files_by_date"]):
        raise ValueError("Source/Catalog changed between inspections; do not run generation")
    return dict(dataset=dataset, inspections=2, status="MATCHED_METADATA_ONLY",
        inspected_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        partitions=len(before["available_dates"]), objects=len(before["objects"]),
        parquet_files=sum(len(v) for v in before["files_by_date"].values()),
        **{k: before[k] for k in keys})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument("--region", required=True, choices=(REGION,))
    parser.add_argument("--dataset", required=True, choices=("HI-Medium", "LI-Medium"))
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9]{12}", args.expected_account_id):
        raise ValueError("Independently verified 12-digit expected account required")
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name=REGION)
    options = Config(connect_timeout=10, read_timeout=60, retries={"total_max_attempts": 1})
    identity = session.client("sts", config=options)
    if identity.meta.region_name != REGION or identity.get_caller_identity().get("Account") != args.expected_account_id:
        raise ValueError("Wrong account/region; no source inspection performed")
    catalog, s3 = (session.client(name, config=options) for name in ("glue", "s3"))
    if any(c.meta.region_name != REGION for c in (catalog, s3)):
        raise ValueError("Wrong source client region")
    evidence = inspect_twice(catalog, s3, args.dataset)
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()  # Success returns normally; all failures propagate.
