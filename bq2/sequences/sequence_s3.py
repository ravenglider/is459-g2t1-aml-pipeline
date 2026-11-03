"""Injected S3 staging/metadata only. No clients, Spark sessions or IO on import.

No publication, Catalog operations, deletion, stale takeover or source reads.
Reservations coordinate cooperating writers; they are NOT S3 Object Lock or an
atomic multi-object transaction. Inventory equality is not a source snapshot.
Real Glue commit artifacts/permissions/SDK conditional support remain unverified.
"""

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from uuid import uuid4

from sequence_aws import require_verified_timezone
from sequence_manifest import _token, _hash, _day, _counts, _check_manifest

BUCKET = "is459-g2t1-aml-processed-bucket"
ROOT = "s3://" + BUCKET + "/"
STAGING = ROOT + "bq2_sequences_staging/"
METADATA = ROOT + "bq2_sequences_metadata/development/"
SOURCE = ROOT + "transactions_clean/dataset=sample/"


def _key(uri):
    if (not isinstance(uri, str) or not uri.startswith((STAGING, METADATA, SOURCE))
            or any(c in uri for c in ("\\", "%", "?", "#"))):
        raise ValueError("Unsafe S3 bucket/prefix; only isolated sample staging/metadata/source inventory allowed")
    key = uri[len(ROOT):]
    if any(part in ("", ".", "..") for part in key.rstrip("/").split("/")):
        raise ValueError("Unsafe S3 key")
    return key


def _attempt(uri):
    _key(uri)
    parts = uri.removeprefix(STAGING).rstrip("/").split("/")
    if not uri.startswith(STAGING) or len(parts) != 2:
        raise ValueError("Expected a staging run/attempt prefix")
    run, attempt = map(_token, parts)
    canonical = STAGING + run + "/" + attempt + "/"
    if uri != canonical:
        raise ValueError("Attempt URI must be canonical with trailing slash")
    return run, attempt


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _get(client, uri):
    response = client.get_object(Bucket=BUCKET, Key=_key(uri))
    body = response["Body"]
    try:
        raw = body.read()
    finally:
        body.close()
    if not isinstance(raw, bytes) or not isinstance(response.get("ETag"), str) or not response["ETag"]:
        raise ValueError("Missing object bytes/version token")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Malformed JSON receipt") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON receipt must be an object")
    return value, response["ETag"], raw


def _put(client, uri, value, etag=None):
    """Conditional write; uncertain responses reconcile exact bytes/current ETag.

    No unconditional fallback. Metadata revisions and owner identity are inside
    the compared bytes. A failed reconciliation preserves the original error.
    """
    if not uri.startswith((STAGING, METADATA)) or "/data/" in uri:
        raise ValueError("JSON writes restricted to staging/run metadata")
    raw = _bytes(value)
    condition = {"IfNoneMatch": "*"} if etag is None else {"IfMatch": etag}
    try:
        response = client.put_object(Bucket=BUCKET, Key=_key(uri), Body=raw,
                                     ContentType="application/json", **condition)
        if response.get("ETag"):
            return response["ETag"]
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code in ("412", "409", "PreconditionFailed", "ConditionalRequestConflict"):
            raise ValueError("Conditional S3 conflict; no overwrite or takeover permitted") from exc
        try:
            _, observed_etag, observed = _get(client, uri)
            if observed == raw:
                return observed_etag
        except Exception as read_error:
            exc.add_note("Lost-response reconciliation failed: " + str(read_error))
        raise
    _, observed_etag, observed = _get(client, uri)
    if observed != raw:
        raise ValueError("Unconfirmed S3 write outcome")
    return observed_etag


def inventory_s3_objects(client, prefix):
    """Complete paginated metadata, sorted by full URI. No content-hash claim.

    VersionId is included only if supplied; ListObjectsV2 normally omits it.
    ETags are opaque change/CAS tokens, not universally MD5 or SHA-256 hashes.
    Listings across pages are not a transactional snapshot.
    """
    key = _key(prefix)
    if not prefix.endswith("/"):
        raise ValueError("Inventory prefix must end with slash")
    records, seen, tokens, token = [], set(), set(), None
    while True:
        args = dict(Bucket=BUCKET, Prefix=key)
        if token is not None:
            args["ContinuationToken"] = token
        response = client.list_objects_v2(**args)
        for item in response.get("Contents", []):
            name, size, modified, etag = (item.get(k) for k in ("Key", "Size", "LastModified", "ETag"))
            if (not isinstance(name, str) or not name.startswith(key) or name in seen
                    or type(size) is not int or size < 0 or not isinstance(modified, datetime)
                    or modified.tzinfo is None or not isinstance(etag, str) or not etag):
                raise ValueError("Duplicate or inconsistent S3 inventory metadata")
            _key(ROOT + name)
            seen.add(name)
            record = dict(path=ROOT + name, size=size,
                          modification_time=modified.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), etag=etag)
            if "VersionId" in item:
                if not isinstance(item["VersionId"], str) or not item["VersionId"]:
                    raise ValueError("Invalid object version ID")
                record["version_id"] = item["VersionId"]
            records.append(record)
        if response.get("IsTruncated") is not True:
            break
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or not token or token in tokens:
            raise ValueError("Invalid pagination token")
        tokens.add(token)
    return sorted(records, key=lambda item: item["path"])


def _fingerprint(records):
    from sequence_source import fingerprint_source_inventory
    return fingerprint_source_inventory(records)


def _status(value, uri):
    run, attempt = _attempt(uri)
    if (value.get("format_version") != 1 or value.get("run_id") != run or value.get("attempt_id") != attempt
            or value.get("dataset") != "sample" or value.get("destination") != "development"
            or value.get("timezone") != "UTC" or type(value.get("revision")) is not int or value["revision"] < 0
            or value.get("state") not in ("WRITING", "VALIDATING", "VALIDATED", "FAILED")):
        raise ValueError("Invalid staging status ownership/revision/state")
    _token(value.get("owner_token"))
    _day(value.get("start_date"))
    _hash(value.get("source_fingerprint"))
    _counts(value.get("expected_counts"))
    return value


def reserve_staging_attempt_s3(client, run_id, attempt_id, *, dataset, destination,
                               start_date, timezone, timezone_verified, timezone_evidence,
                               expected_counts, source_fingerprint):
    """Reserve a fresh attempt by conditional creation, never reuse an old prefix."""
    if dataset != "sample" or destination != "development":
        raise ValueError("Only sample/development staging is supported")
    require_verified_timezone(timezone, timezone_verified, timezone_evidence)
    uri = STAGING + _token(run_id) + "/" + _token(attempt_id) + "/"
    _day(start_date)
    _hash(source_fingerprint)
    counts = _counts(expected_counts)
    if inventory_s3_objects(client, uri):
        raise ValueError("Existing staging attempt cannot be reused")
    record = dict(format_version=1, run_id=run_id, attempt_id=attempt_id, dataset=dataset,
                  destination=destination, start_date=start_date, timezone=timezone,
                  timezone_evidence=timezone_evidence, expected_counts=counts,
                  source_fingerprint=source_fingerprint, owner_token=uuid4().hex, revision=0, state="WRITING")
    etag = _put(client, uri + "metadata/status.json", record)
    return dict(uri=uri, status=record, etag=etag)


def _advance(client, reservation, state, **extra):
    uri, old = reservation["uri"], reservation["status"]
    _status(old, uri)
    actual, etag, _ = _get(client, uri + "metadata/status.json")
    if actual != old or etag != reservation["etag"]:
        raise ValueError("Staging owner/revision changed")
    allowed = {"WRITING": {"VALIDATING", "FAILED"}, "VALIDATING": {"VALIDATED", "FAILED"}}
    if state not in allowed.get(old["state"], set()):
        raise ValueError("Illegal staging transition")
    updated = dict(old, **extra, state=state, revision=old["revision"] + 1)
    token = _put(client, uri + "metadata/status.json", updated, etag)
    reservation.update(status=updated, etag=token)


def _data_files(uri, records, status):
    expected = uri + "data/dataset=sample/start_date=" + status["start_date"] + "/"
    files = []
    for record in records:
        path = record["path"]
        if path == uri + "data/_SUCCESS":
            continue
        if not path.startswith(expected) or "/" in path[len(expected):] or not path.endswith(".parquet"):
            raise ValueError("Unexpected partition, temporary or unknown staging artifact")
        files.append(path)
    if bool(files) != bool(sum(_counts(status["expected_counts"]).values())):
        raise ValueError("Staged files disagree with explicit empty/nonempty counts")
    return files


class _SparkParquetTransport:
    """Real Spark IO; tests may override path mapping, never analytical checks."""
    def path(self, uri):
        return uri

    def write(self, output, uri, max_records_per_file):
        (output.write.mode("error").option("compression", "snappy")
         .option("maxRecordsPerFile", max_records_per_file)
         .partitionBy("dataset", "start_date").parquet(self.path(uri)))

    def read(self, spark, uri, files, status):
        from sequence_staging import SEQUENCE_SCHEMA, _signature
        from pyspark.sql.types import StructType
        if not files:
            return spark.createDataFrame([], SEQUENCE_SCHEMA)
        session = spark.newSession()
        session.conf.set("spark.sql.session.timeZone", status["timezone"])
        session.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        physical = StructType(SEQUENCE_SCHEMA.fields[:-2])
        jvm, conf = spark.sparkContext._jvm, spark.sparkContext._jsc.hadoopConfiguration()
        for file in files:
            path = self.path(file)
            schema = session.read.option("recursiveFileLookup", "true").parquet(path).schema
            if _signature(schema) != _signature(physical):
                raise ValueError("Incompatible physical Parquet schema")
            reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(
                jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(jvm.org.apache.hadoop.fs.Path(path), conf))
            try:
                blocks = reader.getFooter().getBlocks()
                for i in range(blocks.size()):
                    columns = blocks.get(i).getColumns()
                    for k in range(columns.size()):
                        if columns.get(k).getCodec().name() != "SNAPPY":
                            raise ValueError("Staged Parquet must be Snappy compressed")
            finally:
                reader.close()
        return (session.read.option("basePath", self.path(uri)).parquet(*[self.path(f) for f in files])
                .select(*SEQUENCE_SCHEMA.fieldNames()))


def stage_sequence_batch_s3(client, output, run_id, attempt_id, *, dataset, destination,
                            start_date, timezone, timezone_verified, timezone_evidence,
                            expected_counts, source_fingerprint, max_records_per_file, transport=None):
    """Consume output INSIDE the caller's batch context; own no caches/session.

    The injected client/transport are the only IO capabilities. Empty attempts
    have explicit receipts and no required data objects. Failures are retained.
    """
    from sequence_staging import validate_sequence_output
    if dataset != "sample" or destination != "development":
        raise ValueError("Only sample/development staging is supported")
    _token(run_id)
    _token(attempt_id)
    _day(start_date)
    _hash(source_fingerprint)
    _counts(expected_counts)
    if type(max_records_per_file) is not int or max_records_per_file <= 0:
        raise ValueError("max_records_per_file must be positive")
    require_verified_timezone(timezone, timezone_verified, timezone_evidence)
    validate_sequence_output(output, dataset, start_date, timezone, expected_counts)
    reservation = reserve_staging_attempt_s3(client, run_id, attempt_id, dataset=dataset,
        destination=destination, start_date=start_date, timezone=timezone,
        timezone_verified=timezone_verified, timezone_evidence=timezone_evidence,
        expected_counts=expected_counts, source_fingerprint=source_fingerprint)
    uri = reservation["uri"]
    io = transport or _SparkParquetTransport()
    try:
        if sum(_counts(expected_counts).values()):
            io.write(output, uri + "data/", max_records_per_file)
        _advance(client, reservation, "VALIDATING")
        inventory = inventory_s3_objects(client, uri + "data/")
        files = _data_files(uri, inventory, reservation["status"])
        frame = io.read(output.sparkSession, uri + "data/", files, reservation["status"])
        report = validate_sequence_output(frame, dataset, start_date, timezone, expected_counts)
        if inventory_s3_objects(client, uri + "data/") != inventory:
            raise ValueError("Staged data inventory changed during validation")
        inventory_record = dict(records=inventory, fingerprint=_fingerprint(inventory))
        _put(client, uri + "metadata/inventory.json", inventory_record)
        _put(client, uri + "metadata/validation.json", report)
        allowed = {uri + "metadata/" + name + ".json" for name in ("status", "validation", "inventory")}
        if any(r["path"] not in allowed and not r["path"].startswith(uri + "data/")
               for r in inventory_s3_objects(client, uri)):
            raise ValueError("Unknown or temporary attempt artifacts")
        if inventory_s3_objects(client, uri + "data/") != inventory:
            raise ValueError("Staged data changed before final status")
        _advance(client, reservation, "VALIDATED", receipt_fingerprints={
            "inventory": _sha(_bytes(inventory_record)), "validation": _sha(_bytes(report))})
        return uri
    except Exception as exc:
        try:
            _advance(client, reservation, "FAILED", error_type=type(exc).__name__, error=str(exc))
        except Exception as receipt_error:
            exc.add_note("Could not record FAILED; preserve incomplete attempt: " + str(receipt_error))
        raise


def _load_validated(client, uri):
    run, _ = _attempt(uri)
    status, _, status_raw = _get(client, uri + "metadata/status.json")
    _status(status, uri)
    if status["state"] != "VALIDATED" or status["revision"] != 2:
        raise ValueError("Attempt is not VALIDATED")
    report, _, report_raw = _get(client, uri + "metadata/validation.json")
    inventory, _, inventory_raw = _get(client, uri + "metadata/inventory.json")
    if status.get("receipt_fingerprints") != dict(validation=_sha(report_raw), inventory=_sha(inventory_raw)):
        raise ValueError("Receipt fingerprints changed")
    counts = _counts(status["expected_counts"])
    if (_counts(report.get("counts")) != counts or type(report.get("row_count")) is not int
            or report["row_count"] != sum(counts.values()) or report.get("schema_valid") is not True
            or report.get("values_valid") is not True):
        raise ValueError("Inconsistent validation receipt")
    records = inventory_s3_objects(client, uri + "data/")
    if inventory.get("records") != records or inventory.get("fingerprint") != _fingerprint(records):
        raise ValueError("Staged artifacts changed")
    _data_files(uri, records, status)
    allowed_metadata = {uri + "metadata/" + name + ".json" for name in ("status", "validation", "inventory")}
    if any(r["path"] not in allowed_metadata and not r["path"].startswith(uri + "data/")
           for r in inventory_s3_objects(client, uri)):
        raise ValueError("Unknown or temporary attempt artifacts")
    return dict(run_id=run, attempt_path=uri, status=status, counts=counts,
                fingerprints=dict(status=_sha(status_raw), validation=_sha(report_raw), inventory=_sha(inventory_raw)),
                status_receipt=uri + "metadata/status.json", validation_receipt=uri + "metadata/validation.json")


def read_staged_batch_s3(client, spark, attempt_uri, *, timezone_verified,
                         timezone_evidence, transport=None):
    """Accept only complete receipts AND genuine distributed read-back validation."""
    from sequence_staging import validate_sequence_output
    loaded = _load_validated(client, attempt_uri)
    status = loaded["status"]
    require_verified_timezone(status["timezone"], timezone_verified, timezone_evidence)
    inventory = inventory_s3_objects(client, attempt_uri + "data/")
    files = _data_files(attempt_uri, inventory, status)
    frame = (transport or _SparkParquetTransport()).read(spark, attempt_uri + "data/", files, status)
    validate_sequence_output(frame, "sample", status["start_date"], "UTC", loaded["counts"])
    if _load_validated(client, attempt_uri) != loaded:
        raise ValueError("Staging evidence changed during read-back")
    return frame


def _sample_manifest(manifest):
    _check_manifest(manifest)
    if manifest["dataset"] != "sample" or manifest["destination"] != "development":
        raise ValueError("Only sample/development manifests supported")
    if manifest["state"] not in ("CREATED", "STAGING", "READY_TO_PLAN", "FAILED") or manifest.get("plan") is not None:
        raise ValueError("S3 manifest cannot proceed beyond READY_TO_PLAN")
    _hash(manifest["source_inventory"]["fingerprint"])
    require_verified_timezone(manifest["timezone"], True, manifest["timezone_evidence"])


def _loader(client, manifest):
    def load(uri):
        evidence = _load_validated(client, uri)
        if evidence["status"]["source_fingerprint"] != manifest["source_inventory"]["fingerprint"]:
            raise ValueError("Attempt source fingerprint differs from manifest")
        return evidence
    return load


def attach_validated_attempt_s3(client, manifest, attempt_uri):
    """Attach S3 receipt evidence using the SAME local manifest ownership rules."""
    from sequence_manifest import attach_validated_attempt
    _sample_manifest(manifest)
    return attach_validated_attempt(manifest, attempt_uri, receipt_loader=_loader(client, manifest))


def write_manifest_s3(client, manifest, *, owner_token, expected_revision=None,
                       expected_etag=None, current_source_fingerprint=None):
    """Conditional owner/revision update, never publication. Return current ETag.

    READY_TO_PLAN is recomputed through shared readiness rules and S3 receipts.
    Ownership is stored in the JSON, not inferred from an arbitrary prefix.
    Callers supply current source fingerprint; no source listing occurs here.
    """
    from sequence_manifest import validate_run_readiness
    _sample_manifest(manifest)
    _token(owner_token)
    value = deepcopy(manifest)
    value["storage_owner"] = owner_token
    uri = METADATA + manifest["run_id"] + "/manifest.json"
    if manifest["state"] in ("STAGING", "READY_TO_PLAN"):
        # Rebuild only receipt attachments through the shared ownership rules.
        from sequence_manifest import attach_validated_attempt
        rebuilt = dict(manifest, state="CREATED", batches={})
        for batch in manifest["batches"].values():
            rebuilt = attach_validated_attempt(rebuilt, batch["attempt_path"],
                                               receipt_loader=_loader(client, manifest))
        if rebuilt["batches"] != manifest["batches"]:
            raise ValueError("Manifest attachment evidence inconsistent")
    if manifest["state"] == "READY_TO_PLAN":
        readiness = validate_run_readiness(manifest, current_source_fingerprint=current_source_fingerprint,
                                           receipt_loader=_loader(client, manifest))
        if not isinstance(manifest.get("totals"), dict):
            raise ValueError("Manifest readiness totals are required")
        for name in ("counts", "row_count", "source_fingerprint", "receipts_verified"):
            if manifest["totals"].get(name) != readiness[name]:
                raise ValueError("Manifest readiness totals inconsistent")
    if expected_revision is None:
        if expected_etag is not None or manifest["state"] != "CREATED" or manifest["revision"] != 0:
            raise ValueError("First manifest write must be CREATED revision zero")
        return _put(client, uri, value)
    if type(expected_revision) is not int or not isinstance(expected_etag, str) or not expected_etag:
        raise ValueError("Explicit prior revision and ETag required")
    old, etag, raw = _get(client, uri)
    # Idempotent caller retry after a confirmed-but-lost response; no new write.
    if raw == _bytes(value):
        return etag
    _sample_manifest(old)
    if (etag != expected_etag or old.get("storage_owner") != owner_token
            or old["revision"] != expected_revision or manifest["revision"] <= expected_revision):
        raise ValueError("Manifest owner/revision/ETag conflict")
    allowed = {"CREATED": {"STAGING", "FAILED"}, "STAGING": {"STAGING", "READY_TO_PLAN", "FAILED"},
               "READY_TO_PLAN": {"FAILED"}, "FAILED": set()}
    if manifest["state"] not in allowed[old["state"]]:
        raise ValueError("Illegal manifest transition")
    immutable = set(old) - {"state", "revision", "updated_at", "batches", "totals", "error", "storage_owner"}
    if any(old[key] != manifest.get(key) for key in immutable):
        raise ValueError("Manifest source/version/coverage identity changed")
    if any(manifest["batches"].get(day) != batch for day, batch in old["batches"].items()):
        raise ValueError("Selected attempts cannot be removed or replaced within a run")
    return _put(client, uri, value, expected_etag)
