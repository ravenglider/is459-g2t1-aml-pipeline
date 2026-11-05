"""Bounded sample staging-run evidence only, never a publication manifest.

Injected S3 storage and explicitly gated B1 coordination; no clients/Spark creation.
Receipt and generation evidence are attestations, not proof of a transactional
snapshot or completeness of the underlying HI/LI data. Terminal success means
staging only. Failed/incomplete attempts must be preserved for diagnosis.
"""
from copy import deepcopy
import json
from sequence_manifest import _token, _hash, _counts, _now
from sequence_sample_generation import validate_config, OWNER_DATE, INPUT_DATES
from sequence_aws import SAMPLE_DATES
from sequence_s3 import STAGING, METADATA, _bytes, _get, _put, _load_validated

KIND = "bounded_sample_staging_run"
STATES = {"CREATED", "STAGING", "STAGED_VALIDATED", "FAILED"}
MUTABLE = {"state", "revision", "updated_at", "generation", "attempt_evidence", "error"}


def create_staging_run(config, attempt_id, max_records_per_file, *, owner_token, builder_version):
    """Pure constructor; no previous-publication values or table lookup required."""
    config = validate_config(config)
    _token(attempt_id)
    _token(owner_token)
    if not isinstance(builder_version, str) or not builder_version.strip():
        raise ValueError("Explicit builder version required")
    if type(max_records_per_file) is not int or max_records_per_file <= 0:
        raise ValueError("Positive max_records_per_file required")
    now = _now()
    result = dict(format_version=1, manifest_kind=KIND, dataset="sample", destination="development",
        run_id=config["run_id"], attempt_id=attempt_id, storage_owner=owner_token,
        manifest_uri=METADATA+config["run_id"]+"/staging_run.json",
        attempt_uri=STAGING+config["run_id"]+"/"+attempt_id+"/",
        config=config, builder_version=builder_version, rule_version="v1", published=False,
        scopes=dict(observed_extract=dict(dates=list(SAMPLE_DATES), expected_rows=500000,
            expected_laundering=35, underlying_dataset_complete=False),
            generation_input=list(INPUT_DATES), output_owner=OWNER_DATE),
        max_records_per_file=max_records_per_file, generation=None, attempt_evidence=None,
        error=None, state="CREATED", revision=0, created_at=now, updated_at=now)
    validate_staging_run(result)
    return result


def _receipt_summary(manifest, receipt):
    if not isinstance(receipt, dict) or not isinstance(receipt.get("status"), dict):
        raise ValueError("Validated staging receipt evidence required")
    status = receipt["status"]
    counts = _counts(receipt.get("counts"))
    if (receipt.get("run_id") != manifest["run_id"] or receipt.get("attempt_path") != manifest["attempt_uri"]
            or status.get("run_id") != manifest["run_id"] or status.get("attempt_id") != manifest["attempt_id"]
            or status.get("state") != "VALIDATED" or status.get("revision") != 2
            or status.get("dataset") != "sample" or status.get("destination") != "development"
            or status.get("start_date") != OWNER_DATE or status.get("timezone") != "UTC"
            or status.get("source_fingerprint") != manifest["config"]["expected_source"]["source_fingerprint"]
            or _counts(status.get("expected_counts")) != counts):
        raise ValueError("Receipt scope, source, ownership or validation mismatch")
    fingerprints = receipt.get("fingerprints")
    if not isinstance(fingerprints, dict) or set(fingerprints) != {"status", "inventory", "validation"}:
        raise ValueError("All staging receipt fingerprints required")
    for value in fingerprints.values(): _hash(value)
    expected_status = manifest["attempt_uri"]+"metadata/status.json"
    expected_validation = manifest["attempt_uri"]+"metadata/validation.json"
    if receipt.get("status_receipt") != expected_status or receipt.get("validation_receipt") != expected_validation:
        raise ValueError("Receipt locations disagree")
    return dict(counts={str(k):v for k,v in counts.items()}, row_count=sum(counts.values()),
                fingerprints=deepcopy(fingerprints), status_receipt=expected_status,
                validation_receipt=expected_validation)


def _validate_completion(manifest):
    generation, attached = manifest["generation"], manifest["attempt_evidence"]
    if not isinstance(generation, dict) or not isinstance(attached, dict):
        raise ValueError("Completed generation and validated receipt evidence required")
    if set(attached) != {"counts", "row_count", "fingerprints", "status_receipt", "validation_receipt"}:
        raise ValueError("Complete selected attempt evidence required")
    if (not isinstance(attached["fingerprints"], dict)
            or set(attached["fingerprints"]) != {"status", "inventory", "validation"}):
        raise ValueError("All receipt fingerprints required")
    for value in attached["fingerprints"].values(): _hash(value)
    if (attached["status_receipt"] != manifest["attempt_uri"]+"metadata/status.json"
            or attached["validation_receipt"] != manifest["attempt_uri"]+"metadata/validation.json"):
        raise ValueError("Selected receipt paths changed")
    config = manifest["config"]
    if (generation.get("status") != "GENERATION_COMPLETE" or generation.get("dataset") != "sample"
            or generation.get("owner_date") != OWNER_DATE or generation.get("input_dates") != list(INPUT_DATES)
            or generation.get("run_id") != manifest["run_id"]
            or generation.get("builder_fingerprint") != config["builder_fingerprint"]
            or generation.get("synthetic_baseline") is not False
            or generation.get("published") is not False
            or generation.get("source_before") != config["expected_source"]
            or generation.get("source_after") != config["expected_source"]):
        raise ValueError("Generation did not complete with matching source/version/scope evidence")
    sample = generation.get("sample_validation", {})
    if not isinstance(sample, dict):
        raise ValueError("Sample validation evidence required")
    identity = sample.get("identity_check", {})
    if not isinstance(identity, dict):
        raise ValueError("Sample identity evidence required")
    dates = sample.get("counts_by_date")
    if (not isinstance(dates, list) or any(not isinstance(r, dict) for r in dates)
            or [r.get("txn_date") for r in dates] != list(SAMPLE_DATES)
            or any(type(r.get("rows")) is not int or r["rows"] <= 0
                   or type(r.get("laundering")) is not int or not 0 <= r["laundering"] <= r["rows"] for r in dates)
            or sum(r["rows"] for r in dates) != 500000 or sum(r["laundering"] for r in dates) != 35):
        raise ValueError("Complete observed six-date sample counts required")
    if (sample.get("total_rows") != 500000 or sample.get("laundering_rows") != 35
            or sample.get("required_fields_valid") is not True or sample.get("timestamp_date_consistent") is not True
            or sample.get("timestamp_coverage", {}).get("timezone") != "UTC"
            or identity.get("dataset") != "sample" or identity.get("row_count") != 500000
            or identity.get("identity_check_passed") is not True
            or identity.get("verification_scope") != "provided_dataframe_only"
            or identity.get("dataset_wide_verified") is not False):
        raise ValueError("Observed-sample validation/identity evidence required; no global identity claim allowed")
    validation = generation.get("validation", {})
    if not isinstance(validation, dict):
        raise ValueError("Output validation evidence required")
    counts = _counts(validation.get("counts"))
    if (validation.get("schema_valid") is not True or validation.get("values_valid") is not True
            or validation.get("row_count") != sum(counts.values())
            or attached.get("counts") != {str(k):v for k,v in counts.items()}
            or attached.get("row_count") != sum(counts.values())):
        raise ValueError("Output validation and receipt counts disagree")


def validate_staging_run(manifest):
    """Validate this distinct format; never promote sample identity to global."""
    if not isinstance(manifest, dict) or manifest.get("manifest_kind") != KIND:
        raise ValueError("Distinct staging-run manifest required")
    keys = {"format_version", "manifest_kind", "dataset", "destination", "run_id", "attempt_id",
        "storage_owner", "manifest_uri", "attempt_uri", "config", "builder_version", "rule_version",
        "published", "scopes", "max_records_per_file", "generation", "attempt_evidence", "error",
        "state", "revision", "created_at", "updated_at"}
    if set(manifest) != keys or manifest["format_version"] != 1 or manifest["state"] not in STATES:
        raise ValueError("Invalid staging-run fields/version/state")
    config = validate_config(manifest["config"])
    for key in ("run_id", "attempt_id", "storage_owner"): _token(manifest[key])
    if (manifest["run_id"] != config["run_id"] or manifest["dataset"] != "sample"
            or manifest["destination"] != "development" or manifest["rule_version"] != "v1"
            or manifest["published"] is not False
            or manifest["manifest_uri"] != METADATA+manifest["run_id"]+"/staging_run.json"
            or manifest["attempt_uri"] != STAGING+manifest["run_id"]+"/"+manifest["attempt_id"]+"/"):
        raise ValueError("Staging identity, scope or fixed locations changed")
    expected_scopes = dict(observed_extract=dict(dates=list(SAMPLE_DATES), expected_rows=500000,
        expected_laundering=35, underlying_dataset_complete=False), generation_input=list(INPUT_DATES), output_owner=OWNER_DATE)
    if manifest["scopes"] != expected_scopes:
        raise ValueError("Observed extract, generation input and ownership scopes must remain separate")
    if (type(manifest["revision"]) is not int or manifest["revision"] < 0
            or type(manifest["max_records_per_file"]) is not int or manifest["max_records_per_file"] <= 0
            or not isinstance(manifest["builder_version"], str) or not manifest["builder_version"].strip()):
        raise ValueError("Invalid revision, builder version or file sizing")
    for key in ("created_at", "updated_at"):
        if not isinstance(manifest[key], str) or not manifest[key].strip():
            raise ValueError("Manifest timestamps required")
    if manifest["state"] == "CREATED":
        if manifest["revision"] != 0 or any(manifest[k] is not None for k in ("generation", "attempt_evidence", "error")):
            raise ValueError("CREATED requires revision zero and pending evidence")
    if manifest["state"] in ("STAGING", "STAGED_VALIDATED"):
        if manifest["revision"] != {"STAGING": 1, "STAGED_VALIDATED": 2}[manifest["state"]]:
            raise ValueError("State/revision mismatch")
        _validate_completion(manifest)
        if manifest["error"] is not None:
            raise ValueError("Successful staging states cannot contain an error")
    if manifest["state"] == "FAILED" and (not isinstance(manifest["error"], str) or not manifest["error"].strip()):
        raise ValueError("FAILED requires an error reference")
    if manifest["state"] == "FAILED":
        if manifest["revision"] not in (1, 2):
            raise ValueError("Failed state/revision mismatch")
        if manifest["revision"] == 1:
            if manifest["generation"] is not None or manifest["attempt_evidence"] is not None:
                raise ValueError("Failure from CREATED must retain pending evidence")
        else:
            _validate_completion(manifest)
    return manifest


def transition_staging_run(manifest, state, *, generation=None, receipt=None, current_source=None, error=None):
    """Pure transitions. Supplied evidence is rechecked by the storage adapter."""
    validate_staging_run(manifest)
    allowed = {"CREATED": {"STAGING", "FAILED"}, "STAGING": {"STAGED_VALIDATED", "FAILED"}}
    if state not in allowed.get(manifest["state"], set()):
        raise ValueError("Forbidden staging-run transition")
    result = deepcopy(manifest)
    if state == "STAGING":
        # JSON normalizes integer count keys to strings; freeze that form before
        # comparisons so durable reads and lost-response retries are identical.
        result["generation"] = json.loads(_bytes(generation))
        result["attempt_evidence"] = _receipt_summary(manifest, receipt)
    elif generation is not None or receipt is not None:
        raise ValueError("Selected completion evidence cannot be replaced")
    if state == "STAGED_VALIDATED" and current_source != manifest["config"]["expected_source"]:
        raise ValueError("Fresh matching source fingerprints required")
    result.update(state=state, revision=manifest["revision"]+1, updated_at=_now(), error=error)
    validate_staging_run(result)
    return result


def _same_identity(old, new):
    if any(old[k] != new[k] for k in set(old)-MUTABLE):
        raise ValueError("Immutable staging-run identity changed")


def write_staging_run_s3(client, manifest, *, expected_revision=None, expected_etag=None, current_source=None):
    """Conditional storage only. No implicit retries, execution or publication.

    Exact stored bytes reconcile a confirmed-but-lost response, even for terminal
    state, without another write. Different bytes require owner/revision/ETag CAS.
    """
    validate_staging_run(manifest)
    uri = manifest["manifest_uri"]
    if expected_revision is None:
        if expected_etag is not None or manifest["state"] != "CREATED" or manifest["revision"] != 0:
            raise ValueError("Exclusive first write must be CREATED revision zero")
        return _put(client, uri, manifest)
    if type(expected_revision) is not int or expected_revision < 0 or not isinstance(expected_etag, str) or not expected_etag:
        raise ValueError("Explicit prior revision and ETag required")
    old, etag, raw = _get(client, uri)
    validate_staging_run(old)
    _same_identity(old, manifest)
    if raw == _bytes(manifest):
        if expected_revision != manifest["revision"]-1:
            raise ValueError("Exact replay must reference the preceding revision")
        return etag
    if old["revision"] != expected_revision or etag != expected_etag or manifest["revision"] != expected_revision+1:
        raise ValueError("Staging-run revision/ETag conflict")
    if old["state"] in ("STAGED_VALIDATED", "FAILED"):
        raise ValueError("Terminal staging-run is immutable")
    if manifest["state"] in ("STAGING", "STAGED_VALIDATED"):
        receipt = _load_validated(client, manifest["attempt_uri"])
        if _receipt_summary(manifest, receipt) != manifest["attempt_evidence"]:
            raise ValueError("Current validated receipt evidence changed")
    rebuilt = transition_staging_run(old, manifest["state"], generation=manifest["generation"] if old["state"] == "CREATED" and manifest["state"] == "STAGING" else None,
        receipt=receipt if old["state"] == "CREATED" and manifest["state"] == "STAGING" else None,
        current_source=current_source, error=manifest["error"])
    for key in ("generation", "attempt_evidence", "state", "revision", "error"):
        if rebuilt[key] != manifest[key]:
            raise ValueError("Staging-run evidence or transition changed")
    return _put(client, uri, manifest, etag)


def read_staging_run_s3(client, run_id):
    """Read a manifest and ETag; success state is historical, not a new scan."""
    _token(run_id)
    uri = METADATA+run_id+"/staging_run.json"
    value, etag, _ = _get(client, uri)
    validate_staging_run(value)
    if value["run_id"] != run_id:
        raise ValueError("Stored run identity mismatch")
    return value, etag


def coordinate_staging_run(config, attempt_id, max_records_per_file, *, owner_token,
                           builder_version, allow_writes=False, destination="development",
                           glue_context, catalog_client, s3_client, identity_client, transport=None):
    """Persist CREATED, run B1, then durably verify staging-only completion.

    No automatic resume or retries. An uncertain/failed terminal verification
    raises even if S3 might contain terminal success: investigate, do not overwrite.
    Only exact known owned nonterminal versions may be marked FAILED. The original
    exception always propagates if recording failure is unsafe or also fails.
    """
    if allow_writes is not True:
        raise ValueError("Explicit boolean True write activation required")
    if destination != "development":
        raise ValueError("Only development staging is allowed")
    current = create_staging_run(config, attempt_id, max_records_per_file,
        owner_token=owner_token, builder_version=builder_version)
    config = current["config"]
    # Metadata is an AWS write too: verify injected account/region before CREATED.
    for client in (catalog_client, s3_client, identity_client):
        if client.meta.region_name != "us-east-1":
            raise ValueError("Client region must be us-east-1")
    if identity_client.get_caller_identity().get("Account") != config["account_id"]:
        raise ValueError("Wrong AWS account")
    from sequence_sample_staging import stage_bounded_sample
    from sequence_aws import inspect_sample_source
    from sequence_sample_generation import FINGERPRINTS

    def verify(expected, expected_etag):
        observed, observed_etag = read_staging_run_s3(s3_client, current["run_id"])
        if observed_etag != expected_etag or _bytes(observed) != _bytes(expected):
            raise ValueError("Durable staging-run verification conflict")

    # Creation errors are deliberately outside the failure handler: an existing
    # run or unresolved creation must never be taken over to record our failure.
    etag = write_staging_run_s3(s3_client, current)
    verify(current, etag)
    known = [(deepcopy(current), etag)]
    try:
        staged = stage_bounded_sample(config, attempt_id, max_records_per_file,
            allow_writes=True, destination="development", glue_context=glue_context,
            catalog_client=catalog_client, s3_client=s3_client, identity_client=identity_client,
            transport=transport)
        if (staged.attempt_uri != current["attempt_uri"]
                or staged.report.get("status") != "STAGED_GENERATION_COMPLETE"
                or staged.report.get("published") is not False
                or staged.report.get("run_manifest_ready") is not False):
            raise ValueError("B1 did not return matching completed staging evidence")
        receipt = _load_validated(s3_client, current["attempt_uri"])
        pending = transition_staging_run(current, "STAGING", generation=staged.report.get("generation"), receipt=receipt)
        known.append((deepcopy(pending), None))
        next_etag = write_staging_run_s3(s3_client, pending, expected_revision=current["revision"], expected_etag=etag)
        verify(pending, next_etag)
        current, etag = pending, next_etag
        known.append((deepcopy(current), etag))
        inventory = inspect_sample_source(catalog_client, s3_client)
        source = {key: inventory[key] for key in FINGERPRINTS}
        pending = transition_staging_run(current, "STAGED_VALIDATED", current_source=source)
        known.append((deepcopy(pending), None))
        next_etag = write_staging_run_s3(s3_client, pending, expected_revision=current["revision"],
            expected_etag=etag, current_source=source)
        verify(pending, next_etag)
        return dict(manifest_uri=pending["manifest_uri"], attempt_uri=pending["attempt_uri"],
            manifest=deepcopy(pending), etag=next_etag, published=False)
    except BaseException as original:
        try:
            observed, observed_etag = read_staging_run_s3(s3_client, current["run_id"])
            if (observed["state"] not in ("CREATED", "STAGING")
                    or not any(_bytes(observed) == _bytes(version)
                        and (tag is None or tag == observed_etag) for version, tag in known)):
                raise ValueError("Failure recording unsafe: conflicting, unknown or terminal run state")
            failed = transition_staging_run(observed, "FAILED", error="Coordinator failure: "+type(original).__name__)
            failure_etag = write_staging_run_s3(s3_client, failed,
                expected_revision=observed["revision"], expected_etag=observed_etag)
            verify(failed, failure_etag)
        except BaseException as recording_error:
            original.add_note("FAILED recording not confirmed ("+type(recording_error).__name__+"); preserve run and attempt for investigation")
        raise
