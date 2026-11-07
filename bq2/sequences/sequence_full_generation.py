"""Full HI/LI generation coordinator, isolated validated staging only.

One immutable selected source, one dataset per run, all owner dates. A full
source disk cache avoids re-reading S3 for each overlapping continuation window.
Publication/Catalog integration is an explicit separate, unfinished boundary.
"""
from copy import deepcopy
import re
import time

from sequence_aws import require_verified_timezone
from sequence_manifest import _token, _hash, _now
from sequence_full_source import (
    dataset_spec, owner_dates, inspect_full_source, read_full_catalog,
    snapshot_identity, assert_full_source_unchanged,
)

LIMIT_NAMES = ("max_pair_candidates", "max_pair_links", "max_projected_triples",
               "max_triples_per_middle", "max_output_rows")


def validate_full_config(config):
    required = {"dataset", "account_id", "region", "run_id", "attempt_id", "owner_token",
        "builder_version", "builder_fingerprint", "timezone_evidence", "no_refresh_acknowledged",
        "no_refresh_evidence", "expected_source", "limits", "limits_evidence",
        "max_records_per_file", "max_run_seconds", "verified_empty_dates"}
    if not isinstance(config, dict) or set(config) != required:
        raise ValueError("Explicit full-generation configuration fields required")
    dataset_spec(config["dataset"])
    if config["region"] != "us-east-1" or not isinstance(config["account_id"], str) or not re.fullmatch(r"[0-9]{12}", config["account_id"]):
        raise ValueError("Verified expected account/us-east-1 required")
    for name in ("run_id", "attempt_id", "owner_token"):
        _token(config[name])
    if not isinstance(config["builder_version"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", config["builder_version"]):
        raise ValueError("Safe builder version required")
    _hash(config["builder_fingerprint"])
    require_verified_timezone("UTC", True, config["timezone_evidence"])
    if config["no_refresh_acknowledged"] is not True:
        raise ValueError("Frozen source/no-refresh acknowledgment required")
    for name in ("no_refresh_evidence", "limits_evidence"):
        if not isinstance(config[name], str) or not config[name].strip():
            raise ValueError("Independent coordination/measurement evidence required")
    if not isinstance(config["limits"], dict) or set(config["limits"]) != set(LIMIT_NAMES):
        raise ValueError("All five explicit growth limits required")
    for value in [*config["limits"].values(), config["max_records_per_file"], config["max_run_seconds"]]:
        if type(value) is not int or value <= 0:
            raise ValueError("Limits/file sizing/time budget must be positive integers")
    if not isinstance(config["expected_source"], dict) or set(config["expected_source"]) != {
            "source_fingerprint", "table_fingerprint", "partitions_fingerprint"}:
        raise ValueError("All three fresh source/Catalog fingerprints required")
    for value in config["expected_source"].values():
        _hash(value)
    empty = config["verified_empty_dates"]
    if (not isinstance(empty, dict) or not set(empty) <= set(owner_dates(config["dataset"]))
            or any(not isinstance(v, str) or not v.strip() for v in empty.values())):
        raise ValueError("Explicit empty-date evidence map required")
    return deepcopy(config)


def profile_full_source(frame, dataset, snapshot):
    """Validate required fields/unique IDs once on the complete cached dataset.

    Coverage is tied to independent metadata; the DataFrame check itself still
    reports provided_dataframe_only, never a claim about unseen source activity.
    """
    from bq2_sequence_builder import validate_transactions
    from sequence_source import validate_cleaned_schema
    from pyspark.sql import functions as F
    validate_cleaned_schema(frame)
    validate_transactions(frame)
    rows = frame.groupBy("dataset", "txn_date").agg(F.count("*").alias("rows"),
        F.sum("is_laundering").alias("laundering")).collect()
    days = set(snapshot["available_dates"])
    if any(r.dataset != dataset or r.txn_date not in days for r in rows):
        raise ValueError("Full source data escapes the selected snapshot")
    counts = {r.txn_date: int(r.rows) for r in rows}
    total, laundering = sum(counts.values()), sum(int(r.laundering) for r in rows)
    spec = dataset_spec(dataset)
    if total != spec["rows"] or laundering != spec["laundering"]:
        raise ValueError(f"Unexpected {dataset} baseline: rows={total}, laundering={laundering}")
    return dict(row_count=total, laundering_rows=laundering, rows_by_date=counts,
        identity_check_passed=True, verification_scope="provided_dataframe_only", dataset_wide_verified=False,
        full_selected_snapshot_checked=True, checked_dates=snapshot["owner_dates"], source=snapshot_identity(snapshot))


def run_full_generation(config, *, allow_writes=False, glue_context, catalog_client,
                        s3_client, identity_client, stager=None):
    """Generate all dates and return a durable generation-to-publication handoff.

    No publication hook is invoked. An injected stager can support local testing;
    the Glue CLI does not expose test transports or synthetic baseline overrides.
    """
    if allow_writes is not True:
        raise ValueError("Explicit boolean True staging activation required")
    config = validate_full_config(config)
    from sequence_glue_runtime import validate_glue_runtime, validate_s3_conditional_writes
    from sequence_batch import GrowthLimits, BatchResult, process_start_date_batch
    from contextlib import nullcontext
    from sequence_staging import SEQUENCE_SCHEMA
    from sequence_full_staging import FullS3Stager
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    spark = glue_context.spark_session
    validate_glue_runtime(glue_version="5.0", spark_version=spark.version,
        java_version=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Explicit UTC session required")
    for client in (catalog_client, s3_client, identity_client):
        if client.meta.region_name != "us-east-1":
            raise ValueError("Wrong client region")
    if identity_client.get_caller_identity().get("Account") != config["account_id"]:
        raise ValueError("Wrong AWS account")
    validate_s3_conditional_writes(s3_client)
    started = time.monotonic()

    def deadline():
        # Cannot interrupt an in-flight Spark action: also configure Glue Timeout.
        if time.monotonic()-started >= config["max_run_seconds"]:
            raise TimeoutError("Full generation time budget exhausted")

    def inspect():
        deadline()
        return inspect_full_source(catalog_client, s3_client, config["dataset"],
            verified_empty_dates=config["verified_empty_dates"])

    before = inspect()
    if snapshot_identity(before) != config["expected_source"]:
        raise ValueError("Expected full source snapshot does not match")
    expected_days = list(owner_dates(config["dataset"]))
    if ([p.start_date.isoformat() for p in before["plans"]] != expected_days
            or before["owner_dates"] != expected_days or any(not p.ready_to_process for p in before["plans"])):
        raise ValueError("Missing/duplicate owner dates or blocked plans")
    manifest = dict(format_version=1, manifest_kind="full_sequence_generation_handoff", state="CREATED",
        dataset=config["dataset"], run_id=config["run_id"], attempt_id=config["attempt_id"],
        owner_token=config["owner_token"], rule_version="v1", builder=dict(version=config["builder_version"],
        fingerprint=config["builder_fingerprint"]), source=snapshot_identity(before),
        owner_dates=expected_days, source_dates=before["owner_dates"],
        schema=[dict(name=f.name, type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA],
        limits=config["limits"], limits_evidence=config["limits_evidence"],
        timezone="UTC", timezone_evidence=config["timezone_evidence"], no_refresh_evidence=config["no_refresh_evidence"],
        published=False, catalog_updated=False, publication_required=True, batches=[], created_at=_now())
    store = stager if stager is not None else FullS3Stager(s3_client, config)
    store.reserve(deepcopy(manifest))
    full, owned, error = None, False, None
    try:
        deadline()
        full = read_full_catalog(glue_context, config["dataset"], before["available_dates"],
            snapshot=before, timezone_evidence=config["timezone_evidence"])
        manager = spark._jsparkSession.sharedState().cacheManager()
        if not manager.lookupCachedData(full._jdf).nonEmpty():
            full.persist(StorageLevel.DISK_ONLY)
            owned = True
        profile = profile_full_source(full, config["dataset"], before)
        manifest["source_validation"] = profile
        for plan in before["plans"]:
            deadline()
            day = plan.start_date.isoformat()
            dates = [d.isoformat() for d in plan.available_input_dates]
            selected = full.where(F.col("txn_date").isin(dates))
            if profile["rows_by_date"].get(day, 0) == 0:
                # A complete source scan proved there are no starting transfers.
                # Continuation-only pairs cannot contribute an owned sequence.
                empty = spark.createDataFrame([], SEQUENCE_SCHEMA)
                context = nullcontext(BatchResult(empty, dict(pair_candidates=0, pair_links=0,
                    starting_pairs=0, projected_triples=0, largest_middle_expansion=0, actual_triples=0, output_rows=0)))
            else:
                context = process_start_date_batch(selected, config["dataset"], plan, "UTC", GrowthLimits(**config["limits"]))
            with context as batch:
                counts = {2: batch.metrics["starting_pairs"], 3: batch.metrics["actual_triples"]}
                receipt = store.stage(batch.output, start_date=day, expected_counts=counts)
                if (receipt.get("validation_status") != "VALIDATED" or receipt.get("dataset") != config["dataset"]
                        or receipt.get("start_date") != day or receipt.get("row_count") != sum(counts.values())
                        or {int(k): v for k,v in receipt.get("counts", {}).items()} != counts
                        or receipt.get("schema_valid") is not True or receipt.get("values_valid") is not True
                        or receipt.get("run_id") != config["run_id"] or receipt.get("attempt_id") != config["attempt_id"]
                        or receipt.get("source") != manifest["source"] or receipt.get("schema") != manifest["schema"]
                        or receipt.get("builder") != manifest["builder"] or receipt.get("rule_version") != "v1"):
                    raise ValueError("Stager returned incomplete/mismatched validation evidence")
                manifest["batches"].append(dict(receipt, input_dates=dates,
                    required_input_dates=[d.isoformat() for d in plan.required_input_dates],
                    verified_empty_input_dates=[d.isoformat() for d in plan.verified_empty_input_dates],
                    unavailable_after_dataset_end=[d.isoformat() for d in plan.unavailable_after_dataset_end],
                    growth=batch.metrics))
            assert_full_source_unchanged(before, inspect())
    except Exception as exc:
        error = exc
    finally:
        if owned:
            try:
                full.unpersist(blocking=True)
            except Exception as cleanup:
                if error is None:
                    error = cleanup
                else:
                    error.add_note("Full source cache cleanup failed: "+type(cleanup).__name__)
    try:
        if error is not None:
            raise error
        assert_full_source_unchanged(before, inspect())
        if [b["start_date"] for b in manifest["batches"]] != expected_days:
            raise ValueError("Incomplete output owner-date coverage")
        totals = {str(k): sum(b["counts"].get(k, b["counts"].get(str(k), 0)) for b in manifest["batches"]) for k in (2, 3)}
        manifest.update(state="STAGED_VALIDATED", counts=totals, row_count=sum(totals.values()),
            parquet_files=sum(b["parquet_files"] for b in manifest["batches"]),
            size_bytes=sum(b["size_bytes"] for b in manifest["batches"]),
            source_after=snapshot_identity(before), updated_at=_now(), elapsed_seconds=round(time.monotonic()-started, 3))
        return store.finish(deepcopy(manifest))
    except Exception as original:
        try:
            store.fail(original)
        except Exception as recording:
            original.add_note("Could not durably record full-generation failure: "+type(recording).__name__)
        raise
