"""Injected, no-write orchestration for one sample owner date.

No CLI, runtime/client creation or publication. Consume output inside the context.
Inventory equality detects changes, not transactional snapshots. All six observed
sample dates are validated; completeness of underlying HI continuations is unknown.
"""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
import re

from sequence_aws import SAMPLE_DATES

OWNER_DATE = "2022-09-01"
INPUT_DATES = SAMPLE_DATES[:3]
FINGERPRINTS = ("source_fingerprint", "table_fingerprint", "partitions_fingerprint")
LIMIT_NAMES = ("max_pair_candidates", "max_pair_links", "max_projected_triples",
               "max_triples_per_middle", "max_output_rows")


def validate_config(config):
    """Reject unsafe config before any injected client operation."""
    from glue_sample_preflight import validate_config as validate_preflight_config
    extra = {"owner_date", "limits", "expected_source"}
    if not isinstance(config, dict) or not extra <= config.keys():
        raise ValueError("Explicit owner_date, limits and expected_source required")
    validate_preflight_config({k: v for k, v in config.items() if k not in extra})
    if config["owner_date"] != OWNER_DATE:
        raise ValueError("Only owner date 2022-09-01 is supported")
    limits = config["limits"]
    if (not isinstance(limits, dict) or set(limits) != set(LIMIT_NAMES)
            or any(type(v) is not int or v <= 0 for v in limits.values())):
        raise ValueError("All five positive explicit growth limits required")
    expected = config["expected_source"]
    if (not isinstance(expected, dict) or set(expected) != set(FINGERPRINTS)
            or any(not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{64}", v)
                   for v in expected.values())):
        raise ValueError("All three independently recorded source fingerprints required")
    return deepcopy(config)


@dataclass(frozen=True)
class SampleGenerationResult:
    output: object
    report: dict


def _check_inventory(before, after):
    if (any(before[k] != after[k] for k in FINGERPRINTS)
            or before["objects"] != after["objects"]
            or before["dates"] != after["dates"] or before["files"] != after["files"]):
        raise ValueError("Source or Catalog inventory changed during generation")


@contextmanager
def generate_sample_sequences(config, *, glue_context, catalog_client, s3_client, identity_client,
                              _synthetic_baseline=None):
    """Yield validated in-memory output; never write, publish or stop Spark.

    Before yield, generation and output validation have passed and inventories
    match. Only normal context exit marks GENERATION_COMPLETE. A later writer
    must not certify success before that exit. Consumers must not retain output
    beyond this context. Current checks cover the provided sample frame only.
    """
    config = validate_config(config)
    from pyspark.sql import functions as F
    from glue_sample_preflight import _runtime_versions, validate_and_profile_frame
    from sequence_aws import inspect_sample_source, validate_sample_physical_schema, read_catalog_selected
    from sequence_processing import plan_start_date_batches
    from sequence_batch import _persist_owned, GrowthLimits, process_start_date_batch
    from sequence_staging import validate_sequence_output

    for client in (catalog_client, s3_client, identity_client):
        if client.meta.region_name != "us-east-1":
            raise ValueError("Client region must be us-east-1")
    if identity_client.get_caller_identity().get("Account") != config["account_id"]:
        raise ValueError("Wrong AWS account")
    spark = glue_context.spark_session
    baseline = _baseline_options(spark, _synthetic_baseline)
    runtime = _runtime_versions(spark)
    before = inspect_sample_source(catalog_client, s3_client)
    if {k: before[k] for k in FINGERPRINTS} != config["expected_source"]:
        raise ValueError("Stale or mismatched source evidence")
    if before["dates"] != list(SAMPLE_DATES):
        raise ValueError("All six observed sample partitions are required")
    plan, = plan_start_date_batches(OWNER_DATE, OWNER_DATE,
        dataset_start_date=SAMPLE_DATES[0], dataset_end_date=SAMPLE_DATES[-1],
        available_partition_dates=before["dates"])
    if not plan.ready_to_process or tuple(d.isoformat() for d in plan.available_input_dates) != INPUT_DATES:
        raise ValueError("Missing required continuation partitions")
    validate_sample_physical_schema(spark, before["files"])
    owned = []
    report = dict(status="GENERATING", runtime=runtime, dataset="sample", owner_date=OWNER_DATE,
        input_dates=list(INPUT_DATES), run_id=config["run_id"], builder_fingerprint=config["builder_fingerprint"],
        synthetic_baseline=bool(baseline),
        source_before=config["expected_source"], report_storage_enabled=False, published=False,
        limitation="First-500000-row sample may omit continuations; no full underlying dataset identity certification")
    error = None
    try:
        # Full six-date read establishes sample-scoped identity before date filtering.
        source = read_catalog_selected(glue_context, "sample", SAMPLE_DATES,
            available_dates=before["dates"], timezone="UTC", timezone_verified=True,
            timezone_evidence=config["timezone_evidence"], catalog_table=before["table"])
        source = _persist_owned(source, owned)
        report["sample_validation"] = validate_and_profile_frame(source, **baseline)
        selected = source.where(F.col("txn_date").isin(list(INPUT_DATES)))
        with process_start_date_batch(selected, "sample", plan, "UTC", GrowthLimits(**config["limits"])) as batch:
            counts = {2: batch.metrics["starting_pairs"], 3: batch.metrics["actual_triples"]}
            report["validation"] = validate_sequence_output(batch.output, "sample", OWNER_DATE, "UTC", counts)
            report["batch_metrics"] = deepcopy(batch.metrics)
            _check_inventory(before, inspect_sample_source(catalog_client, s3_client))
            report["status"] = "OUTPUT_VALIDATED_CONTEXT_OPEN"
            yield SampleGenerationResult(batch.output, report)
    except BaseException as exc:
        error = exc
        report["status"] = "FAILED"
        raise
    finally:
        # Recheck even on consumer/generation failure; do not mask the original error.
        failures = []
        try:
            after = inspect_sample_source(catalog_client, s3_client)
            _check_inventory(before, after)
            report["source_after"] = {k: after[k] for k in FINGERPRINTS}
        except Exception as exc:
            failures.append(exc)
        for frame in reversed(owned):
            try:
                frame.unpersist(blocking=True)
            except Exception as exc:
                failures.append(exc)
        if failures:
            report["status"] = "FAILED"
            if error is None:
                raise failures[0]
            for failure in failures:
                error.add_note("Generation cleanup/source check failed: " + str(failure))
    report["status"] = "GENERATION_COMPLETE"


def _baseline_options(spark, override):
    """Internal fixture hook only; Glue keeps the profiler's fixed defaults.

    Gate on the active context, never on config labels or environment variables.
    Not exposed in config or any CLI. This does not bypass other validation.
    """
    if override is None:
        return {}
    master = spark.sparkContext.master
    if not isinstance(master, str) or not re.fullmatch(r"local(?:\[[^\[\]\r\n]+\])?", master):
        raise ValueError("Synthetic baseline requires an active local Spark master")
    if (not isinstance(override, dict) or set(override) != {"expected_rows", "expected_laundering"}
            or type(override["expected_rows"]) is not int or not 0 < override["expected_rows"] < 500000
            or type(override["expected_laundering"]) is not int
            or not 0 <= override["expected_laundering"] <= min(35, override["expected_rows"])):
        raise ValueError("Explicit smaller synthetic row/label counts required")
    return dict(override)
