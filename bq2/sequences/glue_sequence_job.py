"""Offline-first Glue 5.0 entry point. CLI validates JSON; it does NOT run Glue.

No S3 IO, publication or runtime initialization occurs on import or via main().
Explicit runtime/transform helpers are for a later approved orchestrator. Caller
attestations are prerequisites, not independent verification by this module.
"""

import argparse
from contextlib import contextmanager
import json
import re
import sys

from sequence_processing import _iso_date, plan_start_date_batches
from sequence_aws import require_verified_timezone


LIMIT_NAMES = ("max_pair_candidates", "max_pair_links", "max_projected_triples",
               "max_triples_per_middle", "max_output_rows")
BUCKET = "s3://is459-g2t1-aml-processed-bucket/"
OUTPUTS = {"staging_root": BUCKET + "bq2_sequences_staging/",
           "metadata_root": BUCKET + "bq2_sequences_metadata/development/",
           "development_root": BUCKET + "bq2_sequences_dev/"}


def validate_config(config):
    """Pure validation; no dependency imports, Spark session or AWS operations."""
    required = {"glue_version", "dataset", "destination", "run_id", "attempt_id",
                "start_date", "end_date", "source_start_date", "source_end_date",
                "available_dates", "verified_empty_dates", "timezone",
                "timezone_verified", "timezone_evidence", "coverage_verified",
                "coverage_evidence", "source_inventory_fingerprint", "limits", *OUTPUTS}
    if not isinstance(config, dict) or set(config) != required:
        raise ValueError("Config must contain exactly: " + ", ".join(sorted(required)))
    if config["glue_version"] != "5.0" or config["dataset"] != "sample" or config["destination"] != "development":
        raise ValueError("Only Glue 5.0 / sample / development is supported")
    for key in ("run_id", "attempt_id"):
        if not isinstance(config[key], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", config[key]):
            raise ValueError("Unsafe " + key)
    for key, expected in OUTPUTS.items():
        if config[key] != expected:
            raise ValueError(key + " must use the approved isolated development prefix")
    if config["timezone"] != "UTC":
        raise ValueError("Current batch runner supports UTC only; this does not verify source semantics")
    for key in ("timezone_verified", "coverage_verified"):
        if type(config[key]) is not bool:
            raise ValueError(key + " must be a boolean")
    for key in ("timezone_evidence", "coverage_evidence"):
        if not isinstance(config[key], str) or (config[key.replace("evidence", "verified")] and not config[key].strip()):
            raise ValueError(key + " is required for a verified claim")
    if not isinstance(config["source_inventory_fingerprint"], str) or not re.fullmatch(r"[0-9a-f]{64}", config["source_inventory_fingerprint"]):
        raise ValueError("An explicit source inventory SHA-256 fingerprint is required")
    limits = config["limits"]
    if not isinstance(limits, dict) or set(limits) != set(LIMIT_NAMES) or any(type(v) is not int or v < 0 for v in limits.values()):
        raise ValueError("All five explicit nonnegative growth limits are required")
    for key in ("available_dates", "verified_empty_dates"):
        values = config[key]
        if (not isinstance(values, list) or any(not isinstance(value, str) for value in values)
                or len(values) != len(set(values))):
            raise ValueError(key + " must be a list of distinct date strings")
        for value in values:
            _iso_date(value, key)
    plans = plan_start_date_batches(config["start_date"], config["end_date"],
        dataset_start_date=config["source_start_date"], dataset_end_date=config["source_end_date"],
        available_partition_dates=config["available_dates"], verified_empty_dates=config["verified_empty_dates"])
    # Complete coverage evidence must classify ALL source dates, not just D/D+1/D+2.
    first = _iso_date(config["source_start_date"], "source_start_date")
    last = _iso_date(config["source_end_date"], "source_end_date")
    classified = set(config["available_dates"]) | set(config["verified_empty_dates"])
    if len(classified) != (last - first).days + 1 or any(not p.ready_to_process for p in plans):
        raise ValueError("Every source date must be classified; missing partitions block execution")
    return plans


def require_execution_prerequisites(config, evidence):
    """Check independently supplied full-sample evidence, never certify a batch."""
    plans = validate_config(config)
    require_verified_timezone(config["timezone"], config["timezone_verified"], config["timezone_evidence"])
    if not config["coverage_verified"] or not config["coverage_evidence"].strip():
        raise ValueError("Independently verified complete sample coverage is required")
    if not isinstance(evidence, dict):
        raise ValueError("Full-sample source evidence is required")
    expected_dates = sorted(config["available_dates"] + config["verified_empty_dates"])
    identity = evidence.get("identity_check", {})
    fp = config["source_inventory_fingerprint"]
    if (not isinstance(identity, dict) or evidence.get("dataset") != "sample" or evidence.get("covered_dates") != expected_dates
            or evidence.get("inventory_before") != fp or evidence.get("inventory_after") != fp
            or evidence.get("complete_inventory_verified") is not True
            or evidence.get("physical_schema_verified") is not True
            or evidence.get("required_fields_verified") is not True
            or evidence.get("refresh_coordination_confirmed") is not True
            or identity.get("dataset") != "sample" or identity.get("identity_check_passed") is not True
            or identity.get("verification_scope") != "provided_dataframe_only"
            or type(identity.get("row_count")) is not int or identity["row_count"] < 0
            or not isinstance(evidence.get("verification_reference"), str)
            or not evidence["verification_reference"].strip()):
        raise ValueError("Full-sample identity, physical schema, coverage and consistent inventory evidence required")
    return plans


def initialize_glue(config, evidence, job_name):
    """Explicit opt-in runtime initialization; never called by the offline CLI."""
    require_execution_prerequisites(config, evidence)
    if job_name != "is459-g2t1-bq2-sequence-builder":
        raise ValueError("Unexpected development Glue job name")
    if sys.version_info[:2] != (3, 11):
        raise ValueError("Glue target requires Python 3.11")
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    java = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    if spark.version != "3.5.4" or java.split(".")[0] != "17":
        raise ValueError("Glue 5.0 target requires Spark 3.5.4 and Java 17")
    spark.conf.set("spark.sql.session.timeZone", config["timezone"])
    job = Job(context)
    job.init(job_name, {"JOB_NAME": job_name, "DATASET": "sample"})
    return context, job


@contextmanager
def prepared_batch(transactions, config, evidence, plan):
    """Reuse existing owned-cache context; no source reads, staging or publication."""
    plans = require_execution_prerequisites(config, evidence)
    if plan not in plans:
        raise ValueError("Batch plan is not owned by this configuration")
    from sequence_batch import GrowthLimits, process_start_date_batch
    with process_start_date_batch(transactions, "sample", plan, config["timezone"],
                                  GrowthLimits(**config["limits"])) as result:
        yield result


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="OFFLINE config validation only; no Glue execution")
    parser.add_argument("--config", required=True, help="Local JSON config; never an S3 URI")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_arguments(argv)
    if "://" in args.config or args.config.startswith(("\\\\", "//")):
        raise ValueError("Config must be a local file")
    with open(args.config, encoding="utf-8") as handle:
        config = json.load(handle)
    plans = validate_config(config)
    print(json.dumps({"mode": "offline_validation_only", "batch_count": len(plans),
                      "aws_execution_enabled": False,
                      "note": "Timezone, complete source evidence and AWS behavior remain independently unverified"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
