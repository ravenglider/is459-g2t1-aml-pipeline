"""Sample-only READ validation/profile entry point; NEVER writes S3 or sequences.

Imports are inert. main requires --ALLOW_AWS_READS=true before SDK/runtime creation.
Report destinations are DESIGN ONLY: evidence is returned/printed, storage stays
disabled. Glue/CloudWatch may themselves store logs/temp artifacts in a real run.
No existing builder execution gate is weakened to bootstrap source validation.
"""

import argparse
import json
import re
import sys
import time

from sequence_aws import DATABASE, TABLE, SAMPLE_DATES, require_verified_timezone

REGION = "us-east-1"
JOB_NAME = "is459-g2t1-bq2-sample-preflight"
REPORT_ROOT = "s3://is459-g2t1-aml-processed-bucket/bq2_sequences_metadata/development/preflight/"


class PreflightFailure(RuntimeError):
    """Carries a compact FAILED report; caller must propagate failure to Glue."""
    def __init__(self, report):
        self.report = report
        super().__init__(report["error"]["message"])


def validate_config(config):
    required = {"account_id", "region", "dataset", "run_id", "timezone", "timezone_evidence",
                "no_refresh_acknowledged", "no_refresh_evidence", "builder_fingerprint"}
    if not isinstance(config, dict) or set(config) != required:
        raise ValueError("Explicit preflight config fields required: " + ", ".join(sorted(required)))
    if config["region"] != REGION or config["dataset"] != "sample":
        raise ValueError("Preflight is sample-only in us-east-1")
    if not isinstance(config["account_id"], str) or not re.fullmatch(r"[0-9]{12}", config["account_id"]):
        raise ValueError("Explicit expected 12-digit group account ID required")
    if not isinstance(config["run_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", config["run_id"]):
        raise ValueError("Unsafe run ID")
    require_verified_timezone(config["timezone"], True, config["timezone_evidence"])
    if (config["no_refresh_acknowledged"] is not True or not isinstance(config["no_refresh_evidence"], str)
            or not config["no_refresh_evidence"].strip()):
        raise ValueError("Explicit coordinated no-refresh acknowledgment required")
    if not isinstance(config["builder_fingerprint"], str) or not re.fullmatch(r"[a-f0-9]{64}", config["builder_fingerprint"]):
        raise ValueError("Actual builder/package SHA-256 fingerprint required")
    return config


def _runtime_versions(spark):
    java = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    versions = dict(glue_target="5.0", spark=spark.version, python=sys.version.split()[0], java=java)
    if sys.version_info[:2] != (3, 11) or spark.version != "3.5.4" or java.split(".")[0] != "17":
        raise ValueError("Requires Glue 5.0 target: Spark 3.5.4, Python 3.11, Java 17")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session must explicitly use UTC")
    return versions


def validate_and_profile_frame(frame, *, expected_rows=500000, expected_laundering=35):
    """Small-fixture baselines injectable for tests; real run uses fixed defaults.

    No pairs/triples: estimate_pair_candidates joins grouped degrees only. Full
    source validation precedes profile filtering. This function owns no cache.
    """
    from pyspark.sql import functions as F
    from sequence_source import validate_cleaned_schema, validate_dataset_identity
    from bq2_sequence_builder import validate_transactions
    from sequence_batch import estimate_pair_candidates
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Sample validation requires UTC session")
    validate_cleaned_schema(frame)
    validate_transactions(frame)
    identity = validate_dataset_identity(frame.select("dataset", "txn_id"), "sample")
    rows = frame.groupBy("txn_date").agg(F.count("*").alias("rows"),
        F.sum("is_laundering").alias("laundering"), F.min("txn_ts").alias("first_ts"),
        F.max("txn_ts").alias("last_ts")).orderBy("txn_date").collect()
    if [r.txn_date for r in rows] != list(SAMPLE_DATES):
        raise ValueError("Actual sample date coverage differs from all six expected partitions")
    total, laundering = sum(r.rows for r in rows), sum(r.laundering for r in rows)
    if total != expected_rows or laundering != expected_laundering:
        raise ValueError(f"Unexpected sample baseline: rows={total}, laundering={laundering}")
    # Collected aggregate timestamps are rendered in Spark, avoiding Python host
    # timezone effects in evidence (Spark stores/validates original timestamps).
    coverage = frame.agg(F.date_format(F.min("txn_ts"), "yyyy-MM-dd HH:mm:ss").alias("first_ts"),
                         F.date_format(F.max("txn_ts"), "yyyy-MM-dd HH:mm:ss").alias("last_ts")).first().asDict()
    subset = frame.where(F.col("txn_date").isin(list(SAMPLE_DATES[:3])))
    bound = estimate_pair_candidates(subset, list(SAMPLE_DATES[:2]))
    return dict(total_rows=total, laundering_rows=laundering, identity_check=identity,
        counts_by_date=[dict(txn_date=r.txn_date, rows=r.rows, laundering=r.laundering) for r in rows],
        timestamp_coverage=dict(coverage, timezone="UTC"),
        pair_candidates=dict(owner_date=SAMPLE_DATES[0], input_dates=list(SAMPLE_DATES[:3]),
            preceding_dates=list(SAMPLE_DATES[:2]), conservative_upper_bound=bound,
            pairs_materialized=False, triples_materialized=False),
        required_fields_valid=True, timestamp_date_consistent=True)


def run_preflight(config, *, glue_context, catalog_client, s3_client, identity_client):
    """Injected READ clients only. Return evidence; attach FAILED report on errors.

    No put/copy/delete/Catalog mutation APIs or report writer exist here. Source
    equality is change evidence, not a transactional snapshot or refresh lock.
    """
    validate_config(config)  # Reject unsafe configuration BEFORE any client call.
    from sequence_aws import inspect_sample_source, validate_sample_physical_schema, read_catalog_selected
    from sequence_batch import _persist_owned
    started = time.monotonic()
    report = dict(format_version=1, run_id=config["run_id"], mode="PREFLIGHT_ONLY", status="RUNNING",
        account_id=config["account_id"], region=REGION, dataset="sample", table=DATABASE + "." + TABLE,
        builder_fingerprint=config["builder_fingerprint"], timezone="UTC",
        timezone_evidence=config["timezone_evidence"], no_refresh_evidence=config["no_refresh_evidence"],
        report_location_design=REPORT_ROOT + config["run_id"] + "/report.json", report_storage_enabled=False,
        phase="identity", phases_seconds={},
        limitation="Complete sample extract only; first-500000-row selection may omit continuations and truncate final day")
    owned = []
    error = None
    try:
        if identity_client.get_caller_identity().get("Account") != config["account_id"]:
            raise ValueError("Wrong AWS account")
        for client in (catalog_client, s3_client, identity_client):
            if client.meta.region_name != REGION:
                raise ValueError("Client region must be us-east-1")
        spark = glue_context.spark_session
        report["runtime"] = _runtime_versions(spark)
        report["phase"] = "inventory_before"
        before = inspect_sample_source(catalog_client, s3_client)
        report["source_before"] = {key: before[key] for key in ("source_fingerprint", "table_fingerprint", "partitions_fingerprint")}
        report["source_objects"] = before["objects"]
        report["phase"] = "physical_schema"
        validate_sample_physical_schema(spark, before["files"])
        report["physical_schema_valid"] = True
        report["phase"] = "read_validate_profile"
        phase_started = time.monotonic()
        frame = read_catalog_selected(glue_context, "sample", SAMPLE_DATES, available_dates=before["dates"],
            timezone="UTC", timezone_verified=True, timezone_evidence=config["timezone_evidence"], catalog_table=before["table"])
        frame = _persist_owned(frame, owned)
        report["metrics"] = validate_and_profile_frame(frame)
        report["phases_seconds"]["read_validate_profile"] = round(time.monotonic() - phase_started, 3)
        report["phase"] = "inventory_after"
        after = inspect_sample_source(catalog_client, s3_client)
        report["source_after"] = {key: after[key] for key in ("source_fingerprint", "table_fingerprint", "partitions_fingerprint")}
        if report["source_before"] != report["source_after"] or before["objects"] != after["objects"]:
            raise ValueError("Source or Catalog inventory changed during preflight")
        report["coverage_evidence"] = dict(dataset="sample", checked_dates=list(SAMPLE_DATES), verified=True,
            source_inventory_fingerprint=before["source_fingerprint"],
            evidence="All six Catalog/S3 dates checked before/after; full selected sample scan completed")
        report["status"] = "VALIDATED_READ_ONLY"
        report["phase"] = "complete"
    except Exception as exc:
        error = exc
    finally:
        for frame in reversed(owned):
            try:
                frame.unpersist(blocking=True)
            except Exception as cleanup_error:
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note("Owned cache cleanup failed: " + str(cleanup_error))
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    if error is not None:
        report["status"] = "FAILED"
        report["error"] = dict(type=type(error).__name__, message=str(error))
        raise PreflightFailure(report) from error
    return report


class _SingleValue(argparse.Action):
    """Reject duplicate safety/config values, including mixed activation aliases."""
    def __call__(self, parser, namespace, values, option_string=None):
        provided = getattr(namespace, "_provided_options", set())
        if self.dest in provided:
            parser.error("duplicate argument: " + option_string)
        provided.add(self.dest)
        namespace._provided_options = provided
        setattr(namespace, self.dest, values)


class PreflightArgumentError(SystemExit):
    """Exit code 2 plus redacted exception detail, even if stderr is unavailable."""
    def __init__(self, diagnostic):
        self.diagnostic = diagnostic
        super().__init__(2)

    def __str__(self):
        return json.dumps(self.diagnostic, sort_keys=True)


class _PreflightParser(argparse.ArgumentParser):
    def fail(self, reason, names=(), unparsed_count=0):
        # Only option NAMES; never raw argv, parser messages, values or evidence.
        safe = []
        for name in names:
            if re.fullmatch(r"--[A-Za-z][A-Za-z0-9_-]{0,63}", name) and not re.search(r"(?:AKIA|ASIA)[A-Z0-9]{16}", name):
                safe.append(name)
            else:
                safe.append("<redacted-option-name>")
        diagnostic = dict(status="ARGUMENT_ERROR", reason=reason, arguments=sorted(set(safe)),
                          unparsed_count=unparsed_count, preflight_completed=False, values_logged=False)
        line = "PREFLIGHT_ARGUMENTS | " + json.dumps(diagnostic, sort_keys=True)
        print(line, flush=True)
        print(line, file=sys.stderr, flush=True)
        # Suppress argparse.ArgumentError chaining: its text can contain values.
        raise PreflightArgumentError(diagnostic) from None

    def error(self, message):
        # argparse's default invalid-choice/unknown messages include values.
        # Derive only known option headers, then discard the original message.
        if message.startswith("the following arguments are required:"):
            tail = message.split(":", 1)[1]
            names = [name.strip() for name in tail.split(",") if name.strip() in self._option_string_actions]
            self.fail("missing_required_arguments", names)
        if message.startswith("duplicate argument:"):
            name = message.split(":", 1)[1].strip()
            self.fail("duplicate_argument", [name] if name in self._option_string_actions else [])
        match = re.match(r"argument ([^:]+):", message)
        names = [name for name in match.group(1).split("/") if name in self._option_string_actions] if match else []
        reason = "invalid_value" if "invalid choice:" in message else (
            "missing_value" if "expected one argument" in message else "malformed_argument")
        self.fail(reason, names)


def parse_arguments(argv=None):
    parser = _PreflightParser(prog="glue_sample_preflight", description="Sample preflight; explicit opt-in required for any AWS reads",
                              allow_abbrev=False)
    for name in ("EXPECTED_ACCOUNT_ID", "RUN_ID", "TIMEZONE_EVIDENCE", "NO_REFRESH_EVIDENCE", "BUILDER_FINGERPRINT"):
        parser.add_argument("--" + name, required=True, action=_SingleValue)
    parser.add_argument("--NO_REFRESH_ACKNOWLEDGED", choices=("true", "false"), required=True, action=_SingleValue)
    parser.add_argument("--DATASET", default="sample", action=_SingleValue)
    parser.add_argument("--REGION", default=REGION, action=_SingleValue)
    parser.add_argument("--JOB_NAME", default=JOB_NAME, action=_SingleValue)
    # Keep the previous spelling as an alias, but NEVER accept valueless activation.
    parser.add_argument("--ALLOW_AWS_READS", "--allow-aws-reads", dest="ALLOW_AWS_READS",
                        choices=("true", "false"), default="false", action=_SingleValue)
    # Glue may supply standard engine arguments. Only these known options are
    # tolerated; misspelled/custom unsafe arguments never silently disappear.
    # Reserved options in awslabs/aws-glue-libs glue-5.0 utils.py/job.py plus
    # documented Glue job arguments. Recognizing these does not configure Glue.
    for name in ("JOB_ID", "JOB_RUN_ID", "SECURITY_CONFIGURATION", "TempDir", "RedshiftTempDir",
                 "extra-py-files", "job-bookmark-option", "job-bookmark-from", "job-bookmark-to",
                 "continuation-option", "encryption-type", "job-language", "spark-event-logs-path",
                 "scriptLocation", "endpoint", "mode", "continuous-log-logGroup",
                 "continuous-log-logStreamPrefix", "continuous-log-conversionPattern",
                 "custom-logGroup-prefix", "custom-logStream-prefix"):
        parser.add_argument("--" + name)
    for name in ("enable-metrics", "enable-continuous-cloudwatch-log", "enable-job-insights",
                 "enable-observability-metrics", "enable-spark-ui", "enable-continuous-log-filter",
                 "enable-auto-scaling", "enable-s3-parquet-optimized-committer",
                 "enable-rename-algorithm-v2", "enable-glue-datacatalog", "enable-data-lineage"):
        parser.add_argument("--" + name, nargs="?", const="true", choices=("true", "false", ""))
    parser.add_argument("--conf", action="append")  # Spark legitimately accepts repeated configurations.
    parsed, unknown = parser.parse_known_args(argv)
    if unknown:
        # Unlike getResolvedOptions, do NOT discard arbitrary extras. Strip
        # inline '=value' before emitting names; positional values stay redacted.
        names = [token.split("=", 1)[0] for token in unknown if token.startswith("--")]
        parser.fail("unrecognized_arguments", names, len(unknown))
    return parsed


def main(argv=None):
    args = parse_arguments(argv)
    config = dict(account_id=args.EXPECTED_ACCOUNT_ID, region=args.REGION, dataset=args.DATASET,
        run_id=args.RUN_ID, timezone="UTC", timezone_evidence=args.TIMEZONE_EVIDENCE,
        no_refresh_acknowledged=args.NO_REFRESH_ACKNOWLEDGED == "true", no_refresh_evidence=args.NO_REFRESH_EVIDENCE,
        builder_fingerprint=args.BUILDER_FINGERPRINT)
    validate_config(config)
    if args.JOB_NAME != JOB_NAME:
        raise ValueError("Unexpected dedicated preflight job name")
    if args.ALLOW_AWS_READS != "true":
        print(json.dumps(dict(mode="OFFLINE_CONFIG_VALIDATION", status="NOT_RUN", preflight_completed=False,
                              aws_reads_enabled=False, report_storage_enabled=False)))
        # Local validation stays backward-compatible. A Glue-shaped invocation
        # must fail rather than leave a green job that never validated any data.
        glue_invocation = ("JOB_NAME" in getattr(args, "_provided_options", set())
                           or args.JOB_RUN_ID is not None or args.JOB_ID is not None)
        return 2 if glue_invocation else 0
    # Never reached by local tests: runtime construction is explicit opt-in.
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    session = boto3.Session(region_name=REGION)
    identity = session.client("sts")
    if identity.get_caller_identity().get("Account") != config["account_id"]:
        raise ValueError("Wrong AWS account; Spark not initialized")
    context = GlueContext(SparkContext.getOrCreate())
    context.spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(context)
    job.init(JOB_NAME, {"JOB_NAME": JOB_NAME, "DATASET": "sample"})
    try:
        report = run_preflight(config, glue_context=context, catalog_client=session.client("glue"),
                               s3_client=session.client("s3"), identity_client=identity)
    except PreflightFailure as exc:
        print("PREFLIGHT | " + json.dumps(exc.report, sort_keys=True))
        raise
    print("PREFLIGHT | " + json.dumps(report, sort_keys=True))
    job.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
