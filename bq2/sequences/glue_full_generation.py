"""Reusable HI/LI Glue 5.0 entry point: full generation + staging, NOT publication.

Deploy separately from the successful sample entrypoint. The eventual single
production job must add reviewed publication/Catalog integration before it can
claim Sonia's final contract. Importing this module creates no clients or Spark.
"""
import argparse
import json
import re
import sys

from glue_sample_generation import (
    _Parser, _SingleValue, _OpaqueRuntimeValue, _summarize_unrecognized,
    _validate_sdk_installation, SCRIPT_ROOT, TEMP_ROOT, JOB_NAME,
)
from sequence_full_generation import LIMIT_NAMES, validate_full_config, run_full_generation


def parse_arguments(argv=None):
    parser = _Parser(prog="glue_full_generation", allow_abbrev=False, add_help=False)
    for name in ("EXPECTED_ACCOUNT_ID", "DATASET", "REGION", "RUN_ID", "ATTEMPT_ID", "OWNER_TOKEN",
                 "BUILDER_VERSION", "BUILDER_FINGERPRINT", "SOURCE_FINGERPRINT", "TABLE_FINGERPRINT",
                 "PARTITIONS_FINGERPRINT", "TIMEZONE_EVIDENCE", "NO_REFRESH_EVIDENCE", "LIMITS_EVIDENCE",
                 "MAX_RECORDS_PER_FILE", "MAX_RUN_SECONDS", *(n.upper() for n in LIMIT_NAMES)):
        parser.add_argument("--"+name, required=True, action=_SingleValue)
    parser.add_argument("--ALLOW_AWS_WRITES", choices=("true", "false"), default="false", action=_SingleValue)
    parser.add_argument("--NO_REFRESH_ACKNOWLEDGED", choices=("true", "false"), required=True, action=_SingleValue)
    parser.add_argument("--JOB_NAME", default=JOB_NAME, choices=(JOB_NAME,), action=_SingleValue)
    # Optional bootstrap metadata may be consumed by Glue before Python starts.
    for name in ("JOB_ID", "JOB_RUN_ID", "SECURITY_CONFIGURATION", "TempDir", "extra-py-files", "scriptLocation",
                 "job-language", "job-bookmark-option", "continuous-log-logGroup", "continuous-log-logStreamPrefix",
                 "custom-logGroup-prefix", "custom-logStream-prefix", "additional-python-modules"):
        parser.add_argument("--"+name, action=_SingleValue)
    parser.add_argument("--python-modules-installer-option", choices=("--no-index",), action=_SingleValue)
    for name in ("enable-metrics", "enable-continuous-cloudwatch-log", "enable-job-insights",
                 "enable-observability-metrics", "enable-s3-parquet-optimized-committer", "enable-glue-datacatalog"):
        parser.add_argument("--"+name, nargs="?", const="true", choices=("true", "false", ""), action=_SingleValue)
    for name in ("enable-auto-scaling", "enable-spark-ui"):
        parser.add_argument("--"+name, choices=("false",), action=_SingleValue)
    for name in ("glue-di-packages-correlation-ids", "internal-lib-urls", "tenant-internal"):
        parser.add_argument("--"+name, nargs="?", action=_OpaqueRuntimeValue, default=argparse.SUPPRESS)
    tokens = list(sys.argv[1:] if argv is None else argv)
    normalized, i = [], 0
    while i < len(tokens):
        if tokens[i] == "--python-modules-installer-option" and i+1 < len(tokens) and tokens[i+1] == "--no-index":
            normalized.append(tokens[i]+"=--no-index")
            i += 2
        else:
            normalized.append(tokens[i])
            i += 1
    args, unknown = parser.parse_known_args(normalized)
    if unknown:
        parser.fail("unrecognized_arguments", unparsed_count=len(unknown),
                    token_summary=_summarize_unrecognized(normalized, unknown, parser._option_string_actions))
    return args


def validate_arguments(args):
    if args.ALLOW_AWS_WRITES != "true":
        raise ValueError("Explicit --ALLOW_AWS_WRITES=true required")
    def positive(name):
        value = getattr(args, name)
        if not re.fullmatch(r"[1-9][0-9]{0,17}", value):
            raise ValueError("Explicit positive integer required: "+name)
        return int(value)
    for name, suffix in (("extra_py_files", "role5_full_modules.zip"), ("scriptLocation", "glue_full_generation.py"),
                         ("additional_python_modules", "role5_sdk.gluewheels.zip")):
        value = getattr(args, name)
        if value is not None and not re.fullmatch(re.escape(SCRIPT_ROOT)+r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/"+re.escape(suffix), value):
            raise ValueError("Unexpected full-generation artifact location")
    if args.TempDir not in (None, TEMP_ROOT, TEMP_ROOT[:-1]) or args.job_language not in (None, "python"):
        raise ValueError("Unsafe Glue runtime settings")
    if args.job_bookmark_option not in (None, "job-bookmark-disable"):
        raise ValueError("Bookmarks must be disabled")
    return validate_full_config(dict(dataset=args.DATASET, account_id=args.EXPECTED_ACCOUNT_ID,
        region=args.REGION, run_id=args.RUN_ID, attempt_id=args.ATTEMPT_ID, owner_token=args.OWNER_TOKEN,
        builder_version=args.BUILDER_VERSION, builder_fingerprint=args.BUILDER_FINGERPRINT,
        timezone_evidence=args.TIMEZONE_EVIDENCE, no_refresh_acknowledged=args.NO_REFRESH_ACKNOWLEDGED == "true",
        no_refresh_evidence=args.NO_REFRESH_EVIDENCE, limits={n:positive(n.upper()) for n in LIMIT_NAMES},
        limits_evidence=args.LIMITS_EVIDENCE, max_records_per_file=positive("MAX_RECORDS_PER_FILE"),
        max_run_seconds=positive("MAX_RUN_SECONDS"), verified_empty_dates={},
        expected_source={n.lower():getattr(args, n) for n in ("SOURCE_FINGERPRINT", "TABLE_FINGERPRINT", "PARTITIONS_FINGERPRINT")}))


def _initialize(config):
    _validate_sdk_installation()
    import boto3
    from botocore.config import Config
    from sequence_glue_runtime import validate_s3_conditional_writes, validate_glue_runtime
    session = boto3.Session(region_name="us-east-1")
    options = Config(connect_timeout=10, read_timeout=60, retries={"total_max_attempts":1})
    identity = session.client("sts", config=options)
    if identity.get_caller_identity().get("Account") != config["account_id"]:
        raise ValueError("Wrong AWS account; Spark not started")
    s3 = session.client("s3", config=options)
    validate_s3_conditional_writes(s3)
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    validate_glue_runtime(glue_version="5.0", spark_version=spark.version,
        java_version=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(context)
    job.init(JOB_NAME, {"JOB_NAME":JOB_NAME, "DATASET":config["dataset"]})
    return dict(glue_context=context, identity_client=identity, s3_client=s3,
                catalog_client=session.client("glue", config=options)), job


def main(argv=None):
    config = validate_arguments(parse_arguments(argv))
    dependencies, job = _initialize(config)
    result = run_full_generation(config, allow_writes=True, **dependencies)
    manifest = result["manifest"]
    if manifest["state"] != "STAGED_VALIDATED" or manifest["published"] is not False:
        raise ValueError("Incomplete generation-only handoff")
    job.commit()
    print("FULL_GENERATION | "+json.dumps(dict(status="STAGED_VALIDATED", dataset=config["dataset"],
        run_id=config["run_id"], attempt_id=config["attempt_id"], counts=manifest["counts"],
        row_count=manifest["row_count"], manifest_uri=result["manifest_uri"],
        published=False, catalog_updated=False, publication_required=True), sort_keys=True))
    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code:
        raise SystemExit(exit_code)
