"""Dedicated opt-in Glue sample generation/staging entry script.

Deploy this script separately with the 13-module generation ZIP. Imports are
inert; no SDK installation, publication, Catalog mutation or synthetic hooks.
"""
import argparse
import json
import re
import sys

from glue_sample_preflight import _PreflightParser, _SingleValue

JOB_NAME = "is459-g2t1-bq2-sequence-builder"
REGION = "us-east-1"
TEMP_ROOT = "s3://is459-g2t1-aml-athena-bucket/glue-temp/"
SCRIPT_ROOT = "s3://is459-g2t1-aml-athena-bucket/scripts/role5/"
LIMITS = ("MAX_PAIR_CANDIDATES", "MAX_PAIR_LINKS", "MAX_PROJECTED_TRIPLES",
          "MAX_TRIPLES_PER_MIDDLE", "MAX_OUTPUT_ROWS")


class GenerationArgumentError(SystemExit):
    def __init__(self, diagnostic):
        self.diagnostic = diagnostic
        super().__init__(2)

    def __str__(self):
        return json.dumps(self.diagnostic, sort_keys=True)


class _Parser(_PreflightParser):
    def fail(self, reason, names=(), unparsed_count=0, token_summary=None):
        # Only allowlisted option names are safe; an unknown name can itself be a secret.
        safe = [name for name in names if name in self._option_string_actions]
        diagnostic = dict(status="ARGUMENT_ERROR", reason=reason, arguments=sorted(set(safe)),
            unparsed_count=unparsed_count, generation_completed=False, values_logged=False)
        if token_summary is not None:
            diagnostic["unrecognized_token_summary"] = token_summary
        line = "GENERATION_ARGUMENTS | "+json.dumps(diagnostic, sort_keys=True)
        print(line, flush=True)
        print(line, file=sys.stderr, flush=True)
        raise GenerationArgumentError(diagnostic) from None


class _OpaqueRuntimeValue(_SingleValue):
    def __call__(self, parser, namespace, values, option_string=None):
        # Retain duplicate detection, but never retain opaque runtime values.
        super().__call__(parser, namespace, None, option_string)
        delattr(namespace, self.dest)


def _summarize_unrecognized(tokens, unknown, option_strings):
    """Fixed enums/counts only; no values, paths, lengths, hashes or argv indices.

    This parser declares no positionals: every token before the first exact
    recognized option is an extra. parse_known_args preserves those prefix
    extras first, so repeated/interned values need no identity/value matching.
    At most six categories times two positions are emitted in a fixed order.
    """
    first_option = next((i for i, token in enumerate(tokens)
                         if token.split("=", 1)[0] in option_strings), len(tokens))
    categories = ("empty", "single_dash_token", "boolean_literal", "entry_script_basename_match",
                  "other_positional", "unknown_long_option")
    positions = ("before_first_recognized_option", "after_first_recognized_option")
    counts = {}
    for index, token in enumerate(unknown):
        if token == "":
            category = "empty"
        elif token.startswith("--"):
            category = "unknown_long_option"
        elif token.startswith("-"):
            category = "single_dash_token"
        elif token.lower() in ("true", "false"):
            category = "boolean_literal"
        elif token.replace("\\", "/").rsplit("/", 1)[-1] == "glue_sample_generation.py":
            category = "entry_script_basename_match"
        else:
            category = "other_positional"
        position = positions[0 if index < first_option else 1]
        counts[category, position] = counts.get((category, position), 0) + 1
    return [dict(category=category, position=position, count=counts[category, position])
            for category in categories for position in positions if (category, position) in counts]


def parse_arguments(argv=None):
    parser = _Parser(prog="glue_sample_generation", allow_abbrev=False, add_help=False)
    for name in ("EXPECTED_ACCOUNT_ID", "RUN_ID", "ATTEMPT_ID", "OWNER_TOKEN", "BUILDER_VERSION",
                 "BUILDER_FINGERPRINT", "SOURCE_FINGERPRINT", "TABLE_FINGERPRINT", "PARTITIONS_FINGERPRINT",
                 "TIMEZONE_EVIDENCE", "NO_REFRESH_EVIDENCE", "MAX_RECORDS_PER_FILE", *LIMITS):
        parser.add_argument("--"+name, required=True, action=_SingleValue)
    parser.add_argument("--REGION", required=True, choices=(REGION,), action=_SingleValue)
    parser.add_argument("--NO_REFRESH_ACKNOWLEDGED", required=True, choices=("true", "false"), action=_SingleValue)
    parser.add_argument("--ALLOW_AWS_WRITES", choices=("true", "false"), default="false", action=_SingleValue)
    for name, value in (("DATASET", "sample"), ("OWNER_DATE", "2022-09-01"), ("JOB_NAME", JOB_NAME)):
        parser.add_argument("--"+name, choices=(value,), default=value, action=_SingleValue)
    # Known Glue runtime metadata; values are not forwarded as Role 5 config.
    for name in ("JOB_ID", "JOB_RUN_ID", "SECURITY_CONFIGURATION", "TempDir", "extra-py-files", "scriptLocation",
                 "job-language", "job-bookmark-option", "continuous-log-logGroup", "continuous-log-logStreamPrefix",
                 "custom-logGroup-prefix", "custom-logStream-prefix"):
        parser.add_argument("--"+name, action=_SingleValue)
    for name in ("enable-metrics", "enable-continuous-cloudwatch-log", "enable-job-insights",
                 "enable-observability-metrics", "enable-s3-parquet-optimized-committer", "enable-glue-datacatalog"):
        parser.add_argument("--"+name, nargs="?", const="true", choices=("true", "false", ""), action=_SingleValue)
    for name in ("enable-auto-scaling", "enable-spark-ui"):
        parser.add_argument("--"+name, choices=("false",), action=_SingleValue)
    # Glue installs the separate SDK bundle during bootstrap, never in Python.
    parser.add_argument("--additional-python-modules", action=_SingleValue)
    parser.add_argument("--python-modules-installer-option", choices=("--no-index",), action=_SingleValue)
    for name in ("glue-di-packages-correlation-ids", "internal-lib-urls", "tenant-internal"):
        parser.add_argument("--"+name, nargs="?", action=_OpaqueRuntimeValue, default=argparse.SUPPRESS)
    # argparse otherwise mistakes this one reviewed option-like value for a flag.
    # Do not generalize to arbitrary installer switches or unknown arguments.
    tokens = list(sys.argv[1:] if argv is None else argv)
    normalized = []
    index = 0
    while index < len(tokens):
        if (tokens[index] == "--python-modules-installer-option" and index + 1 < len(tokens)
                and tokens[index + 1] == "--no-index"):
            normalized.append("--python-modules-installer-option=--no-index")
            index += 2
        else:
            normalized.append(tokens[index])
            index += 1
    # No --conf, arbitrary dependencies, path overrides or synthetic test options.
    args, unknown = parser.parse_known_args(normalized)
    if unknown:
        parser.fail("unrecognized_arguments", unparsed_count=len(unknown),
                    token_summary=_summarize_unrecognized(normalized, unknown, parser._option_string_actions))
    return args


def validate_arguments(args):
    """Pure argument-to-config conversion; called before any runtime construction."""
    if args.ALLOW_AWS_WRITES != "true":
        raise GenerationArgumentError(dict(status="NOT_RUN", reason="write_activation_required",
            generation_completed=False, values_logged=False))
    if args.additional_python_modules is None or args.python_modules_installer_option is None:
        raise GenerationArgumentError(dict(status="ARGUMENT_ERROR", reason="sdk_dependency_arguments_required",
            generation_completed=False, values_logged=False))
    sdk_pattern = re.escape(SCRIPT_ROOT)+r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/role5_sdk\.gluewheels\.zip"
    if (re.fullmatch(sdk_pattern, args.additional_python_modules) is None
            or args.python_modules_installer_option != "--no-index"):
        raise ValueError("Only the approved versioned SDK bundle and offline installer option are allowed")
    from sequence_sample_run import create_staging_run
    def positive(name):
        value = getattr(args, name.replace("-", "_"))
        if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,17}", value):
            raise ValueError(name+" must be a positive decimal integer")
        return int(value)
    if args.TempDir is not None and args.TempDir not in (TEMP_ROOT, TEMP_ROOT[:-1]):
        raise ValueError("Only the approved Glue temporary prefix is allowed")
    for name, suffix in (("extra-py-files", "role5_modules.zip"), ("scriptLocation", "glue_sample_generation.py")):
        value = getattr(args, name.replace("-", "_"))
        pattern = re.escape(SCRIPT_ROOT)+r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/"+re.escape(suffix)
        if value is not None and re.fullmatch(pattern, value) is None:
            raise ValueError("Unexpected generation script/package location")
    if args.job_language not in (None, "python"):
        raise ValueError("Python job language required")
    if args.job_bookmark_option not in (None, "job-bookmark-disable"):
        raise ValueError("Bookmarks must remain disabled")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", args.BUILDER_VERSION):
        raise ValueError("Safe explicit builder version required")
    if args.JOB_NAME != JOB_NAME:
        raise ValueError("Unexpected dedicated generation job name")
    config = dict(account_id=args.EXPECTED_ACCOUNT_ID, region=args.REGION, dataset=args.DATASET,
        run_id=args.RUN_ID, owner_date=args.OWNER_DATE, timezone="UTC", timezone_evidence=args.TIMEZONE_EVIDENCE,
        no_refresh_acknowledged=args.NO_REFRESH_ACKNOWLEDGED == "true", no_refresh_evidence=args.NO_REFRESH_EVIDENCE,
        builder_fingerprint=args.BUILDER_FINGERPRINT,
        limits={name.lower():positive(name) for name in LIMITS},
        expected_source=dict(source_fingerprint=args.SOURCE_FINGERPRINT, table_fingerprint=args.TABLE_FINGERPRINT,
                             partitions_fingerprint=args.PARTITIONS_FINGERPRINT))
    maximum = positive("MAX_RECORDS_PER_FILE")
    checked = create_staging_run(config, args.ATTEMPT_ID, maximum, owner_token=args.OWNER_TOKEN, builder_version=args.BUILDER_VERSION)
    return dict(config=checked["config"], attempt_id=args.ATTEMPT_ID, max_records_per_file=maximum,
                owner_token=args.OWNER_TOKEN, builder_version=args.BUILDER_VERSION, allow_writes=True)


def _check_clients(settings, catalog_client, s3_client, identity_client):
    from sequence_glue_runtime import validate_s3_conditional_writes
    if settings.get("allow_writes") is not True:
        raise ValueError("Explicit write activation required")
    for client in (catalog_client, s3_client, identity_client):
        if client.meta.region_name != REGION:
            raise ValueError("Client region must be us-east-1")
    if identity_client.get_caller_identity().get("Account") != settings["config"]["account_id"]:
        raise ValueError("Wrong AWS account")
    return validate_s3_conditional_writes(s3_client)


def _initialize_dependencies(settings):
    """Only main's fully validated explicit activation reaches runtime creation."""
    import boto3
    from botocore.config import Config
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    session = boto3.Session(region_name=REGION)
    options = Config(connect_timeout=10, read_timeout=60, retries={"total_max_attempts": 1})
    clients = {name:session.client(service, config=options) for name, service in
        (("catalog_client", "glue"), ("s3_client", "s3"), ("identity_client", "sts"))}
    _check_clients(settings, **clients)  # Wrong account/SDK must stop before Spark.
    context = GlueContext(SparkContext.getOrCreate())
    context.spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    _check_runtime(context)
    job = Job(context)
    job.init(JOB_NAME, {"JOB_NAME":JOB_NAME, "DATASET":"sample"})
    return dict(glue_context=context, **clients), job


def _check_runtime(glue_context):
    from sequence_glue_runtime import validate_glue_runtime
    spark = glue_context.spark_session
    runtime = validate_glue_runtime(glue_version="5.0", spark_version=spark.version,
        java_version=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session must explicitly use UTC")
    return runtime


def run_generation(settings, *, glue_context, catalog_client, s3_client, identity_client):
    """Injected runtime path: compatibility checks precede every coordinator write."""
    from sequence_sample_run import create_staging_run, coordinate_staging_run
    create_staging_run(settings["config"], settings["attempt_id"], settings["max_records_per_file"],
        owner_token=settings["owner_token"], builder_version=settings["builder_version"])
    capabilities = _check_clients(settings, catalog_client, s3_client, identity_client)
    runtime = _check_runtime(glue_context)
    result = coordinate_staging_run(**settings, glue_context=glue_context, catalog_client=catalog_client,
                                  s3_client=s3_client, identity_client=identity_client)
    if result["manifest"].get("state") != "STAGED_VALIDATED" or result.get("published") is not False:
        raise ValueError("Coordinator did not verify terminal staging-only success")
    return dict(result, runtime=runtime, s3_capabilities=capabilities)


def main(argv=None):
    settings = validate_arguments(parse_arguments(argv))
    try:
        dependencies, job = _initialize_dependencies(settings)
        result = run_generation(settings, **dependencies)
        job.commit()
    except Exception as error:
        from sequence_glue_runtime import GlueCompatibilityError
        report = dict(status="FAILED", generation_completed=False, error_type=type(error).__name__, values_logged=False)
        if isinstance(error, GlueCompatibilityError):
            report["compatibility"] = error.diagnostic
        print("SAMPLE_GENERATION | "+json.dumps(report, sort_keys=True), flush=True)
        raise
    manifest = result["manifest"]
    print("SAMPLE_GENERATION | "+json.dumps(dict(status="STAGED_VALIDATED", published=False,
        run_id=manifest["run_id"], manifest_uri=result["manifest_uri"], attempt_uri=result["attempt_uri"],
        counts=manifest["attempt_evidence"]["counts"], runtime=result["runtime"],
        conditional_writes_supported=True), sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code:
        raise SystemExit(exit_code)
