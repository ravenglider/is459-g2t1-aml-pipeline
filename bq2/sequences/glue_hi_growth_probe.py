"""Temporary HI September 1 owner-batch profiler. Application READS ONLY.

No triples, output enrichment, Parquet writes, manifests, publication or Catalog
mutations. Glue itself may write logs/temp artifacts. Imports start no runtime.
Deploy separately using the existing full-module ZIP and verified SDK wheels.
"""
import argparse
import json
import re
import sys
import time

from glue_sample_generation import (_Parser, _SingleValue, _OpaqueRuntimeValue,
    _summarize_unrecognized, _validate_sdk_installation, SCRIPT_ROOT, TEMP_ROOT)
from sequence_full_source import inspect_full_source, snapshot_identity, assert_full_source_unchanged
from sequence_aws import require_verified_timezone

JOB_NAME = "is459-g2t1-bq2-hi-growth-probe"
DATES = ("2022-09-01", "2022-09-02", "2022-09-03")
PRECEDING_DATES = DATES[:2]
DATASET = "HI-Medium"


class GrowthProbeFailure(RuntimeError):
    def __init__(self, report):
        self.report = report
        super().__init__(report["error_type"])


def parse_arguments(argv=None):
    parser = _Parser(prog="glue_hi_growth_probe", allow_abbrev=False, add_help=False)
    for name in ("EXPECTED_ACCOUNT_ID", "REGION", "SOURCE_FINGERPRINT", "TABLE_FINGERPRINT",
                 "PARTITIONS_FINGERPRINT", "TIMEZONE_EVIDENCE", "NO_REFRESH_EVIDENCE",
                 "LIMITS_EVIDENCE", "MAX_INPUT_ROWS", "MAX_PAIR_CANDIDATES", "MAX_PAIR_LINKS", "MAX_RUN_SECONDS"):
        parser.add_argument("--"+name, required=True, action=_SingleValue)
    parser.add_argument("--ALLOW_AWS_READS", choices=("true", "false"), default="false", action=_SingleValue)
    parser.add_argument("--NO_REFRESH_ACKNOWLEDGED", choices=("true", "false"), required=True, action=_SingleValue)
    parser.add_argument("--JOB_NAME", choices=(JOB_NAME,), default=JOB_NAME, action=_SingleValue)
    for name in ("JOB_ID", "JOB_RUN_ID", "SECURITY_CONFIGURATION", "TempDir", "extra-py-files", "scriptLocation",
                 "job-language", "job-bookmark-option", "additional-python-modules", "continuous-log-logGroup",
                 "continuous-log-logStreamPrefix", "custom-logGroup-prefix", "custom-logStream-prefix"):
        parser.add_argument("--"+name, action=_SingleValue)
    parser.add_argument("--python-modules-installer-option", choices=("--no-index",), action=_SingleValue)
    for name in ("enable-metrics", "enable-continuous-cloudwatch-log", "enable-observability-metrics", "enable-job-insights",
                 "enable-glue-datacatalog", "enable-s3-parquet-optimized-committer"):
        parser.add_argument("--"+name, nargs="?", const="true", choices=("true", "false", ""), action=_SingleValue)
    for name in ("enable-auto-scaling", "enable-spark-ui"):
        parser.add_argument("--"+name, choices=("false",), action=_SingleValue)
    for name in ("glue-di-packages-correlation-ids", "internal-lib-urls", "tenant-internal"):
        parser.add_argument("--"+name, nargs="?", action=_OpaqueRuntimeValue, default=argparse.SUPPRESS)
    tokens = list(sys.argv[1:] if argv is None else argv)
    normalized, i = [], 0
    while i < len(tokens):
        if tokens[i] == "--python-modules-installer-option" and i+1 < len(tokens) and tokens[i+1] == "--no-index":
            normalized.append(tokens[i]+"=--no-index"); i += 2
        else:
            normalized.append(tokens[i]); i += 1
    args, unknown = parser.parse_known_args(normalized)
    if unknown:
        parser.fail("unrecognized_arguments", unparsed_count=len(unknown),
            token_summary=_summarize_unrecognized(normalized, unknown, parser._option_string_actions))
    return args


def validate_arguments(args):
    if args.ALLOW_AWS_READS != "true" or args.NO_REFRESH_ACKNOWLEDGED != "true":
        raise ValueError("Explicit read activation and frozen-source acknowledgment required")
    if args.REGION != "us-east-1" or not re.fullmatch(r"[0-9]{12}", args.EXPECTED_ACCOUNT_ID):
        raise ValueError("Verified account/us-east-1 required")
    require_verified_timezone("UTC", True, args.TIMEZONE_EVIDENCE)
    if not args.NO_REFRESH_EVIDENCE.strip() or not args.LIMITS_EVIDENCE.strip():
        raise ValueError("Actual source-freeze and computational-budget evidence required")
    for name in ("SOURCE_FINGERPRINT", "TABLE_FINGERPRINT", "PARTITIONS_FINGERPRINT"):
        if not re.fullmatch(r"[a-f0-9]{64}", getattr(args,name)):
            raise ValueError("Fresh full-HI fingerprints required")
    for name in ("MAX_INPUT_ROWS", "MAX_PAIR_CANDIDATES", "MAX_PAIR_LINKS", "MAX_RUN_SECONDS"):
        if not re.fullmatch(r"[1-9][0-9]{0,17}", getattr(args,name)):
            raise ValueError("Explicit positive integer required: "+name)
    for name, filename in (("extra_py_files","role5_full_modules.zip"), ("scriptLocation","glue_hi_growth_probe.py"),
                           ("additional_python_modules","role5_sdk.gluewheels.zip")):
        value = getattr(args,name)
        if value is not None and not re.fullmatch(re.escape(SCRIPT_ROOT)+r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/"+re.escape(filename),value):
            raise ValueError("Unapproved probe artifact location")
    if (args.TempDir not in (None,TEMP_ROOT,TEMP_ROOT[:-1]) or args.job_language not in (None,"python")
            or args.job_bookmark_option not in (None,"job-bookmark-disable")):
        raise ValueError("Unsafe Glue runtime settings")
    return args


def _initialize(args):
    _validate_sdk_installation()
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name="us-east-1")
    options = Config(connect_timeout=10,read_timeout=60,retries={"total_max_attempts":1})
    identity = session.client("sts",config=options)
    if identity.get_caller_identity().get("Account") != args.EXPECTED_ACCOUNT_ID:
        raise ValueError("Wrong AWS account; Spark not initialized")
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    context = GlueContext(SparkContext.getOrCreate())
    from sequence_glue_runtime import validate_glue_runtime
    spark = context.spark_session
    validate_glue_runtime(glue_version="5.0",spark_version=spark.version,
        java_version=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    spark.conf.set("spark.sql.session.timeZone","UTC")
    return dict(glue_context=context,identity_client=identity,catalog_client=session.client("glue",config=options),
                s3_client=session.client("s3",config=options))


def _read_selected(context, snapshot):
    from sequence_source import CLEANED_SCHEMA, _check_schema, build_partition_predicate, validate_cleaned_schema
    from pyspark.sql.types import StructType
    session = context.spark_session.newSession()
    session.conf.set("spark.sql.session.timeZone","UTC")
    for day in DATES:
        for uri in snapshot["files_by_date"][day]:
            _check_schema(session.read.option("recursiveFileLookup","true").parquet(uri).schema,
                          StructType(CLEANED_SCHEMA.fields[:-2]))
    frame = context.create_dynamic_frame.from_catalog(database="is459_g2t1_aml",table_name="transactions_clean",
        push_down_predicate=build_partition_predicate(DATASET,DATES)).toDF()
    if len(frame.columns)!=20 or set(frame.columns)!=set(CLEANED_SCHEMA.fieldNames()):
        raise ValueError("Expected exact 20-column cleaned schema")
    frame=frame.select(*CLEANED_SCHEMA.fieldNames())
    validate_cleaned_schema(frame)
    return frame


def run_probe(args, *, glue_context, identity_client, catalog_client, s3_client):
    args=validate_arguments(args)
    from sequence_batch import (_persist_owned, _enforce_limit, estimate_pair_candidates, estimate_three_hop_growth)
    from bq2_sequence_builder import validate_transactions, build_two_hop_links
    from pyspark.sql import functions as F
    started=time.monotonic(); owned=[]; before=None; error=None
    report=dict(status="RUNNING",dataset=DATASET,owner_date=DATES[0],input_dates=list(DATES),
        preceding_dates=list(PRECEDING_DATES),verification_scope="provided_dataframe_only",dataset_wide_verified=False,
        triples_materialized=False,outputs_written=False,phase="identity",limits_evidence=args.LIMITS_EVIDENCE,
        limits={n:int(getattr(args,n)) for n in ("MAX_INPUT_ROWS","MAX_PAIR_CANDIDATES","MAX_PAIR_LINKS","MAX_RUN_SECONDS")},
        phases_seconds={})
    spark=glue_context.spark_session
    def deadline():
        if time.monotonic()-started>=int(args.MAX_RUN_SECONDS):
            raise TimeoutError("Probe elapsed budget exhausted; service timeout must also be configured")
    def inspect():
        return inspect_full_source(catalog_client,s3_client,DATASET)
    try:
        for client in (identity_client,catalog_client,s3_client):
            if client.meta.region_name!="us-east-1": raise ValueError("Wrong AWS client region")
        if identity_client.get_caller_identity().get("Account")!=args.EXPECTED_ACCOUNT_ID:
            raise ValueError("Wrong AWS account")
        if spark.conf.get("spark.sql.session.timeZone")!="UTC": raise ValueError("Explicit UTC session required")
        report["phase"]="inventory_before"; before=inspect()
        expected={n.lower():getattr(args,n) for n in ("SOURCE_FINGERPRINT","TABLE_FINGERPRINT","PARTITIONS_FINGERPRINT")}
        if snapshot_identity(before)!=expected: raise ValueError("Fresh full-HI fingerprints disagree")
        report["source_before"]=expected
        if any(d not in before["available_dates"] or not before["files_by_date"].get(d) for d in DATES):
            raise ValueError("Missing probe partitions")
        deadline(); report["phase"]="read_validate"
        phase=time.monotonic(); source=_persist_owned(_read_selected(glue_context,before),owned)
        report["input_rows"]=source.count()
        _enforce_limit("input_rows",report["input_rows"],int(args.MAX_INPUT_ROWS))
        validate_transactions(source)
        if source.where((F.col("dataset")!=DATASET) | ~F.col("txn_date").isin(list(DATES))).limit(1).count():
            raise ValueError("Source rows escape HI three-date selection")
        report["phases_seconds"]["read_validate"]=round(time.monotonic()-phase,3)
        deadline(); report["phase"]="candidate_estimate"; phase=time.monotonic()
        report["pair_candidates"]=estimate_pair_candidates(source,list(PRECEDING_DATES))
        report["phases_seconds"]["candidate_estimate"]=round(time.monotonic()-phase,3)
        deadline()
        _enforce_limit("pair_candidates",report["pair_candidates"],int(args.MAX_PAIR_CANDIDATES))
        report["phase"]="valid_pairs"; phase=time.monotonic()
        pairs=_persist_owned(build_two_hop_links(source,first_dates=PRECEDING_DATES),owned)
        report["valid_pairs"]=pairs.count()
        _enforce_limit("pair_links",report["valid_pairs"],int(args.MAX_PAIR_LINKS))
        deadline()
        first_ids=source.where(F.col("txn_date")==DATES[0]).select("dataset",F.col("txn_id").alias("first_txn_id"))
        starting=pairs.join(first_ids,["dataset","first_txn_id"],"left_semi")
        report["starting_pairs"]=starting.count()
        report["projected_triples"],report["maximum_middle_expansion"]=estimate_three_hop_growth(starting,pairs)
        report["projected_output_rows"]=report["starting_pairs"]+report["projected_triples"]
        report["phases_seconds"]["pairs_and_degrees"]=round(time.monotonic()-phase,3)
        report["resource_observations"]=dict(master=spark.sparkContext.master,
            default_parallelism=spark.sparkContext.defaultParallelism,
            shuffle_partitions=spark.conf.get("spark.sql.shuffle.partitions"),
            storage="MEMORY_AND_DISK for owned reused frames",
            limitation="No peak memory/disk/CPU measurement; inspect Glue metrics. One window is not full-month calibration.")
        deadline()
    except Exception as exc:
        error=exc
    finally:
        for frame in reversed(owned):
            try: frame.unpersist(blocking=True)
            except Exception as cleanup:
                if error is None: error=cleanup
                else: error.add_note("Owned probe cleanup failed: "+type(cleanup).__name__)
        if before is not None:
            try:
                after=inspect(); report["source_after"]=snapshot_identity(after)
                assert_full_source_unchanged(before,after)
            except Exception as changed:
                if error is None: error=changed
                else: error.add_note("Final source inspection failed: "+type(changed).__name__)
        report["elapsed_seconds"]=round(time.monotonic()-started,3)
    if error is not None:
        report.update(status="FAILED",error_type=type(error).__name__)
        raise GrowthProbeFailure(report) from error
    report.update(status="PROFILED_READ_ONLY",phase="complete")
    return report


def main(argv=None):
    args=validate_arguments(parse_arguments(argv))
    try:
        report=run_probe(args,**_initialize(args))
    except GrowthProbeFailure as exc:
        print("HI_GROWTH_PROBE | "+json.dumps(exc.report,sort_keys=True),flush=True)
        raise
    print("HI_GROWTH_PROBE | "+json.dumps(report,sort_keys=True),flush=True)
    return 0


if __name__=="__main__":
    main()
