"""HI exact counts only: grouped timestamp range windows, never pair/triple IDs.

Separate diagnostic entrypoint. Application reads only; Glue logs/temp may write.
Validated unique IDs plus strict time order make repeated IDs impossible in a
chain. Group multiplicity preserves distinct transactions sharing all attributes.
"""
import argparse
import json
import sys
import re
import time
from glue_sample_generation import _Parser, _SingleValue, _OpaqueRuntimeValue, _summarize_unrecognized, SCRIPT_ROOT, TEMP_ROOT
from sequence_aws import require_verified_timezone
from sequence_full_source import inspect_full_source, snapshot_identity, assert_full_source_unchanged
from glue_hi_growth_probe import _initialize, _read_selected

JOB_NAME="is459-g2t1-bq2-hi-exact-growth-probe"
DATES=("2022-09-01","2022-09-02","2022-09-03")
DATASET="HI-Medium"
LIMIT_NAMES=("MAX_INPUT_ROWS","MAX_TIMESTAMP_GROUPS","MAX_EVENT_GROUPS","MAX_GROUPS_PER_KEY","SHUFFLE_PARTITIONS","MAX_RUN_SECONDS")

class ExactGrowthFailure(RuntimeError):
    def __init__(self, report):
        self.report=report
        super().__init__(report["error_type"])

def parse_arguments(argv=None):
    parser = _Parser(prog="glue_hi_exact_growth_probe", allow_abbrev=False, add_help=False)
    for name in ("EXPECTED_ACCOUNT_ID", "REGION", "SOURCE_FINGERPRINT", "TABLE_FINGERPRINT",
                 "PARTITIONS_FINGERPRINT", "TIMEZONE_EVIDENCE", "NO_REFRESH_EVIDENCE",
                 "LIMITS_EVIDENCE", "MAX_INPUT_ROWS", "MAX_TIMESTAMP_GROUPS", "MAX_EVENT_GROUPS", "MAX_GROUPS_PER_KEY", "SHUFFLE_PARTITIONS", "MAX_RUN_SECONDS"):
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


def _exact_counts(frame, args, deadline, report):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F, Window
    from sequence_source import validate_cleaned_schema
    from bq2_sequence_builder import validate_transactions
    from sequence_batch import _enforce_limit
    from pyspark.sql.types import LongType
    spark=frame.sparkSession
    if spark.conf.get("spark.sql.session.timeZone")!="UTC":
        raise ValueError("Verified UTC session required")
    previous_shuffle=spark.conf.get("spark.sql.shuffle.partitions")
    owned=[]
    def persist(value):
        manager=spark._jsparkSession.sharedState().cacheManager()
        if not manager.lookupCachedData(value._jdf).nonEmpty():
            value.persist(StorageLevel.DISK_ONLY);owned.append(value)
        return value
    def release(value):
        for i,existing in enumerate(owned):
            if existing is value:
                value.unpersist(blocking=True);owned.pop(i);break
    def bound(name, actual):
        metric={"MAX_INPUT_ROWS":"input_rows","MAX_TIMESTAMP_GROUPS":"timestamp_groups",
            "MAX_EVENT_GROUPS":"peak_event_groups","MAX_GROUPS_PER_KEY":"peak_groups_per_key"}[name]
        report[metric]=max(report.get(metric,0),actual)
        _enforce_limit(name,actual,int(getattr(args,name)))
        deadline()
    try:
        spark.conf.set("spark.sql.shuffle.partitions",args.SHUFFLE_PARTITIONS)
        report["phase"]="source_validation"
        validate_cleaned_schema(frame);source=persist(frame)
        bound("MAX_INPUT_ROWS",source.count())
        validate_transactions(source)
        if source.where((F.col("dataset")!=DATASET)|~F.col("txn_date").isin(list(DATES))).limit(1).count():
            raise ValueError("Source escapes fixed HI date scope")
        deadline()
        # IDs intentionally disappear only AFTER exact source identity validation.
        groups=persist(source.select("dataset","from_bank","from_account","to_bank","to_account",
            F.col("txn_ts").cast(LongType()).alias("second"))
            .groupBy("dataset","from_bank","from_account","to_bank","to_account","second")
            .count().withColumnRenamed("count","multiplicity"))
        bound("MAX_TIMESTAMP_GROUPS",groups.count())
        release(source)
        start=F.unix_timestamp(F.lit(DATES[0]+" 00:00:00"))
        end=start+F.lit(86400)
        middle_end=start+F.lit(172800)
        owner=groups.where((F.col("second")>=start)&(F.col("second")<end))
        middle=groups.where(F.col("second")<middle_end)
        def events(weighted, bank, account, queries, query_bank, query_account, lower, upper, field):
            report["phase"]=field+"_window"
            weighted=weighted.select("dataset",F.col(bank).alias("bank"),F.col(account).alias("account"),
                "second",F.col("multiplicity").cast("decimal(38,0)").alias("weight"))
            queries=queries.select("dataset",F.col(query_bank).alias("bank"),F.col(query_account).alias("account"),
                "second",F.lit(0).cast("decimal(38,0)").alias("weight"))
            combined=persist(weighted.unionByName(queries).groupBy("dataset","bank","account","second")
                .agg(F.sum("weight").alias("weight")))
            bound("MAX_EVENT_GROUPS",combined.count())
            largest=combined.groupBy("dataset","bank","account").count().agg(F.max("count")).first()[0] or 0
            bound("MAX_GROUPS_PER_KEY",int(largest))
            window=Window.partitionBy("dataset","bank","account").orderBy("second").rangeBetween(lower,upper)
            result=persist(combined.select("dataset","bank","account","second",
                F.coalesce(F.sum("weight").over(window),F.lit(0)).alias(field)))
            result.count();deadline();release(combined)
            return result
        # One row per queried key/timestamp prevents event multiplicity from
        # multiplying joins. Timestamp range endpoints are inclusive seconds.
        predecessors=events(owner,"to_bank","to_account",middle,"from_bank","from_account",-86400,-1,"predecessors")
        successors=events(groups,"from_bank","from_account",middle,"to_bank","to_account",1,86400,"successors")
        pred=predecessors.select("dataset",F.col("bank").alias("from_bank"),F.col("account").alias("from_account"),"second","predecessors")
        succ=successors.select("dataset",F.col("bank").alias("to_bank"),F.col("account").alias("to_account"),"second","successors")
        degree=(middle.join(pred,["dataset","from_bank","from_account","second"])
            .join(succ,["dataset","to_bank","to_account","second"]))
        m=F.col("multiplicity").cast("decimal(38,0)")
        p=F.col("predecessors");s=F.col("successors")
        values=degree.select((m*p).alias("pairs"),(m*s).alias("all_continuations"),
            F.when(p>0,m*s).otherwise(0).alias("reachable_continuations"),
            (m*p*s).alias("triples"),(p*s).alias("middle_expansion"),p.alias("predecessors"),
            F.when(p>0,s).otherwise(0).alias("successors"))
        sums=("pairs","all_continuations","reachable_continuations","triples")
        row=values.agg(F.count("*").alias("groups"),
            *[F.sum(n).alias(n) for n in sums],
            *[F.count(n).alias("valid_"+n) for n in (*sums,"middle_expansion")],
            F.max("middle_expansion").alias("maximum_middle_expansion"),
            F.max("predecessors").alias("maximum_predecessors"),F.max("successors").alias("maximum_successors")).first()
        if any(row["valid_"+n]!=row.groups for n in (*sums,"middle_expansion")) or (row.groups and any(row[n] is None for n in sums)):
            raise OverflowError("Exact growth exceeds decimal(38,0); no count accepted")
        report.update(owner_pairs=int(row.pairs or 0),all_valid_pairs_preceding_sep1_sep2=int(row.all_continuations or 0),
            reachable_continuation_pairs=int(row.reachable_continuations or 0),projected_triples=int(row.triples or 0),
            maximum_middle_expansion=int(row.maximum_middle_expansion or 0),maximum_predecessors=int(row.maximum_predecessors or 0),
            maximum_successors=int(row.maximum_successors or 0))
        report["projected_output_rows"]=report["owner_pairs"]+report["projected_triples"]
        deadline()
    finally:
        active=sys.exc_info()[1];cleanup_error=None
        for value in reversed(owned):
            try:value.unpersist(blocking=True)
            except Exception as exc:
                if cleanup_error is None:cleanup_error=exc
        try:spark.conf.set("spark.sql.shuffle.partitions",previous_shuffle)
        except Exception as exc:
            if cleanup_error is None:cleanup_error=exc
        if cleanup_error is not None:
            if active is not None:active.add_note("Exact profiler cleanup failed: "+type(cleanup_error).__name__)
            else:raise cleanup_error


def run_probe(args, *, glue_context,identity_client,catalog_client,s3_client):
    validate_arguments(args)
    started=time.monotonic();before=None;error=None
    report=dict(status="RUNNING",dataset=DATASET,owner_date=DATES[0],input_dates=list(DATES),
        phase="identity",pair_ids_materialized=False,triples_materialized=False,outputs_written=False,
        verification_scope="provided_dataframe_only",dataset_wide_verified=False,
        limits={name:int(getattr(args,name)) for name in LIMIT_NAMES},limits_evidence=args.LIMITS_EVIDENCE)
    def deadline():
        if time.monotonic()-started>=int(args.MAX_RUN_SECONDS):raise TimeoutError("Exact profiler elapsed budget exhausted")
    def inspect():return inspect_full_source(catalog_client,s3_client,DATASET)
    try:
        for client in (identity_client,catalog_client,s3_client):
            if client.meta.region_name!="us-east-1":raise ValueError("Wrong AWS client region")
        if identity_client.get_caller_identity().get("Account")!=args.EXPECTED_ACCOUNT_ID:raise ValueError("Wrong AWS account")
        if glue_context.spark_session.conf.get("spark.sql.session.timeZone")!="UTC":raise ValueError("UTC required")
        before=inspect()
        expected={name.lower():getattr(args,name) for name in ("SOURCE_FINGERPRINT","TABLE_FINGERPRINT","PARTITIONS_FINGERPRINT")}
        if snapshot_identity(before)!=expected:raise ValueError("Frozen full-HI fingerprints disagree")
        report["source_before"]=expected
        if any(d not in before["available_dates"] or not before["files_by_date"].get(d) for d in DATES):
            raise ValueError("Missing HI input partitions")
        deadline();report["phase"]="read"
        _exact_counts(_read_selected(glue_context,before),args,deadline,report)
    except Exception as exc:error=exc
    finally:
        if before is not None:
            try:
                after=inspect();report["source_after"]=snapshot_identity(after)
                assert_full_source_unchanged(before,after)
            except Exception as changed:
                if error is None:error=changed
                else:error.add_note("Final source inspection failed: "+type(changed).__name__)
        report["elapsed_seconds"]=round(time.monotonic()-started,3)
    if error is not None:
        report.update(status="FAILED",error_type=type(error).__name__)
        raise ExactGrowthFailure(report) from error
    report.update(status="EXACT_COUNTS_READ_ONLY",phase="complete")
    return report


def main(argv=None):
    args=validate_arguments(parse_arguments(argv))
    try:report=run_probe(args,**_initialize(args))
    except ExactGrowthFailure as exc:
        print("HI_EXACT_GROWTH | "+json.dumps(exc.report,sort_keys=True),flush=True);raise
    print("HI_EXACT_GROWTH | "+json.dumps(report,sort_keys=True),flush=True)
    return 0


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
    for name in ("MAX_INPUT_ROWS", "MAX_TIMESTAMP_GROUPS", "MAX_EVENT_GROUPS", "MAX_GROUPS_PER_KEY", "SHUFFLE_PARTITIONS", "MAX_RUN_SECONDS"):
        if not re.fullmatch(r"[1-9][0-9]{0,17}", getattr(args,name)):
            raise ValueError("Explicit positive integer required: "+name)
    if int(args.SHUFFLE_PARTITIONS)>4096:
        raise ValueError("Shuffle partition budget exceeds reviewed operational range")
    for name, filename in (("extra_py_files","role5_exact_modules.zip"), ("scriptLocation","glue_hi_exact_growth_probe.py"),
                           ("additional_python_modules","role5_sdk.gluewheels.zip")):
        value = getattr(args,name)
        if value is not None and not re.fullmatch(re.escape(SCRIPT_ROOT)+r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/"+re.escape(filename),value):
            raise ValueError("Unapproved probe artifact location")
    if (args.TempDir not in (None,TEMP_ROOT,TEMP_ROOT[:-1]) or args.job_language not in (None,"python")
            or args.job_bookmark_option not in (None,"job-bookmark-disable")):
        raise ValueError("Unsafe Glue runtime settings")
    return args


if __name__=="__main__":main()
