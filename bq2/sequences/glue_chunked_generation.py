"""Separate HI/LI chunked staging job. NO publication or Catalog mutations.

single_chunk is diagnostic only. full_dataset requires all approved owner dates.
CONFIG_JSON uses the existing full-generation config; no destination overrides.
"""
import argparse
import json
import re
import sys
from types import SimpleNamespace
from glue_sample_generation import _Parser,_SingleValue,_OpaqueRuntimeValue,_summarize_unrecognized,SCRIPT_ROOT,TEMP_ROOT
from glue_hi_growth_probe import _initialize
from sequence_full_generation import validate_full_config
from sequence_full_source import owner_dates
from sequence_chunked_generation import ChunkLimits,run_chunked_generation,validate_chunk_limits

JOB_NAME="is459-g2t1-bq2-chunked-sequence-builder"


def _json(raw):
    def unique(pairs):
        result={}
        for k,v in pairs:
            if k in result:raise ValueError("Duplicate configuration key")
            result[k]=v
        return result
    return json.loads(raw,object_pairs_hook=unique)


def parse_arguments(argv=None):
    p=_Parser(prog="glue_chunked_generation",allow_abbrev=False,add_help=False)
    for n in ("DATASET","CONFIG_JSON","CHUNK_LIMITS_JSON","MODE","MAX_COPY_BYTES"):
        p.add_argument("--"+n,required=True,action=_SingleValue)
    p.add_argument("--ALLOW_AWS_WRITES",choices=("true","false"),default="false",action=_SingleValue)
    p.add_argument("--JOB_NAME",choices=(JOB_NAME,),default=JOB_NAME,action=_SingleValue)
    for n in ("OWNER_DATE","ROOT_INDEX","ROOT_GROUP_START","ROOT_GROUP_COUNT","INVOCATION_ID","RESUME_ETAG",
        "EXPECTED_COUNTS_JSON","JOB_ID","JOB_RUN_ID","SECURITY_CONFIGURATION",
        "TempDir","extra-py-files","scriptLocation","additional-python-modules","job-language","job-bookmark-option",
        "continuous-log-logGroup","continuous-log-logStreamPrefix","custom-logGroup-prefix","custom-logStream-prefix"):
        p.add_argument("--"+n,action=_SingleValue)
    p.add_argument("--PREVIOUS_RUN_STOPPED",choices=("true","false"),default="false",action=_SingleValue)
    p.add_argument("--python-modules-installer-option",choices=("--no-index",),action=_SingleValue)
    for n in ("enable-auto-scaling","enable-spark-ui"):
        p.add_argument("--"+n,choices=("false",),action=_SingleValue)
    for n in ("enable-metrics","enable-observability-metrics","enable-job-insights","enable-continuous-cloudwatch-log",
        "enable-glue-datacatalog","enable-s3-parquet-optimized-committer"):
        p.add_argument("--"+n,nargs="?",const="true",choices=("true","false",""),action=_SingleValue)
    for n in ("glue-di-packages-correlation-ids","internal-lib-urls","tenant-internal"):
        p.add_argument("--"+n,nargs="?",action=_OpaqueRuntimeValue,default=argparse.SUPPRESS)
    tokens=list(sys.argv[1:] if argv is None else argv);normalized=[];i=0
    while i<len(tokens):
        if tokens[i]=="--python-modules-installer-option" and i+1<len(tokens) and tokens[i+1]=="--no-index":
            normalized.append(tokens[i]+"=--no-index");i+=2
        else:normalized.append(tokens[i]);i+=1
    args,unknown=p.parse_known_args(normalized)
    if unknown:p.fail("unrecognized_arguments",unparsed_count=len(unknown),token_summary=_summarize_unrecognized(normalized,unknown,p._option_string_actions))
    return args


def validate_arguments(args):
    if args.ALLOW_AWS_WRITES!="true":raise ValueError("Explicit staging activation required")
    config=validate_full_config(_json(args.CONFIG_JSON))
    if args.DATASET!=config["dataset"]:raise ValueError("Dataset disagreement")
    limits=ChunkLimits(**_json(args.CHUNK_LIMITS_JSON))
    validate_chunk_limits(config,limits)
    if args.MODE not in ("single_chunk","full_dataset","single_owner_date"):raise ValueError("Explicit mode required")
    root=None
    if args.MODE=="single_chunk":
        if args.OWNER_DATE not in owner_dates(config["dataset"]) or args.ROOT_INDEX is None or not re.fullmatch(r"0|[1-9][0-9]*",args.ROOT_INDEX):
            raise ValueError("Valid owned date/root required")
        root=int(args.ROOT_INDEX)
        if root>=limits.root_chunks:raise ValueError("Root outside partition plan")
    elif args.MODE=="single_owner_date":
        from sequence_manifest import _token
        if args.OWNER_DATE not in owner_dates(config["dataset"]) or args.ROOT_INDEX is not None:
            raise ValueError("Explicit HI owner date required")
        for value in (args.ROOT_GROUP_START,args.ROOT_GROUP_COUNT):
            if not isinstance(value,str) or not re.fullmatch(r"0|[1-9][0-9]{0,8}",value):raise ValueError("Bounded root group required")
        if int(args.ROOT_GROUP_COUNT)<=0 or int(args.ROOT_GROUP_START)+int(args.ROOT_GROUP_COUNT)>limits.root_chunks:
            raise ValueError("Root group outside ownership plan")
        _token(args.INVOCATION_ID)
        if args.RESUME_ETAG is not None and (not args.RESUME_ETAG.strip() or args.PREVIOUS_RUN_STOPPED!="true"):
            raise ValueError("Confirmed stopped predecessor and exact ETag required")
    elif args.ROOT_INDEX is not None or args.OWNER_DATE is not None:raise ValueError("Full dataset cannot restrict dates/roots")
    if args.MODE!="single_owner_date" and (any(getattr(args,k) is not None for k in
        ("ROOT_GROUP_START","ROOT_GROUP_COUNT","INVOCATION_ID","RESUME_ETAG")) or args.PREVIOUS_RUN_STOPPED!="false"):
        raise ValueError("Recovery arguments require single_owner_date")
    if not re.fullmatch(r"[1-9][0-9]{0,17}",args.MAX_COPY_BYTES):raise ValueError("Positive copy-byte bound required")
    expected={} if args.EXPECTED_COUNTS_JSON is None else _json(args.EXPECTED_COUNTS_JSON)
    if not isinstance(expected,dict) or not set(expected)<=set(owner_dates(config["dataset"])):raise ValueError("Invalid expected count dates")
    for counts in expected.values():
        if not isinstance(counts,dict) or set(counts)!={"2","3"} or any(type(v) is not int or v<0 for v in counts.values()):raise ValueError("Invalid independent counts")
    if args.MODE=="single_owner_date" and set(expected)!={args.OWNER_DATE}:raise ValueError("Exact independent date counts required")
    for n,file in (("extra_py_files","role5_chunked_modules.zip"),("scriptLocation","glue_chunked_generation.py"),("additional_python_modules","role5_sdk.gluewheels.zip")):
        value=getattr(args,n)
        if value is not None and not re.fullmatch(re.escape(SCRIPT_ROOT)+r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/"+re.escape(file),value):raise ValueError("Unapproved artifact URI")
    if args.TempDir not in (None,TEMP_ROOT,TEMP_ROOT[:-1]) or args.job_language not in (None,"python") or args.job_bookmark_option not in (None,"job-bookmark-disable"):
        raise ValueError("Unsafe Glue runtime settings")
    return config,limits,root,expected


def main(argv=None):
    args=parse_arguments(argv);config,limits,root,expected=validate_arguments(args)
    dependencies=_initialize(SimpleNamespace(EXPECTED_ACCOUNT_ID=config["account_id"]))
    recovery={} if args.MODE!="single_owner_date" else dict(root_group_start=int(args.ROOT_GROUP_START),
        root_group_count=int(args.ROOT_GROUP_COUNT),invocation_id=args.INVOCATION_ID,resume_etag=args.RESUME_ETAG,
        previous_run_stopped=args.PREVIOUS_RUN_STOPPED=="true")
    result=run_chunked_generation(config,limits,allow_writes=True,mode=args.MODE,owner_date=args.OWNER_DATE,root_index=root,
        max_copy_bytes=int(args.MAX_COPY_BYTES),expected_counts=expected,**recovery,**dependencies)
    print("CHUNKED_GENERATION | "+json.dumps(dict(state=result["manifest"]["state"],manifest_uri=result["manifest_uri"],
        dataset=config["dataset"],published=False,catalog_updated=False),sort_keys=True),flush=True)
    return 0


if __name__=="__main__":main()
