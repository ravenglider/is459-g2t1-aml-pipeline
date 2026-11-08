"""Read-only AWS inspection -> exclusive LOCAL StartJobRun review JSON.

Uses the already deployed release, NOT the experimental batched kernel. Never
starts/stops jobs or writes AWS objects. --enable-writes changes JSON only.
"""
import argparse
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import re
import uuid

JOB="is459-g2t1-bq2-chunked-sequence-builder"
RUN="jr_aad8c6a7ed99b0b6c15eb4115726f28c147174ce671126a440e2fc7aed223e2e"
REGION="us-east-1"
BUCKET="is459-g2t1-aml-processed-bucket"
OLD_KEY="bq2_sequences_staging/full/dataset=HI-Medium/hi_date_20261109_015223_721777a904ea/attempt01_8b835eb16df4/metadata/owner-dates/2022-09-01/run.json"
PREFIX="s3://is459-g2t1-aml-athena-bucket/scripts/role5/hi_owner_date_staging_20261109_01/"
HASH="79cb1a38cc53083d4e3daef53abb2f7f7a60e582c7235fc6a1139e3c78c35969"


def require(ok,message):
    if not ok:raise ValueError(message)


def prepare_pilot(job,run,progress,account,*,freeze_evidence,limits_evidence,enable_writes=False):
    require(isinstance(account,str) and re.fullmatch(r"[0-9]{12}",account),"Explicit verified account required")
    require(type(enable_writes) is bool,"Boolean JSON activation required")
    for value in (freeze_evidence,limits_evidence):require(isinstance(value,str) and bool(value.strip()),"Actual reviewed evidence required")
    require(job["Name"]==JOB and (job["GlueVersion"],job["WorkerType"],job["NumberOfWorkers"],job["ExecutionClass"],job["Timeout"],job["MaxRetries"])==("5.0","G.1X",5,"STANDARD",30,0),"Unexpected permanent job configuration")
    require(job["ExecutionProperty"]["MaxConcurrentRuns"]==1 and job.get("JobRunQueuingEnabled",False) is False,"Concurrency/queue mismatch")
    require(job["Command"]==dict(Name="glueetl",PythonVersion="3",ScriptLocation=PREFIX+"glue_chunked_generation.py"),"Unexpected deployed entry point")
    defaults=job.get("DefaultArguments",{});fixed=job.get("NonOverridableArguments",{});effective=dict(defaults,**fixed)
    require(defaults.get("--ALLOW_AWS_WRITES")=="false" and "--ALLOW_AWS_WRITES" not in fixed,"Permanent writes must remain disabled and overridable")
    require("--ROOT_INDEX" not in effective and "--conf" not in effective,"Legacy selector/unsafe Spark setting")
    for key,value in {"--extra-py-files":PREFIX+"role5_chunked_modules.zip","--additional-python-modules":PREFIX+"role5_sdk.gluewheels.zip","--python-modules-installer-option":"--no-index","--job-bookmark-option":"job-bookmark-disable"}.items():
        require(effective.get(key)==value,"Bootstrap mismatch: "+key)
    require(effective.get("--enable-auto-scaling","false")=="false","Autoscaling must be disabled")
    require(run["Id"]==RUN and run["JobRunState"]=="SUCCEEDED","Expected successful four-root benchmark")
    previous=run["Arguments"]
    require(all(previous.get(k)==v for k,v in {"--MODE":"single_owner_date","--DATASET":"HI-Medium","--OWNER_DATE":"2022-09-01","--ROOT_GROUP_START":"0","--ROOT_GROUP_COUNT":"4"}.items()),"Unexpected benchmark scope")
    cfg=json.loads(previous["--CONFIG_JSON"]);old_limits=json.loads(previous["--CHUNK_LIMITS_JSON"])
    require(old_limits["root_chunks"]==4096 and cfg["account_id"]==account and cfg["dataset"]=="HI-Medium" and cfg["region"]==REGION and cfg["builder_fingerprint"]==HASH,"Benchmark identity mismatch")
    require(progress["manifest_kind"]=="owner_date_generation_progress" and progress["state"]=="PARTIAL_CHUNKS" and progress["published"] is False and progress["catalog_updated"] is False,"Invalid progress evidence")
    require(progress["run_id"]==cfg["run_id"] and progress["attempt_id"]==cfg["attempt_id"] and progress["owner_token"]==cfg["owner_token"],"Progress owner mismatch")
    require(progress["source"]==progress["source_after"]==cfg["expected_source"],"Source evidence mismatch")
    require(set(progress["root_progress"])=={"0","1","2","3"},"Unexpected completed roots")
    require({str(k):sum(r["counts"][str(k)] for r in progress["root_progress"].values()) for k in (2,3)}=={"2":9454,"3":23079},"Benchmark counts mismatch")
    require(cfg["no_refresh_acknowledged"] is True and bool(cfg["timezone_evidence"].strip()),"Verified UTC/frozen source evidence missing")
    stamp=datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S")
    cfg=deepcopy(cfg)
    cfg.update(run_id="hi_coarse_"+stamp+"_"+uuid.uuid4().hex[:12],attempt_id="attempt01_"+uuid.uuid4().hex[:12],
        owner_token="owner_"+uuid.uuid4().hex,no_refresh_evidence=freeze_evidence,limits_evidence=limits_evidence,
        max_run_seconds=1500,max_records_per_file=50000,
        limits=dict(max_pair_candidates=250000,max_pair_links=100000,max_projected_triples=100000,max_triples_per_middle=100000,max_output_rows=100000))
    limits=dict(root_chunks=256,max_preceding_rows=25000,max_pair_candidates=250000,max_pair_links=100000,
        max_projected_triples=100000,max_middle_expansion=100000,max_output_rows=100000,
        max_total_pair_links=60000000,max_total_output_rows=60000000,max_chunks=25000,max_split_nodes=100000)
    args={"--ALLOW_AWS_WRITES":"true" if enable_writes else "false","--DATASET":"HI-Medium","--MODE":"single_owner_date",
        "--OWNER_DATE":"2022-09-01","--ROOT_GROUP_START":"0","--ROOT_GROUP_COUNT":"1","--INVOCATION_ID":"coarse0000_"+uuid.uuid4().hex[:12],
        "--MAX_COPY_BYTES":"67108864","--CONFIG_JSON":json.dumps(cfg,sort_keys=True,separators=(",",":")),
        "--CHUNK_LIMITS_JSON":json.dumps(limits,sort_keys=True,separators=(",",":")),
        "--EXPECTED_COUNTS_JSON":json.dumps({"2022-09-01":{"2":14679196,"3":36965485}},sort_keys=True,separators=(",",":"))}
    require(not(set(args)&set(fixed)),"Run scope is pinned in NonOverridableArguments")
    return dict(JobName=JOB,WorkerType="G.1X",NumberOfWorkers=10,ExecutionClass="STANDARD",Timeout=30,JobRunQueuingEnabled=False,Arguments=args)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    p.add_argument("--expected-account-id",required=True);p.add_argument("--freeze-evidence",required=True)
    p.add_argument("--limits-evidence",required=True);p.add_argument("--source-still-frozen",action="store_true")
    p.add_argument("--enable-writes",action="store_true");p.add_argument("--output",required=True)
    a=p.parse_args(argv)
    require(re.fullmatch(r"[0-9]{12}",a.expected_account_id),"Invalid account")
    require(a.source_still_frozen and a.freeze_evidence.strip() and a.limits_evidence.strip(),"Explicit fresh source/budget acknowledgment required")
    path=Path(a.output)
    require(not path.exists() and not path.is_symlink() and not any(x.is_symlink() for x in path.absolute().parents),"Fresh safe local output required")
    import boto3
    from botocore.config import Config
    session=boto3.Session(region_name=REGION);options=Config(connect_timeout=10,read_timeout=60,retries={"total_max_attempts":1})
    require(session.client("sts",config=options).get_caller_identity()["Account"]==a.expected_account_id,"Wrong account")
    glue=session.client("glue",config=options);s3=session.client("s3",config=options)
    terminal={"SUCCEEDED","FAILED","STOPPED","TIMEOUT","ERROR","EXPIRED"}
    for page in glue.get_paginator("get_job_runs").paginate(JobName=JOB):
        require(all(r["JobRunState"] in terminal for r in page["JobRuns"]),"Active/waiting job exists")
    response=s3.get_object(Bucket=BUCKET,Key=OLD_KEY)
    try:progress=json.loads(response["Body"].read())
    finally:response["Body"].close()
    request=prepare_pilot(glue.get_job(JobName=JOB)["Job"],glue.get_job_run(JobName=JOB,RunId=RUN)["JobRun"],progress,
        a.expected_account_id,freeze_evidence=a.freeze_evidence,limits_evidence=a.limits_evidence,enable_writes=a.enable_writes)
    cfg=json.loads(request["Arguments"]["--CONFIG_JSON"])
    key="bq2_sequences_staging/full/dataset=HI-Medium/"+cfg["run_id"]+"/"+cfg["attempt_id"]+"/"
    require(not s3.list_objects_v2(Bucket=BUCKET,Prefix=key,MaxKeys=1).get("Contents"),"Fresh attempt already exists")
    with path.open("x",encoding="utf-8") as f:json.dump(request,f,sort_keys=True,indent=2);f.write("\n")
    print("LOCAL REVIEW JSON:",path.resolve())
    print("Scope: root 0/256 only, NOT root 0/4096; PARTIAL_CHUNKS, no publication")
    print("Staging:","s3://"+BUCKET+"/"+key)
    print("JSON activation:",request["Arguments"]["--ALLOW_AWS_WRITES"],"; NO JOB STARTED; NO AWS WRITES")


if __name__=="__main__":main()
