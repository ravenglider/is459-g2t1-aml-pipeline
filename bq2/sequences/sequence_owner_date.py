"""Fenced, recoverable owner-date staging. No publication or Catalog writes.

Resume requires the exact current manifest ETag and an independently confirmed
stopped predecessor. CAS fences metadata, not already executing Spark/S3 tasks;
each invocation therefore writes chunks into a separate immutable workspace.
"""
from copy import deepcopy
import hashlib
import time
from pyspark import StorageLevel
from pyspark.sql import functions as F
from sequence_chunked_generation import ChunkBudget,ChunkLimits,generate_date_chunks,validate_chunk_limits
from sequence_chunked_staging import ChunkS3Stager
from sequence_full_generation import validate_full_config
from sequence_full_source import (inspect_full_source,snapshot_identity,assert_full_source_unchanged,
    read_full_catalog,owner_dates,inventory_objects)
from sequence_manifest import _token,_digest
from sequence_s3 import _get,_put
from sequence_source import validate_cleaned_schema,fingerprint_source_inventory
from bq2_sequence_builder import validate_transactions
from sequence_staging import SEQUENCE_SCHEMA,validate_sequence_output
from sequence_glue_runtime import validate_glue_runtime,validate_s3_conditional_writes


class OwnerDateStager(ChunkS3Stager):
    recoverable=True

    @property
    def manifest_uri(self):return self.root+"metadata/owner-dates/"+self.day+"/run.json"

    def _write_run(self,manifest):
        etag=_put(self.client,self.manifest_uri,manifest,self.etag)
        actual,observed_etag,_=_get(self.client,self.manifest_uri)
        if (actual,observed_etag)!=(manifest,etag):raise ValueError("Unconfirmed owner-date manifest")
        self.manifest,self.etag=deepcopy(manifest),etag

    def _assert_owner(self):
        if self.manifest is None:raise ValueError("Date manifest not reserved")
        actual,etag,_=_get(self.client,self.manifest_uri)
        if (actual,etag)!=(self.manifest,self.etag):raise ValueError("Owner-date revision conflict")

    def reserve(self,manifest):
        for prefix in (self.manifest_uri.removesuffix("run.json"),self.root+"batches/"+self.day+"/",self.root+"chunk-work/"+self.day+"/"):
            if inventory_objects(self.client,prefix):raise ValueError("Date attempt already exists")
        self._write_run(manifest)

    def open(self,manifest,invocation_id,resume_etag,previous_run_stopped):
        _token(invocation_id)
        self.day=manifest["owner_dates"][0]
        if resume_etag is None:
            self.reserve(dict(manifest,invocation_id=invocation_id,revision=0,root_progress={},budget={}))
        else:
            if previous_run_stopped is not True:raise ValueError("Stopped predecessor confirmation required")
            actual,etag,_=_get(self.client,self.manifest_uri)
            if etag!=resume_etag:raise ValueError("Stale recovery ETag")
            if actual.get("identity")!=manifest["identity"] or actual.get("manifest_kind")!=manifest["manifest_kind"]:
                raise ValueError("Recovery identity/configuration mismatch")
            if actual.get("state") not in ("CREATED","PARTIAL_CHUNKS"):raise ValueError("Run is not recoverable")
            if invocation_id in actual.get("invocations",[actual.get("invocation_id")]):raise ValueError("Fresh invocation ID required")
            self.manifest,self.etag=actual,etag
            self._write_run(dict(actual,invocation_id=invocation_id,revision=actual["revision"]+1,
                invocations=actual.get("invocations",[actual["invocation_id"]])+[invocation_id]))
        if "invocations" not in self.manifest:
            self._write_run(dict(self.manifest,invocations=[invocation_id],revision=self.manifest["revision"]+1))
        self.completed_roots=deepcopy(self.manifest["root_progress"])

    def save(self,**changes):
        self._assert_owner()
        if self.manifest.get("state")=="DATE_STAGED_VALIDATED":raise ValueError("Terminal date manifest is immutable")
        if changes.get("state") not in (None,"PARTIAL_CHUNKS","DATE_STAGED_VALIDATED"):
            raise ValueError("Unsupported owner-date lifecycle state")
        if changes.get("state")=="DATE_STAGED_VALIDATED":
            date=self.dates.get(self.day)
            batches=changes.get("batches",[])
            if (date is None or date["status"].get("state")!="VALIDATED" or len(batches)!=1 or
                changes.get("source_after")!=self.manifest["source"]):raise ValueError("Incomplete terminal date evidence")
            batch=batches[0];receipt,_,_=_get(self.client,date["root"]+"metadata/validation.json")
            if (_digest(receipt)!=batch.get("receipt_fingerprint") or any(batch.get(k)!=v for k,v in receipt.items()) or
                changes.get("counts")!=receipt["counts"] or changes.get("row_count")!=receipt["row_count"] or
                inventory_objects(self.client,receipt["staging_location"])!=receipt["files"]):
                raise ValueError("Terminal date receipt changed")
        self._write_run(dict(self.manifest,revision=self.manifest["revision"]+1,**changes))

    def begin_date(self,day,root_chunks,owner_rows):
        root=self.root+"batches/"+day+"/";work=self.root+"chunk-work/"+day+"/"
        if not inventory_objects(self.client,root):
            super().begin_date(day,root_chunks,owner_rows)
        else:
            status,etag,_=_get(self.client,root+"metadata/status.json")
            for key,value in dict(dataset=self.config["dataset"],start_date=day,root_chunks=root_chunks,
                owner_rows=owner_rows,owner_token=self.config["owner_token"],run_id=self.config["run_id"],
                attempt_id=self.config["attempt_id"]).items():
                if status.get(key)!=value:raise ValueError("Date recovery ownership mismatch")
            if status.get("state") not in ("WRITING","VALIDATING","VALIDATED"):raise ValueError("Invalid recovered date status")
            self.dates[day]=dict(root=root,work=work,status_uri=root+"metadata/status.json",status=status,
                etag=etag,chunks={},roots={})
        date=self.dates[day];date["root_uris"]={}
        for key,record in self.completed_roots.items():
            root_index=int(key)
            if not 0<=root_index<root_chunks:raise ValueError("Invalid recovered root")
            if not record["receipt_uri"].startswith(work+"invocations/") or not record["receipt_uri"].endswith("/root-"+str(root_index)+".json"):
                raise ValueError("Unsafe recovered root location")
            receipt,_,_=_get(self.client,record["receipt_uri"])
            if _digest(receipt)!=record["receipt_fingerprint"]:raise ValueError("Root receipt changed")
            if receipt["root"]!=root_index or receipt.get("state")!="COMPLETE":raise ValueError("Invalid root receipt")
            if type(receipt.get("input_rows")) is not int or receipt["input_rows"]<0:raise ValueError("Invalid root input count")
            chunks=receipt["chunks"]
            if set(receipt["chunk_ids"])!=set(chunks) or len(receipt["chunk_ids"])!=len(chunks):raise ValueError("Root chunk coverage changed")
            for cid,chunk in chunks.items():
                expected_prefix=work+"invocations/"
                uri=chunk["receipt_uri"]
                if not uri.startswith(expected_prefix) or not uri.endswith("/"+cid+"/validation.json"):
                    raise ValueError("Unsafe recovered chunk location")
                observed,_,_=_get(self.client,uri)
                if observed!={k:v for k,v in chunk.items() if k!="receipt_uri"}:raise ValueError("Chunk receipt changed")
                if chunk.get("chunk_id")!=cid or chunk.get("root")!=root_index:raise ValueError("Recovered chunk ownership changed")
                status,_,_=_get(self.client,uri.removesuffix("validation.json")+"status.json")
                if status.get("state")!="VALIDATED" or status.get("owner_token")!=self.config["owner_token"]:
                    raise ValueError("Recovered chunk is not validated/owned")
                data=uri.removesuffix("validation.json")+"data/"
                if inventory_objects(self.client,data)!=chunk["files"] or fingerprint_source_inventory(chunk["files"])!=chunk["inventory_fingerprint"]:
                    raise ValueError("Recovered chunk inventory changed")
                if cid in date["chunks"]:raise ValueError("Duplicate recovered chunk")
                date["chunks"][cid]=chunk
            totals={str(k):sum(c["counts"][str(k)] for c in chunks.values()) for k in (2,3)}
            if totals!=receipt["counts"] or totals!=record["counts"]:raise ValueError("Recovered counts disagree")
            date["roots"][root_index]=receipt;date["root_uris"][root_index]=record["receipt_uri"]

    def write_chunk(self,day,root,chunk_id,output,counts,*,ownership):
        date=self.dates[day];base=date["work"]
        date["work"]=base+"invocations/"+self.manifest["invocation_id"]+"/"
        try:return super().write_chunk(day,root,chunk_id,output,counts,ownership=ownership)
        finally:date["work"]=base

    def complete_root(self,day,root,input_rows,counts,chunk_ids):
        date=self._owned_date(day)
        actual={cid for cid,c in date["chunks"].items() if c["root"]==root}
        if root in date["roots"] or len(chunk_ids)!=len(set(chunk_ids)) or set(chunk_ids)!=actual:
            raise ValueError("Duplicate/incomplete root")
        totals={str(k):sum(date["chunks"][cid]["counts"][str(k)] for cid in chunk_ids) for k in (2,3)}
        if totals!={str(k):v for k,v in counts.items()}:raise ValueError("Root counts disagree")
        receipt=dict(root=root,input_rows=input_rows,counts=totals,chunk_ids=chunk_ids,state="COMPLETE",
            chunks={cid:date["chunks"][cid] for cid in chunk_ids})
        uri=date["work"]+"invocations/"+self.manifest["invocation_id"]+"/root-"+str(root)+".json"
        _put(self.client,uri,receipt)
        record=dict(counts=totals,receipt_uri=uri,receipt_fingerprint=_digest(receipt))
        progress=dict(self.manifest["root_progress"],**{str(root):record})
        self.save(state="PARTIAL_CHUNKS",root_progress=progress)
        self.completed_roots=deepcopy(progress);date["roots"][root]=receipt;date["root_uris"][root]=uri
        return receipt

    def finalize_date(self,day,expected_counts):
        date=self._owned_date(day)
        if set(date["roots"])!=set(range(date["status"]["root_chunks"])) or sum(r["input_rows"] for r in date["roots"].values())!=date["status"]["owner_rows"]:
            raise ValueError("Incomplete recovered ownership coverage")
        if date["status"]["state"]=="VALIDATED":
            receipt,_,_=_get(self.client,date["root"]+"metadata/validation.json")
            for key,value in dict(dataset=self.config["dataset"],start_date=day,run_id=self.config["run_id"],
                attempt_id=self.config["attempt_id"],source=self.manifest["source"],builder=self.manifest["builder"],
                schema=self.manifest["schema"],rule_version="v1",validation_status="VALIDATED",
                staging_location=date["root"]+"data/").items():
                if receipt.get(key)!=value:raise ValueError("Final receipt identity changed")
            if (receipt["counts"]!={str(k):v for k,v in expected_counts.items()} or
                inventory_objects(self.client,receipt["staging_location"])!=receipt["files"]):raise ValueError("Final date changed")
            back=self.transport.read(self.spark,receipt["staging_location"],
                [r["path"] for r in receipt["files"] if r["path"].endswith(".parquet")],date["status"])
            validate_sequence_output(back,self.config["dataset"],day,"UTC",{int(k):v for k,v in expected_counts.items()})
            return dict(receipt,receipt_uri=date["root"]+"metadata/validation.json",receipt_fingerprint=_digest(receipt))
        return super().finalize_date(day,expected_counts)


class DurableBudget(ChunkBudget):
    def __init__(self,limits,deadline,stager):
        super().__init__(limits,deadline);self.stager=stager
        for name in ("nodes","chunks","pair_links","output_rows"):setattr(self,name,stager.manifest["budget"].get(name,0))
        for name,cap in (("nodes",limits.max_split_nodes),("chunks",limits.max_chunks),
                         ("pair_links",limits.max_total_pair_links),("output_rows",limits.max_total_output_rows)):
            if not 0<=getattr(self,name)<=cap:raise ValueError("Recovered budget invalid")
    def record(self):self.stager.save(budget={n:getattr(self,n) for n in ("nodes","chunks","pair_links","output_rows")})
    def node(self):super().node();self.record()
    def pairs(self,count):super().pairs(count);self.record()
    def expansion(self,count):super().expansion(count);self.record()


def run_owner_date(config,limits,*,allow_writes,owner_date,root_group_start,root_group_count,invocation_id,
    resume_etag,previous_run_stopped,max_copy_bytes,expected_counts,glue_context,catalog_client,s3_client,identity_client,transport=None,
    group_limits=None):
    if allow_writes is not True:raise ValueError("Explicit staging activation required")
    config=validate_full_config(config);validate_chunk_limits(config,limits);_token(invocation_id)
    if group_limits is not None and (not isinstance(group_limits,dict) or set(group_limits)!={"max_group_input_rows","max_group_pair_work","max_group_output_rows"}
        or any(type(v) is not int or v<=0 for v in group_limits.values())):raise ValueError("Explicit positive batched group budgets required")
    if owner_date not in owner_dates(config["dataset"]):raise ValueError("Approved owner date required")
    if any(type(v) is not int or v<0 for v in (root_group_start,root_group_count)) or root_group_count==0 or root_group_start+root_group_count>limits.root_chunks:
        raise ValueError("Explicit bounded root group required")
    if set(expected_counts)!={owner_date} or set(expected_counts[owner_date])!={"2","3"} or any(type(v) is not int or v<0 for v in expected_counts[owner_date].values()):
        raise ValueError("Exact independent date counts required")
    if limits.max_total_output_rows<sum(expected_counts[owner_date].values()) or limits.max_total_pair_links<expected_counts[owner_date]["2"]:
        raise ValueError("Date cumulative limits cannot cover independent counts")
    if resume_etag is not None and (not isinstance(resume_etag,str) or not resume_etag or previous_run_stopped is not True):raise ValueError("Explicit fenced recovery required")
    spark=glue_context.spark_session
    validate_glue_runtime(glue_version="5.0",spark_version=spark.version,java_version=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    if spark.conf.get("spark.sql.session.timeZone")!="UTC":raise ValueError("UTC required")
    for client in (catalog_client,s3_client,identity_client):
        if client.meta.region_name!="us-east-1":raise ValueError("Wrong region")
    if identity_client.get_caller_identity().get("Account")!=config["account_id"]:raise ValueError("Wrong account")
    validate_s3_conditional_writes(s3_client)
    if "ChecksumSHA256" not in s3_client.meta.service_model.operation_model("PutObject").input_shape.members:raise ValueError("Checksum support required")
    started=time.monotonic()
    def deadline():
        if time.monotonic()-started>=config["max_run_seconds"]:raise TimeoutError("Owner-date invocation deadline")
    def inspect():
        deadline();return inspect_full_source(catalog_client,s3_client,config["dataset"],verified_empty_dates=config["verified_empty_dates"])
    before=inspect()
    if snapshot_identity(before)!=config["expected_source"]:raise ValueError("Frozen source changed")
    plan,=[p for p in before["plans"] if p.start_date.isoformat()==owner_date]
    if not plan.ready_to_process:raise ValueError("Incomplete date plan")
    dates=[d.isoformat() for d in plan.available_input_dates]
    identity=_digest(dict(config=config,limits=limits.__dict__,owner_date=owner_date,expected_counts=expected_counts))
    if group_limits is not None:identity=_digest(dict(identity=identity,group_limits=group_limits,mode="batched_owner_date"))
    manifest=dict(format_version=1,manifest_kind="owner_date_generation_progress",identity=identity,state="CREATED",
        dataset=config["dataset"],owner_dates=[owner_date],source_dates=dates,source=config["expected_source"],
        run_id=config["run_id"],attempt_id=config["attempt_id"],owner_token=config["owner_token"],published=False,catalog_updated=False,
        builder=dict(version=config["builder_version"],fingerprint=config["builder_fingerprint"]),rule_version="v1",
        schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA],batches=[],
        verification_scope="provided_dataframe_only",dataset_wide_verified=False)
    stager_type=OwnerDateStager
    if group_limits is not None:
        from sequence_batched_staging import BatchedDateStager
        stager_type=BatchedDateStager
        manifest["manifest_kind"]="batched_owner_date_generation_progress"
    stager=stager_type(s3_client,config,spark=spark,max_copy_bytes=max_copy_bytes,transport=transport,deadline=deadline)
    stager.open(manifest,invocation_id,resume_etag,previous_run_stopped)
    stager.save(active_root_group=dict(start=root_group_start,count=root_group_count),
        predecessor_stopped_acknowledged=previous_run_stopped)
    source=None;owned=False;error=None;result=None
    try:
        # The existing reader requires exact selected-snapshot coverage. Keep
        # the full fingerprint evidence, restricting only its read inventory.
        selected=dict(before,available_dates=dates,files_by_date={d:before["files_by_date"][d] for d in dates})
        source=read_full_catalog(glue_context,config["dataset"],dates,snapshot=selected,timezone_evidence=config["timezone_evidence"])
        validate_cleaned_schema(source)
        if not spark._jsparkSession.sharedState().cacheManager().lookupCachedData(source._jdf).nonEmpty():
            source.persist(StorageLevel.DISK_ONLY);owned=True
        validate_transactions(source)
        budget=DurableBudget(limits,deadline,stager)
        if group_limits is None:
            result=generate_date_chunks(source,config["dataset"],plan,limits,stager,budget=budget,
                root_group=range(root_group_start,root_group_start+root_group_count),expected_counts=expected_counts[owner_date])
        else:
            from sequence_batched_kernel import generate_root_group
            stager.begin_date(owner_date,limits.root_chunks,source.where(F.col("txn_date")==owner_date).count())
            roots=[r for r in range(root_group_start,root_group_start+root_group_count) if str(r) not in stager.completed_roots]
            if roots:
                def consume(output,counts,root_counts,ownership):
                    # Shared chunk stored ONCE. Actual per-root ownership/counts
                    # are reconciled by the kernel and the atomic group receipt.
                    cid="c"+hashlib.sha256(ownership.encode()).hexdigest()[:60]
                    stager.write_chunk(owner_date,min(roots),cid,output,counts,ownership=ownership)
                result=generate_root_group(source,config["dataset"],plan,limits,roots,budget=budget,consume=consume,**group_limits)
            else:result=dict(state="ALREADY_COMMITTED")
    except Exception as exc:error=exc
    finally:
        if owned:
            try:source.unpersist(blocking=True)
            except Exception as exc:
                if error is None:error=exc
                else:error.add_note("Source cleanup failed: "+type(exc).__name__)
        try:assert_full_source_unchanged(before,inspect())
        except Exception as exc:
            if error is None:error=exc
            else:error.add_note("Final source inspection failed: "+type(exc).__name__)
    if error is not None:
        try:stager.save(state="PARTIAL_CHUNKS",last_error=type(error).__name__)
        except Exception as exc:error.add_note("Progress recording failed: "+type(exc).__name__)
        raise error.with_traceback(error.__traceback__)
    if group_limits is not None:
        # Both kernel caches and source cache have been cleaned, and the final
        # source inspection succeeded. Only now may the group become recoverable.
        if result.get("state")=="GENERATED_NOT_STAGED":stager.commit_group(owner_date,result,
            verify_source=lambda:assert_full_source_unchanged(before,inspect()))
        result=dict(state="PARTIAL_CHUNKS")
        if set(stager.completed_roots)=={str(r) for r in range(limits.root_chunks)}:
            result=stager.finalize_date(owner_date,{int(k):v for k,v in expected_counts[owner_date].items()})
            assert_full_source_unchanged(before,inspect())
    if result.get("validation_status")=="VALIDATED":
        # This is ONE DATE evidence, deliberately not a full-dataset publication handoff.
        batch=dict(result,input_dates=dates,required_input_dates=[d.isoformat() for d in plan.required_input_dates],
            unavailable_after_dataset_end=[d.isoformat() for d in plan.unavailable_after_dataset_end],
            verified_empty_input_dates=[d.isoformat() for d in plan.verified_empty_input_dates])
        stager.save(state="DATE_STAGED_VALIDATED",batches=[batch],row_count=result["row_count"],counts=result["counts"],
            source_after=config["expected_source"])
    else:stager.save(state="PARTIAL_CHUNKS",source_after=config["expected_source"])
    return dict(manifest=deepcopy(stager.manifest),manifest_uri=stager.manifest_uri,etag=stager.etag,published=False)
