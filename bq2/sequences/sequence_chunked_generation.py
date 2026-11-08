"""Bounded full-data chain generation. No sample path or publication calls.

Root ownership is pmod(xxhash64(dataset, A ID), root_chunks). Adaptive ID ranges
partition join sides and triple products without dropping branches. Counts are
checked before expansion; validated output is consumed before caches are freed.
"""
from dataclasses import dataclass, fields
import hashlib
from pyspark import StorageLevel
from pyspark.sql import functions as F
from sequence_batch import _enforce_limit, _sum_products, estimate_three_hop_growth
from bq2_sequence_builder import (_build_two_hop_links_validated,
    build_three_hop_links, build_sequence_output, validate_transactions)
from sequence_staging import SEQUENCE_SCHEMA


@dataclass(frozen=True)
class ChunkLimits:
    root_chunks: int
    max_preceding_rows: int
    max_pair_candidates: int
    max_pair_links: int
    max_projected_triples: int
    max_middle_expansion: int
    max_output_rows: int
    max_total_pair_links: int
    max_total_output_rows: int
    max_chunks: int
    max_split_nodes: int

    def __post_init__(self):
        for f in fields(self):
            if type(getattr(self,f.name)) is not int or getattr(self,f.name)<=0:
                raise ValueError("Explicit positive chunk limit required: "+f.name)
        if self.max_preceding_rows>2147483646:raise ValueError("Preceding-row limit exceeds Spark limit API capacity")


def validate_chunk_limits(config,limits):
    if not isinstance(limits,ChunkLimits):raise ValueError("Explicit chunk limits required")
    for old,new in (("max_pair_candidates","max_pair_candidates"),("max_pair_links","max_pair_links"),
        ("max_projected_triples","max_projected_triples"),("max_triples_per_middle","max_middle_expansion"),
        ("max_output_rows","max_output_rows")):
        if getattr(limits,new)>config["limits"][old]:raise ValueError("Chunk bound exceeds configured resource ceiling: "+old)


def _split(frame, column):
    row=frame.agg(F.min(column).alias("lo"),F.max(column).alias("hi")).first()
    if row.lo is None or row.lo==row.hi:return None
    middle=(row.lo+row.hi)//2  # Python integer arithmetic preserves signed bigint.
    return frame.where(F.col(column)<=middle),frame.where(F.col(column)>middle)


def _following_degrees(following):
    return (following.withColumn("bucket",_hour_bucket())
        .groupBy("dataset","from_bank","from_account","bucket").count()
        .select("dataset",F.col("from_bank").alias("bank"),F.col("from_account").alias("account"),
            F.col("bucket").alias("b_bucket"),F.col("count").alias("successors")))


def restricted_candidates(first, following, following_degrees=None):
    keys=["dataset","bank","account"]
    left=(first.withColumn("bucket",_hour_bucket())
        .groupBy("dataset","to_bank","to_account","bucket").count()
        .select("dataset",F.col("to_bank").alias("bank"),F.col("to_account").alias("account"),
            F.col("bucket").alias("a_bucket"),F.col("count").alias("predecessors")))
    right=_following_degrees(following) if following_degrees is None else following_degrees
    return _sum_products(left.join(right,keys).where(F.col("b_bucket").between(F.col("a_bucket"),F.col("a_bucket")+24)))[0]


def _hour_bucket():
    return F.floor(F.col("txn_ts").cast("bigint").cast("decimal(20,0)")/F.lit(3600)).cast("bigint")


class ChunkBudget:
    """One shared cumulative budget across dates; retries never reset it silently."""
    def __init__(self,limits,deadline):
        self.limits=limits;self.deadline=deadline;self.nodes=0;self.chunks=0
        self.pair_links=0;self.output_rows=0
    def node(self):
        self.deadline();self.nodes+=1
        _enforce_limit("split_nodes",self.nodes,self.limits.max_split_nodes)
    def pairs(self,count):
        _enforce_limit("total_pair_links",self.pair_links+count,self.limits.max_total_pair_links)
        self.pair_links+=count
    def expansion(self,count):
        self.deadline()
        _enforce_limit("total_output_rows",self.output_rows+count,self.limits.max_total_output_rows)
        _enforce_limit("chunks",self.chunks+1,self.limits.max_chunks)
        self.output_rows+=count;self.chunks+=1


def generate_date_chunks(source,dataset,plan,limits,stager,*,budget,selected_root=None,expected_counts=None):
    owned=[];checkpoints={}
    def persist(frame):
        manager=frame.sparkSession._jsparkSession.sharedState().cacheManager()
        if not manager.lookupCachedData(frame._jdf).nonEmpty():
            frame.persist(StorageLevel.DISK_ONLY);owned.append(frame)
        return frame
    try:
        source=persist(source)
        degrees=persist(_following_degrees(source))
        return _generate_date_chunks(source,dataset,plan,limits,stager,budget=budget,
            selected_root=selected_root,expected_counts=expected_counts,following_degrees=degrees,checkpoints=checkpoints)
    finally:
        import sys
        active=sys.exc_info()[1];error=None
        for rdd in list(checkpoints.values()):
            try:rdd.unpersist(True)
            except Exception as exc:
                if error is None:error=exc
        checkpoints.clear()
        for frame in reversed(owned):
            try:frame.unpersist(blocking=True)
            except Exception as exc:
                if error is None:error=exc
        if error is not None:
            if active is not None:active.add_note("Date cache cleanup failed: "+type(error).__name__)
            else:raise error


def _generate_date_chunks(source,dataset,plan,limits,stager,*,budget,selected_root,expected_counts,following_degrees,checkpoints):
    """Source is one complete validated window; stager owns an immutable date.

    Public caller validates source once here. Full-dataset callers additionally
    validate global identities before slicing windows. A selected root is a
    diagnostic partial attempt and CANNOT finalize its date or full run.
    """
    from sequence_processing import plan_start_date_batches
    from sequence_full_source import dataset_spec
    dataset_spec(dataset)
    if not isinstance(limits,ChunkLimits) or budget.limits!=limits:raise ValueError("Matching explicit limits required")
    if source.sparkSession.conf.get("spark.sql.session.timeZone")!="UTC":raise ValueError("UTC required")
    dates=[d.isoformat() for d in plan.available_input_dates]
    rebuilt,=plan_start_date_batches(plan.start_date.isoformat(),plan.start_date.isoformat(),
        dataset_start_date=plan.dataset_start_date.isoformat(),dataset_end_date=plan.dataset_end_date.isoformat(),
        available_partition_dates=dates,verified_empty_dates=[d.isoformat() for d in plan.verified_empty_input_dates])
    if rebuilt!=plan or not plan.ready_to_process:raise ValueError("Invalid/incomplete date plan")
    validate_transactions(source)
    if source.where((F.col("dataset")!=dataset)|~F.col("txn_date").isin(dates)).limit(1).count():
        raise ValueError("Window escapes approved source coverage")
    if selected_root is not None and (type(selected_root) is not int or not 0<=selected_root<limits.root_chunks):
        raise ValueError("Invalid root selector")
    day=plan.start_date.isoformat();owners=source.where(F.col("txn_date")==day)
    owner_rows=owners.count();stager.begin_date(day,limits.root_chunks,owner_rows)
    totals={2:0,3:0};root_receipts=[]
    def cached(frame):
        # Cache alone preserves nested SQL lineage and can exhaust driver heap
        # during AQE plan rendering. Checkpoint ONLY bounded link/output frames.
        # Local checkpoint loss fails the run; no external/source writes occur.
        value=frame.localCheckpoint(eager=False)
        rdd=value._jdf.queryExecution().logical().rdd()
        checkpoints[id(value)]=rdd
        try:value.count()
        except Exception:
            release_cached(value);raise
        return value
    def release_cached(frame):
        import sys
        active=sys.exc_info()[1];rdd=checkpoints.get(id(frame))
        if rdd is not None:
            try:rdd.unpersist(True);checkpoints.pop(id(frame),None)
            except Exception as cleanup:
                if active is None:raise
                active.add_note("Owned checkpoint cleanup failed: "+type(cleanup).__name__)
    def links(first,following,path):
        budget.node()
        bound=restricted_candidates(first,following,following_degrees if following is source else None)
        cap=min(limits.max_pair_candidates,limits.max_pair_links,limits.max_output_rows)
        if not bound:return
        first_count=first.limit(limits.max_preceding_rows+1).count()
        if bound>cap or first_count>limits.max_preceding_rows:
            split=_split(first,"txn_id")
            if split is not None:
                for i,part in enumerate(split):yield from links(part,following,path+"a"+str(i))
                return
            split=_split(following,"txn_id")
            if split is None:raise ValueError("Unsplittable candidate bound exceeds positive limit")
            for i,part in enumerate(split):yield from links(first,part,path+"b"+str(i))
            return
        result=cached(_build_two_hop_links_validated(source,first_txn_ids=first.select("dataset","txn_id"),
            second_txn_ids=following.select("dataset","txn_id"),_hourly=True))
        try:
            count=result.count()
            _enforce_limit("pair_links",count,limits.max_pair_links)
            budget.pairs(count)
            if count:yield result,count,path
        finally:release_cached(result)

    def emit(pairs,triples,counts,path,root):
        # Caller has projected/enforced counts before materializing triples.
        ids=(pairs.select("dataset",F.col("first_txn_id").alias("txn_id"))
            .unionByName(pairs.select("dataset",F.col("second_txn_id").alias("txn_id"))))
        if counts[3]:
            for column in ("first_txn_id","second_txn_id","third_txn_id"):
                ids=ids.unionByName(triples.select("dataset",F.col(column).alias("txn_id")))
        members=source.join(ids,["dataset","txn_id"],"left_semi")
        output=cached(build_sequence_output(members,pairs,triples))
        try:
            chunk_id="c"+hashlib.sha256(path.encode()).hexdigest()[:60]
            receipt=stager.write_chunk(day,root,chunk_id,output,counts,ownership=path)
            for length in totals:totals[length]+=counts[length]
            return receipt
        finally:release_cached(output)

    def triples_for(starting,continuations,path,root):
        budget.node()
        projected,largest=estimate_three_hop_growth(starting,continuations)
        if not projected:return []
        if projected>min(limits.max_projected_triples,limits.max_output_rows) or largest>limits.max_middle_expansion:
            split=_split(starting,"first_txn_id")
            if split is not None:
                return sum((triples_for(part,continuations,path+"x"+str(i),root) for i,part in enumerate(split)),[])
            for column in ("first_txn_id","second_txn_id"):
                split=_split(continuations,column)
                if split is not None:
                    return sum((triples_for(starting,part,path+"y"+column+str(i),root) for i,part in enumerate(split)),[])
            raise ValueError("Unsplittable triple exceeds positive limit")
        budget.expansion(projected)  # All guards precede build_three_hop_links.
        triples=cached(build_three_hop_links(continuations,starting_links=starting))
        try:
            if triples.count()!=projected:raise ValueError("Triple projection disagreement")
            empty=pairs_empty(source)
            return [emit(empty,triples,{2:0,3:projected},path+"t",root)]
        finally:release_cached(triples)

    for root in (range(limits.root_chunks) if selected_root is None else [selected_root]):
        budget.deadline();before=dict(totals);receipts=[]
        first=owners.where(F.pmod(F.xxhash64("dataset","txn_id"),F.lit(limits.root_chunks))==root)
        input_rows=first.count()
        for pairs,count,path in links(first,source,"root"+str(root)):
            budget.expansion(count)
            empty=source.sparkSession.createDataFrame([],"dataset string, first_txn_id long, second_txn_id long, third_txn_id long")
            receipts.append(emit(pairs,empty,{2:count,3:0},path+"p",root))
            middle_ids=pairs.select("dataset",F.col("second_txn_id").alias("txn_id"))
            reached=source.join(middle_ids,["dataset","txn_id"],"left_semi")
            for continuation,_,branch in links(reached,source,path+"c"):
                receipts.extend(triples_for(pairs,continuation,branch,root))
        root_counts={k:totals[k]-before[k] for k in totals}
        root_receipts.append(stager.complete_root(day,root,input_rows,root_counts,[r["chunk_id"] for r in receipts]))
    if selected_root is None:
        if expected_counts is not None and totals!={int(k):v for k,v in expected_counts.items()}:
            raise ValueError("Independent expected date counts disagree")
        return stager.finalize_date(day,totals)
    return dict(state="PARTIAL_CHUNKS",start_date=day,counts={str(k):v for k,v in totals.items()},roots=root_receipts,published=False)


def pairs_empty(source):
    return source.sparkSession.createDataFrame([],"dataset string, first_txn_id long, second_txn_id long")


def run_chunked_generation(config,chunk_limits,*,allow_writes=False,mode,owner_date=None,root_index=None,
                           max_copy_bytes,expected_counts,glue_context,catalog_client,s3_client,identity_client,transport=None):
    """Complete full dataset handoff or explicitly incomplete isolated root check.

    Publication is deliberately not invoked. Only full_dataset may call finish;
    single_chunk records PARTIAL_CHUNKS and cannot be consumed by publication.
    """
    import time
    from copy import deepcopy
    from sequence_full_generation import validate_full_config,profile_full_source
    from sequence_full_source import inspect_full_source,snapshot_identity,assert_full_source_unchanged,read_full_catalog,owner_dates
    from sequence_chunked_staging import ChunkS3Stager
    from sequence_glue_runtime import validate_s3_conditional_writes,validate_glue_runtime
    from sequence_source import CLEANED_SCHEMA,_check_schema,validate_cleaned_schema,build_partition_predicate
    from pyspark.sql.types import StructType
    if allow_writes is not True:raise ValueError("Explicit staging activation required")
    config=validate_full_config(config)
    validate_chunk_limits(config,chunk_limits)
    if not isinstance(chunk_limits,ChunkLimits) or mode not in ("single_chunk","full_dataset"):
        raise ValueError("Explicit execution mode and chunk limits required")
    if type(max_copy_bytes) is not int or max_copy_bytes<=0:raise ValueError("Positive copy-byte bound required")
    days=list(owner_dates(config["dataset"]))
    if mode=="single_chunk":
        if owner_date not in days or type(root_index) is not int or not 0<=root_index<chunk_limits.root_chunks:
            raise ValueError("Explicit owned date/root required")
    elif owner_date is not None or root_index is not None:raise ValueError("Full dataset cannot restrict ownership")
    if not isinstance(expected_counts,dict) or not set(expected_counts)<=set(days):raise ValueError("Invalid expected date coverage")
    for value in expected_counts.values():
        if set(value)!={"2","3"} or any(type(n) is not int or n<0 for n in value.values()):raise ValueError("Invalid independent expected counts")
    spark=glue_context.spark_session
    validate_glue_runtime(glue_version="5.0",spark_version=spark.version,
        java_version=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    if spark.conf.get("spark.sql.session.timeZone")!="UTC":raise ValueError("UTC required")
    for client in (identity_client,catalog_client,s3_client):
        if client.meta.region_name!="us-east-1":raise ValueError("Wrong AWS client region")
    if identity_client.get_caller_identity().get("Account")!=config["account_id"]:raise ValueError("Wrong AWS account")
    validate_s3_conditional_writes(s3_client)
    if "ChecksumSHA256" not in s3_client.meta.service_model.operation_model("PutObject").input_shape.members:
        raise ValueError("SDK SHA-256 checksum support required")
    started=time.monotonic()
    def deadline():
        if time.monotonic()-started>=config["max_run_seconds"]:raise TimeoutError("Chunked run elapsed budget exhausted")
    def inspect():
        deadline();return inspect_full_source(catalog_client,s3_client,config["dataset"],verified_empty_dates=config["verified_empty_dates"])
    before=inspect()
    if snapshot_identity(before)!=config["expected_source"]:raise ValueError("Frozen source fingerprints disagree")
    if [p.start_date.isoformat() for p in before["plans"]]!=days or any(not p.ready_to_process for p in before["plans"]):
        raise ValueError("Incomplete full date coverage")
    selected_plans=before["plans"] if mode=="full_dataset" else [p for p in before["plans"] if p.start_date.isoformat()==owner_date]
    manifest=dict(format_version=1,manifest_kind="full_sequence_generation_handoff",state="CREATED",published=False,
        catalog_updated=False,publication_required=True,dataset=config["dataset"],run_id=config["run_id"],attempt_id=config["attempt_id"],
        owner_token=config["owner_token"],owner_dates=days,source_dates=days,source=config["expected_source"],
        schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA],rule_version="v1",
        builder=dict(version=config["builder_version"],fingerprint=config["builder_fingerprint"]),batches=[],execution_mode=mode,
        chunk_limits={f.name:getattr(chunk_limits,f.name) for f in fields(chunk_limits)})
    stager=ChunkS3Stager(s3_client,config,spark=spark,max_copy_bytes=max_copy_bytes,transport=transport,deadline=deadline)
    stager.reserve(manifest)
    source=None;owned=False;error=None;partials=[];budget=ChunkBudget(chunk_limits,deadline)
    try:
        if mode=="full_dataset":
            source=read_full_catalog(glue_context,config["dataset"],before["available_dates"],snapshot=before,timezone_evidence=config["timezone_evidence"])
        else:
            dates=[d.isoformat() for d in selected_plans[0].available_input_dates]
            session=spark.newSession();session.conf.set("spark.sql.session.timeZone","UTC")
            for day in dates:
                for uri in before["files_by_date"][day]:
                    _check_schema(session.read.option("recursiveFileLookup","true").parquet(uri).schema,StructType(CLEANED_SCHEMA.fields[:-2]))
            source=glue_context.create_dynamic_frame.from_catalog(database="is459_g2t1_aml",table_name="transactions_clean",
                push_down_predicate=build_partition_predicate(config["dataset"],dates)).toDF()
            if len(source.columns)!=20 or set(source.columns)!=set(CLEANED_SCHEMA.fieldNames()):raise ValueError("Wrong cleaned schema")
            source=source.select(*CLEANED_SCHEMA.fieldNames());validate_cleaned_schema(source)
        manager=spark._jsparkSession.sharedState().cacheManager()
        if not manager.lookupCachedData(source._jdf).nonEmpty():source.persist(StorageLevel.DISK_ONLY);owned=True
        if mode=="full_dataset":manifest["source_validation"]=profile_full_source(source,config["dataset"],before)
        else:validate_transactions(source)
        for plan in selected_plans:
            deadline();day=plan.start_date.isoformat();dates=[d.isoformat() for d in plan.available_input_dates]
            receipt=generate_date_chunks(source.where(F.col("txn_date").isin(dates)),config["dataset"],plan,chunk_limits,stager,
                budget=budget,selected_root=root_index if mode=="single_chunk" else None,expected_counts=expected_counts.get(day))
            if mode=="full_dataset":manifest["batches"].append(dict(receipt,input_dates=dates,
                required_input_dates=[d.isoformat() for d in plan.required_input_dates],
                verified_empty_input_dates=[d.isoformat() for d in plan.verified_empty_input_dates],
                unavailable_after_dataset_end=[d.isoformat() for d in plan.unavailable_after_dataset_end]))
            else:partials.append(receipt)
            assert_full_source_unchanged(before,inspect())
    except Exception as exc:error=exc
    finally:
        if owned:
            try:source.unpersist(blocking=True)
            except Exception as cleanup:
                if error is None:error=cleanup
                else:error.add_note("Chunked source cleanup failed: "+type(cleanup).__name__)
        try:assert_full_source_unchanged(before,inspect())
        except Exception as changed:
            if error is None:error=changed
            else:error.add_note("Final source inventory failed: "+type(changed).__name__)
    if error is not None:
        try:stager.fail(error)
        except Exception as recording:error.add_note("Failure receipt unavailable: "+type(recording).__name__)
        raise error.with_traceback(error.__traceback__)
    manifest.update(source_after=config["expected_source"],elapsed_seconds=round(time.monotonic()-started,3),
        chunk_metrics=dict(split_nodes=budget.nodes,materialized_pair_links_work=budget.pair_links,
            validated_output_rows=budget.output_rows,chunks=budget.chunks))
    if mode=="single_chunk":
        manifest.update(state="PARTIAL_CHUNKS",partial_roots=partials,verification_scope="provided_dataframe_only",dataset_wide_verified=False)
        stager._write_run(manifest)
        return dict(manifest=manifest,manifest_uri=stager.root+"metadata/run.json",published=False)
    if [b["start_date"] for b in manifest["batches"]]!=days:raise ValueError("Full owner coverage not complete")
    counts={str(k):sum(b["counts"][str(k)] for b in manifest["batches"]) for k in (2,3)}
    manifest.update(state="STAGED_VALIDATED",counts=counts,row_count=sum(counts.values()),
        parquet_files=sum(b["parquet_files"] for b in manifest["batches"]),size_bytes=sum(b["size_bytes"] for b in manifest["batches"]))
    return stager.finish(deepcopy(manifest))
