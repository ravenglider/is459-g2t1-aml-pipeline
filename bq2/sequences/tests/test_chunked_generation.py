"""Real Spark equivalence and Parquet staging; fake S3/Glue only, never AWS."""
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock,patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sequence_chunked_generation as generation
from sequence_chunked_generation import ChunkLimits,ChunkBudget,generate_date_chunks
from sequence_chunked_staging import ChunkS3Stager
import glue_chunked_generation as entry
import package_chunked_generation as package
from sequence_processing import plan_start_date_batches
from sequence_source import CLEANED_SCHEMA
from sequence_staging import SEQUENCE_SCHEMA
from bq2_sequence_builder import build_two_hop_links,build_three_hop_links,build_sequence_output
from fixtures import transaction
from test_full_generation import config
from test_full_publication import PublicationS3,PublicationTransport


def limits(**changes):
    return ChunkLimits(**dict(dict(root_chunks=1,max_preceding_rows=100,max_pair_candidates=10,max_pair_links=10,max_projected_triples=10,
        max_middle_expansion=10,max_output_rows=10,max_total_pair_links=1000,max_total_output_rows=1000,
        max_chunks=100,max_split_nodes=1000),**changes))


def args(mode="single_chunk"):
    result=["--DATASET","HI-Medium","--CONFIG_JSON",json.dumps(config()),"--CHUNK_LIMITS_JSON",json.dumps(limits().__dict__),
        "--MODE",mode,"--MAX_COPY_BYTES","1000000","--ALLOW_AWS_WRITES","true"]
    if mode=="single_chunk":result += ["--OWNER_DATE","2022-09-01","--ROOT_INDEX","0"]
    return result


class ProtocolTests(unittest.TestCase):
    def test_owner_date_cli_requires_bounded_group_counts_and_recovery_evidence(self):
        valid=args("single_owner_date")+["--OWNER_DATE","2022-09-01","--ROOT_GROUP_START","0","--ROOT_GROUP_COUNT","1",
            "--INVOCATION_ID","invoke01","--EXPECTED_COUNTS_JSON",json.dumps({"2022-09-01":{"2":4,"3":8}})]
        entry.validate_arguments(entry.parse_arguments(valid))
        for key,value in (("ROOT_GROUP_START","-1"),("ROOT_GROUP_COUNT","0"),("INVOCATION_ID","../bad"),
                          ("EXPECTED_COUNTS_JSON","{}"),("OWNER_DATE","2022-09-29")):
            bad=list(valid);bad[bad.index("--"+key)+1]=value
            with patch.object(entry,"_initialize") as initialize,self.assertRaises((ValueError,SystemExit)):
                entry.main(bad)
            initialize.assert_not_called()
        with patch.object(entry,"_initialize") as initialize,self.assertRaises(ValueError):
            entry.main(valid+["--RESUME_ETAG",'"old"'])
        initialize.assert_not_called()
        entry.validate_arguments(entry.parse_arguments(valid+["--RESUME_ETAG",'"old"',"--PREVIOUS_RUN_STOPPED","true"]))

    def test_limits_and_cli_fail_before_runtime(self):
        for field in limits().__dict__:
            for value in (0,-1,True):
                with self.assertRaises(ValueError):limits(**{field:value})
        variants=[args()+["--ALLOW_PUBLICATION","true"],args()+["--conf","unsafe"],args()+["--DATASET","LI-Medium"]]
        for key,value in (("ALLOW_AWS_WRITES","false"),("ROOT_INDEX","999"),("MAX_COPY_BYTES","0"),("OWNER_DATE","2022-08-01")):
            a=args();a[a.index("--"+key)+1]=value;variants.append(a)
        with patch.object(entry,"_initialize") as initialize,patch("sys.stdout"),patch("sys.stderr"):
            for a in variants:
                with self.assertRaises((ValueError,SystemExit)):entry.main(a)
            initialize.assert_not_called()
        entry.validate_arguments(entry.parse_arguments(args("full_dataset")))

    def test_partial_result_remains_partial_and_errors_propagate(self):
        result=dict(manifest=dict(state="PARTIAL_CHUNKS"),manifest_uri="staged")
        with patch.object(entry,"_initialize",return_value={}),patch.object(entry,"run_chunked_generation",return_value=result),patch("sys.stdout",new_callable=io.StringIO) as out:
            self.assertEqual(entry.main(args()),0)
        self.assertIn("PARTIAL_CHUNKS",out.getvalue());self.assertIn('"published": false',out.getvalue())
        with patch.object(entry,"_initialize",return_value={}),patch.object(entry,"run_chunked_generation",side_effect=ValueError("incomplete")),self.assertRaises(ValueError):entry.main(args())

    def test_separate_package_deterministic_and_import_safe(self):
        import subprocess
        with tempfile.TemporaryDirectory() as folder:
            a,b=Path(folder)/"a.zip",Path(folder)/"b.zip"
            self.assertEqual(package.build_package(a),package.build_package(b))
            with self.assertRaises(FileExistsError):package.build_package(a)
            import zipfile
            with zipfile.ZipFile(a) as z:
                self.assertEqual(len(z.namelist()),22);self.assertIsNone(z.testzip())
            code="""import importlib,importlib.abc,importlib.util,sys,socket,zipfile
from unittest.mock import patch
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'boto3','botocore','awsglue'}:raise AssertionError('cloud import')
sys.meta_path.insert(0,Block())
def denied(*a,**k):raise AssertionError('startup forbidden')
with patch.object(socket.socket,'connect',denied):
 from pyspark import SparkContext
 from pyspark.sql import SparkSession
 sys.path.insert(0,sys.argv[1])
 with patch.object(SparkContext,'__init__',denied),patch.object(SparkSession.Builder,'getOrCreate',denied):
  for name in zipfile.ZipFile(sys.argv[1]).namelist():importlib.import_module(name[:-3])
  spec=importlib.util.spec_from_file_location('entry',sys.argv[2]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
"""
            r=subprocess.run([sys.executable,"-I","-B","-c",code,str(a),str(Path(entry.__file__).resolve())],cwd=folder,capture_output=True,text=True)
            self.assertEqual(r.returncode,0,r.stderr)


class OwnerProtocolTests(unittest.TestCase):
    def store(self,client=None,day="2022-09-01",invoke="invoke01",etag=None,stopped=False,identity="frozen"):
        from sequence_owner_date import OwnerDateStager
        client=client or PublicationS3()
        store=OwnerDateStager(client,config(),spark=MagicMock(),max_copy_bytes=1000000)
        manifest=dict(manifest_kind="owner_date_generation_progress",identity=identity,state="CREATED",
            owner_dates=[day],source=config()["expected_source"],builder={},schema=[],rule_version="v1",published=False)
        store.open(manifest,invoke,etag,stopped)
        return store

    def test_independent_dates_have_disjoint_progress_and_no_catalog(self):
        first=self.store();second=self.store(first.client,day="2022-09-02")
        first.begin_date("2022-09-01",1,0);second.begin_date("2022-09-02",1,0)
        first.complete_root("2022-09-01",0,0,{2:0,3:0},[])
        second.complete_root("2022-09-02",0,0,{2:0,3:0},[])
        first._assert_owner();second._assert_owner()
        self.assertNotEqual(first.manifest_uri,second.manifest_uri)
        self.assertFalse(any(k.startswith("bq2_sequences/") for k in first.client.objects))

    def test_recovery_requires_stopped_fresh_invocation_exact_etag_identity(self):
        first=self.store()
        for options in (dict(stopped=False),dict(etag='"stale"',stopped=True),dict(stopped=True,identity="changed"),dict(stopped=True,invoke="invoke01")):
            kwargs=dict(client=first.client,invoke="invoke02",etag=first.etag,stopped=True);kwargs.update(options)
            with self.assertRaises(ValueError):self.store(**kwargs)
        second=self.store(first.client,invoke="invoke02",etag=first.etag,stopped=True)
        with self.assertRaises(ValueError):first.save(state="PARTIAL_CHUNKS")
        with self.assertRaises(ValueError):second.save(state="DATE_STAGED_VALIDATED")
        # Simulate a stored terminal record; recovery must never reopen it.
        second._write_run(dict(second.manifest,state="DATE_STAGED_VALIDATED"))
        with self.assertRaises(ValueError):second.save(state="PARTIAL_CHUNKS")
        with self.assertRaises(ValueError):self.store(second.client,invoke="invoke03",etag=second.etag,stopped=True)

    def test_tampered_completed_root_receipt_rejected(self):
        first=self.store();first.begin_date("2022-09-01",1,0)
        first.complete_root("2022-09-01",0,0,{2:0,3:0},[])
        record=first.completed_roots["0"]
        first.client.seed(record["receipt_uri"].split("/",3)[3],b'{}')
        second=self.store(first.client,invoke="invoke02",etag=first.etag,stopped=True)
        with self.assertRaisesRegex(ValueError,"Root receipt changed"):second.begin_date("2022-09-01",1,0)

    def test_durable_budget_lost_response_reconciliation_and_limit(self):
        from sequence_owner_date import DurableBudget
        first=self.store();budget=DurableBudget(limits(max_total_output_rows=2),lambda:None,first)
        first.client.lose_response=True
        budget.expansion(2)
        second=self.store(first.client,invoke="invoke02",etag=first.etag,stopped=True)
        recovered=DurableBudget(limits(max_total_output_rows=2),lambda:None,second)
        self.assertEqual(recovered.output_rows,2)
        with self.assertRaises(Exception):recovered.expansion(1)


class MemoryStager:
    def __init__(self):self.rows=[];self.roots={};self.final=False;self.chunks=[]
    def begin_date(self,day,roots,owner_rows):self.expected_roots=roots;self.owner_rows=owner_rows
    def write_chunk(self,day,root,cid,output,counts,ownership):
        self.rows.extend(output.collect());self.chunks.append(cid)
        return dict(chunk_id=cid,counts=counts)
    def complete_root(self,day,root,input_rows,counts,cids):
        self.roots[root]=dict(input_rows=input_rows,counts=counts);return self.roots[root]
    def finalize_date(self,day,counts):
        assert set(self.roots)==set(range(self.expected_roots))
        assert sum(r["input_rows"] for r in self.roots.values())==self.owner_rows
        assert len(self.chunks)==len(set(self.chunks))
        self.final=True;return dict(start_date=day,counts=counts,state="VALIDATED")


class ChunkSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import time
        from pyspark.sql import SparkSession
        oldtz=os.environ.get("TZ");oldpy=os.environ.get("PYSPARK_PYTHON")
        os.environ["TZ"]="UTC";os.environ["PYSPARK_PYTHON"]=sys.executable
        if hasattr(time,"tzset"):time.tzset()
        def restore():
            for key,old in (("TZ",oldtz),("PYSPARK_PYTHON",oldpy)):
                if old is None:os.environ.pop(key,None)
                else:os.environ[key]=old
            if hasattr(time,"tzset"):time.tzset()
        cls.addClassCleanup(restore)
        cls.spark=(SparkSession.builder.master("local[2]").appName("chunked-full-offline")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2")
            .config("spark.ui.enabled","false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop);cls.spark.sparkContext.setLogLevel("ERROR")
        cls.rows=[transaction(-9,"2022-09-01 00:00",(1,"A"),(2,"B"),dataset="HI-Medium",label=1),
            transaction(9007199254740993,"2022-09-01 00:00",(1,"X"),(2,"B"),dataset="HI-Medium"),
            transaction(3,"2022-09-02 00:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
            transaction(4,"2022-09-02 00:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
            transaction(5,"2022-09-03 00:00",(2,"C"),(3,"D"),dataset="HI-Medium"),
            transaction(6,"2022-09-03 00:00",(2,"C"),(3,"E"),dataset="HI-Medium")]
        cls.frame=cls.spark.createDataFrame(cls.rows,CLEANED_SCHEMA)
        cls.plan,=plan_start_date_batches("2022-09-01","2022-09-01",dataset_start_date="2022-09-01",dataset_end_date="2022-09-28",available_partition_dates=["2022-09-01","2022-09-02","2022-09-03"])
    def canonical(self,rows):return sorted(json.dumps(r.asDict(),sort_keys=True,default=str) for r in rows)
    def unbatched(self,frame=None):
        frame=self.frame if frame is None else frame
        from pyspark.sql import functions as F
        pairs=build_two_hop_links(frame)
        ids=frame.where(F.col("txn_date")=="2022-09-01").select("dataset",F.col("txn_id").alias("first_txn_id"))
        starts=pairs.join(ids,["dataset","first_txn_id"],"left_semi")
        triples=build_three_hop_links(pairs,starting_links=starts)
        return build_sequence_output(frame,starts,triples)
    def check_clean(self):self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)

    def owner_stager(self,folder,client=None,invocation="invoke01",resume=None,identity="frozen"):
        from sequence_owner_date import OwnerDateStager
        client=client or PublicationS3()
        stager=OwnerDateStager(client,config(),spark=self.spark,max_copy_bytes=1000000,
            transport=PublicationTransport(folder,client))
        manifest=dict(manifest_kind="owner_date_generation_progress",identity=identity,state="CREATED",
            owner_dates=["2022-09-01"],source=config()["expected_source"],schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA],
            builder=dict(version="test",fingerprint="a"*64),rule_version="v1",published=False)
        stager.open(manifest,invocation,resume,resume is not None)
        return stager

    def test_owner_date_group_recovery_all_columns_and_fenced_old_writer(self):
        from sequence_owner_date import DurableBudget
        lim=limits(root_chunks=2,max_pair_candidates=20,max_pair_links=20,max_output_rows=20)
        with tempfile.TemporaryDirectory() as folder:
            first=self.owner_stager(folder)
            part=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,first,
                budget=DurableBudget(lim,lambda:None,first),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(part["state"],"PARTIAL_CHUNKS")
            oldetag=first.etag;oldkeys=set(first.client.objects)
            second=self.owner_stager(folder,first.client,"invoke02",oldetag)
            with self.assertRaises(ValueError):first.save(state="PARTIAL_CHUNKS")
            with self.assertRaises(ValueError):self.owner_stager(folder,first.client,"invoke03",oldetag)
            done=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,second,
                budget=DurableBudget(lim,lambda:None,second),root_group=range(2),expected_counts={"2":4,"3":8})
            self.assertEqual(done["row_count"],12);self.assertEqual(len(second.completed_roots),2)
            self.assertTrue(oldkeys<=set(first.client.objects))
            back=second.transport.read(self.spark,done["staging_location"],[r["path"] for r in done["files"]],{"timezone":"UTC"})
            self.assertEqual(self.canonical(back.collect()),self.canonical(self.unbatched().collect()))
            self.assertEqual(done["validation_status"],"VALIDATED");self.assertEqual(len(done["schema"]),21)
        self.check_clean()

    def test_owner_date_interrupted_chunk_isolated_and_budget_preserved(self):
        from sequence_owner_date import DurableBudget
        lim=limits(root_chunks=1,max_pair_candidates=20,max_pair_links=20,max_output_rows=20)
        with tempfile.TemporaryDirectory() as folder:
            first=self.owner_stager(folder)
            original=first.write_chunk
            def interrupted(*a,**k):original(*a,**k);raise TimeoutError("after validated orphan chunk")
            first.write_chunk=interrupted
            with self.assertRaises(TimeoutError):generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,first,
                budget=DurableBudget(lim,lambda:None,first),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(first.completed_roots,{})
            used=first.manifest["budget"]["output_rows"]
            second=self.owner_stager(folder,first.client,"invoke02",first.etag)
            self.assertEqual(second.manifest["budget"]["output_rows"],used)
            done=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,second,
                budget=DurableBudget(lim,lambda:None,second),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(done["row_count"],12)
            self.assertTrue(any("/invoke01/" in k for k in first.client.objects))
            self.assertTrue(all("invoke01" not in r["path"] for r in done["files"]))
        self.check_clean()

    def test_owner_date_count_mismatch_cannot_finalize(self):
        from sequence_owner_date import DurableBudget
        lim=limits(root_chunks=1,max_pair_candidates=20,max_pair_links=20,max_output_rows=20)
        with tempfile.TemporaryDirectory() as folder:
            stager=self.owner_stager(folder)
            with self.assertRaisesRegex(ValueError,"Independent expected"):
                generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,stager,
                    budget=DurableBudget(lim,lambda:None,stager),root_group=range(1),expected_counts={"2":4,"3":9})
            self.assertEqual(stager.manifest["state"],"PARTIAL_CHUNKS")
        self.check_clean()

    def test_owner_date_partial_final_copy_recovers_without_overwriting(self):
        from sequence_owner_date import DurableBudget
        lim=limits(root_chunks=1,max_pair_candidates=20,max_pair_links=20,max_output_rows=20)
        with tempfile.TemporaryDirectory() as folder:
            first=self.owner_stager(folder);put=first.client.put_object;calls=[]
            def fail_copy(**kw):
                if "/batches/" in kw["Key"] and kw["Key"].endswith(".parquet"):
                    calls.append(kw["Key"])
                    if len(calls)==2:raise TimeoutError("interrupted final copy")
                return put(**kw)
            first.client.put_object=fail_copy
            with self.assertRaises(TimeoutError):generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,first,
                budget=DurableBudget(lim,lambda:None,first),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(len(first.completed_roots),1)
            retained=deepcopy(first.client.objects[calls[0]])
            first.client.put_object=put
            second=self.owner_stager(folder,first.client,"invoke02",first.etag)
            done=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,second,
                budget=DurableBudget(lim,lambda:None,second),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(done["row_count"],12);self.assertEqual(first.client.objects[calls[0]],retained)
            self.assertEqual(second.dates["2022-09-01"]["status"]["revision"],2)
        self.check_clean()

    def test_owner_date_validating_receipt_failure_recovers_and_empty_date(self):
        from sequence_owner_date import DurableBudget
        lim=limits(root_chunks=1,max_pair_candidates=20,max_pair_links=20,max_output_rows=20)
        with tempfile.TemporaryDirectory() as folder:
            first=self.owner_stager(folder);put=first.client.put_object
            def fail_receipt(**kw):
                if "/batches/" in kw["Key"] and kw["Key"].endswith("metadata/validation.json"):
                    raise TimeoutError("receipt not committed")
                return put(**kw)
            first.client.put_object=fail_receipt
            with self.assertRaises(TimeoutError):generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,first,
                budget=DurableBudget(lim,lambda:None,first),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(first.dates["2022-09-01"]["status"]["state"],"VALIDATING")
            first.client.put_object=put
            second=self.owner_stager(folder,first.client,"invoke02",first.etag)
            done=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,second,
                budget=DurableBudget(lim,lambda:None,second),root_group=range(1),expected_counts={"2":4,"3":8})
            self.assertEqual(done["row_count"],12);self.assertEqual(second.dates["2022-09-01"]["status"]["revision"],2)
        with tempfile.TemporaryDirectory() as folder:
            empty=self.owner_stager(folder)
            done=generate_date_chunks(self.frame.limit(0),"HI-Medium",self.plan,lim,empty,
                budget=DurableBudget(lim,lambda:None,empty),root_group=range(1),expected_counts={"2":0,"3":0})
            self.assertEqual(done["row_count"],0);self.assertEqual(done["files"],[])
        self.check_clean()

    def test_owner_date_coordinator_actual_generation_and_final_source_gate(self):
        import sequence_owner_date as owner
        from test_full_generation import snapshot
        context=MagicMock();context.spark_session=self.spark
        identity=MagicMock();identity.meta.region_name="us-east-1";identity.get_caller_identity.return_value={"Account":"123456789012"}
        catalog=MagicMock();catalog.meta.region_name="us-east-1"
        lim=limits(root_chunks=1,max_pair_candidates=20,max_pair_links=20,max_output_rows=20)
        for changed in (False,True):
            with tempfile.TemporaryDirectory() as folder:
                client=PublicationS3();transport=PublicationTransport(folder,client)
                with patch.object(owner,"inspect_full_source",return_value=snapshot()) as inspect, \
                     patch.object(owner,"read_full_catalog",return_value=self.frame) as read, \
                     patch.object(owner,"assert_full_source_unchanged",side_effect=ValueError("source changed") if changed else None):
                    kwargs=dict(allow_writes=True,mode="single_owner_date",owner_date="2022-09-01",root_group_start=0,
                        root_group_count=1,invocation_id="invoke01",max_copy_bytes=1000000,expected_counts={"2022-09-01":{"2":4,"3":8}},
                        glue_context=context,catalog_client=catalog,s3_client=client,identity_client=identity,transport=transport)
                    if changed:
                        with self.assertRaisesRegex(ValueError,"source changed"):generation.run_chunked_generation(config(),lim,**kwargs)
                    else:
                        result=generation.run_chunked_generation(config(),lim,**kwargs)
                        self.assertEqual(result["manifest"]["state"],"DATE_STAGED_VALIDATED")
                        self.assertEqual(result["manifest"]["row_count"],12)
                        self.assertFalse(result["manifest"]["dataset_wide_verified"])
                    read.assert_called_once();self.assertEqual(read.call_args.args[2],["2022-09-01","2022-09-02","2022-09-03"])
                    self.assertEqual(inspect.call_count,2)
                    states=[json.loads(v["raw"])["state"] for k,v in client.objects.items() if k.endswith("/run.json")]
                    self.assertEqual(states,["PARTIAL_CHUNKS"] if changed else ["DATE_STAGED_VALIDATED"])
                    self.assertFalse(any(k.startswith("bq2_sequences/") for k in client.objects))
            self.check_clean()

    def test_all_columns_equivalence_hot_a_hot_middle_boundaries_bigints(self):
        stager=MemoryStager();lim=limits(max_pair_candidates=2,max_pair_links=2,max_middle_expansion=1,max_output_rows=2,max_projected_triples=2)
        result=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,stager,budget=ChunkBudget(lim,lambda:None),expected_counts={"2":4,"3":8})
        self.assertEqual(result["counts"],{2:4,3:8});self.assertTrue(stager.final)
        self.assertEqual(self.canonical(stager.rows),self.canonical(self.unbatched().collect()))
        self.assertEqual(len(stager.rows[0]),21);self.assertEqual(len({r.sequence_id for r in stager.rows}),12)
        self.assertGreater(len(stager.chunks),3);self.check_clean()

    def test_adjacent_root_ownership_and_partial_not_finalized(self):
        lim=limits(root_chunks=2)
        left,right=MemoryStager(),MemoryStager()
        for root,stager in enumerate((left,right)):
            result=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,stager,budget=ChunkBudget(lim,lambda:None),selected_root=root)
            self.assertEqual(result["state"],"PARTIAL_CHUNKS");self.assertFalse(stager.final)
        self.assertFalse({r.sequence_id for r in left.rows}&{r.sequence_id for r in right.rows})
        self.assertEqual(self.canonical(left.rows+right.rows),self.canonical(self.unbatched().collect()));self.check_clean()

    def test_cumulative_guard_before_triples_and_empty(self):
        lim=limits(max_total_output_rows=1)
        with patch.object(generation,"build_three_hop_links",side_effect=AssertionError("must not expand")) as triple,self.assertRaises(ValueError):
            generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,MemoryStager(),budget=ChunkBudget(lim,lambda:None))
        triple.assert_not_called();self.check_clean()
        lim=limits();stager=MemoryStager()
        result=generate_date_chunks(self.spark.createDataFrame([],CLEANED_SCHEMA),"HI-Medium",self.plan,lim,stager,budget=ChunkBudget(lim,lambda:None))
        self.assertEqual(result["counts"],{2:0,3:0});self.check_clean()

    def new_stager(self,folder,run="chunks"):
        cfg=config();cfg["run_id"]=run;client=PublicationS3();transport=PublicationTransport(folder,client)
        stager=ChunkS3Stager(client,cfg,spark=self.spark,max_copy_bytes=1000000,transport=transport)
        manifest=dict(state="CREATED",manifest_kind="full_sequence_generation_handoff",published=False,owner_dates=["2022-09-01"],
            source=cfg["expected_source"],schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA],
            builder=dict(version=cfg["builder_version"],fingerprint=cfg["builder_fingerprint"]))
        stager.reserve(manifest);return stager,client

    def test_real_parquet_date_receipt_and_tamper_rejection(self):
        with tempfile.TemporaryDirectory() as folder:
            stager,client=self.new_stager(folder)
            lim=limits()
            final=generate_date_chunks(self.frame,"HI-Medium",self.plan,lim,stager,budget=ChunkBudget(lim,lambda:None),expected_counts={"2":4,"3":8})
            self.assertEqual(final["row_count"],12);self.assertEqual(final["validation_status"],"VALIDATED")
            back=stager.transport.read(self.spark,final["staging_location"],[r["path"] for r in final["files"]],{"timezone":"UTC"})
            self.assertEqual(self.canonical(back.collect()),self.canonical(self.unbatched().collect()))
            with self.assertRaises(ValueError):stager.finalize_date("2022-09-01",{2:4,3:8})
            with self.assertRaises(ValueError):stager.reserve(stager.manifest)
        self.check_clean()

    def test_hourly_join_exact_boundaries_and_li_isolation(self):
        from bq2_sequence_builder import _build_two_hop_links_validated
        from pyspark.sql import functions as F
        for dataset in ("HI-Medium","LI-Medium"):
            rows=[transaction(1,"2022-09-01 23:59",(1,"A"),(2,"B"),dataset=dataset),
                transaction(2,"2022-09-02 00:00",(2,"B"),(3,"C"),dataset=dataset),
                transaction(3,"2022-09-02 23:59",(2,"B"),(3,"D"),dataset=dataset),
                transaction(4,"2022-09-03 00:00",(2,"B"),(3,"E"),dataset=dataset),
                transaction(5,"2022-09-01 23:59",(2,"B"),(3,"F"),dataset=dataset)]
            frame=self.spark.createDataFrame(rows,CLEANED_SCHEMA)
            ids=frame.where(F.col("txn_id")==1).select("dataset","txn_id")
            actual=_build_two_hop_links_validated(frame,first_txn_ids=ids,second_txn_ids=frame.select("dataset","txn_id"),_hourly=True)
            self.assertEqual({r.second_txn_id for r in actual.collect()},{2,3})
            expected=build_two_hop_links(frame).where(F.col("first_txn_id")==1)
            self.assertEqual(self.canonical(actual.collect()),self.canonical(expected.collect()))

    def test_hourly_parquet_cached_self_derived_selectors_have_disjoint_attributes(self):
        """Glue-shaped plan regression, not a claim to emulate Amazon's analyzer."""
        from bq2_sequence_builder import _build_two_hop_links_validated
        from pyspark.sql import functions as F
        from pyspark import StorageLevel
        with tempfile.TemporaryDirectory() as folder:
            self.frame.write.partitionBy("dataset","txn_date").parquet(folder+"/source")
            session=self.spark.newSession()
            session.conf.set("spark.sql.session.timeZone","UTC")
            session.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled","false")
            source=(session.read.parquet(folder+"/source").select(*CLEANED_SCHEMA.fieldNames())
                .persist(StorageLevel.DISK_ONLY))
            try:
                source.count()
                nested=source.alias("a").join(source.select("dataset","txn_id").alias("b"),["dataset","txn_id"],"left_semi")
                first=nested.where(F.col("txn_date")=="2022-09-01").select("dataset","txn_id")
                ab=_build_two_hop_links_validated(nested,first_txn_ids=first,second_txn_ids=nested.select("dataset","txn_id"),_hourly=True)
                self.assertEqual(ab.count(),4)
                reached=nested.join(ab.select("dataset",F.col("second_txn_id").alias("txn_id")),["dataset","txn_id"],"left_semi")
                bc=_build_two_hop_links_validated(nested,first_txn_ids=reached.select("dataset","txn_id"),
                    second_txn_ids=nested.select("dataset","txn_id"),_hourly=True)
                self.assertEqual(bc.count(),4)
                self.assertEqual(build_three_hop_links(bc,starting_links=ab).count(),8)
                plan=bc._jdf.queryExecution().analyzed().toString()
                self.assertIn("_r5_first_dataset",plan);self.assertIn("_r5_second_dataset",plan)
                # The externally consumed intermediate contract is unchanged.
                self.assertEqual(bc.columns,["dataset","first_txn_id","second_txn_id"])
            finally:source.unpersist(blocking=True)
        self.check_clean()

    def test_interrupted_chunks_and_foreign_work_never_finalize(self):
        with tempfile.TemporaryDirectory() as folder:
            stager,client=self.new_stager(folder);stager.begin_date("2022-09-01",2,2)
            stager.complete_root("2022-09-01",0,1,{2:0,3:0},[])
            with self.assertRaises(ValueError):stager.finalize_date("2022-09-01",{2:0,3:0})
            stager.complete_root("2022-09-01",1,1,{2:0,3:0},[])
            date=stager.dates["2022-09-01"]
            client.seed((date["work"]+"unexpected").split("/",3)[3],b"partial")
            with self.assertRaises(ValueError):stager.finalize_date("2022-09-01",{2:0,3:0})
            status=client.objects[date["status_uri"].split("/",3)[3]]["raw"]
            self.assertEqual(json.loads(status)["state"],"WRITING")

    def test_full_hi_li_coverage_and_failed_date_cannot_finish(self):
        # Orchestration-only mocks; real row correctness is tested above.
        from test_full_generation import snapshot
        from sequence_full_source import dataset_spec
        for dataset,days in (("HI-Medium",28),("LI-Medium",27)):
            cfg=config(dataset);snap=snapshot(dataset)
            client=PublicationS3();catalog=MagicMock();catalog.meta.region_name="us-east-1"
            identity=MagicMock();identity.meta.region_name="us-east-1"
            identity.get_caller_identity.return_value={"Account":cfg["account_id"]}
            store=MagicMock();store.finish.side_effect=lambda m:dict(manifest=m,manifest_uri="synthetic")
            profile=dict(row_count=dataset_spec(dataset)["rows"],identity_check_passed=True)
            def completed(source,dataset,plan,*a,**kw):
                return dict(start_date=plan.start_date.isoformat(),counts={"2":0,"3":0},parquet_files=0,size_bytes=0)
            with patch("sequence_full_source.inspect_full_source",return_value=snap), \
                 patch("sequence_full_source.read_full_catalog",return_value=self.frame), \
                 patch("sequence_full_generation.profile_full_source",return_value=profile), \
                 patch("sequence_chunked_staging.ChunkS3Stager",return_value=store), \
                 patch.object(generation,"generate_date_chunks",side_effect=completed) as generate:
                result=generation.run_chunked_generation(cfg,limits(),allow_writes=True,mode="full_dataset",
                    max_copy_bytes=1000000,expected_counts={},glue_context=type("Context",(),{"spark_session":self.spark})(),
                    catalog_client=catalog,s3_client=client,identity_client=identity)
                self.assertEqual(result["manifest"]["state"],"STAGED_VALIDATED")
                self.assertEqual(len(result["manifest"]["batches"]),days);self.assertEqual(generate.call_count,days)
                store.finish.assert_called_once();self.assertFalse(result["manifest"]["published"])
                store.reset_mock();generate.side_effect=ValueError("injected date failure")
                with self.assertRaises(ValueError):
                    generation.run_chunked_generation(cfg,limits(),allow_writes=True,mode="full_dataset",
                        max_copy_bytes=1000000,expected_counts={},glue_context=type("Context",(),{"spark_session":self.spark})(),
                        catalog_client=catalog,s3_client=client,identity_client=identity)
                store.finish.assert_not_called();store.fail.assert_called_once()
            self.check_clean()

    def test_single_root_entry_real_generation_and_parquet_is_not_full_delivery(self):
        from test_full_generation import snapshot
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType
        context=MagicMock();context.spark_session=self.spark
        context.create_dynamic_frame.from_catalog.return_value.toDF.return_value=self.frame
        catalog=MagicMock();catalog.meta.region_name="us-east-1"
        identity=MagicMock();identity.meta.region_name="us-east-1"
        identity.get_caller_identity.return_value={"Account":"123456789012"}
        original=SparkSession.newSession;calls=[]
        def session(spark):
            calls.append(True)
            if len(calls)==1:
                footer=MagicMock();footer.read.option.return_value.parquet.return_value.schema=StructType(CLEANED_SCHEMA.fields[:-2])
                return footer  # External source footer boundary only.
            return original(spark)
        with tempfile.TemporaryDirectory() as folder:
            client=PublicationS3();transport=PublicationTransport(folder,client)
            actual=generation.run_chunked_generation
            def run(*a,**kw):return actual(*a,**dict(kw,transport=transport))
            with patch("sequence_full_source.inspect_full_source",return_value=snapshot()), \
                 patch.object(SparkSession,"newSession",session), \
                 patch.object(entry,"_initialize",return_value=dict(glue_context=context,catalog_client=catalog,s3_client=client,identity_client=identity)), \
                 patch.object(entry,"run_chunked_generation",side_effect=run),patch("sys.stdout",new_callable=io.StringIO) as out:
                self.assertEqual(entry.main(args()),0)
            self.assertIn("PARTIAL_CHUNKS",out.getvalue())
            context.create_dynamic_frame.from_catalog.assert_called_once_with(database="is459_g2t1_aml",table_name="transactions_clean",
                push_down_predicate="dataset = 'HI-Medium' AND txn_date IN ('2022-09-01', '2022-09-02', '2022-09-03')")
            manifests=[json.loads(v["raw"]) for k,v in client.objects.items() if k.endswith("metadata/run.json")]
            self.assertEqual(manifests[0]["state"],"PARTIAL_CHUNKS")
            self.assertFalse(manifests[0]["published"]);self.assertEqual(manifests[0]["chunk_metrics"]["validated_output_rows"],12)
            self.assertTrue(any(k.endswith(".parquet") for k in client.objects))
            self.assertFalse(any(k.startswith("bq2_sequences/") for k in client.objects))
            self.assertFalse(any("/batches/" in k and k.endswith("validation.json") for k in client.objects))
        self.check_clean()


if __name__=="__main__":unittest.main()
