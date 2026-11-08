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
                self.assertEqual(len(z.namelist()),21);self.assertIsNone(z.testzip())
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
