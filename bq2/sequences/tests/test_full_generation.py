"""HI/LI coverage, source/protocol fakes and real local Spark/Parquet tests.

Never AWS. Mock orchestration tests are separate from the real builder/Parquet
integration below. Synthetic baseline patching is TEST ONLY, not a runtime hook.
"""
from contextlib import contextmanager, ExitStack
from copy import deepcopy
from datetime import datetime, timedelta
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sequence_full_source as source
import sequence_full_generation as generation
from sequence_full_staging import FullS3Stager
from sequence_processing import plan_start_date_batches
from sequence_batch import BatchResult, GrowthLimitExceeded
from sequence_source import CLEANED_SCHEMA
from sequence_staging import SEQUENCE_SCHEMA
from sequence_s3 import _SparkParquetTransport
from test_sequence_s3 import FakeS3
from test_sequence_aws import table
from test_sequence_glue_runtime import client_model
from fixtures import transaction


def config(dataset="HI-Medium"):
    return dict(dataset=dataset, account_id="123456789012", region="us-east-1", run_id="full_run",
        attempt_id="attempt1", owner_token="owner1", builder_version="synthetic",
        builder_fingerprint="a"*64, timezone_evidence="Synthetic UTC verification",
        no_refresh_acknowledged=True, no_refresh_evidence="Synthetic source freeze",
        expected_source=dict(source_fingerprint="b"*64, table_fingerprint="c"*64, partitions_fingerprint="d"*64),
        limits=dict.fromkeys(generation.LIMIT_NAMES,100), limits_evidence="Synthetic measured bounds",
        max_records_per_file=2, max_run_seconds=3600, verified_empty_dates={})


def snapshot(dataset="HI-Medium", empty=()):
    spec = source.dataset_spec(dataset)
    days = source.owner_dates(dataset)
    available = [d for d in days if d not in empty]
    plans = plan_start_date_batches(spec["first"], spec["last"],dataset_start_date=spec["first"],
        dataset_end_date=spec["last"], available_partition_dates=available, verified_empty_dates=empty)
    return dict(dataset=dataset, table=table(), owner_dates=list(days), plans=plans, available_dates=available,
        verified_empty_dates={d:"Synthetic independent empty attestation" for d in empty},
        files_by_date={d:[source.SOURCE_ROOT+"dataset="+dataset+"/txn_date="+d+"/part.parquet"] for d in available},
        objects=[], **config(dataset)["expected_source"])


def source_clients(dataset="HI-Medium"):
    s3 = FakeS3()
    metadata = table()
    metadata["StorageDescriptor"]["InputFormat"] = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    partitions = []
    for d in source.owner_dates(dataset):
        location = source.SOURCE_ROOT+"dataset="+dataset+"/txn_date="+d
        partitions.append(dict(Values=[dataset,d],StorageDescriptor=dict(metadata["StorageDescriptor"],Location=location)))
        s3.seed(location.split("/",3)[3]+"/part.parquet")
    catalog = MagicMock()
    catalog.get_table.return_value = dict(Table=metadata)
    def pages(**kwargs):
        start = int(kwargs.get("NextToken",0))
        response = dict(Partitions=deepcopy(partitions[start:start+4]))
        if start+4 < len(partitions):
            response["NextToken"] = str(start+4)
        return response
    catalog.get_partitions.side_effect = pages
    return catalog,s3,partitions


class FullSourceTests(unittest.TestCase):
    def test_both_datasets_complete_windows_and_boundaries(self):
        for dataset, total in (("HI-Medium",28),("LI-Medium",27)):
            catalog,s3,parts = source_clients(dataset)
            result = source.inspect_full_source(catalog,s3,dataset)
            self.assertEqual(len(result["plans"]),total)
            self.assertEqual(len(set(result["owner_dates"])),total)
            self.assertEqual([d.isoformat() for d in result["plans"][0].available_input_dates],
                ["2022-09-01","2022-09-02","2022-09-03"])
            self.assertEqual(len(result["plans"][-1].available_input_dates),1)
            self.assertEqual(len(result["plans"][-1].unavailable_after_dataset_end),2)
            self.assertTrue(all(p.ready_to_process for p in result["plans"]))
            self.assertTrue(all(c.kwargs["Expression"]==f"dataset = '{dataset}'" for c in catalog.get_partitions.call_args_list))
            self.assertTrue(all(f"dataset={dataset}/" in c[1]["Prefix"] for c in s3.calls if c[0]=="list"))
            again = source.inspect_full_source(catalog,s3,dataset)
            source.assert_full_source_unchanged(result,again)
            parts.reverse()
            reverse = source.inspect_full_source(catalog,s3,dataset)
            self.assertEqual(source.snapshot_identity(result),source.snapshot_identity(reverse))

    def test_invalid_dataset_rejected_without_clients(self):
        for dataset in ("sample","HI","LI","HI-Medium' OR true",None):
            catalog,s3=MagicMock(),MagicMock()
            with self.assertRaises(ValueError):source.inspect_full_source(catalog,s3,dataset)
            catalog.get_table.assert_not_called();s3.list_objects_v2.assert_not_called()

    def test_success_markers_are_inventory_only_never_parquet(self):
        catalog,s3,_=source_clients()
        prefix="transactions_clean/dataset=HI-Medium/"
        s3.seed(prefix+"_SUCCESS",b"")
        s3.seed(prefix+"txn_date=2022-09-01/_SUCCESS",b"")
        result=source.inspect_full_source(catalog,s3,"HI-Medium")
        self.assertEqual(len(result["objects"]),30)
        self.assertEqual(sum(len(files) for files in result["files_by_date"].values()),28)

    def test_missing_partition_files_and_explicit_empty_evidence(self):
        catalog,s3,parts = source_clients()
        day=parts.pop(4)["Values"][1]
        for key in list(s3.objects):
            if "txn_date="+day+"/" in key:del s3.objects[key]
        with self.assertRaisesRegex(ValueError,"Missing required"):source.inspect_full_source(catalog,s3,"HI-Medium")
        result=source.inspect_full_source(catalog,s3,"HI-Medium",verified_empty_dates={day:"Synthetic upstream empty proof"})
        self.assertNotIn(day,result["available_dates"])
        self.assertTrue(all(p.ready_to_process for p in result["plans"]))
        with self.assertRaises(ValueError):source.inspect_full_source(catalog,s3,"HI-Medium",verified_empty_dates={day:""})

    def test_location_dataset_schema_and_extra_files_fail(self):
        for mode in ("location","dataset","schema","extra","missingfile"):
            catalog,s3,parts=source_clients()
            if mode=="location":parts[0]["StorageDescriptor"]["Location"]+="//"
            elif mode=="dataset":parts[0]["Values"][0]="LI-Medium"
            elif mode=="schema":parts[0]["StorageDescriptor"]["Columns"][0]["Type"]="double"
            elif mode=="extra":s3.seed("transactions_clean/dataset=HI-Medium/unexpected")
            else:del s3.objects[next(iter(s3.objects))]
            with self.subTest(mode=mode),self.assertRaises(ValueError):source.inspect_full_source(catalog,s3,"HI-Medium")

    def test_source_change_detection(self):
        catalog,s3,_=source_clients()
        before=source.inspect_full_source(catalog,s3,"HI-Medium")
        s3.seed(next(iter(s3.objects)),b"refreshed")
        with self.assertRaisesRegex(ValueError,"snapshot changed"):
            source.assert_full_source_unchanged(before,source.inspect_full_source(catalog,s3,"HI-Medium"))

    def test_reader_exact_pushdown_schema_and_no_unrestricted_read(self):
        context=MagicMock(); context.spark_session.conf.get.return_value="UTC"
        frame=MagicMock();frame.columns=CLEANED_SCHEMA.fieldNames();frame.select.return_value=frame;frame.schema=CLEANED_SCHEMA
        context.create_dynamic_frame.from_catalog.return_value.toDF.return_value=frame
        for dataset in ("HI-Medium","LI-Medium"):
            snap=snapshot(dataset)
            with patch("sequence_source._check_schema") as physical:
                self.assertIs(source.read_full_catalog(context,dataset,snap["available_dates"],snapshot=snap,timezone_evidence="synthetic"),frame)
                self.assertEqual(physical.call_count,len(snap["available_dates"])+1) # footers + logical schema
            predicate=context.create_dynamic_frame.from_catalog.call_args.kwargs["push_down_predicate"]
            self.assertTrue(predicate.startswith(f"dataset = '{dataset}' AND txn_date IN ("))
            self.assertIn(snap["owner_dates"][-1],predicate)
            context.create_dynamic_frame.from_catalog.reset_mock()
            with self.assertRaises(ValueError):source.read_full_catalog(context,dataset,[],snapshot=snap,timezone_evidence="synthetic")
            context.create_dynamic_frame.from_catalog.assert_not_called()


class MemoryStager:
    """Fake publication boundary only; no claim of Parquet/S3 validation here."""
    def __init__(self):self.events=[];self.manifest=None;self.failure=None
    def reserve(self,m):self.events.append("reserve");self.manifest=deepcopy(m)
    def stage(self,output,*,start_date,expected_counts):
        self.events.append(start_date)
        return dict(dataset=self.manifest["dataset"],start_date=start_date,counts=expected_counts,
            row_count=sum(expected_counts.values()),validation_status="VALIDATED",schema_valid=True,values_valid=True,
            run_id=self.manifest["run_id"],attempt_id=self.manifest["attempt_id"],source=self.manifest["source"],
            schema=self.manifest["schema"],builder=self.manifest["builder"],rule_version="v1",
            parquet_files=1 if sum(expected_counts.values()) else 0,size_bytes=20 if sum(expected_counts.values()) else 0)
    def finish(self,m):self.events.append("finish");self.manifest=m;return dict(manifest=m,manifest_uri="synthetic-handoff")
    def fail(self,e):self.events.append("fail");self.failure=e


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.cfg=config();self.snap=snapshot();self.context=MagicMock();self.spark=self.context.spark_session
        self.spark.conf.get.return_value="UTC";self.spark.version="3.5.4"
        self.spark.sparkContext._jvm.java.lang.System.getProperty.return_value="17.0.20"
        self.spark._jsparkSession.sharedState.return_value.cacheManager.return_value.lookupCachedData.return_value.nonEmpty.return_value=False
        self.frame=MagicMock();self.stager=MemoryStager()
        self.catalog=MagicMock();self.identity=MagicMock();self.s3,_=client_model()
        for client in (self.catalog,self.identity,self.s3):client.meta.region_name="us-east-1"
        self.identity.get_caller_identity.return_value={"Account":"123456789012"}

    @contextmanager
    def harness(self, *, snapshots=None, profile_error=None, batch_error=None):
        @contextmanager
        def batch(*args):
            try:
                if batch_error:raise batch_error
                yield BatchResult(MagicMock(),dict(starting_pairs=1,actual_triples=1))
            finally:self.stager.events.append("batch_cleanup")
        with ExitStack() as stack:
            self.inspect=stack.enter_context(patch.object(generation,"inspect_full_source",side_effect=snapshots or lambda_snapshot(self)))
            self.reader=stack.enter_context(patch.object(generation,"read_full_catalog",return_value=self.frame))
            self.profile=stack.enter_context(patch.object(generation,"profile_full_source",side_effect=profile_error,
                return_value=dict(rows_by_date={d:1 for d in self.snap["owner_dates"]},verification_scope="provided_dataframe_only",dataset_wide_verified=False)))
            stack.enter_context(patch("pyspark.sql.functions.col",return_value=MagicMock()))
            self.batch=stack.enter_context(patch("sequence_batch.process_start_date_batch",side_effect=batch))
            yield

    def run_generation(self):
        return generation.run_full_generation(self.cfg,allow_writes=True,glue_context=self.context,
            catalog_client=self.catalog,s3_client=self.s3,identity_client=self.identity,stager=self.stager)

    def test_full_hi_li_all_owner_dates_exact_windows_and_cleanup(self):
        for dataset,count in (("HI-Medium",28),("LI-Medium",27)):
            self.setUp();self.cfg=config(dataset);self.snap=snapshot(dataset)
            with self.harness():result=self.run_generation()
            m=result["manifest"]
            self.assertEqual(m["state"],"STAGED_VALIDATED");self.assertFalse(m["published"]);self.assertFalse(m["catalog_updated"])
            self.assertEqual([b["start_date"] for b in m["batches"]],list(source.owner_dates(dataset)))
            self.assertEqual(m["counts"],{"2":count,"3":count})
            self.assertEqual(m["batches"][0]["input_dates"],["2022-09-01","2022-09-02","2022-09-03"])
            self.assertEqual(len(m["batches"][-1]["unavailable_after_dataset_end"]),2)
            self.reader.assert_called_once();self.profile.assert_called_once()
            self.assertEqual(self.batch.call_count,count)
            self.frame.unpersist.assert_called_once_with(blocking=True)
            self.assertEqual(self.stager.events[0],"reserve");self.assertEqual(self.stager.events[-1],"finish")

    def test_growth_and_validation_failures_stop_and_cleanup(self):
        for failure in (GrowthLimitExceeded("bound exceeded"),ValueError("source IDs invalid")):
            self.setUp()
            options=dict(batch_error=failure) if isinstance(failure,GrowthLimitExceeded) else dict(profile_error=failure)
            with self.harness(**options),self.assertRaises(type(failure)) as caught:self.run_generation()
            self.assertIs(caught.exception,failure);self.assertIs(self.stager.failure,failure)
            self.assertNotIn("finish",self.stager.events);self.frame.unpersist.assert_called_once()

    def test_each_growth_limit_is_forwarded_without_defaults(self):
        for index,name in enumerate(generation.LIMIT_NAMES):self.cfg["limits"][name]=index+1
        with self.harness():self.run_generation()
        self.assertEqual(self.batch.call_args.args[4].__dict__,self.cfg["limits"])

    def test_final_source_change_prevents_terminal_success(self):
        changed=deepcopy(self.snap);changed["source_fingerprint"]="f"*64
        with self.harness(snapshots=[deepcopy(self.snap)]*29+[changed]),self.assertRaisesRegex(ValueError,"snapshot changed"):self.run_generation()
        self.assertEqual(self.stager.events[-1],"fail");self.assertNotIn("finish",self.stager.events)

    def test_cleanup_and_failure_recording_preserve_original(self):
        failure=RuntimeError("cleanup failed");self.frame.unpersist.side_effect=failure
        self.stager.fail=MagicMock(side_effect=RuntimeError("metadata unavailable"))
        with self.harness(),self.assertRaises(RuntimeError) as caught:self.run_generation()
        self.assertIs(caught.exception,failure);self.assertIn("record full-generation",failure.__notes__[0])
        self.assertNotIn("finish",self.stager.events)

    def test_duplicate_or_blocked_plans_rejected_before_reservation(self):
        for mode in ("duplicate","missing"):
            self.setUp()
            plans=list(self.snap["plans"])
            self.snap["plans"]=tuple(plans+[plans[0]]) if mode=="duplicate" else tuple(plans[:-1])
            with self.harness(),self.assertRaises(ValueError):self.run_generation()
            self.assertEqual(self.stager.events,[])

    def test_bad_activation_limits_snapshot_and_account_never_write(self):
        self.s3.put_object.reset_mock()
        with self.harness():
            for activation in (False,None,"true",1):
                with self.assertRaises(ValueError):generation.run_full_generation(self.cfg,allow_writes=activation,
                    glue_context=self.context,catalog_client=self.catalog,s3_client=self.s3,identity_client=self.identity,stager=self.stager)
            self.cfg["expected_source"]["source_fingerprint"]="e"*64
            with self.assertRaises(ValueError):self.run_generation()
        self.assertEqual(self.stager.events,[])
        for name in generation.LIMIT_NAMES:
            bad=config();bad["limits"][name]=0
            with self.assertRaises(ValueError):generation.validate_full_config(bad)
        self.identity.get_caller_identity.return_value={"Account":"999999999999"}
        with self.assertRaisesRegex(ValueError,"account"):self.run_generation()
        self.s3.put_object.assert_not_called()

    def test_mismatched_staging_receipt_is_not_run_success(self):
        original=self.stager.stage
        self.stager.stage=lambda *a,**k:dict(original(*a,**k),run_id="other_run")
        with self.harness(),self.assertRaisesRegex(ValueError,"mismatched"):self.run_generation()
        self.assertNotIn("finish",self.stager.events)

    def test_deadline_prevents_generation_and_writes(self):
        with self.harness(),patch("sequence_full_generation.time.monotonic",side_effect=[0,3601]),self.assertRaises(TimeoutError):
            self.run_generation()
        self.assertEqual(self.stager.events,[])

    def test_source_validation_empty_owner_skips_continuation_expansion(self):
        with self.harness():
            self.profile.return_value=dict(rows_by_date={},verification_scope="provided_dataframe_only",dataset_wide_verified=False)
            result=self.run_generation()
            self.batch.assert_not_called()
        self.assertEqual(result["manifest"]["row_count"],0)
        self.assertEqual(len(result["manifest"]["batches"]),28)


def lambda_snapshot(test):
    return lambda *args,**kwargs:deepcopy(test.snap)


class FullStagingProtocolTests(unittest.TestCase):
    def setUp(self):
        self.client=FakeS3();self.cfg=config();self.store=FullS3Stager(self.client,self.cfg,transport=MagicMock())
        self.output=MagicMock()
        self.manifest=dict(state="CREATED",owner_dates=["2022-09-01"],source=self.cfg["expected_source"],schema=[],builder={})
        self.report=dict(counts={2:1,3:1},row_count=2,schema_valid=True,values_valid=True)
        def write(output,uri,size):self.client.seed(uri.split("/",3)[3]+"dataset=HI-Medium/start_date=2022-09-01/part.parquet",b"synthetic parquet")
        self.store.transport.write.side_effect=write

    def stage(self):
        with patch("sequence_staging.validate_sequence_output",return_value=self.report):
            return self.store.stage(self.output,start_date="2022-09-01",expected_counts={2:1,3:1})

    def test_immutable_validated_staging_and_run_readback(self):
        self.store.reserve(self.manifest);receipt=self.stage()
        self.assertEqual(receipt["parquet_files"],1)
        self.assertEqual(receipt["validation_status"],"VALIDATED")
        result=self.store.finish(dict(self.manifest,state="STAGED_VALIDATED",published=False,batches=[receipt]))
        self.assertEqual(result["manifest"]["state"],"STAGED_VALIDATED")
        with self.assertRaises(ValueError):FullS3Stager(self.client,self.cfg).reserve(self.manifest)
        puts=[c[1]["Key"] for c in self.client.calls if c[0]=="put"]
        self.assertTrue(all(k.startswith("bq2_sequences_staging/full/dataset=HI-Medium/") for k in puts))
        with self.assertRaises(ValueError):self.store.fail(RuntimeError("late"))

    def test_empty_batch_without_files_is_explicit(self):
        self.store.reserve(self.manifest);self.report.update(counts={2:0,3:0},row_count=0)
        with patch("sequence_staging.validate_sequence_output",return_value=self.report):
            receipt=self.store.stage(self.output,start_date="2022-09-01",expected_counts={2:0,3:0})
        self.assertEqual(receipt["files"],[]);self.assertEqual(receipt["parquet_files"],0)
        self.store.transport.write.assert_not_called()

    def test_write_and_readback_failures_preserve_attempts(self):
        for mode in ("write","read"):
            self.setUp();self.store.reserve(self.manifest)
            getattr(self.store.transport,mode).side_effect=RuntimeError("transport failure")
            with self.assertRaisesRegex(RuntimeError,"transport failure"):self.stage()
            status=json.loads(self.client.objects[(self.store.root+"batches/2022-09-01/metadata/status.json").split("/",3)[3]]["raw"])
            self.assertEqual(status["state"],"FAILED")
            with self.assertRaises(ValueError):self.stage()

    def test_changed_receipt_and_owner_conflicts_block_completion(self):
        self.store.reserve(self.manifest);receipt=self.stage()
        self.client.seed(receipt["receipt_uri"].split("/",3)[3],b"{}")
        with self.assertRaisesRegex(ValueError,"evidence changed"):
            self.store.finish(dict(self.manifest,state="STAGED_VALIDATED",published=False,batches=[receipt]))
        self.client.seed((self.store.root+"metadata/run.json").split("/",3)[3],b"{}")
        with self.assertRaisesRegex(ValueError,"ownership"):self.store.fail(RuntimeError("failed"))


class LocalTransport(_SparkParquetTransport):
    def __init__(self,root,client):self.root=Path(root);self.client=client
    def path(self,uri):return (self.root/uri.split("/",3)[3]).as_posix()
    def write(self,output,uri,max_records_per_file):
        super().write(output,uri,max_records_per_file)
        for path in Path(self.path(uri)).rglob("*"):
            if path.is_file() and not path.name.startswith("."):
                self.client.seed(uri.split("/",3)[3]+path.relative_to(self.path(uri)).as_posix(),path.read_bytes())


class FullSparkIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        os.environ["PYSPARK_PYTHON"]=sys.executable
        cls.spark=(SparkSession.builder.master("local[2]").appName("role5-full-integration")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2")
            .config("spark.ui.enabled","false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR");cls.addClassCleanup(cls.spark.stop)

    def test_actual_hi_li_batch_arrays_and_real_parquet(self):
        from bq2_sequence_builder import build_two_hop_links,build_three_hop_links,build_sequence_output
        from sequence_batch import process_start_date_batch,GrowthLimits
        from sequence_staging import validate_sequence_schema
        for dataset in ("HI-Medium","LI-Medium"):
            last=source.dataset_spec(dataset)["last"]
            first=(datetime.fromisoformat(last)-timedelta(days=2)).date().isoformat()
            middle=(datetime.fromisoformat(last)-timedelta(days=1)).date().isoformat()
            rows=[transaction(-9007199254740993,first+" 09:00",(1,"A"),(2,"B"),dataset=dataset,label=1),
                  transaction(9007199254740993,middle+" 09:00",(2,"B"),(2,"C"),dataset=dataset),
                  transaction(9223372036854775807,last+" 09:00",(2,"C"),(3,"D"),dataset=dataset)]
            frame=self.spark.createDataFrame(rows,CLEANED_SCHEMA)
            spec=source.dataset_spec(dataset)
            plans=plan_start_date_batches(first,last,dataset_start_date=spec["first"],dataset_end_date=last,
                available_partition_dates=source.owner_dates(dataset))
            client=FakeS3();cfg=config(dataset)
            with tempfile.TemporaryDirectory() as folder:
                store=FullS3Stager(client,cfg,transport=LocalTransport(folder,client))
                store.reserve(dict(state="CREATED",owner_dates=[p.start_date.isoformat() for p in plans],
                    source=cfg["expected_source"],schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA],builder={}))
                with process_start_date_batch(frame,dataset,plans[0],"UTC",GrowthLimits(**cfg["limits"])) as batch:
                    validate_sequence_schema(batch.output)
                    data=batch.output.collect()
                    self.assertEqual({r.sequence_length for r in data},{2,3})
                    triple=next(r for r in data if r.sequence_length==3)
                    self.assertEqual(triple.txn_ids,[r["txn_id"] for r in rows])
                    self.assertEqual(triple.duration_seconds,172800)
                    self.assertEqual(triple.gap_seconds,[86400,86400])
                    receipt=store.stage(batch.output,start_date=first,expected_counts={2:1,3:1})
                    self.assertEqual(receipt["row_count"],2)
                    self.assertGreater(receipt["parquet_files"],0)
                self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)

    def test_profile_checks_cross_date_duplicate_ids_and_mixing(self):
        dataset="HI-Medium";snap=snapshot(dataset)
        row=transaction(1,"2022-09-01 09:00",(1,"A"),(2,"B"),dataset=dataset)
        other=transaction(1,"2022-09-28 09:00",(3,"C"),(4,"D"),dataset=dataset)
        with self.assertRaisesRegex(ValueError,"Duplicate"):
            generation.profile_full_source(self.spark.createDataFrame([row,other],CLEANED_SCHEMA),dataset,snap)
        foreign=transaction(2,"2022-09-02 09:00",(1,"A"),(2,"B"),dataset="LI-Medium")
        with self.assertRaisesRegex(ValueError,"escapes"):
            generation.profile_full_source(self.spark.createDataFrame([row,foreign],CLEANED_SCHEMA),dataset,snap)

    def test_full_coordinator_real_builder_and_parquet_all_dates(self):
        """Real generation/profile/staging; fake external source metadata and S3.

        A test-only patched baseline permits three fictional rows; the CLI has
        no baseline override. Every one of the 28 owners still gets a receipt.
        """
        cfg=config();snap=snapshot()
        rows=[transaction(101,"2022-09-26 09:00",(1,"A"),(2,"B"),dataset="HI-Medium",label=1),
              transaction(102,"2022-09-27 09:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
              transaction(103,"2022-09-28 09:00",(2,"C"),(3,"D"),dataset="HI-Medium")]
        frame=self.spark.createDataFrame(rows,CLEANED_SCHEMA)
        client=FakeS3();model,_=client_model();client.meta=model.meta;client.meta.region_name="us-east-1"
        context=SimpleNamespace(spark_session=self.spark)
        catalog,identity=MagicMock(),MagicMock()
        catalog.meta.region_name=identity.meta.region_name="us-east-1"
        identity.get_caller_identity.return_value={"Account":cfg["account_id"]}
        test_baseline=dict(source.dataset_spec("HI-Medium"),rows=3,laundering=1)
        with tempfile.TemporaryDirectory() as folder:
            store=FullS3Stager(client,cfg,transport=LocalTransport(folder,client))
            with patch.object(generation,"inspect_full_source",side_effect=lambda *a,**k:deepcopy(snap)), \
                 patch.object(generation,"read_full_catalog",return_value=frame) as reader, \
                 patch.object(generation,"dataset_spec",return_value=test_baseline):
                result=generation.run_full_generation(cfg,allow_writes=True,glue_context=context,
                    catalog_client=catalog,s3_client=client,identity_client=identity,stager=store)
            reader.assert_called_once()
            manifest=result["manifest"]
            self.assertEqual(len(manifest["batches"]),28)
            self.assertEqual(manifest["counts"],{"2":2,"3":1})
            self.assertEqual(manifest["row_count"],3)
            self.assertEqual(sum(b["row_count"]==0 for b in manifest["batches"]),26)
            self.assertEqual(manifest["state"],"STAGED_VALIDATED")
            self.assertFalse(manifest["published"]);self.assertFalse(manifest["catalog_updated"])
            self.assertFalse(manifest["source_validation"]["dataset_wide_verified"])
            self.assertTrue(manifest["source_validation"]["full_selected_snapshot_checked"])
            saved=json.loads(client.objects[result["manifest_uri"].split("/",3)[3]]["raw"])
            self.assertEqual(saved,manifest)
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)
            catalog.get_table.assert_not_called() # external source boundary mocked; no Catalog mutations


if __name__=="__main__":unittest.main()
