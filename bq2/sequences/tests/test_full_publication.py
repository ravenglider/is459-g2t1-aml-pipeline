"""Offline production publication protocol and separate real Parquet integration.

Fake clients are not AWS certification. Only the final integration test runs real
Spark validation; protocol tests explicitly mock those distributed validators.
"""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock,patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sequence_full_publication as publication
from sequence_full_staging import STAGING_ROOT,FullS3Stager
from sequence_manifest import _digest
from sequence_source import fingerprint_source_inventory,CLEANED_SCHEMA
from sequence_staging import SEQUENCE_SCHEMA
from test_full_generation import config,LocalTransport
from test_sequence_glue_runtime import client_model
from test_sequence_s3 import FakeS3,FakeError
from fixtures import transaction


class PublicationS3(FakeS3):
    def __init__(self):
        super().__init__()
        model,_=client_model(("IfNoneMatch","IfMatch","ChecksumSHA256"))
        self.meta=model.meta;self.meta.region_name="us-east-1"
        self.data_failure=False;self.data_response_lost=False;self.corrupt_destination=False
    def get_object(self,**kwargs):
        key=kwargs["Key"]
        if "IfMatch" in kwargs and (key not in self.objects or self.objects[key]["ETag"]!=kwargs["IfMatch"]):
            raise FakeError("PreconditionFailed")
        return super().get_object(**kwargs)
    def put_object(self,**kwargs):
        data=kwargs["Key"].startswith("bq2_sequences/") and kwargs["Key"].endswith(".parquet")
        if data:
            import base64
            assert kwargs["IfNoneMatch"]=="*" and kwargs["ServerSideEncryption"]=="AES256"
            assert kwargs["ChecksumSHA256"]==base64.b64encode(hashlib.sha256(kwargs["Body"]).digest()).decode()
            if self.data_failure:raise RuntimeError("copy failed before commit")
        result=super().put_object(**kwargs)
        if data and self.corrupt_destination:self.seed(kwargs["Key"],b"corrupted")
        if data and self.data_response_lost:
            self.data_response_lost=False
            raise TimeoutError("copy response lost")
        return result


class Catalog:
    def __init__(self):
        self.table=None;self.parts={};self.calls=[];self.meta=SimpleNamespace(region_name="us-east-1")
        self.fail_on=None;self.ambiguous=False;self.mutations=0;self.incompatible=False
    def get_table(self,**kwargs):
        assert kwargs==dict(DatabaseName=publication.DATABASE,Name=publication.TABLE)
        if self.table is None:raise FakeError("EntityNotFoundException")
        return dict(Table=deepcopy(self.table))
    def get_partitions(self,**kwargs):
        assert kwargs["DatabaseName"]==publication.DATABASE and kwargs["TableName"]==publication.TABLE
        values=sorted(self.parts.values(),key=lambda p:p["Values"])
        start=int(kwargs.get("NextToken",0));out=dict(Partitions=deepcopy(values[start:start+5]))
        if start+5<len(values):out["NextToken"]=str(start+5)
        return out
    def create_table(self,**kwargs):
        self.calls.append(("create_table",deepcopy(kwargs)))
        if self.table is not None:raise FakeError("AlreadyExistsException")
        self.table=dict(deepcopy(kwargs["TableInput"]),VersionId="1")
    def _mutate(self,name,kwargs,apply):
        assert kwargs["DatabaseName"]==publication.DATABASE and kwargs["TableName"]==publication.TABLE
        self.calls.append((name,deepcopy(kwargs)));self.mutations+=1
        if self.fail_on==self.mutations and not self.ambiguous:raise RuntimeError("Catalog failed before commit")
        apply()
        if self.fail_on==self.mutations:raise TimeoutError("Catalog response lost after commit")
    def create_partition(self,**kwargs):
        def apply():
            value=deepcopy(kwargs["PartitionInput"]);key=tuple(value["Values"])
            if key in self.parts:raise FakeError("AlreadyExistsException")
            self.parts[key]=value
        self._mutate("create_partition",kwargs,apply)
    def update_partition(self,**kwargs):
        def apply():self.parts[tuple(kwargs["PartitionValueList"])]=deepcopy(kwargs["PartitionInput"])
        self._mutate("update_partition",kwargs,apply)
    def delete_partition(self,**kwargs):
        self._mutate("delete_partition",kwargs,lambda:self.parts.pop(tuple(kwargs["PartitionValues"])))


def handoff(client,cfg,*,first_payload=b"synthetic parquet"):
    """Full-date synthetic receipts; protocol tests do not certify Parquet bytes."""
    from sequence_full_source import owner_dates,dataset_spec
    days=list(owner_dates(cfg["dataset"]))
    root=STAGING_ROOT+"dataset="+cfg["dataset"]+"/"+cfg["run_id"]+"/"+cfg["attempt_id"]+"/"
    store=publication._Store(client)
    schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA]
    builder=dict(version=cfg["builder_version"],fingerprint=cfg["builder_fingerprint"])
    batches=[]
    for i,day in enumerate(days):
        active=i==0 and first_payload is not None
        base=root+"batches/"+day+"/";counts={"2":1 if active else 0,"3":1 if active else 0}
        if active:client.seed((base+"data/dataset="+cfg["dataset"]+"/start_date="+day+"/part-000.parquet").split("/",3)[3],first_payload)
        records=store.inventory(base+"data/")
        receipt=dict(dataset=cfg["dataset"],start_date=day,run_id=cfg["run_id"],attempt_id=cfg["attempt_id"],
            staging_location=base+"data/",files=records,inventory_fingerprint=fingerprint_source_inventory(records),
            counts=counts,row_count=sum(counts.values()),parquet_files=len(records),size_bytes=sum(r["size"] for r in records),
            schema_valid=True,values_valid=True,validation_status="VALIDATED",source=cfg["expected_source"],
            schema=schema,builder=builder,rule_version="v1")
        status=dict(state="VALIDATED",revision=2,run_id=cfg["run_id"],attempt_id=cfg["attempt_id"],
            owner_token=cfg["owner_token"],dataset=cfg["dataset"],start_date=day,timezone="UTC",expected_counts=counts)
        client.seed((base+"metadata/status.json").split("/",3)[3],publication._raw(status))
        uri=base+"metadata/validation.json";client.seed(uri.split("/",3)[3],publication._raw(receipt))
        batches.append(dict(receipt,receipt_uri=uri,receipt_fingerprint=_digest(receipt)))
    spec=dataset_spec(cfg["dataset"])
    m=dict(format_version=1,manifest_kind="full_sequence_generation_handoff",state="STAGED_VALIDATED",published=False,
        catalog_updated=False,publication_required=True,dataset=cfg["dataset"],run_id=cfg["run_id"],attempt_id=cfg["attempt_id"],
        owner_token=cfg["owner_token"],owner_dates=days,source_dates=days,source=cfg["expected_source"],source_after=cfg["expected_source"],
        schema=schema,builder=builder,rule_version="v1",batches=batches,counts={"2":int(first_payload is not None),"3":int(first_payload is not None)},row_count=2*int(first_payload is not None),
        parquet_files=int(first_payload is not None),size_bytes=len(first_payload or b""),source_validation=dict(row_count=spec["rows"],laundering_rows=spec["laundering"],
            identity_check_passed=True,full_selected_snapshot_checked=True,verification_scope="provided_dataframe_only",
            dataset_wide_verified=False,checked_dates=days,source=cfg["expected_source"]))
    uri=root+"metadata/run.json";etag=client.seed(uri.split("/",3)[3],publication._raw(m))
    return dict(manifest=m,manifest_uri=uri,etag=etag)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.cfg=config();self.client=PublicationS3();self.catalog=Catalog()
        self.handoff=handoff(self.client,self.cfg)
        self.identity=MagicMock();self.identity.meta.region_name="us-east-1"
        self.identity.get_caller_identity.return_value={"Account":self.cfg["account_id"]}
        self.context=MagicMock();self.context.spark_session.conf.get.return_value="UTC"
        self.transport=MagicMock()

    def publish(self,**kwargs):
        with patch.object(publication,"inspect_full_source",return_value=deepcopy(self.cfg["expected_source"])), \
             patch("sequence_staging.validate_sequence_output") as validate:
            validate.side_effect=lambda frame,dataset,day,tz,counts:dict(counts=counts,row_count=sum(counts.values()),schema_valid=True,values_valid=True)
            return publication.publish_full_generation(self.cfg,self.handoff,allow_publication=True,readers_paused=True,
                maintenance_evidence="Synthetic table-wide maintenance agreement",max_copy_bytes=1000000,
                glue_context=self.context,catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity,
                transport=self.transport,**kwargs)

    def production_data(self):return {k:v["raw"] for k,v in self.client.objects.items() if k.startswith("bq2_sequences/")}

    def test_nonempty_success_marker_is_counted_but_never_published(self):
        batch=self.handoff["manifest"]["batches"][0]
        marker=batch["staging_location"]+"_SUCCESS"
        self.client.seed(marker.split("/",3)[3],b'{"committed":true}')
        records=publication._Store(self.client).inventory(batch["staging_location"])
        receipt_uri=batch["receipt_uri"]
        receipt=publication._Store(self.client).json(receipt_uri)[0]
        receipt.update(files=records,inventory_fingerprint=fingerprint_source_inventory(records),
                       size_bytes=sum(r["size"] for r in records))
        self.client.seed(receipt_uri.split("/",3)[3],publication._raw(receipt))
        batch.update(receipt,receipt_fingerprint=_digest(receipt))
        manifest=self.handoff["manifest"]
        manifest["size_bytes"]=sum(b["size_bytes"] for b in manifest["batches"])
        self.handoff["etag"]=self.client.seed(self.handoff["manifest_uri"].split("/",3)[3],publication._raw(manifest))
        result=self.publish()
        self.assertTrue(result["published"])
        self.assertEqual(len(self.production_data()),1)
        self.assertTrue(all(k.endswith(".parquet") for k in self.production_data()))
        self.assertEqual(self.client.objects[marker.split("/",3)[3]]["raw"],b'{"committed":true}')

    def test_first_hi_and_li_publication_exact_schema_and_all_dates(self):
        for dataset,days in (("HI-Medium",28),("LI-Medium",27)):
            self.setUp();self.cfg=config(dataset);self.client=PublicationS3();self.handoff=handoff(self.client,self.cfg)
            original={k:v["raw"] for k,v in self.client.objects.items()}
            result=self.publish()
            self.assertTrue(result["published"]);self.assertTrue(result["catalog_updated"])
            self.assertEqual(result["publication"]["state"],"PUBLISHED")
            self.assertEqual(len(self.catalog.parts),days)
            self.assertEqual(self.catalog.table["StorageDescriptor"]["Columns"],publication._columns())
            self.assertEqual(self.catalog.table["PartitionKeys"],[dict(Name="dataset",Type="string"),dict(Name="start_date",Type="string")])
            self.assertTrue(all(v["StorageDescriptor"]["Location"].startswith(publication.ROOT+"dataset="+dataset+"/versions/") for v in self.catalog.parts.values()))
            self.assertEqual(result["counts"],{"2":1,"3":1})
            for key,raw in original.items():self.assertEqual(self.client.objects[key]["raw"],raw)
            lock=publication._Store(self.client).json(publication.LOCK)[0]
            self.assertEqual(lock["state"],"RELEASED")

    def test_rerun_preserves_other_dataset_old_files_and_empty_dates(self):
        first=self.publish();previous=deepcopy(self.catalog.parts);old_data=self.production_data()
        other_cfg=config("LI-Medium");other_cfg["run_id"]="li_run"
        self.cfg=other_cfg;self.handoff=handoff(self.client,self.cfg);self.publish()
        other=deepcopy({k:v for k,v in self.catalog.parts.items() if k[0]=="LI-Medium"})
        self.cfg=config();self.cfg["run_id"]="hi_rerun";self.cfg["owner_token"]="owner2"
        self.handoff=handoff(self.client,self.cfg,first_payload=b"replacement parquet")
        result=self.publish()
        self.assertEqual({k:v for k,v in self.catalog.parts.items() if k[0]=="LI-Medium"},other)
        for key,raw in old_data.items():self.assertEqual(self.client.objects[key]["raw"],raw)
        self.assertNotEqual(self.catalog.parts[("HI-Medium","2022-09-01")],previous[("HI-Medium","2022-09-01")])
        empty=self.catalog.parts[("HI-Medium","2022-09-02")]["StorageDescriptor"]["Location"]
        self.assertEqual(publication._Store(self.client).inventory(empty),[])
        self.assertEqual(result["publication"]["row_count"] if "row_count" in result["publication"] else result["row_count"],2)

    def test_failed_or_tampered_generation_rejected_before_any_publication_write(self):
        for mode in ("state","schema","source","receipt","inventory","counts"):
            self.setUp()
            if mode in ("state","schema","source","counts"):
                m=self.handoff["manifest"]
                if mode=="state":m["state"]="FAILED"
                elif mode=="schema":m["schema"]=[]
                elif mode=="source":m["source"]["source_fingerprint"]="e"*64
                else:m["row_count"]=100
                self.handoff["etag"]=self.client.seed(self.handoff["manifest_uri"].split("/",3)[3],publication._raw(m))
            elif mode=="receipt":self.client.seed(self.handoff["manifest"]["batches"][0]["receipt_uri"].split("/",3)[3],b"{}")
            else:
                key=self.handoff["manifest"]["batches"][0]["files"][0]["path"].split("/",3)[3];self.client.seed(key,b"changed")
            with self.subTest(mode=mode),self.assertRaises(ValueError):self.publish()
            self.assertFalse(any(c[0]=="put" for c in self.client.calls));self.assertEqual(self.catalog.calls,[])

    def test_incompatible_existing_table_is_not_modified(self):
        self.catalog.table=publication.table_input();self.catalog.table["PartitionKeys"][1]["Type"]="date"
        with self.assertRaisesRegex(ValueError,"Incompatible"):self.publish()
        self.assertEqual(self.catalog.calls,[]);self.assertEqual(self.production_data(),{})

    def test_empty_rerun_replaces_stale_rows_without_deleting_old_files(self):
        self.publish();old=self.production_data()
        self.cfg=dict(self.cfg,run_id="empty_run",owner_token="empty_owner")
        self.handoff=handoff(self.client,self.cfg,first_payload=None)
        result=self.publish()
        self.assertEqual(result["row_count"],0)
        for part in self.catalog.parts.values():
            self.assertEqual(publication._Store(self.client).inventory(part["StorageDescriptor"]["Location"]),[])
        for k,v in old.items():self.assertEqual(self.client.objects[k]["raw"],v)

    def test_selected_stale_catalog_date_removed_but_old_files_retained(self):
        self.catalog.table=publication.table_input()
        location=publication.ROOT+"dataset=HI-Medium/legacy/start_date=2022-09-29/"
        self.catalog.parts[("HI-Medium","2022-09-29")]=dict(Values=["HI-Medium","2022-09-29"],
            StorageDescriptor=dict(publication.table_input()["StorageDescriptor"],Location=location))
        key=(location+"old.parquet").split("/",3)[3];self.client.seed(key,b"old data")
        self.publish()
        self.assertNotIn(("HI-Medium","2022-09-29"),self.catalog.parts)
        self.assertEqual(self.client.objects[key]["raw"],b"old data")

    def test_failed_rerun_before_catalog_preserves_previous_publication(self):
        self.publish();previous=deepcopy(self.catalog.parts);old=self.production_data()
        self.cfg=dict(self.cfg,run_id="failed_run",owner_token="new_owner")
        self.handoff=handoff(self.client,self.cfg)
        self.client.data_failure=True
        with self.assertRaises(RuntimeError):self.publish()
        self.assertEqual(self.catalog.parts,previous)
        for k,v in old.items():self.assertEqual(self.client.objects[k]["raw"],v)

    def test_metadata_lost_response_reconciles_without_blind_retry(self):
        self.client.lose_response=True
        self.assertTrue(self.publish()["published"])
        puts=[c for c in self.client.calls if c[0]=="put" and c[1]["Key"]==publication.LOCK.split("/",3)[3]]
        self.assertEqual(len(puts),2) # exclusive claim plus release, no retry

    def test_failure_recording_preserves_original_copy_exception(self):
        original=self.client.put_object
        error=RuntimeError("original copy failure")
        def put(**kw):
            if kw["Key"].startswith("bq2_sequences/"):raise error
            if kw["Key"].endswith("publication.json") and json.loads(kw["Body"]).get("state")=="RECOVERY_REQUIRED":
                raise RuntimeError("failure receipt unavailable")
            return original(**kw)
        with patch.object(self.client,"put_object",side_effect=put),self.assertRaises(RuntimeError) as caught:self.publish()
        self.assertIs(caught.exception,error)
        self.assertIn("failure recording unavailable",error.__notes__[0])

    def test_unexpected_table_creation_and_stale_table_version_stop(self):
        for already_exists in (False,True):
            self.setUp()
            if already_exists:self.catalog.table=dict(publication.table_input(),VersionId="1")
            original=self.client.put_object
            def mutate(**kw):
                result=original(**kw)
                if kw["Key"].startswith("bq2_sequences/"):
                    self.catalog.table=dict(publication.table_input(),VersionId="2")
                return result
            with patch.object(self.client,"put_object",side_effect=mutate),self.assertRaisesRegex(ValueError,"Stale|Unexpected table"):
                self.publish()
            self.assertEqual(self.catalog.calls,[])

    def test_copy_failure_and_lost_response_resume_without_overwrite(self):
        for mode in ("failure","lost"):
            self.setUp()
            if mode=="failure":self.client.data_failure=True
            else:self.client.data_response_lost=True
            with self.subTest(mode=mode),self.assertRaises((RuntimeError,TimeoutError)):self.publish()
            self.assertEqual(self.catalog.calls,[])
            self.client.data_failure=False
            before=len([c for c in self.client.calls if c[0]=="put" and c[1]["Key"].startswith("bq2_sequences/")])
            result=self.publish(resume=True)
            self.assertTrue(result["published"])
            after=len([c for c in self.client.calls if c[0]=="put" and c[1]["Key"].startswith("bq2_sequences/")])
            self.assertEqual(after-before,0 if mode=="lost" else 1)

    def test_failed_and_ambiguous_catalog_updates_reconcile_safely(self):
        for ambiguous in (False,True):
            self.setUp();self.catalog.fail_on=2;self.catalog.ambiguous=ambiguous
            with self.subTest(ambiguous=ambiguous),self.assertRaises((RuntimeError,TimeoutError)):self.publish()
            self.assertEqual(len(self.catalog.parts),2 if ambiguous else 1)
            self.catalog.fail_on=None
            result=self.publish(resume=True)
            self.assertTrue(result["published"]);self.assertEqual(len(self.catalog.parts),28)
            creates=[call for call in self.catalog.calls if call[0]=="create_partition" and call[1]["PartitionInput"]["Values"]==["HI-Medium","2022-09-01"]]
            self.assertEqual(len(creates),1)

    def test_destination_corruption_prevents_catalog_switch(self):
        self.client.corrupt_destination=True
        with self.assertRaisesRegex(ValueError,"SHA-256"):self.publish()
        self.assertEqual(self.catalog.calls,[])

    def test_partial_copy_preserves_prior_file_and_resume_never_overwrites(self):
        batch=self.handoff["manifest"]["batches"][0]
        path=batch["staging_location"]+"dataset=HI-Medium/start_date=2022-09-01/part-001.parquet"
        self.client.seed(path.split("/",3)[3],b"second parquet")
        records=publication._Store(self.client).inventory(batch["staging_location"])
        batch.update(files=records,inventory_fingerprint=fingerprint_source_inventory(records),
            parquet_files=2,size_bytes=sum(r["size"] for r in records))
        raw={k:v for k,v in batch.items() if k not in ("receipt_uri","receipt_fingerprint")}
        batch["receipt_fingerprint"]=_digest(raw)
        self.client.seed(batch["receipt_uri"].split("/",3)[3],publication._raw(raw))
        self.handoff["manifest"].update(parquet_files=2,size_bytes=batch["size_bytes"])
        self.handoff["etag"]=self.client.seed(self.handoff["manifest_uri"].split("/",3)[3],publication._raw(self.handoff["manifest"]))
        original=self.client.put_object
        count=0
        def put(**kw):
            nonlocal count
            if kw["Key"].startswith("bq2_sequences/"):
                count+=1
                if count==2:raise RuntimeError("second copy failed")
            return original(**kw)
        with patch.object(self.client,"put_object",side_effect=put),self.assertRaisesRegex(RuntimeError,"second copy"):self.publish()
        self.assertEqual(len(self.production_data()),1);self.assertEqual(self.catalog.calls,[])
        first=deepcopy(self.production_data())
        self.assertTrue(self.publish(resume=True)["published"])
        for k,v in first.items():self.assertEqual(self.production_data()[k],v)
        self.assertEqual(len(self.production_data()),2)

    def test_incorrect_catalog_readback_never_reports_success(self):
        original=self.catalog.create_partition
        def wrong(**kw):
            original(**kw)
            part=self.catalog.parts[tuple(kw["PartitionInput"]["Values"])]
            part["StorageDescriptor"]["Location"]=publication.ROOT+"dataset=HI-Medium/wrong/"
        self.catalog.create_partition=wrong
        with self.assertRaisesRegex(ValueError,"read-back"):self.publish()
        receipt=[json.loads(obj["raw"]) for key,obj in self.client.objects.items() if key.endswith("publication.json")][0]
        self.assertEqual(receipt["state"],"RECOVERY_REQUIRED");self.assertFalse(receipt["published"])

    def test_concurrent_publisher_and_stale_attempt_rejected(self):
        self.client.data_failure=True
        with self.assertRaises(RuntimeError):self.publish()
        self.cfg=dict(self.cfg,run_id="another_run",owner_token="another_owner")
        self.handoff=handoff(self.client,self.cfg)
        with self.assertRaisesRegex(ValueError,"Another publisher"):self.publish()
        self.assertEqual(self.catalog.calls,[])

    def test_conflicting_catalog_state_is_never_overwritten_on_resume(self):
        self.catalog.fail_on=2
        with self.assertRaises(RuntimeError):self.publish()
        key=("HI-Medium","2022-09-02")
        self.catalog.parts[key]=deepcopy(self.catalog.parts[("HI-Medium","2022-09-01")])
        self.catalog.parts[key]["Values"]=list(key)
        self.catalog.parts[key]["StorageDescriptor"]["Location"]=publication.ROOT+"dataset=HI-Medium/foreign/start_date=2022-09-02/"
        before=deepcopy(self.catalog.parts[key]);self.catalog.fail_on=None
        with self.assertRaisesRegex(ValueError,"Conflicting"):self.publish(resume=True)
        self.assertEqual(self.catalog.parts[key],before)

    def test_tampered_recovery_plan_cannot_target_other_dataset(self):
        self.catalog.fail_on=2
        with self.assertRaises(RuntimeError):self.publish()
        self.catalog.fail_on=None
        uri=next(k for k in self.client.objects if k.endswith("publication.json"))
        receipt=json.loads(self.client.objects[uri]["raw"])
        receipt["actions"][1]["after"]["Values"][0]="LI-Medium"
        # Even a recomputed checksum cannot authorize an out-of-scope action.
        receipt["plan_fingerprint"]=publication._plan_fingerprint(receipt)
        self.client.seed(uri,publication._raw(receipt))
        count=len(self.catalog.calls)
        with self.assertRaisesRegex(ValueError,"escapes"):self.publish(resume=True)
        self.assertEqual(len(self.catalog.calls),count)

    def test_altered_recovery_plan_fingerprint_is_rejected(self):
        self.client.data_failure=True
        with self.assertRaises(RuntimeError):self.publish()
        uri=next(k for k in self.client.objects if k.endswith("publication.json"))
        receipt=json.loads(self.client.objects[uri]["raw"]);receipt["plan_fingerprint"]="0"*64
        self.client.seed(uri,publication._raw(receipt));self.client.data_failure=False
        with self.assertRaisesRegex(ValueError,"state/plan"):self.publish(resume=True)
        self.assertEqual(self.catalog.calls,[])

    def test_terminal_recovery_is_immutable_and_no_duplicate_catalog_actions(self):
        result=self.publish();raw=self.client.objects[result["publication_uri"].split("/",3)[3]]["raw"]
        calls=deepcopy(self.catalog.calls)
        again=self.publish(resume=True)
        self.assertTrue(again["published"]);self.assertEqual(self.catalog.calls,calls)
        self.assertEqual(self.client.objects[result["publication_uri"].split("/",3)[3]]["raw"],raw)

    def test_stage_only_recovery_can_begin_publication_without_regeneration(self):
        original=self.client.objects[self.handoff["manifest_uri"].split("/",3)[3]]["raw"]
        self.assertTrue(self.publish(resume=True)["published"])
        self.assertEqual(self.client.objects[self.handoff["manifest_uri"].split("/",3)[3]]["raw"],original)

    def test_missing_publication_receipt_with_active_lock_blocks_recovery(self):
        self.client.data_failure=True
        with self.assertRaises(RuntimeError):self.publish()
        key=next(k for k in self.client.objects if k.endswith("publication.json"))
        del self.client.objects[key];self.client.data_failure=False
        with self.assertRaisesRegex(ValueError,"Another publisher"):self.publish(resume=True)
        self.assertEqual(self.catalog.calls,[])

    def test_source_refresh_and_activation_block_writes(self):
        with patch.object(publication,"inspect_full_source",return_value=dict(self.cfg["expected_source"],source_fingerprint="e"*64)):
            with self.assertRaisesRegex(ValueError,"Frozen source"):
                publication.publish_full_generation(self.cfg,self.handoff,allow_publication=True,readers_paused=True,
                    maintenance_evidence="synthetic",max_copy_bytes=1000000,glue_context=self.context,
                    catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity,transport=self.transport)
        for allow,pause in ((False,True),(True,False),("true",True),(1,True)):
            with self.assertRaises(ValueError):publication.publish_full_generation(self.cfg,self.handoff,
                allow_publication=allow,readers_paused=pause,maintenance_evidence="synthetic",max_copy_bytes=1000000,
                glue_context=self.context,catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity)
        self.assertFalse(any(c[0]=="put" for c in self.client.calls))

    def test_validation_failure_never_switches_catalog(self):
        with patch.object(publication,"inspect_full_source",return_value=self.cfg["expected_source"]), \
             patch("sequence_staging.validate_sequence_output",side_effect=ValueError("bad output")), \
             self.assertRaisesRegex(ValueError,"bad output"):
            publication.publish_full_generation(self.cfg,self.handoff,allow_publication=True,readers_paused=True,
                maintenance_evidence="synthetic",max_copy_bytes=1000000,glue_context=self.context,
                catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity,transport=self.transport)
        self.assertEqual(self.catalog.calls,[])


class PublicationTransport(LocalTransport):
    def read(self,spark,uri,files,status):
        for file in files:
            path=Path(self.path(file));path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(self.client.objects[file.split("/",3)[3]]["raw"])
        return super().read(spark,uri,files,status)


class RealPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        os.environ["PYSPARK_PYTHON"]=sys.executable
        cls.spark=(SparkSession.builder.master("local[2]").appName("full-publication-integration")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2")
            .config("spark.ui.enabled","false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR");cls.addClassCleanup(cls.spark.stop)

    def test_real_builder_staging_copy_validation_catalog_and_receipt(self):
        import glue_full_generation as entry
        from test_glue_full_generation import argv
        from sequence_full_source import dataset_spec
        from test_full_generation import snapshot
        cfg=config();client=PublicationS3();catalog=Catalog()
        identity=MagicMock();identity.meta.region_name="us-east-1";identity.get_caller_identity.return_value={"Account":cfg["account_id"]}
        context=SimpleNamespace(spark_session=self.spark)
        source_catalog=MagicMock();source_catalog.meta.region_name="us-east-1"
        rows=[transaction(1,"2022-09-26 09:00",(1,"A"),(2,"B"),dataset="HI-Medium",label=1),
              transaction(2,"2022-09-27 09:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
              transaction(3,"2022-09-28 09:00",(2,"C"),(3,"D"),dataset="HI-Medium")]
        frame=self.spark.createDataFrame(rows,CLEANED_SCHEMA)
        synthetic=dict(dataset_spec("HI-Medium"),rows=3,laundering=1)
        with tempfile.TemporaryDirectory() as folder:
            transport=PublicationTransport(folder,client)
            job=MagicMock()
            dependencies=dict(glue_context=context,catalog_client=catalog,s3_client=client,identity_client=identity)
            original_generation=[]
            actual_publish=publication.publish_full_generation
            def publish(*args,**kwargs):
                generated=args[1]
                original_generation.append((generated["manifest_uri"],client.objects[generated["manifest_uri"].split("/",3)[3]]["raw"]))
                return actual_publish(*args,**kwargs)
            with patch("sequence_full_generation.inspect_full_source",return_value=snapshot()), \
                 patch("sequence_full_generation.read_full_catalog",return_value=frame), \
                 patch("sequence_full_generation.dataset_spec",return_value=synthetic), \
                 patch.object(publication,"inspect_full_source",return_value=cfg["expected_source"]), \
                 patch.object(publication,"dataset_spec",return_value=synthetic), \
                 patch("sequence_full_staging._SparkParquetTransport",return_value=transport), \
                 patch.object(publication,"_SparkParquetTransport",return_value=transport), \
                 patch.object(entry,"_initialize",return_value=(dependencies,job)), \
                 patch.object(entry,"publish_full_generation",side_effect=publish), \
                 patch("sys.stdout"):
                self.assertEqual(entry.main(argv()),0)
            job.commit.assert_called_once()
            result=[json.loads(obj["raw"]) for key,obj in client.objects.items() if key.endswith("publication.json")][0]
            self.assertTrue(result["published"]);self.assertEqual(result["binding"]["counts"],{"2":2,"3":1})
            self.assertEqual(len(catalog.parts),28)
            uri,original=original_generation[0]
            self.assertEqual(client.objects[uri.split("/",3)[3]]["raw"],original)
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)


if __name__=="__main__":unittest.main()
