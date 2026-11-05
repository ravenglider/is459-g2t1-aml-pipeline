"""Pure manifest and fake-S3 conditional protocol tests; never network/Spark IO."""
from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sequence_sample_run as run
import sequence_s3 as s3
from test_sequence_sample_staging import configuration
from test_sequence_s3 import FakeS3
from sequence_aws import SAMPLE_DATES


def manifest():
    return run.create_staging_run(configuration(),"attempt",10,owner_token="owner",builder_version="synthetic_commit")


def generation(m, counts):
    fp = m["config"]["expected_source"]
    return dict(status="GENERATION_COMPLETE",dataset="sample",owner_date="2022-09-01",
        input_dates=list(SAMPLE_DATES[:3]),run_id=m["run_id"],
        builder_fingerprint=m["config"]["builder_fingerprint"],synthetic_baseline=False,published=False,
        source_before=deepcopy(fp),source_after=deepcopy(fp),sample_validation=dict(total_rows=500000,
            laundering_rows=35,required_fields_valid=True,timestamp_date_consistent=True,
            timestamp_coverage={"timezone":"UTC"},counts_by_date=[dict(txn_date=d,rows=83333 if i<5 else 83335,
                laundering=35 if i==0 else 0) for i,d in enumerate(SAMPLE_DATES)],
            identity_check=dict(dataset="sample",row_count=500000,identity_check_passed=True,
                verification_scope="provided_dataframe_only",dataset_wide_verified=False)),
        validation=dict(counts=counts,row_count=sum(counts.values()),schema_valid=True,values_valid=True))


def receipt(client,m,counts):
    transport = MagicMock()
    transport.write.side_effect = lambda frame,uri,maximum: client.seed(s3._key(uri)+"dataset=sample/start_date=2022-09-01/part.snappy.parquet")
    checked = dict(counts=counts,row_count=sum(counts.values()),schema_valid=True,values_valid=True)
    with patch("sequence_staging.validate_sequence_output",return_value=checked):
        uri = s3.stage_sequence_batch_s3(client,MagicMock(),m["run_id"],m["attempt_id"],dataset="sample",
            destination="development",start_date="2022-09-01",timezone="UTC",timezone_verified=True,
            timezone_evidence="synthetic UTC",expected_counts=counts,
            source_fingerprint=m["config"]["expected_source"]["source_fingerprint"],max_records_per_file=10,transport=transport)
    return s3._load_validated(client,uri)


class StagingRunTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3()
        self.created = manifest()

    def staged(self,counts=None):
        counts = {2:1,3:1} if counts is None else counts
        evidence = receipt(self.client,self.created,counts)
        return run.transition_staging_run(self.created,"STAGING",generation=generation(self.created,counts),receipt=evidence)

    def test_creation_pending_scopes_and_no_publication_fingerprints(self):
        m = self.created
        self.assertEqual(m["manifest_kind"],"bounded_sample_staging_run")
        self.assertIsNone(m["generation"])
        self.assertIsNone(m["attempt_evidence"])
        self.assertNotIn("previous_publication",m)
        self.assertNotIn("expected_current_publication_fingerprint",m)
        self.assertFalse(m["published"])
        self.assertEqual(m["scopes"]["observed_extract"]["dates"],list(SAMPLE_DATES))
        self.assertFalse(m["scopes"]["observed_extract"]["underlying_dataset_complete"])
        self.assertEqual(m["scopes"]["generation_input"],list(SAMPLE_DATES[:3]))
        self.assertTrue(m["manifest_uri"].endswith("/staging_run.json"))
        self.assertEqual(self.client.calls,[])

    def test_invalid_scope_config_ids_and_file_sizes(self):
        for field,value in (("dataset","HI-Medium"),("owner_date","2022-09-02"),("timezone","Asia/Singapore"),
                ("timezone_evidence",""),("no_refresh_acknowledged",False),("expected_source",{}),
                ("staging_root","s3://other/")):
            with self.subTest(field=field),self.assertRaises(ValueError):
                run.create_staging_run(dict(configuration(),**{field:value}),"attempt",10,owner_token="owner",builder_version="version")
        for field,value in (("attempt_id","../bad"),("owner_token","a/b"),("max_records_per_file",True),
                ("max_records_per_file",0),("builder_version","")):
            options=dict(config=configuration(),attempt_id="attempt",max_records_per_file=10,owner_token="owner",builder_version="version")
            options[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError): run.create_staging_run(**options)

    def test_nonempty_and_explicit_empty_complete_lifecycles(self):
        for counts in ({2:1,3:1},{2:0,3:0}):
            self.client=FakeS3()
            tag=run.write_staging_run_s3(self.client,self.created)
            staged=self.staged(counts)
            tag2=run.write_staging_run_s3(self.client,staged,expected_revision=0,expected_etag=tag)
            final=run.transition_staging_run(staged,"STAGED_VALIDATED",current_source=self.created["config"]["expected_source"])
            run.write_staging_run_s3(self.client,final,expected_revision=1,expected_etag=tag2,
                current_source=self.created["config"]["expected_source"])
            self.assertEqual(final["attempt_evidence"]["row_count"],sum(counts.values()))
            self.assertFalse(final["published"])
            self.assertFalse(final["generation"]["sample_validation"]["identity_check"]["dataset_wide_verified"])
            self.assertEqual((self.created["revision"],staged["revision"],final["revision"]),(0,1,2))

    def test_pending_and_invalid_completion_evidence_rejected(self):
        evidence=receipt(self.client,self.created,{2:1,3:1})
        valid=generation(self.created,{2:1,3:1})
        cases=[None]
        for key,value in (("status","OUTPUT_VALIDATED_CONTEXT_OPEN"),("source_after",{}),
                ("synthetic_baseline",True),("published",True),("input_dates",[]),("builder_fingerprint","0"*64)):
            cases.append(dict(valid,**{key:value}))
        global_claim=deepcopy(valid)
        global_claim["sample_validation"]["identity_check"]["dataset_wide_verified"]=True
        cases.append(global_claim)
        for value in cases:
            with self.subTest(value=value),self.assertRaises(ValueError):
                run.transition_staging_run(self.created,"STAGING",generation=value,receipt=evidence)
        with self.assertRaises(ValueError):
            run.transition_staging_run(self.created,"STAGING",generation=valid,receipt=None)
        wrong=deepcopy(evidence)
        wrong["status"]["state"]="WRITING"
        with self.assertRaises(ValueError): run.transition_staging_run(self.created,"STAGING",generation=valid,receipt=wrong)

    def test_states_failure_and_terminal_immutability(self):
        for state in ("READY_TO_PLAN","PLANNED","PUBLISHING","PUBLISHED","STAGED_VALIDATED"):
            with self.subTest(state=state),self.assertRaises(ValueError): run.transition_staging_run(self.created,state)
        with self.assertRaises(ValueError): run.transition_staging_run(self.created,"FAILED")
        failed=run.transition_staging_run(self.created,"FAILED",error="generation failed")
        self.assertIsNone(failed["generation"])
        staged=self.staged()
        final=run.transition_staging_run(staged,"STAGED_VALIDATED",current_source=self.created["config"]["expected_source"])
        for terminal in (failed,final):
            for state in run.STATES:
                with self.subTest(state=state),self.assertRaises(ValueError): run.transition_staging_run(terminal,state,error="change")
        with self.assertRaises(ValueError): run.transition_staging_run(staged,"STAGED_VALIDATED",current_source={})

    def test_publication_apis_reject_distinct_format(self):
        from sequence_manifest import _check_manifest
        from sequence_publication import plan_development_publication
        for state in ("CREATED","READY_TO_PLAN","PUBLISHED"):
            m=dict(self.created,state=state)
            with self.assertRaises(ValueError): _check_manifest(m)
            with self.assertRaises(ValueError):
                plan_development_publication(m,"pub",["2022-09-01"],frozen_run_fingerprint="a"*64,
                    table={},partitions=[],staging_evidence={},recovery_evidence={},current_publication=None,
                    current_source_fingerprint="a"*64,owner="owner",maintenance_ack={})
            with self.assertRaises(ValueError): s3.write_manifest_s3(self.client,m,owner_token="owner")
        self.assertEqual(self.client.calls,[])

    def test_conditional_storage_read_and_terminal_replay(self):
        tag=run.write_staging_run_s3(self.client,self.created)
        self.assertEqual(run.read_staging_run_s3(self.client,self.created["run_id"]),(self.created,tag))
        staged=self.staged()
        tag2=run.write_staging_run_s3(self.client,staged,expected_revision=0,expected_etag=tag)
        final=run.transition_staging_run(staged,"STAGED_VALIDATED",current_source=self.created["config"]["expected_source"])
        tag3=run.write_staging_run_s3(self.client,final,expected_revision=1,expected_etag=tag2,current_source=self.created["config"]["expected_source"])
        puts=sum(name=="put" for name,_ in self.client.calls)
        self.assertEqual(run.write_staging_run_s3(self.client,final,expected_revision=1,expected_etag=tag2),tag3)
        self.assertEqual(sum(name=="put" for name,_ in self.client.calls),puts)
        with self.assertRaises(ValueError):
            run.write_staging_run_s3(self.client,dict(final,revision=3),expected_revision=2,expected_etag=tag3)

    def test_exclusive_create_conflicts_and_immutable_identity(self):
        tag=run.write_staging_run_s3(self.client,self.created)
        with self.assertRaises(ValueError): run.write_staging_run_s3(self.client,self.created)
        staged=self.staged()
        for changes in ({"storage_owner":"other"},{"max_records_per_file":20},
                {"builder_version":"other"},{"revision":2}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                run.write_staging_run_s3(self.client,dict(staged,**changes),expected_revision=0,expected_etag=tag)
        for changes in ({"expected_revision":2},{"expected_etag":"stale"}):
            kwargs=dict(expected_revision=0,expected_etag=tag)
            kwargs.update(changes)
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                run.write_staging_run_s3(self.client,staged,**kwargs)

    def test_lost_response_exact_reconciliation_and_uncertain_failures(self):
        self.client.lose_response=True
        tag=run.write_staging_run_s3(self.client,self.created)
        staged=self.staged()
        self.client.lose_response=True
        run.write_staging_run_s3(self.client,staged,expected_revision=0,expected_etag=tag)
        self.assertEqual(run.read_staging_run_s3(self.client,self.created["run_id"])[0],staged)
        other=run.create_staging_run(dict(configuration(),run_id="other"),"attempt",10,owner_token="owner",builder_version="version")
        self.client.fail_before=True
        with self.assertRaises(TimeoutError): run.write_staging_run_s3(self.client,other)
        with patch.object(self.client,"put_object",side_effect=TimeoutError("unknown")):
            failed=run.transition_staging_run(staged,"FAILED",error="failure")
            with self.assertRaises(TimeoutError):
                run.write_staging_run_s3(self.client,failed,expected_revision=1,
                    expected_etag=run.read_staging_run_s3(self.client,staged["run_id"])[1])

    def test_receipt_change_or_stale_source_blocks_terminal_write(self):
        tag=run.write_staging_run_s3(self.client,self.created)
        staged=self.staged()
        tag2=run.write_staging_run_s3(self.client,staged,expected_revision=0,expected_etag=tag)
        final=run.transition_staging_run(staged,"STAGED_VALIDATED",current_source=self.created["config"]["expected_source"])
        with self.assertRaises(ValueError):
            run.write_staging_run_s3(self.client,final,expected_revision=1,expected_etag=tag2,current_source={})
        key=s3._key(staged["attempt_uri"]+"metadata/validation.json")
        self.client.seed(key,b'{}')
        with self.assertRaises(ValueError):
            run.write_staging_run_s3(self.client,final,expected_revision=1,expected_etag=tag2,current_source=self.created["config"]["expected_source"])

    def test_concurrent_cas_conflict_and_missing_response_etag_are_not_retried(self):
        tag=run.write_staging_run_s3(self.client,self.created)
        failed=run.transition_staging_run(self.created,"FAILED",error="failure")
        original=self.client.put_object
        key=s3._key(self.created["manifest_uri"])
        def race(**kwargs):
            self.client.seed(key,self.client.objects[key]["raw"])
            return original(**kwargs)
        with patch.object(self.client,"put_object",side_effect=race) as write:
            with self.assertRaisesRegex(ValueError,"conflict"):
                run.write_staging_run_s3(self.client,failed,expected_revision=0,expected_etag=tag)
            write.assert_called_once()
        _,tag=run.read_staging_run_s3(self.client,self.created["run_id"])
        with patch.object(self.client,"put_object",return_value={}) as write:
            with self.assertRaisesRegex(ValueError,"Unconfirmed"):
                run.write_staging_run_s3(self.client,failed,expected_revision=0,expected_etag=tag)
            write.assert_called_once()
        self.assertEqual(run.read_staging_run_s3(self.client,self.created["run_id"])[0],self.created)

    def test_storage_cannot_change_evidence_after_attachment(self):
        tag=run.write_staging_run_s3(self.client,self.created)
        staged=self.staged()
        tag2=run.write_staging_run_s3(self.client,staged,expected_revision=0,expected_etag=tag)
        failed=run.transition_staging_run(staged,"FAILED",error="failed")
        failed["generation"]["sample_validation"]["identity_check"]["dataset_wide_verified"]=True
        with self.assertRaises(ValueError):
            run.write_staging_run_s3(self.client,failed,expected_revision=1,expected_etag=tag2)

    def test_import_safe_without_cloud_or_spark(self):
        code="""import importlib.abc,sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'boto3','botocore','pyspark','awsglue'}: raise AssertionError(fullname)
sys.meta_path.insert(0,Block())
sys.path.insert(0,sys.argv[1])
import sequence_sample_run
"""
        result=subprocess.run([sys.executable,"-I","-c",code,str(Path(run.__file__).parent)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)


if __name__=="__main__": unittest.main()
