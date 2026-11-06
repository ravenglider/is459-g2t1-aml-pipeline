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


class CoordinatorTests(unittest.TestCase):
    """Real manifest storage/receipt checks; B1 execution is mocked here."""
    def setUp(self):
        from test_glue_sample_preflight import FakeCatalog, sample_s3
        self.catalog, self.client = FakeCatalog(), sample_s3()
        self.identity = MagicMock()
        self.identity.meta.region_name="us-east-1"
        self.identity.get_caller_identity.return_value={"Account":configuration()["account_id"]}
        self.events=[]
        self.counts={2:1,3:1}

    def call(self,**changes):
        values=dict(owner_token="owner",builder_version="synthetic_commit",allow_writes=True,
            glue_context=MagicMock(),catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity)
        values.update(changes)
        return run.coordinate_staging_run(configuration(),"attempt",10,**values)

    def b1(self,*args,**kwargs):
        from sequence_sample_staging import StagedSampleResult
        current,_=run.read_staging_run_s3(self.client,args[0]["run_id"])
        self.assertEqual(current["state"],"CREATED")
        self.assertIs(kwargs["allow_writes"],True)
        self.assertEqual(kwargs["destination"],"development")
        self.events.append("B1")
        receipt(self.client,current,self.counts)
        self.events.append("B1_final_checks_and_cleanup_complete")
        return StagedSampleResult(current["attempt_uri"],dict(status="STAGED_GENERATION_COMPLETE",
            published=False,run_manifest_ready=False,generation=generation(current,self.counts)))

    def state(self):
        return run.read_staging_run_s3(self.client,configuration()["run_id"])[0]

    def test_happy_path_empty_and_nonempty_durable_ordering(self):
        from test_glue_sample_preflight import sample_s3
        for counts in ({2:1,3:1},{2:0,3:0}):
            self.client=sample_s3()
            self.events=[]
            self.counts=counts
            original=self.client.put_object
            def write(**kwargs):
                if kwargs["Key"].endswith("staging_run.json"):
                    import json
                    self.events.append(json.loads(kwargs["Body"])["state"])
                return original(**kwargs)
            with patch.object(self.client,"put_object",side_effect=write),patch(
                    "sequence_sample_staging.stage_bounded_sample",side_effect=self.b1) as b1:
                result=self.call()
            self.assertEqual(self.events,["CREATED","B1","B1_final_checks_and_cleanup_complete","STAGING","STAGED_VALIDATED"])
            self.assertEqual(result["manifest"],self.state())
            self.assertEqual(result["manifest"]["state"],"STAGED_VALIDATED")
            self.assertEqual(result["manifest"]["attempt_evidence"]["row_count"],sum(counts.values()))
            b1.assert_called_once()
            self.assertFalse(result["published"])
            self.assertFalse(result["manifest"]["generation"]["sample_validation"]["identity_check"]["dataset_wide_verified"])
            for name,arguments in self.client.calls:
                if name=="put":
                    self.assertTrue(arguments["Key"].startswith(("bq2_sequences_staging/","bq2_sequences_metadata/development/")))
            self.assertTrue(all(name in ("get_table","get_partitions") for name,_ in self.catalog.calls))

    def test_disabled_or_malformed_activation_has_no_client_operations(self):
        for value in (False,None,"true",1,0):
            with self.subTest(value=value),patch("sequence_sample_staging.stage_bounded_sample") as b1:
                with self.assertRaises(ValueError):self.call(allow_writes=value)
                b1.assert_not_called()
        with self.assertRaises(ValueError):
            run.coordinate_staging_run(configuration(),"attempt",10,owner_token="owner",builder_version="version",
                glue_context=MagicMock(),catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity)
        self.assertEqual(self.client.calls,[])
        self.identity.get_caller_identity.assert_not_called()

    def test_wrong_account_region_or_destination_prevents_manifest_write(self):
        with patch("sequence_sample_staging.stage_bounded_sample") as b1:
            with self.assertRaises(ValueError):self.call(destination="production")
            self.identity.get_caller_identity.return_value={"Account":"999999999999"}
            with self.assertRaises(ValueError):self.call()
            self.identity.meta.region_name="us-west-2"
            with self.assertRaises(ValueError):self.call()
            self.assertEqual(self.client.calls,[])
            b1.assert_not_called()

    def test_no_b1_after_creation_conflict_or_uncertain_creation(self):
        with patch("sequence_sample_staging.stage_bounded_sample") as b1:
            self.client.fail_before=True
            with self.assertRaises(TimeoutError):self.call()
            b1.assert_not_called()
            existing=manifest()
            run.write_staging_run_s3(self.client,existing)
            old=deepcopy(self.client.objects)
            with self.assertRaises(ValueError):self.call()
            self.assertEqual(self.client.objects,old)
            b1.assert_not_called()

    def test_creation_requires_exact_read_verification_before_b1(self):
        with patch("sequence_sample_staging.stage_bounded_sample") as b1,patch.object(
                run,"read_staging_run_s3",side_effect=TimeoutError("cannot verify CREATED")):
            with self.assertRaises(TimeoutError):self.call()
            b1.assert_not_called()
        self.assertEqual(self.state()["state"],"CREATED")

    def test_b1_failures_preserve_original_and_validated_attempt(self):
        from test_glue_sample_preflight import sample_s3
        for message in ("write failed","read-back failed","final source failed","cleanup failed"):
            self.client=sample_s3()
            error=RuntimeError(message)
            def failure(*args,**kwargs):
                if message in ("final source failed","cleanup failed"):self.b1(*args,**kwargs)
                raise error
            with self.subTest(message=message),patch("sequence_sample_staging.stage_bounded_sample",side_effect=failure):
                with self.assertRaises(RuntimeError) as caught:self.call()
                self.assertIs(caught.exception,error)
                self.assertEqual(self.state()["state"],"FAILED")
                if message in ("final source failed","cleanup failed"):
                    self.assertEqual(s3._load_validated(self.client,self.state()["attempt_uri"])["status"]["state"],"VALIDATED")

    def test_recording_failure_does_not_mask_original_traceback(self):
        error=RuntimeError("B1 failure")
        original=self.client.put_object
        def write(**kwargs):
            import json
            if kwargs["Key"].endswith("staging_run.json") and json.loads(kwargs["Body"])["state"]=="FAILED":
                raise TimeoutError("failure recording unavailable")
            return original(**kwargs)
        with patch.object(self.client,"put_object",side_effect=write),patch(
                "sequence_sample_staging.stage_bounded_sample",side_effect=error):
            try:self.call()
            except RuntimeError as caught:
                self.assertIs(caught,error)
                import traceback
                self.assertIn("stage_bounded_sample", "".join(traceback.format_tb(caught.__traceback__)))
                self.assertIn("FAILED recording not confirmed",caught.__notes__[-1])
            else:self.fail("failure swallowed")
        self.assertEqual(self.state()["state"],"CREATED")

    def test_receipt_mismatch_and_changed_source_fail_without_publication(self):
        from test_glue_sample_preflight import sample_s3
        for mode in ("receipt","source"):
            self.client=sample_s3()
            def changed(*args,**kwargs):
                result=self.b1(*args,**kwargs)
                if mode=="receipt":
                    self.client.seed(s3._key(result.attempt_uri+"metadata/validation.json"),b'{}')
                else:
                    key=next(k for k in self.client.objects if k.startswith("transactions_clean/"))
                    self.client.seed(key,b"refreshed source")
                return result
            with self.subTest(mode=mode),patch("sequence_sample_staging.stage_bounded_sample",side_effect=changed):
                with self.assertRaises(ValueError):self.call()
                self.assertEqual(self.state()["state"],"FAILED")

    def test_conflicting_owner_is_not_taken_over_to_record_failure(self):
        def conflict(*args,**kwargs):
            import json
            m=self.state()
            m["storage_owner"]="other"
            self.client.seed(s3._key(m["manifest_uri"]),s3._bytes(m))
            raise RuntimeError("original conflict")
        with patch("sequence_sample_staging.stage_bounded_sample",side_effect=conflict):
            with self.assertRaisesRegex(RuntimeError,"original conflict") as caught:self.call()
        self.assertEqual(self.state()["storage_owner"],"other")
        self.assertEqual(self.state()["state"],"CREATED")
        self.assertIn("FAILED recording not confirmed",caught.exception.__notes__[-1])

    def test_lost_acknowledgments_reconcile_without_repeating_b1(self):
        original=self.client.put_object
        def write(**kwargs):
            if kwargs["Key"].endswith("staging_run.json"):self.client.lose_response=True
            return original(**kwargs)
        with patch.object(self.client,"put_object",side_effect=write),patch(
                "sequence_sample_staging.stage_bounded_sample",side_effect=self.b1) as b1:
            result=self.call()
        b1.assert_called_once()
        self.assertEqual(result["manifest"]["state"],"STAGED_VALIDATED")

    def test_interrupted_staging_write_records_failure_only_if_owned(self):
        original=self.client.put_object
        def write(**kwargs):
            import json
            if kwargs["Key"].endswith("staging_run.json") and json.loads(kwargs["Body"])["state"]=="STAGING":
                raise TimeoutError("STAGING write failed")
            return original(**kwargs)
        with patch.object(self.client,"put_object",side_effect=write),patch(
                "sequence_sample_staging.stage_bounded_sample",side_effect=self.b1) as b1:
            with self.assertRaises(TimeoutError):self.call()
        b1.assert_called_once()
        self.assertEqual(self.state()["state"],"FAILED")

    def test_terminal_read_failure_never_returns_success_or_overwrites_terminal(self):
        original=run.read_staging_run_s3
        def read(client,run_id):
            value,tag=original(client,run_id)
            if value["state"]=="STAGED_VALIDATED":raise TimeoutError("terminal read unavailable")
            return value,tag
        with patch.object(run,"read_staging_run_s3",side_effect=read),patch(
                "sequence_sample_staging.stage_bounded_sample",side_effect=self.b1):
            with self.assertRaisesRegex(TimeoutError,"terminal read unavailable") as caught:self.call()
        self.assertEqual(self.state()["state"],"STAGED_VALIDATED")
        self.assertIn("FAILED recording not confirmed",caught.exception.__notes__[-1])

    def test_terminal_verification_checks_both_exact_bytes_and_etag(self):
        from test_glue_sample_preflight import sample_s3
        original=run.read_staging_run_s3
        for mode in ("bytes","etag"):
            self.client=sample_s3()
            def read(client,run_id):
                value,tag=original(client,run_id)
                if value["state"]=="STAGED_VALIDATED":
                    if mode=="bytes":value=dict(value,updated_at="unexpected timestamp")
                    else:tag="unexpected-etag"
                return value,tag
            with self.subTest(mode=mode),patch.object(run,"read_staging_run_s3",side_effect=read),patch(
                    "sequence_sample_staging.stage_bounded_sample",side_effect=self.b1):
                with self.assertRaisesRegex(ValueError,"verification conflict"):self.call()
            self.assertEqual(self.state()["state"],"STAGED_VALIDATED")

    def test_uncertain_committed_staging_is_failed_conditionally_without_reexecution(self):
        original=run.write_staging_run_s3
        def write(client,m,**kwargs):
            result=original(client,m,**kwargs)
            if m["state"]=="STAGING":raise TimeoutError("unconfirmed intermediate update")
            return result
        with patch.object(run,"write_staging_run_s3",side_effect=write),patch(
                "sequence_sample_staging.stage_bounded_sample",side_effect=self.b1) as b1:
            with self.assertRaisesRegex(TimeoutError,"unconfirmed intermediate"):self.call()
        b1.assert_called_once()
        self.assertEqual(self.state()["state"],"FAILED")
        self.assertEqual(self.state()["revision"],2)



class CoordinatorLocalParquetTests(unittest.TestCase):
    """Real coordinator+B1+Parquet/receipts; Phase A generation is controlled.

    The supplied 500000-row source-validation report is synthetic attestation,
    not an actual 500000-row scan. Fake source clients do not exercise live AWS.
    Requires local Hadoop filesystem support; run unchanged in Ubuntu WSL2.
    """
    @classmethod
    def setUpClass(cls):
        import test_sequence_s3
        test_sequence_s3.RealLocalParquetTests.setUpClass.__func__(cls)

    def test_real_b1_parquet_and_durable_terminal_manifest(self):
        from contextlib import contextmanager
        from tempfile import TemporaryDirectory
        from sequence_sample_generation import SampleGenerationResult
        from test_glue_sample_preflight import FakeCatalog,sample_s3
        from test_sequence_s3 import LocalTransport
        cfg=configuration()
        client=sample_s3()
        catalog=FakeCatalog()
        identity=MagicMock()
        identity.meta.region_name="us-east-1"
        identity.get_caller_identity.return_value={"Account":cfg["account_id"]}
        baseline=self.spark.sparkContext._jsc.sc().getPersistentRDDs().size()
        active=[]
        @contextmanager
        def generate(*args,**kwargs):
            m,_=run.read_staging_run_s3(client,cfg["run_id"])
            self.assertEqual(m["state"],"CREATED")
            frame=self.output.persist()
            frame.count()
            report=generation(m,{2:0,3:1})
            report["status"]="OUTPUT_VALIDATED_CONTEXT_OPEN"
            active.append(True)
            try:
                yield SampleGenerationResult(frame,report)
                report["status"]="GENERATION_COMPLETE"
            finally:
                active.pop()
                frame.unpersist(blocking=True)
        with TemporaryDirectory(prefix="role5-b2b-") as folder:
            transport=LocalTransport(Path(folder),client)
            original=transport.write
            def write(*args):
                self.assertEqual(active,[True])
                return original(*args)
            with patch.object(transport,"write",side_effect=write),patch(
                    "sequence_sample_generation.generate_sample_sequences",side_effect=generate):
                result=run.coordinate_staging_run(cfg,"attempt",1,owner_token="owner",builder_version="synthetic_commit",
                    allow_writes=True,glue_context=MagicMock(),catalog_client=catalog,s3_client=client,
                    identity_client=identity,transport=transport)
            self.assertEqual(active,[])
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),baseline)
            self.assertEqual(result["manifest"]["state"],"STAGED_VALIDATED")
            self.assertEqual(run.read_staging_run_s3(client,cfg["run_id"]),(result["manifest"],result["etag"]))
            frame=s3.read_staged_batch_s3(client,self.spark,result["attempt_uri"],timezone_verified=True,
                timezone_evidence="synthetic UTC",transport=transport)
            self.assertEqual(frame.count(),1)
            self.assertEqual(frame.schema.simpleString(),self.output.schema.simpleString())
            self.assertFalse(result["published"])


if __name__=="__main__": unittest.main()
