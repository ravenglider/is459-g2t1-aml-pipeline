"""Offline B1 tests. Protocol tests mock Phase A and analytical validation.

Separate local Parquet tests use real staging/validation, with a controlled Phase A
context. They do not claim a live Glue/S3 filesystem or full cloud integration.
"""
from contextlib import contextmanager, ExitStack
from copy import deepcopy
from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sequence_sample_staging as staging
import sequence_s3 as adapter
import test_sequence_s3 as s3tests
from test_sequence_sample_generation import config
from test_glue_sample_preflight import FakeCatalog, sample_s3
from sequence_aws import inspect_sample_source
from sequence_sample_generation import SampleGenerationResult


def configuration():
    return config(inspect_sample_source(FakeCatalog(), sample_s3()))


class WrapperTests(unittest.TestCase):
    def setUp(self):
        self.cfg = configuration()
        self.client = s3tests.FakeS3()
        self.output = MagicMock()
        self.events = []
        self.active = False
        self.context, self.catalog, self.identity = MagicMock(), MagicMock(), MagicMock()

    def call(self, **changes):
        values = dict(allow_writes=True, glue_context=self.context, catalog_client=self.catalog,
            s3_client=self.client, identity_client=self.identity, transport=self.transport)
        values.update(changes)
        cfg = values.pop("config", self.cfg)
        attempt = values.pop("attempt_id", "attempt")
        maximum = values.pop("max_records_per_file", 2)
        return staging.stage_bounded_sample(cfg, attempt, maximum, **values)

    @contextmanager
    def harness(self, *, empty=False, exit_error=None, write_error=None, read_error=None, validation_error=None):
        counts = {2:0 if empty else 1, 3:0}
        report = dict(status="OUTPUT_VALIDATED_CONTEXT_OPEN", validation={"counts":counts})
        @contextmanager
        def generate(*args, **kwargs):
            self.events.append("enter")
            self.active = True
            try:
                yield SampleGenerationResult(self.output, report)
            except BaseException:
                report["status"] = "FAILED"
                raise
            else:
                if exit_error:
                    report["status"] = "FAILED"
                    raise exit_error
                report["status"] = "GENERATION_COMPLETE"
            finally:
                self.active = False
                self.events.append("cleanup")
        self.transport = MagicMock()
        def write(frame, uri, maximum):
            self.assertTrue(self.active)
            self.events.append("write")
            self.client.seed(adapter._key(uri)+"dataset=sample/start_date=2022-09-01/part.snappy.parquet")
            if write_error: raise write_error
        self.transport.write.side_effect = write
        self.transport.read.side_effect = read_error
        self.transport.read.return_value = self.output
        checks = dict(counts=counts, row_count=sum(counts.values()), schema_valid=True, values_valid=True)
        forbidden = ["sequence_s3.write_manifest_s3", "sequence_s3.attach_validated_attempt_s3",
            "sequence_publication.plan_development_publication"]
        with ExitStack() as stack:
            guards = [stack.enter_context(patch(name, side_effect=AssertionError("forbidden"))) for name in forbidden]
            self.generate = stack.enter_context(patch("sequence_sample_generation.generate_sample_sequences", side_effect=generate))
            stack.enter_context(patch("sequence_staging.validate_sequence_output",
                side_effect=[checks, validation_error or checks]))
            try:
                yield
            finally:
                for guard in guards: guard.assert_not_called()
                self.catalog.assert_not_called()
                self.assertEqual(self.catalog.mock_calls, [])

    def status(self, attempt="attempt"):
        key = "bq2_sequences_staging/"+self.cfg["run_id"]+"/"+attempt+"/metadata/status.json"
        return json.loads(self.client.objects[key]["raw"])

    def test_activation_rejects_before_generation_clients_and_writers(self):
        with self.harness():
            for value in (False, None, "true", "false", 1, 0):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    self.call(allow_writes=value)
            with self.assertRaises(ValueError):
                staging.stage_bounded_sample(self.cfg,"attempt",2,glue_context=self.context,
                    catalog_client=self.catalog,s3_client=self.client,identity_client=self.identity)
            self.generate.assert_not_called()
            self.transport.write.assert_not_called()
            self.assertEqual(self.client.calls, [])
            self.identity.assert_not_called()

    def test_unsafe_config_ids_scope_and_sizes_rejected_before_io(self):
        changes = [dict(destination="production"), dict(destination=adapter.STAGING)]
        for bad in ("", "../bad", "a/b", "a.b", "x"*65): changes.append(dict(attempt_id=bad))
        for bad in (0,-1,True,"2"): changes.append(dict(max_records_per_file=bad))
        for field,value in (("dataset","HI-Medium"),("owner_date","2022-09-02"),("run_id","../run"),
                ("timezone_evidence",""),("no_refresh_acknowledged",False),("region","us-west-2"),
                ("account_id","bad"),("staging_root","s3://other/root")):
            changes.append(dict(config=dict(self.cfg,**{field:value})))
        for name in self.cfg["limits"]:
            changes.append(dict(config=dict(self.cfg, limits=dict(self.cfg["limits"],**{name:0}))))
        with self.harness():
            for change in changes:
                with self.subTest(change=change), self.assertRaises(ValueError): self.call(**change)
            self.generate.assert_not_called()
            self.assertEqual(self.client.calls, [])

    def test_success_order_counts_prefix_and_no_readiness_claim(self):
        with self.harness():
            result = self.call()
            self.assertEqual(self.events,["enter","write","cleanup"])
            self.assertFalse(self.active)
            self.assertEqual(result.attempt_uri,adapter.STAGING+self.cfg["run_id"]+"/attempt/")
            self.assertEqual(result.report["status"],"STAGED_GENERATION_COMPLETE")
            self.assertFalse(result.report["published"])
            self.assertFalse(result.report["run_manifest_ready"])
            self.assertEqual(self.status()["state"],"VALIDATED")
            self.assertEqual(self.status()["expected_counts"],{"2":1,"3":0})
            self.assertEqual(self.status()["source_fingerprint"],self.cfg["expected_source"]["source_fingerprint"])
            for name,args in self.client.calls:
                self.assertTrue(args.get("Key",args.get("Prefix","")).startswith("bq2_sequences_staging/"))

    def test_empty_result_has_receipts_without_write_or_stale_data(self):
        with self.harness(empty=True):
            self.call()
            self.transport.write.assert_not_called()
            self.assertEqual(self.status()["expected_counts"],{"2":0,"3":0})
            self.assertEqual(self.status()["state"],"VALIDATED")
            self.assertFalse(any("/data/" in key for key in self.client.objects))

    def test_existing_attempt_rejected_without_retry_or_overwrite(self):
        with self.harness():
            self.call()
            old = deepcopy(self.client.objects)
        with self.harness(), self.assertRaisesRegex(ValueError,"cannot be reused"):
            self.call()
        self.assertEqual(self.client.objects,old)
        self.transport.write.assert_not_called()

    def test_partial_write_readback_and_validation_failures_propagate(self):
        for option in ("write_error","read_error","validation_error"):
            self.client = s3tests.FakeS3()
            error = ValueError(option)
            with self.subTest(option=option), self.harness(**{option:error}):
                with self.assertRaises(ValueError) as caught: self.call()
                self.assertIs(caught.exception,error)
                self.assertEqual(self.status()["state"],"FAILED")
                self.assertFalse(self.active)
                self.assertTrue(any("/data/" in key for key in self.client.objects))

    def test_final_source_or_cleanup_failure_preserves_validated_attempt_without_success(self):
        for message in ("source changed","cleanup failed"):
            self.client = s3tests.FakeS3()
            error = RuntimeError(message)
            with self.subTest(message=message), self.harness(exit_error=error):
                with self.assertRaises(RuntimeError) as caught: self.call()
                self.assertIs(caught.exception,error)
                self.assertIn("no overall staging success",caught.exception.__notes__[-1])
                self.assertEqual(self.status()["state"],"VALIDATED")
                self.assertEqual(self.status()["revision"],2)
                self.assertFalse(self.active)

    def test_receipt_failure_is_not_success_and_no_silent_retry(self):
        with self.harness():
            original = self.client.put_object
            def fail(**kwargs):
                if kwargs["Key"].endswith("validation.json"): raise RuntimeError("receipt failure")
                return original(**kwargs)
            with patch.object(self.client,"put_object",side_effect=fail):
                with self.assertRaisesRegex(RuntimeError,"receipt failure"): self.call()
            self.assertEqual(self.status()["state"],"FAILED")
            self.transport.write.assert_called_once()

    def test_generation_failure_blocks_all_staging_operations(self):
        with self.harness():
            self.generate.side_effect = ValueError("stale source inventory")
            with self.assertRaisesRegex(ValueError,"stale source"):
                self.call()
            self.transport.write.assert_not_called()
            self.transport.read.assert_not_called()
            self.assertEqual(self.client.calls,[])

    def test_fresh_attempt_after_failure_preserves_old_artifacts(self):
        with self.harness(write_error=ValueError("partial write")):
            with self.assertRaises(ValueError): self.call()
        previous = deepcopy(self.client.objects)
        with self.harness():
            result = self.call(attempt_id="retry_2")
            self.assertTrue(result.attempt_uri.endswith("/retry_2/"))
            self.assertEqual(self.status("retry_2")["state"],"VALIDATED")
        self.assertEqual({key:self.client.objects[key] for key in previous},previous)

    def test_import_safe_without_cloud_or_spark(self):
        code = """import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self, fullname, path=None, target=None):
  if fullname.split('.')[0] in {'boto3','botocore','pyspark','awsglue'}: raise AssertionError(fullname)
sys.meta_path.insert(0,Block())
sys.path.insert(0,sys.argv[1])
import sequence_sample_staging
"""
        result = subprocess.run([sys.executable,"-I","-c",code,str(Path(staging.__file__).parent)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)


class RealWrapperParquetTests(unittest.TestCase):
    """Real wrapper + staging + Parquet validation; Phase A context is controlled.

    Run under WSL2 for Hadoop local filesystem support. No test is skipped.
    """
    @classmethod
    def setUpClass(cls):
        s3tests.RealLocalParquetTests.setUpClass.__func__(cls)

    def test_nonempty_empty_roundtrip_and_fresh_attempt(self):
        from fixtures import OUTPUT_SCHEMA
        cfg = configuration()
        client = s3tests.FakeS3()
        with tempfile.TemporaryDirectory(prefix="role5-b1-") as folder:
            transport = s3tests.LocalTransport(Path(folder),client)
            for attempt,output,counts in (("nonempty",self.output,{2:0,3:1}),
                    ("empty",self.spark.createDataFrame([],OUTPUT_SCHEMA),{2:0,3:0})):
                original_cache_count = self.spark.sparkContext._jsc.sc().getPersistentRDDs().size()
                @contextmanager
                def generate(*args,**kwargs):
                    frame = output.persist()
                    frame.count()
                    report = dict(status="OUTPUT_VALIDATED_CONTEXT_OPEN",validation={"counts":counts})
                    try:
                        yield SampleGenerationResult(frame,report)
                        report["status"]="GENERATION_COMPLETE"
                    finally:
                        frame.unpersist(blocking=True)
                with patch("sequence_sample_generation.generate_sample_sequences",side_effect=generate):
                    result = staging.stage_bounded_sample(cfg,attempt,1,allow_writes=True,
                        glue_context=MagicMock(),catalog_client=MagicMock(),s3_client=client,
                        identity_client=MagicMock(),transport=transport)
                self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),original_cache_count)
                read = adapter.read_staged_batch_s3(client,self.spark,result.attempt_uri,
                    timezone_verified=True,timezone_evidence="synthetic UTC",transport=transport)
                self.assertEqual(read.schema.simpleString(),OUTPUT_SCHEMA.simpleString())
                self.assertEqual(read.count(),sum(counts.values()))
                status,_,_ = adapter._get(client,result.attempt_uri+"metadata/status.json")
                self.assertEqual(status["state"],"VALIDATED")
                if attempt=="empty":
                    self.assertFalse(any(key.startswith(adapter._key(result.attempt_uri+"data/")) for key in client.objects))


if __name__ == "__main__":
    unittest.main()
