"""Offline group commit/recovery and real Parquet integration; no AWS."""
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from test_chunked_generation import config, limits, ChunkSparkTests, args
from test_full_publication import PublicationS3, PublicationTransport
from sequence_batched_staging import BatchedDateStager
from sequence_batched_kernel import generate_root_group
from sequence_owner_date import DurableBudget
from sequence_staging import SEQUENCE_SCHEMA
import glue_chunked_generation as entry

DAY = "2022-09-01"
GROUP = dict(max_group_input_rows=100, max_group_pair_work=1000, max_group_output_rows=1000)


def store(client, invocation="invoke01", etag=None, spark=None, transport=None):
    value = BatchedDateStager(client, config(), spark=spark or MagicMock(), max_copy_bytes=1000000, transport=transport)
    manifest = dict(manifest_kind="batched_owner_date_generation_progress", identity="batched-frozen", state="CREATED",
        owner_dates=[DAY], source=config()["expected_source"], published=False, catalog_updated=False,
        schema=[dict(name=f.name, type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA], builder={}, rule_version="v1")
    value.open(manifest, invocation, etag, etag is not None)
    return value


def empty_result():
    return dict(state="GENERATED_NOT_STAGED", roots=[0, 1], input_rows={0: 0, 1: 0}, counts={0: {2: 0, 3: 0}, 1: {2: 0, 3: 0}}, chunks=[])


class GroupProtocolTests(unittest.TestCase):
    def test_before_commit_orphans_ignored_after_commit_both_roots_recovered(self):
        client = PublicationS3(); first = store(client); first.begin_date(DAY, 2, 0)
        original = first.save
        with patch.object(first, "save", side_effect=RuntimeError("interrupted before CAS")):
            with self.assertRaises(RuntimeError): first.commit_group(DAY, empty_result(), verify_source=lambda:None)
        self.assertEqual(first.manifest["root_progress"], {})
        second = store(client, "invoke02", first.etag); second.begin_date(DAY, 2, 0)
        self.assertEqual(second.completed_roots, {})
        second.commit_group(DAY, empty_result(), verify_source=lambda:None)
        third = store(client, "invoke03", second.etag); third.begin_date(DAY, 2, 0)
        self.assertEqual(set(third.completed_roots), {"0", "1"})
        self.assertEqual(third.dates[DAY]["chunks"], {})
        with self.assertRaises(ValueError): third.commit_group(DAY, empty_result(), verify_source=lambda:None)
        with self.assertRaises(ValueError): first.save(state="PARTIAL_CHUNKS")

    def test_lost_response_reconciles_and_partial_group_is_rejected(self):
        first = store(PublicationS3()); first.begin_date(DAY, 2, 0)
        first.client.lose_response = True
        first.commit_group(DAY, empty_result(), verify_source=lambda:None)
        # Simulate corruption: a group may never be recovered one root at a time.
        first.save(root_progress={"0": first.manifest["root_progress"]["0"]})
        second = store(first.client, "invoke02", first.etag)
        with self.assertRaisesRegex(ValueError, "Partially committed"):
            second.begin_date(DAY, 2, 0)

    def test_interruption_after_commit_recovers_whole_group_without_replaying(self):
        first=store(PublicationS3());first.begin_date(DAY,2,0)
        save=first.save
        def stop_after_commit(**changes):
            save(**changes)
            raise RuntimeError("process interrupted after commit")
        with patch.object(first,"save",side_effect=stop_after_commit):
            with self.assertRaises(RuntimeError):first.commit_group(DAY,empty_result(),verify_source=lambda:None)
        second=store(first.client,"invoke02",first.etag);second.begin_date(DAY,2,0)
        self.assertEqual(set(second.completed_roots),{"0","1"})
        with self.assertRaises(ValueError):second.commit_group(DAY,empty_result(),verify_source=lambda:None)

    def test_source_recheck_failure_keeps_group_invisible(self):
        first=store(PublicationS3());first.begin_date(DAY,2,0)
        with self.assertRaisesRegex(ValueError,"source changed"):
            first.commit_group(DAY,empty_result(),verify_source=MagicMock(side_effect=ValueError("source changed")))
        self.assertEqual(first.manifest["root_progress"],{})
        second=store(first.client,"invoke02",first.etag);second.begin_date(DAY,2,0)
        self.assertEqual(second.completed_roots,{})

    def test_parser_requires_explicit_group_budgets_before_initialization(self):
        valid = args("batched_owner_date") + ["--OWNER_DATE", DAY, "--ROOT_GROUP_START", "0", "--ROOT_GROUP_COUNT", "1",
            "--INVOCATION_ID", "invoke01", "--EXPECTED_COUNTS_JSON", json.dumps({DAY: {"2": 4, "3": 8}})]
        with patch.object(entry, "_initialize") as initialize:
            with self.assertRaises(ValueError): entry.main(valid)
            initialize.assert_not_called()
        entry.validate_arguments(entry.parse_arguments(valid + ["--GROUP_LIMITS_JSON", json.dumps(GROUP)]))

    def test_batched_package_is_separate_deterministic_and_importable(self):
        import package_chunked_generation as package
        import subprocess, zipfile
        with tempfile.TemporaryDirectory() as folder:
            a, b = Path(folder)/"a.zip", Path(folder)/"b.zip"
            self.assertEqual(package.build_package(a, batched=True), package.build_package(b, batched=True))
            with zipfile.ZipFile(a) as z:
                self.assertEqual(len(z.namelist()), 24); self.assertIsNone(z.testzip())
            code = """import sys,socket
from unittest.mock import patch
sys.path.insert(0,sys.argv[1])
def fail(*a,**k):raise AssertionError('runtime startup')
from pyspark import SparkContext
with patch.object(socket.socket,'connect',fail),patch.object(SparkContext,'getOrCreate',fail):
 import sequence_batched_staging,sequence_batched_kernel,sequence_owner_date
"""
            result = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(a)], cwd=folder, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)


class GroupSparkTests(unittest.TestCase):
    setUpClass = classmethod(ChunkSparkTests.setUpClass.__func__)
    canonical = ChunkSparkTests.canonical
    unbatched = ChunkSparkTests.unbatched

    def test_shared_parquet_commit_recovery_and_unique_finalization(self):
        lim = limits(root_chunks=4, max_pair_candidates=100, max_pair_links=100, max_output_rows=100,
            max_projected_triples=100, max_middle_expansion=100)
        with tempfile.TemporaryDirectory() as folder:
            client = PublicationS3(); transport = PublicationTransport(folder, client)
            first = store(client, spark=self.spark, transport=transport); first.begin_date(DAY, 4, 2)
            def consume(frame, counts, root_counts, ownership):
                cid = "c" + hashlib.sha256(ownership.encode()).hexdigest()[:60]
                first.write_chunk(DAY, 0, cid, frame, counts, ownership=ownership)
            result = generate_root_group(self.frame, "HI-Medium", self.plan, lim, range(4),
                budget=DurableBudget(lim, lambda: None, first), consume=consume, **GROUP)
            self.assertEqual(first.completed_roots, {})
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(), 0)
            orphan_keys=set(client.objects)
            with patch.object(first,"save",side_effect=RuntimeError("interrupted before group visibility")):
                with self.assertRaises(RuntimeError):first.commit_group(DAY,result,verify_source=lambda:None)
            second = store(client, "invoke02", first.etag, self.spark, transport)
            second.begin_date(DAY, 4, 2)
            self.assertEqual(second.completed_roots,{})
            def retry_consume(frame,counts,root_counts,ownership):
                cid="c"+hashlib.sha256(ownership.encode()).hexdigest()[:60]
                second.write_chunk(DAY,0,cid,frame,counts,ownership=ownership)
            result=generate_root_group(self.frame,"HI-Medium",self.plan,lim,range(4),
                budget=DurableBudget(lim,lambda:None,second),consume=retry_consume,**GROUP)
            second.commit_group(DAY,result,verify_source=lambda:None)
            third=store(client,"invoke03",second.etag,self.spark,transport);third.begin_date(DAY,4,2)
            self.assertTrue(orphan_keys<=set(client.objects))
            self.assertEqual(len(third.dates[DAY]["chunks"]), len(result["chunks"]))
            receipt = third.finalize_date(DAY, {2: 4, 3: 8})
            self.assertEqual(receipt["row_count"], 12)
            paths = [r["path"] for r in receipt["files"]]
            self.assertEqual(len(paths), len(set(paths)))
            back = transport.read(self.spark, receipt["staging_location"], paths, {"timezone": "UTC"})
            self.assertEqual(self.canonical(back.collect()), self.canonical(self.unbatched().collect()))

    def test_coordinator_source_failure_never_commits_and_success_finalizes(self):
        import sequence_owner_date as owner
        import sequence_chunked_generation as generation
        from test_full_generation import snapshot
        lim = limits(root_chunks=2, max_pair_candidates=100, max_pair_links=100, max_output_rows=100,
            max_projected_triples=100, max_middle_expansion=100)
        context=MagicMock(); context.spark_session=self.spark
        identity=MagicMock(); identity.meta.region_name="us-east-1"
        identity.get_caller_identity.return_value={"Account":config()["account_id"]}
        catalog=MagicMock(); catalog.meta.region_name="us-east-1"
        for changed in (True, False):
            with tempfile.TemporaryDirectory() as folder:
                client=PublicationS3()
                with patch.object(owner,"inspect_full_source",return_value=snapshot()), \
                     patch.object(owner,"read_full_catalog",return_value=self.frame), \
                     patch.object(owner,"assert_full_source_unchanged",side_effect=ValueError("source changed") if changed else None):
                    kwargs=dict(allow_writes=True,mode="batched_owner_date",owner_date=DAY,root_group_start=0,root_group_count=2,
                        invocation_id="invoke01",max_copy_bytes=1000000,expected_counts={DAY:{"2":4,"3":8}},group_limits=GROUP,
                        glue_context=context,catalog_client=catalog,s3_client=client,identity_client=identity,
                        transport=PublicationTransport(folder,client))
                    if changed:
                        with self.assertRaisesRegex(ValueError,"source changed"):generation.run_chunked_generation(config(),lim,**kwargs)
                        manifest=[json.loads(v["raw"]) for k,v in client.objects.items() if k.endswith("/run.json")][0]
                        self.assertEqual(manifest["root_progress"],{})
                    else:
                        result=generation.run_chunked_generation(config(),lim,**kwargs)
                        self.assertEqual(result["manifest"]["state"],"DATE_STAGED_VALIDATED")
                        self.assertEqual(result["manifest"]["row_count"],12)
                self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)


if __name__ == "__main__": unittest.main()
