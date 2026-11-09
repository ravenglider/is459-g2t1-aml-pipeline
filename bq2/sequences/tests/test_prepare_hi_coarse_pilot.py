"""Offline configuration tests; synthetic metadata, no AWS clients."""
from copy import deepcopy
import json,sys,unittest
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import prepare_hi_coarse_pilot as p
from test_full_generation import config


def fixture():
    cfg=config();cfg["builder_fingerprint"]=p.HASH
    job=dict(Name=p.JOB,GlueVersion="5.0",WorkerType="G.1X",NumberOfWorkers=5,ExecutionClass="STANDARD",Timeout=30,
        MaxRetries=0,ExecutionProperty=dict(MaxConcurrentRuns=1),JobRunQueuingEnabled=False,
        Command=dict(Name="glueetl",PythonVersion="3",ScriptLocation=p.PREFIX+"glue_chunked_generation.py"),
        DefaultArguments={"--ALLOW_AWS_WRITES":"false","--extra-py-files":p.PREFIX+"role5_chunked_modules.zip",
            "--additional-python-modules":p.PREFIX+"role5_sdk.gluewheels.zip","--python-modules-installer-option":"--no-index",
            "--job-bookmark-option":"job-bookmark-disable"})
    run=dict(Id=p.RUN,JobRunState="SUCCEEDED",Arguments={"--MODE":"single_owner_date","--DATASET":"HI-Medium",
        "--OWNER_DATE":"2022-09-01","--ROOT_GROUP_START":"0","--ROOT_GROUP_COUNT":"4",
        "--CONFIG_JSON":json.dumps(cfg),"--CHUNK_LIMITS_JSON":json.dumps(dict(root_chunks=4096))})
    progress=dict(manifest_kind="owner_date_generation_progress",state="PARTIAL_CHUNKS",published=False,catalog_updated=False,
        run_id=cfg["run_id"],attempt_id=cfg["attempt_id"],owner_token=cfg["owner_token"],source=cfg["expected_source"],source_after=cfg["expected_source"],
        root_progress={str(i):dict(counts={"2":9454 if i==0 else 0,"3":23079 if i==0 else 0}) for i in range(4)})
    return job,run,progress


class CoarsePreparationTests(unittest.TestCase):
    def prepare(self,*objects,**kw):return p.prepare_pilot(*objects,"123456789012",freeze_evidence="Synthetic verified frozen source",limits_evidence="Synthetic policy review",**kw)
    def test_fresh_disabled_exact_scope_service_overrides_and_no_old_mutation(self):
        data=fixture();before=deepcopy(data);request=self.prepare(*data);self.assertEqual(data,before)
        self.assertEqual(request["NumberOfWorkers"],10);self.assertEqual(request["Timeout"],30)
        a=request["Arguments"];cfg=json.loads(a["--CONFIG_JSON"]);lim=json.loads(a["--CHUNK_LIMITS_JSON"])
        self.assertEqual(a["--ALLOW_AWS_WRITES"],"false");self.assertNotIn("--ROOT_INDEX",a);self.assertNotIn("--RESUME_ETAG",a)
        self.assertEqual((lim["root_chunks"],a["--ROOT_GROUP_START"],a["--ROOT_GROUP_COUNT"]),(256,"0","1"))
        self.assertEqual(cfg["builder_fingerprint"],p.HASH);self.assertEqual(cfg["max_records_per_file"],50000)
        self.assertEqual(cfg["max_run_seconds"],1500);self.assertEqual(lim["max_total_output_rows"],60000000)
        old=json.loads(data[1]["Arguments"]["--CONFIG_JSON"])
        self.assertNotEqual(cfg["run_id"],old["run_id"]);self.assertNotEqual(cfg["attempt_id"],old["attempt_id"])
    def test_explicit_json_activation_is_boolean_only(self):
        self.assertEqual(self.prepare(*fixture(),enable_writes=True)["Arguments"]["--ALLOW_AWS_WRITES"],"true")
        for value in (1,"true",None):
            with self.assertRaises(ValueError):self.prepare(*fixture(),enable_writes=value)
    def test_bad_job_defaults_or_pinned_scope_rejected(self):
        for change in (lambda j:j["DefaultArguments"].update({"--ROOT_INDEX":"0"}),
                       lambda j:j.update(NonOverridableArguments={"--MODE":"single_owner_date"}),
                       lambda j:j["DefaultArguments"].update({"--ALLOW_AWS_WRITES":"true"}),
                       lambda j:j.update(NumberOfWorkers=20)):
            data=fixture();change(data[0])
            with self.assertRaises(ValueError):self.prepare(*data)
    def test_failed_changed_or_incomplete_benchmark_rejected(self):
        for change in (lambda r,p:r.update(JobRunState="RUNNING"),lambda r,p:p["root_progress"].pop("3"),
                       lambda r,p:p.update(published=True),lambda r,p:p.update(source_after={}),
                       lambda r,p:p["root_progress"]["0"]["counts"].update({"2":1})):
            data=fixture();change(data[1],data[2])
            with self.assertRaises(ValueError):self.prepare(*data)
    def test_real_entrypoint_accepts_reviewed_coarse_configuration_when_activated(self):
        import glue_chunked_generation as entry
        args=self.prepare(*fixture(),enable_writes=True)["Arguments"]
        cfg,lim,root,counts=entry.validate_arguments(entry.parse_arguments([k+"="+v for k,v in args.items()]))
        self.assertEqual(lim.root_chunks,256);self.assertIsNone(root)
        self.assertEqual(sum(counts["2022-09-01"].values()),51644681)


if __name__=="__main__":unittest.main()
