"""Pure full-job CLI, dependency ordering and isolated packaging checks."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import glue_full_generation as entry
import package_full_generation as package
from test_full_generation import config


def argv(dataset="HI-Medium"):
    cfg=config(dataset)
    values=dict(EXPECTED_ACCOUNT_ID=cfg["account_id"],DATASET=dataset,REGION="us-east-1",
        RUN_ID=cfg["run_id"],ATTEMPT_ID=cfg["attempt_id"],OWNER_TOKEN=cfg["owner_token"],
        BUILDER_VERSION=cfg["builder_version"],BUILDER_FINGERPRINT=cfg["builder_fingerprint"],
        TIMEZONE_EVIDENCE=cfg["timezone_evidence"],NO_REFRESH_EVIDENCE=cfg["no_refresh_evidence"],
        NO_REFRESH_ACKNOWLEDGED="true",ALLOW_AWS_WRITES="true",LIMITS_EVIDENCE=cfg["limits_evidence"],
        MAX_RECORDS_PER_FILE=str(cfg["max_records_per_file"]),MAX_RUN_SECONDS=str(cfg["max_run_seconds"]))
    values.update({k.upper():str(v) for k,v in cfg["limits"].items()})
    values.update({k.upper():v for k,v in cfg["expected_source"].items()})
    return [item for k,v in values.items() for item in ("--"+k,v)]


class FullEntryTests(unittest.TestCase):
    def test_exact_hi_li_and_no_default_dataset(self):
        for dataset in ("HI-Medium","LI-Medium"):
            self.assertEqual(entry.validate_arguments(entry.parse_arguments(argv(dataset))),config(dataset))
        args=argv();i=args.index("--DATASET");del args[i:i+2]
        with patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO),self.assertRaises(SystemExit):
            entry.parse_arguments(args)

    def test_invalid_arguments_never_initialize_runtime(self):
        cases=[]
        for key,value in (("DATASET","sample"),("DATASET","HI"),("REGION","us-west-2"),
                ("NO_REFRESH_ACKNOWLEDGED","false"),("SOURCE_FINGERPRINT","bad"),
                ("ALLOW_AWS_WRITES","false"),("MAX_RUN_SECONDS","0"),("MAX_PAIR_LINKS","-1")):
            args=argv();args[args.index("--"+key)+1]=value;cases.append(args)
        cases += [argv()+["--DATASET","LI-Medium"],argv()+["--conf","spark.unsafe=true"],
                  argv()+["--OUTPUT_ROOT","s3://other/"],argv()+["--SYNTHETIC_BASELINE","3"]]
        with patch.object(entry,"_initialize") as initialize,patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for args in cases:
                with self.subTest(args=args),self.assertRaises((ValueError,SystemExit)):entry.main(args)
            initialize.assert_not_called()

    def test_bootstrap_optional_but_when_supplied_strict(self):
        root=entry.SCRIPT_ROOT+"full_release_1/"
        settings=["--additional-python-modules",root+"role5_sdk.gluewheels.zip", "--python-modules-installer-option","--no-index",
                  "--extra-py-files",root+"role5_full_modules.zip","--scriptLocation",root+"glue_full_generation.py",
                  "--job-bookmark-option","job-bookmark-disable","--enable-auto-scaling","false",
                  "--enable-spark-ui","false","--tenant-internal","opaque"]
        self.assertEqual(entry.validate_arguments(entry.parse_arguments(argv()+settings)),config())
        with self.assertRaises(ValueError):entry.validate_arguments(entry.parse_arguments(argv()+["--extra-py-files",root+"role5_modules.zip"]))
        with self.assertRaises(ValueError):entry.validate_arguments(entry.parse_arguments(argv()+["--additional-python-modules","boto3==1.35.93"]))

    def test_sdk_installation_failure_precedes_aws_imports(self):
        error=ValueError("SDK incompatible")
        with patch.object(entry,"_validate_sdk_installation",side_effect=error),patch.dict(sys.modules,{"boto3":None}),self.assertRaises(ValueError) as caught:
            entry._initialize(config())
        self.assertIs(caught.exception,error)

    def test_thin_entry_success_failure_and_no_publication(self):
        job=MagicMock()
        completed=dict(manifest=dict(state="STAGED_VALIDATED",published=False,counts={"2":1,"3":1},row_count=2),manifest_uri="synthetic")
        with patch.object(entry,"_initialize",return_value=({},job)),patch.object(entry,"run_full_generation",return_value=completed) as run,patch("sys.stdout",new_callable=io.StringIO) as out:
            self.assertEqual(entry.main(argv()),0)
            run.assert_called_once_with(config(),allow_writes=True)
            self.assertIn('"catalog_updated": false',out.getvalue())
            job.commit.assert_called_once()
        job.reset_mock()
        with patch.object(entry,"_initialize",return_value=({},job)),patch.object(entry,"run_full_generation",side_effect=RuntimeError("staging failed")),self.assertRaises(RuntimeError):
            entry.main(argv())
        job.commit.assert_not_called()


class FullPackagingTests(unittest.TestCase):
    def test_separate_deterministic_seventeen_modules_and_no_overwrite(self):
        from package_glue_modules import GENERATION_MODULES
        with tempfile.TemporaryDirectory() as folder:
            first,second=Path(folder)/"one.zip",Path(folder)/"two.zip"
            digest=package.build_full_package(first)
            self.assertEqual(package.build_full_package(second),digest)
            self.assertEqual(first.read_bytes(),second.read_bytes())
            with zipfile.ZipFile(first) as archive:
                self.assertEqual(archive.namelist(),sorted(package.FULL_MODULES));self.assertEqual(len(archive.namelist()),17)
                self.assertIsNone(archive.testzip())
                self.assertNotIn("glue_full_generation.py",archive.namelist())
                self.assertEqual(len(GENERATION_MODULES),13)
            with self.assertRaises(FileExistsError):package.build_full_package(first)
            self.assertEqual(hashlib.sha256(first.read_bytes()).hexdigest(),digest)

    def test_isolated_full_imports_never_start_clients_network_or_spark(self):
        probe='''import importlib,importlib.abc,importlib.util,socket,sys,zipfile
from unittest.mock import patch
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'boto3','botocore','awsglue'}:raise AssertionError('cloud import')
sys.meta_path.insert(0,Block())
def denied(*args,**kwargs):raise AssertionError('network/Spark forbidden')
with patch.object(socket.socket,'connect',denied),patch.object(socket.socket,'connect_ex',denied):
 from pyspark import SparkContext
 from pyspark.sql import SparkSession
 sys.path.insert(0,sys.argv[1])
 with patch.object(SparkContext,'__init__',denied),patch.object(SparkSession.Builder,'getOrCreate',denied):
  for name in zipfile.ZipFile(sys.argv[1]).namelist():
   module=importlib.import_module(name[:-3]);assert module.__file__.startswith(sys.argv[1])
  spec=importlib.util.spec_from_file_location('full_entry',sys.argv[2]);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 assert SparkContext._active_spark_context is None
'''
        with tempfile.TemporaryDirectory() as folder:
            archive=Path(folder)/"role5_full_modules.zip";package.build_full_package(archive)
            script=Path(folder)/"glue_full_generation.py";script.write_bytes((Path(entry.__file__)).read_bytes())
            result=subprocess.run([sys.executable,"-I","-c",probe,str(archive),str(script)],cwd=folder,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)


if __name__=="__main__":unittest.main()
