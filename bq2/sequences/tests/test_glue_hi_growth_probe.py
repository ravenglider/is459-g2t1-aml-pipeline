"""Offline protocol gates and real Spark pair/degree calibration; never AWS."""
from copy import deepcopy
import io
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import glue_hi_growth_probe as probe
from test_full_generation import config, snapshot
from sequence_source import CLEANED_SCHEMA
from fixtures import transaction


def argv():
    cfg=config()
    values=dict(EXPECTED_ACCOUNT_ID=cfg["account_id"],REGION="us-east-1",ALLOW_AWS_READS="true",
        NO_REFRESH_ACKNOWLEDGED="true",TIMEZONE_EVIDENCE="Synthetic upstream UTC confirmation",
        NO_REFRESH_EVIDENCE="Synthetic source freeze",LIMITS_EVIDENCE="Synthetic reviewed computational budget",
        MAX_INPUT_ROWS="10",MAX_PAIR_CANDIDATES="100",MAX_PAIR_LINKS="100",MAX_RUN_SECONDS="300")
    values.update({k.upper():v for k,v in cfg["expected_source"].items()})
    return [v for k,value in values.items() for v in ("--"+k,value)]


class ProbeProtocolTests(unittest.TestCase):
    def test_strict_invalid_configuration_precedes_initialization(self):
        cases=[]
        for name,value in (("ALLOW_AWS_READS","false"),("ALLOW_AWS_READS","1"),
            ("REGION","us-west-2"),("NO_REFRESH_ACKNOWLEDGED","false"),("EXPECTED_ACCOUNT_ID","bad"),
            ("SOURCE_FINGERPRINT","bad"),("TIMEZONE_EVIDENCE",""),("LIMITS_EVIDENCE",""),
            ("MAX_INPUT_ROWS","0"),("MAX_PAIR_CANDIDATES","-1"),("MAX_PAIR_LINKS","false"),("MAX_RUN_SECONDS","0")):
            args=argv();args[args.index("--"+name)+1]=value;cases.append(args)
        for name in ("MAX_INPUT_ROWS","MAX_PAIR_CANDIDATES","MAX_PAIR_LINKS","MAX_RUN_SECONDS","ALLOW_AWS_READS"):
            args=argv();i=args.index("--"+name);del args[i:i+2];cases.append(args)
        cases.extend([argv()+["--DATASET","LI-Medium"],argv()+["--OUTPUT_ROOT","s3://anything/"],
            argv()+["--conf","spark.unsafe=true"],argv()+["--ALLOW_AWS_READS","true"],
            argv()+["--ALLOW_AWS_WRITES","true"]])
        with patch.object(probe,"_initialize") as init,patch("sys.stdout"),patch("sys.stderr"):
            for args in cases:
                with self.subTest(args=args),self.assertRaises((ValueError,SystemExit)):probe.main(args)
            init.assert_not_called()

    def test_bootstrap_arguments_and_glue_runtime_allowlist(self):
        root=probe.SCRIPT_ROOT+"hi_probe_20261108_01/"
        args=argv()+["--JOB_NAME",probe.JOB_NAME,"--extra-py-files",root+"role5_full_modules.zip",
            "--scriptLocation",root+"glue_hi_growth_probe.py","--additional-python-modules",root+"role5_sdk.gluewheels.zip",
            "--python-modules-installer-option","--no-index","--job-bookmark-option","job-bookmark-disable",
            "--enable-auto-scaling","false","--enable-spark-ui","false","--tenant-internal","opaque"]
        probe.validate_arguments(probe.parse_arguments(args))
        for extra in (["--extra-py-files",root+"other.zip"],["--TempDir","s3://other/"],
                      ["--job-bookmark-option","job-bookmark-enable"]):
            with self.assertRaises(ValueError):probe.validate_arguments(probe.parse_arguments(argv()+extra))

    def test_reader_exact_pruning_schema_and_selected_footers(self):
        context=MagicMock();snap=snapshot();session=context.spark_session.newSession.return_value
        from pyspark.sql.types import StructType
        session.read.option.return_value.parquet.return_value.schema=StructType(CLEANED_SCHEMA.fields[:-2])
        frame=context.create_dynamic_frame.from_catalog.return_value.toDF.return_value
        frame.columns=CLEANED_SCHEMA.fieldNames();frame.select.return_value=frame;frame.schema=CLEANED_SCHEMA
        self.assertIs(probe._read_selected(context,snap),frame)
        context.create_dynamic_frame.from_catalog.assert_called_once_with(database="is459_g2t1_aml",
            table_name="transactions_clean",push_down_predicate="dataset = 'HI-Medium' AND txn_date IN ('2022-09-01', '2022-09-02', '2022-09-03')")
        self.assertEqual([c.args[0] for c in session.read.option.return_value.parquet.call_args_list],
                         [snap["files_by_date"][d][0] for d in probe.DATES])
        frame.columns=["wrong"]
        with self.assertRaises(ValueError):probe._read_selected(context,snap)

    def test_sdk_guard_precedes_aws_import_and_main_failure_propagates(self):
        with patch.object(probe,"_validate_sdk_installation",side_effect=ValueError("incompatible SDK")), \
             patch.dict(sys.modules,{"boto3":None}),self.assertRaises(ValueError):
            probe._initialize(probe.parse_arguments(argv()))
        error=probe.GrowthProbeFailure(dict(status="FAILED",error_type="GrowthLimitExceeded",pair_candidates=101))
        with patch.object(probe,"_initialize",return_value={}),patch.object(probe,"run_probe",side_effect=error), \
             patch("sys.stdout",new_callable=io.StringIO) as out,self.assertRaises(probe.GrowthProbeFailure) as caught:
            probe.main(argv())
        self.assertIs(caught.exception,error);self.assertIn('"pair_candidates": 101',out.getvalue())
        with patch.object(probe,"_initialize",return_value={}),patch.object(probe,"run_probe",return_value={"status":"PROFILED_READ_ONLY"}),patch("sys.stdout"):
            self.assertEqual(probe.main(argv()),0)

    def test_import_safe_and_no_write_or_triple_api_references(self):
        import ast
        source=Path(probe.__file__).read_text();tree=ast.parse(source)
        forbidden={"build_three_hop_links","build_sequence_output","put_object","copy_object","delete_object",
            "create_table","update_table","create_partition","update_partition","stage","reserve","finish","publish_full_generation"}
        names={n.attr for n in ast.walk(tree) if isinstance(n,ast.Attribute)}|{n.id for n in ast.walk(tree) if isinstance(n,ast.Name)}
        self.assertFalse(names & forbidden)
        import tempfile
        from package_full_generation import build_full_package
        code="import runpy,sys,socket; sys.path.insert(0,sys.argv[2]); sys.modules['boto3']=None; sys.modules['awsglue']=None; socket.create_connection=lambda *a,**k: (_ for _ in ()).throw(AssertionError('network')); runpy.run_path(sys.argv[1],run_name='offline_import')"
        with tempfile.TemporaryDirectory() as folder:
            package=Path(folder)/"role5_full_modules.zip";build_full_package(package)
            result=subprocess.run([sys.executable,"-I","-B","-c",code,str(Path(probe.__file__).resolve()),str(package)],
                                  cwd=folder,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)


class RealSparkProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        old=os.environ.get("PYSPARK_PYTHON");os.environ["PYSPARK_PYTHON"]=sys.executable
        def restore():
            if old is None:os.environ.pop("PYSPARK_PYTHON",None)
            else:os.environ["PYSPARK_PYTHON"]=old
        cls.addClassCleanup(restore)
        cls.spark=(SparkSession.builder.master("local[2]").appName("hi-growth-probe-offline")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2")
            .config("spark.ui.enabled","false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop);cls.spark.sparkContext.setLogLevel("ERROR")
        cls.frame=cls.spark.createDataFrame([
            transaction(-9,"2022-09-01 09:00",(1,"A"),(2,"B"),dataset="HI-Medium"),
            transaction(9007199254740993,"2022-09-01 09:00",(1,"X"),(2,"B"),dataset="HI-Medium"),
            transaction(3,"2022-09-02 09:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
            transaction(4,"2022-09-03 09:00",(2,"C"),(3,"D"),dataset="HI-Medium")],CLEANED_SCHEMA)

    def setUp(self):
        self.context=SimpleNamespace(spark_session=self.spark)
        self.clients=[MagicMock() for _ in range(3)]
        for c in self.clients:c.meta.region_name="us-east-1"
        self.clients[0].get_caller_identity.return_value={"Account":"123456789012"}

    def run_probe(self, args=None, inspections=None, frame=None):
        with patch.object(probe,"inspect_full_source",side_effect=inspections or [snapshot(),snapshot()]) as inspect, \
             patch.object(probe,"_read_selected",return_value=self.frame if frame is None else frame), \
             patch("bq2_sequence_builder.build_three_hop_links",side_effect=AssertionError("triples forbidden")), \
             patch("bq2_sequence_builder.build_sequence_output",side_effect=AssertionError("outputs forbidden")):
            result=probe.run_probe(probe.parse_arguments(args or argv()),glue_context=self.context,
                identity_client=self.clients[0],catalog_client=self.clients[1],s3_client=self.clients[2])
        self.assertEqual(inspect.call_count,2)
        self.assertFalse(self.clients[2].method_calls)
        self.assertFalse(self.clients[1].method_calls)
        return result

    def assert_clean(self):
        self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)

    def test_actual_pair_and_degree_functions_boundaries_branching_bigints(self):
        args=argv()
        for name,value in (("MAX_INPUT_ROWS","4"),("MAX_PAIR_CANDIDATES","3"),("MAX_PAIR_LINKS","3")):
            args[args.index("--"+name)+1]=value
        result=self.run_probe(args)
        self.assertEqual({n:result[n] for n in ("input_rows","pair_candidates","valid_pairs","starting_pairs","projected_triples","maximum_middle_expansion","projected_output_rows")},
            dict(input_rows=4,pair_candidates=3,valid_pairs=3,starting_pairs=2,projected_triples=2,maximum_middle_expansion=2,projected_output_rows=4))
        self.assertEqual(result["status"],"PROFILED_READ_ONLY");self.assertFalse(result["triples_materialized"])
        self.assertEqual(result["source_before"],result["source_after"]);self.assert_clean()

    def test_candidate_ceiling_blocks_pair_join_and_cleans(self):
        args=argv();args[args.index("--MAX_PAIR_CANDIDATES")+1]="2"
        with patch("bq2_sequence_builder.build_two_hop_links",side_effect=AssertionError("must not join")) as build, \
             self.assertRaises(probe.GrowthProbeFailure) as caught:
            self.run_probe(args)
        self.assertEqual(caught.exception.report["pair_candidates"],3)
        self.assertEqual(caught.exception.report["error_type"],"GrowthLimitExceeded")
        build.assert_not_called();self.assert_clean()

    def test_other_limits_source_change_and_validation_failure(self):
        for name,value in (("MAX_INPUT_ROWS","3"),("MAX_PAIR_LINKS","2")):
            args=argv();args[args.index("--"+name)+1]=value
            with self.subTest(name=name),self.assertRaises(probe.GrowthProbeFailure):self.run_probe(args)
            self.assert_clean()
        changed=snapshot();changed["source_fingerprint"]="e"*64
        with self.assertRaises(probe.GrowthProbeFailure):self.run_probe(inspections=[snapshot(),changed])
        self.assert_clean()
        from pyspark.sql import functions as F
        invalid=self.frame.withColumn("dataset",F.lit("LI-Medium"))
        with self.assertRaises(probe.GrowthProbeFailure):self.run_probe(frame=invalid)
        self.assert_clean()

    def test_identity_snapshot_missing_partition_and_cleanup_failures(self):
        self.clients[0].get_caller_identity.return_value={"Account":"000000000000"}
        with self.assertRaises(probe.GrowthProbeFailure):self.run_probe()
        self.clients[0].get_caller_identity.return_value={"Account":"123456789012"}
        wrong=snapshot();wrong["table_fingerprint"]="f"*64
        with self.assertRaises(probe.GrowthProbeFailure):self.run_probe(inspections=[wrong,wrong])
        missing=snapshot();missing["available_dates"].remove("2022-09-02")
        with self.assertRaises(probe.GrowthProbeFailure):self.run_probe(inspections=[missing,missing])
        from pyspark.sql import DataFrame
        original=DataFrame.unpersist
        def cleanup(frame,*args,**kwargs):
            original(frame,*args,**kwargs)
            raise RuntimeError("injected cleanup failure")
        with patch.object(DataFrame,"unpersist",cleanup),self.assertRaises(probe.GrowthProbeFailure) as caught:
            self.run_probe()
        self.assertEqual(caught.exception.report["error_type"],"RuntimeError");self.assert_clean()

    def test_empty_selected_data_and_elapsed_budget(self):
        result=self.run_probe(frame=self.spark.createDataFrame([],CLEANED_SCHEMA))
        self.assertEqual([result[n] for n in ("input_rows","pair_candidates","valid_pairs","starting_pairs","projected_triples","maximum_middle_expansion")],[0]*6)
        self.assert_clean()
        import itertools
        with patch.object(probe.time,"monotonic",side_effect=itertools.count(0,1000)),self.assertRaises(probe.GrowthProbeFailure) as caught:
            self.run_probe()
        self.assertEqual(caught.exception.report["error_type"],"TimeoutError");self.assert_clean()


if __name__=="__main__":unittest.main()
