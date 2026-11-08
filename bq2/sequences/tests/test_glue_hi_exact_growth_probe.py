"""Exact grouped-window counts versus existing enumerating builders. Offline."""
from copy import deepcopy
from datetime import datetime
import io
import os
from pathlib import Path
import random
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock,patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import glue_hi_exact_growth_probe as probe
from test_glue_hi_growth_probe import argv as prior_argv
from test_full_generation import snapshot
from sequence_source import CLEANED_SCHEMA
from fixtures import transaction


def argv():
    old=prior_argv();values=dict(zip(old[::2],old[1::2]))
    for n in ("--MAX_PAIR_CANDIDATES","--MAX_PAIR_LINKS"):values.pop(n)
    values.update({"--MAX_INPUT_ROWS":"1000","--MAX_TIMESTAMP_GROUPS":"1000","--MAX_EVENT_GROUPS":"2000",
        "--MAX_GROUPS_PER_KEY":"1000","--SHUFFLE_PARTITIONS":"2","--MAX_RUN_SECONDS":"300"})
    return [v for k,value in values.items() for v in (k,value)]


class ProtocolTests(unittest.TestCase):
    def test_invalid_arguments_fail_before_runtime(self):
        cases=[]
        for name,value in (("ALLOW_AWS_READS","false"),("REGION","us-west-2"),("EXPECTED_ACCOUNT_ID","bad"),
            ("SOURCE_FINGERPRINT","bad"),("NO_REFRESH_ACKNOWLEDGED","false"),("TIMEZONE_EVIDENCE",""),
            ("LIMITS_EVIDENCE",""),*( (n,"0") for n in probe.LIMIT_NAMES),("SHUFFLE_PARTITIONS","999999")):
            args=argv();args[args.index("--"+name)+1]=value;cases.append(args)
        for name in probe.LIMIT_NAMES:
            args=argv();i=args.index("--"+name);del args[i:i+2];cases.append(args)
        cases.extend([argv()+["--DATASET","LI-Medium"],argv()+["--ALLOW_AWS_WRITES","true"],
            argv()+["--ALLOW_AWS_READS","true"],argv()+["--conf","unsafe"]])
        with patch.object(probe,"_initialize") as init,patch("sys.stdout"),patch("sys.stderr"):
            for args in cases:
                with self.subTest(args=args),self.assertRaises((ValueError,SystemExit)):probe.main(args)
            init.assert_not_called()

    def test_separate_job_bootstrap_and_failure_return(self):
        root=probe.SCRIPT_ROOT+"hi_exact_growth_probe_20261108_01/"
        args=argv()+["--JOB_NAME",probe.JOB_NAME,"--extra-py-files",root+"role5_exact_modules.zip",
            "--scriptLocation",root+"glue_hi_exact_growth_probe.py","--additional-python-modules",root+"role5_sdk.gluewheels.zip",
            "--python-modules-installer-option","--no-index"]
        probe.validate_arguments(probe.parse_arguments(args))
        with self.assertRaises(ValueError):probe.validate_arguments(probe.parse_arguments(argv()+["--extra-py-files",root+"role5_full_modules.zip"]))
        error=probe.ExactGrowthFailure(dict(error_type="GrowthLimitExceeded",status="FAILED"))
        with patch.object(probe,"_initialize",return_value={}),patch.object(probe,"run_probe",side_effect=error),patch("sys.stdout") as out,self.assertRaises(probe.ExactGrowthFailure):
            probe.main(argv())
        self.assertTrue(out.write.called)
        with patch.object(probe,"_initialize",return_value={}),patch.object(probe,"run_probe",return_value={"status":"EXACT_COUNTS_READ_ONLY"}),patch("sys.stdout"):
            self.assertEqual(probe.main(argv()),0)

    def test_no_enumeration_or_write_apis(self):
        import ast
        tree=ast.parse(Path(probe.__file__).read_text())
        names={n.id for n in ast.walk(tree) if isinstance(n,ast.Name)}|{n.attr for n in ast.walk(tree) if isinstance(n,ast.Attribute)}
        self.assertFalse(names & {"build_two_hop_links","build_three_hop_links","build_sequence_output","put_object",
            "copy_object","delete_object","create_table","create_partition","update_partition","publish_full_generation","stage"})


class ExactSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        import time
        old_tz=os.environ.get("TZ");os.environ["TZ"]="UTC"
        if hasattr(time,"tzset"):time.tzset()
        def restore_tz():
            if old_tz is None:os.environ.pop("TZ",None)
            else:os.environ["TZ"]=old_tz
            if hasattr(time,"tzset"):time.tzset()
        cls.addClassCleanup(restore_tz)
        old=os.environ.get("PYSPARK_PYTHON");os.environ["PYSPARK_PYTHON"]=sys.executable
        def restore():
            if old is None:os.environ.pop("PYSPARK_PYTHON",None)
            else:os.environ["PYSPARK_PYTHON"]=old
        cls.addClassCleanup(restore)
        cls.spark=(SparkSession.builder.master("local[2]").appName("hi-exact-counts-offline")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2")
            .config("spark.ui.enabled","false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop);cls.spark.sparkContext.setLogLevel("ERROR")

    def frame(self, rows):return self.spark.createDataFrame(rows,CLEANED_SCHEMA)

    def exact(self,frame,args=None):
        report={}
        probe._exact_counts(frame,probe.validate_arguments(probe.parse_arguments(args or argv())),lambda:None,report)
        self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)
        self.assertEqual(self.spark.conf.get("spark.sql.shuffle.partitions"),"2")
        return report

    def compare(self,rows):
        from bq2_sequence_builder import build_two_hop_links,build_three_hop_links
        frame=self.frame(rows)
        links=build_two_hop_links(frame,first_dates=probe.DATES[:2])
        actual=[r.asDict() for r in links.collect()]
        owners={r["txn_id"] for r in rows if r["txn_date"]==probe.DATES[0]}
        anchored=[r for r in actual if r["first_txn_id"] in owners]
        before={};after={}
        for r in anchored:before[r["second_txn_id"]]=before.get(r["second_txn_id"],0)+1
        for r in actual:after[r["first_txn_id"]]=after.get(r["first_txn_id"],0)+1
        from pyspark.sql import functions as F
        starts=links.where(F.col("first_txn_id").isin(list(owners)))
        triples=build_three_hop_links(links,starting_links=starts).count()
        expected=dict(owner_pairs=len(anchored),all_valid_pairs_preceding_sep1_sep2=len(actual),
            reachable_continuation_pairs=sum(after.get(b,0) for b in before),projected_triples=triples,
            maximum_middle_expansion=max((p*after.get(b,0) for b,p in before.items()),default=0))
        result=self.exact(frame)
        self.assertEqual({n:result[n] for n in expected},expected)
        return result

    def test_boundaries_branching_equal_timestamps_multiplicity_and_bigints(self):
        rows=[
            transaction(-9,"2022-09-01 00:00",(1,"A"),(2,"B"),dataset="HI-Medium"),
            transaction(9007199254740993,"2022-09-01 00:00",(1,"A"),(2,"B"),dataset="HI-Medium"),
            transaction(3,"2022-09-02 00:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
            transaction(4,"2022-09-02 00:00",(2,"B"),(2,"C"),dataset="HI-Medium"),
            transaction(5,"2022-09-03 00:00",(2,"C"),(3,"D"),dataset="HI-Medium"),
            transaction(6,"2022-09-03 00:01",(2,"C"),(3,"E"),dataset="HI-Medium"),
            transaction(7,"2022-09-01 00:00",(2,"B"),(1,"Q"),dataset="HI-Medium"),
            transaction(8,"2022-09-02 00:01",(2,"B"),(1,"Q"),dataset="HI-Medium"),
            transaction(9,"2022-09-01 23:59",(4,"X"),(5,"Y"),dataset="HI-Medium"),
            transaction(10,"2022-09-02 00:00",(5,"Y"),(5,"Z"),dataset="HI-Medium"),
            transaction(11,"2022-09-02 00:01",(5,"Z"),(4,"X"),dataset="HI-Medium")]
        random.Random(42).shuffle(rows)
        result=self.compare(rows)
        self.assertEqual(result["projected_triples"],5)
        self.assertEqual(result["maximum_middle_expansion"],2)

    def test_seeded_random_against_existing_builders(self):
        rng=random.Random(11)
        times=["2022-09-01 09:00","2022-09-01 09:01","2022-09-02 09:00","2022-09-02 09:01","2022-09-03 09:00"]
        accounts=[(1,"A"),(1,"B"),(2,"A")]
        self.compare([transaction(i,rng.choice(times),rng.choice(accounts),rng.choice(accounts),dataset="HI-Medium") for i in range(1,31)])

    def test_empty_and_no_links(self):
        result=self.exact(self.frame([]))
        self.assertEqual(result["owner_pairs"],0);self.assertEqual(result["projected_triples"],0)
        self.compare([transaction(1,"2022-09-01 09:00",(1,"A"),(2,"B"),dataset="HI-Medium")])

    def test_resource_limits_and_invalid_source_cleanup(self):
        rows=[transaction(i,"2022-09-01 09:0"+str(i),(1,"A"),(1,"B"),dataset="HI-Medium") for i in range(1,4)]
        for name in ("MAX_INPUT_ROWS","MAX_TIMESTAMP_GROUPS","MAX_EVENT_GROUPS","MAX_GROUPS_PER_KEY"):
            args=argv();args[args.index("--"+name)+1]="1"
            with self.subTest(name=name),self.assertRaises(ValueError):self.exact(self.frame(rows),args)
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)
        with self.assertRaises(ValueError):self.exact(self.frame(rows+[rows[0]]))
        wrong=dict(rows[0],dataset="LI-Medium")
        with self.assertRaises(ValueError):self.exact(self.frame([wrong]))

    def test_orchestration_final_inventory_and_failure_propagation(self):
        context=SimpleNamespace(spark_session=self.spark);clients=[MagicMock() for _ in range(3)]
        for c in clients:c.meta.region_name="us-east-1"
        clients[0].get_caller_identity.return_value={"Account":"123456789012"}
        frame=self.frame([transaction(1,"2022-09-01 09:00",(1,"A"),(2,"B"),dataset="HI-Medium")])
        for changed in (False,True):
            after=snapshot()
            if changed:after["source_fingerprint"]="e"*64
            with patch.object(probe,"inspect_full_source",side_effect=[snapshot(),after]) as inspect,patch.object(probe,"_read_selected",return_value=frame):
                if changed:
                    with self.assertRaises(probe.ExactGrowthFailure):probe.run_probe(probe.parse_arguments(argv()),glue_context=context,identity_client=clients[0],catalog_client=clients[1],s3_client=clients[2])
                else:
                    result=probe.run_probe(probe.parse_arguments(argv()),glue_context=context,identity_client=clients[0],catalog_client=clients[1],s3_client=clients[2])
                    self.assertEqual(result["status"],"EXACT_COUNTS_READ_ONLY")
                self.assertEqual(inspect.call_count,2)
        self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)
        self.assertFalse(clients[1].method_calls);self.assertFalse(clients[2].method_calls)


if __name__=="__main__":unittest.main()
