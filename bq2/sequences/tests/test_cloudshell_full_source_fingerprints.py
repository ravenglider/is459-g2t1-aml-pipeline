"""Offline parity with the production inspector; no JVM, credentials or AWS."""
import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cloudshell_full_source_fingerprints as helper
import sequence_full_source as production
from sequence_source import CLEANED_SCHEMA
from test_full_generation import source_clients


class FingerprintParityTests(unittest.TestCase):
    def parity(self, dataset="HI-Medium", mutate=None):
        catalog, s3, parts = source_clients(dataset)
        if mutate:
            mutate(catalog, s3, parts)
        expected = production.inspect_full_source(catalog, s3, dataset)
        actual = helper.inspect_full_source(catalog, s3, dataset)
        for key in ("source_fingerprint", "table_fingerprint", "partitions_fingerprint",
                    "objects", "owner_dates", "files_by_date", "available_dates", "table"):
            self.assertEqual(actual[key], expected[key], key)
        self.assertTrue(all(c[0] == "list" for c in s3.calls))
        self.assertEqual(set(catalog.method_calls[i][0] for i in range(len(catalog.method_calls))),
                         {"get_table", "get_partitions"})
        return actual, catalog, s3, parts

    def test_exact_schema_and_both_full_dataset_paginated_parity(self):
        self.assertEqual(list(helper.CLEANED_COLUMNS), [(f.name, f.dataType.simpleString()) for f in CLEANED_SCHEMA])
        for dataset, count in (("HI-Medium", 28), ("LI-Medium", 27)):
            with self.subTest(dataset=dataset):
                actual, catalog, s3, _ = self.parity(dataset)
                self.assertEqual(len(actual["available_dates"]), count)
                self.assertGreater(catalog.get_partitions.call_count, 2)
                self.assertGreaterEqual(len(s3.calls), count)
                result = helper.inspect_twice(catalog, s3, dataset)
                self.assertEqual((result["inspections"], result["partitions"], result["parquet_files"]), (2, count, count))

    def test_ordering_timezone_unicode_and_success_markers(self):
        def mutate(c, s, p):
            p.reverse()
            c.get_table.return_value["Table"].update(Parameters={"note": "frozen \u4ea4\u6613"},
                VersionId="2", UpdateTime=datetime(2026, 11, 1, tzinfo=timezone(timedelta(hours=8))))
            for part in p:
                part["CreationTime"] = datetime(2026, 11, 1, tzinfo=timezone(timedelta(hours=8)))
            for obj in s.objects.values():
                obj["LastModified"] = obj["LastModified"].astimezone(timezone(timedelta(hours=-5)))
            s.seed("transactions_clean/dataset=HI-Medium/_SUCCESS", b"")
        actual, c, s, p = self.parity(mutate=mutate)
        p.reverse()
        self.assertEqual(helper.inspect_full_source(c, s, "HI-Medium")["source_fingerprint"], actual["source_fingerprint"])
        self.assertEqual(len(actual["objects"]), 29)

    def test_all_six_sample_paths_cannot_substitute_for_full_coverage(self):
        c, s, p = source_clients()
        del p[6:]
        for inspector in (helper.inspect_full_source, production.inspect_full_source):
            with self.assertRaises(ValueError): inspector(c, s, "HI-Medium")

    def test_object_and_catalog_changes_change_the_same_fingerprints(self):
        mutations = (
            ("source_fingerprint", lambda c,s,p: s.seed(next(iter(s.objects)), b"different")),
            ("source_fingerprint", lambda c,s,p: s.seed(next(iter(s.objects)).replace("part.parquet", "extra.parquet"))),
            ("source_fingerprint", lambda c,s,p: s.seed("transactions_clean/dataset=HI-Medium/_SUCCESS", b"")),
            ("table_fingerprint", lambda c,s,p: c.get_table.return_value["Table"].update(VersionId="changed")),
            ("partitions_fingerprint", lambda c,s,p: p[0].update(Parameters={"changed": "yes"})),
        )
        for key, mutate in mutations:
            with self.subTest(key=key):
                before, c, s, p = self.parity()
                mutate(c, s, p)
                after = helper.inspect_full_source(c, s, "HI-Medium")
                expected = production.inspect_full_source(c, s, "HI-Medium")
                self.assertNotEqual(before[key], after[key])
                self.assertEqual(after[key], expected[key])

    def test_size_etag_timestamp_and_partition_location_changes_detected(self):
        for field,value in (("raw",b"different size"),("ETag",'"changed"'),
                            ("LastModified",datetime(2026,11,8,tzinfo=timezone.utc))):
            c,s,p=source_clients()
            before=helper.inspect_full_source(c,s,"HI-Medium")
            s.objects[next(iter(s.objects))][field]=value
            after=helper.inspect_full_source(c,s,"HI-Medium")
            self.assertNotEqual(before["source_fingerprint"],after["source_fingerprint"])
            self.assertEqual(after["source_fingerprint"],production.inspect_full_source(c,s,"HI-Medium")["source_fingerprint"])
        c,s,p=source_clients()
        before=helper.inspect_full_source(c,s,"HI-Medium")
        # Both locations are allowed but the raw Catalog change must still hash differently.
        p[0]["StorageDescriptor"]["Location"]+="/"
        after=helper.inspect_full_source(c,s,"HI-Medium")
        self.assertNotEqual(before["partitions_fingerprint"],after["partitions_fingerprint"])
        self.assertEqual(after["partitions_fingerprint"],production.inspect_full_source(c,s,"HI-Medium")["partitions_fingerprint"])

    def test_failure_in_second_inspection_never_prints_accepted_evidence(self):
        c,s,_=source_clients()
        original=helper.inspect_full_source
        with patch.object(helper,"inspect_full_source",side_effect=[original(c,s,"HI-Medium"),RuntimeError("metadata read failed")]), \
             patch("sys.stdout",new_callable=io.StringIO) as output, self.assertRaises(RuntimeError):
            helper.inspect_twice(c,s,"HI-Medium")
        self.assertEqual(output.getvalue(),"")

    def test_missing_extra_duplicate_and_malformed_metadata_rejected_by_both(self):
        def missing_object(c,s,p): del s.objects[next(iter(s.objects))]
        def bad_schema(c,s,p): c.get_table.return_value["Table"]["PartitionKeys"][0]["Type"] = "int"
        mutations = [lambda c,s,p: p.pop(), missing_object, bad_schema,
            lambda c,s,p: p.append(deepcopy(p[0])),
            lambda c,s,p: p[0]["Values"].__setitem__(0,"LI-Medium"),
            lambda c,s,p: p[0]["StorageDescriptor"].update(Location="s3://wrong/"),
            lambda c,s,p: s.seed("transactions_clean/dataset=HI-Medium/unknown.txt"),
            lambda c,s,p: s.seed(next(iter(s.objects)), b""),
            lambda c,s,p: s.objects[next(iter(s.objects))].update(LastModified=datetime(2026,1,1)),
            lambda c,s,p: s.objects[next(iter(s.objects))].update(ETag=None),
            lambda c,s,p: c.get_table.return_value["Table"].update(UpdateTime=datetime(2026,1,1)),
        ]
        for i, mutate in enumerate(mutations):
            c,s,p = source_clients(); mutate(c,s,p)
            for inspector in (helper.inspect_full_source, production.inspect_full_source):
                with self.subTest(case=i, inspector=inspector.__module__), self.assertRaises((ValueError,TypeError,KeyError)):
                    inspector(c,s,"HI-Medium")

    def test_duplicate_object_and_looping_pagination_rejected(self):
        for inspector in (helper.inspect_full_source, production.inspect_full_source):
            c,s,_ = source_clients()
            page = s.list_objects_v2(Bucket=helper.BUCKET, Prefix="transactions_clean/dataset=HI-Medium/")
            s.list_objects_v2 = MagicMock(return_value=page)
            with self.assertRaises(ValueError): inspector(c,s,"HI-Medium")
            c,s,_ = source_clients()
            c.get_partitions.side_effect = None
            c.get_partitions.return_value = dict(Partitions=[],NextToken="loop")
            with self.assertRaises(ValueError): inspector(c,s,"HI-Medium")

    def test_two_inspections_required_and_changes_rejected(self):
        for target in ("object", "table", "partition", "removed"):
            c,s,p = source_clients(); original = helper.inspect_full_source; calls=[]
            def inspect(*args, **kwargs):
                if calls:
                    if target=="object": s.seed(next(iter(s.objects)), b"changed")
                    elif target=="table": c.get_table.return_value["Table"].update(VersionId="new")
                    elif target=="partition": p[0].update(Parameters={"new":"value"})
                    else: p.pop()
                calls.append(True)
                return original(*args, **kwargs)
            with patch.object(helper,"inspect_full_source",side_effect=inspect), self.assertRaises(ValueError):
                helper.inspect_twice(c,s,"HI-Medium")
            self.assertEqual(len(calls),2)

    def test_isolated_helper_import_without_spark_sdk_or_network(self):
        code = """import runpy,sys,socket
sys.modules['pyspark']=None
sys.modules['boto3']=None
def denied(*a,**k): raise AssertionError('network forbidden')
socket.socket=denied
runpy.run_path(sys.argv[1],run_name='offline_import')
print('inert import passed')
"""
        result=subprocess.run([sys.executable,"-I","-B","-c",code,str(Path(helper.__file__).resolve())],
                              capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn("inert import passed",result.stdout)

    def test_hashing_and_inspection_algorithm_match_production_syntax(self):
        root=Path(helper.__file__).parent
        def node(file, name):
            return next(n for n in ast.parse((root/file).read_text()).body
                        if isinstance(n,ast.FunctionDef) and n.name==name)
        for file,name in (("sequence_full_source.py","dataset_spec"),
                          ("sequence_full_source.py","owner_dates"),
                          ("sequence_full_source.py","inventory_objects"),
                          ("sequence_full_source.py","_serial"),
                          ("sequence_manifest.py","_digest"),
                          ("sequence_source.py","fingerprint_source_inventory")):
            self.assertEqual(ast.dump(node(file,name)),ast.dump(node(Path(helper.__file__).name,name)),name)
        expected=node("sequence_full_source.py","inspect_full_source")
        # Only the Spark-linked import and un-hashed batch planning are omitted.
        expected.body=[n for n in expected.body if not isinstance(n,ast.ImportFrom)
            and not (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="plans" for t in n.targets))]
        result=expected.body[-1].value
        result.keywords=[k for k in result.keywords if k.arg!="plans"]
        self.assertEqual(ast.dump(expected),ast.dump(node(Path(helper.__file__).name,"inspect_full_source")))

    def test_cli_account_region_gates_and_only_read_clients(self):
        c,s,_ = source_clients(); c.meta.region_name=helper.REGION
        s.meta=SimpleNamespace(region_name=helper.REGION)
        sts=MagicMock(); sts.meta.region_name=helper.REGION
        sts.get_caller_identity.return_value={"Account":"123456789012"}
        calls=[]
        def client(name,**kwargs):
            calls.append(name); return {"sts":sts,"glue":c,"s3":s}[name]
        session=MagicMock(); session.client.side_effect=client
        modules={"boto3":SimpleNamespace(Session=MagicMock(return_value=session)),
                 "botocore.config":SimpleNamespace(Config=lambda **kw: kw)}
        args=["--expected-account-id","123456789012","--region","us-east-1","--dataset","HI-Medium"]
        with patch.dict(sys.modules,modules), patch("sys.stdout",new_callable=io.StringIO) as output:
            self.assertEqual(helper.main(args),0)
        evidence=json.loads(output.getvalue())
        self.assertEqual(calls,["sts","glue","s3"])
        self.assertEqual(evidence["status"],"MATCHED_METADATA_ONLY")
        self.assertEqual(set(evidence),{"dataset","inspections","status","inspected_at","partitions","objects",
            "parquet_files","source_fingerprint","table_fingerprint","partitions_fingerprint"})
        self.assertTrue(all(call[0]=="list" for call in s.calls))
        calls.clear(); sts.get_caller_identity.return_value={"Account":"000000000000"}
        with patch.dict(sys.modules,modules), self.assertRaises(ValueError): helper.main(args)
        self.assertEqual(calls,["sts"])
        with patch.dict(sys.modules,{"boto3":None}), self.assertRaises(ValueError):
            helper.main(["--expected-account-id","bad","--region","us-east-1","--dataset","HI-Medium"])


if __name__ == "__main__":
    unittest.main()
