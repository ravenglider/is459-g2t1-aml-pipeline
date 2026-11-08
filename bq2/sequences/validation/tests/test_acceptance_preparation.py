"""Local contract and evidence-binding checks; no Spark, SDK or AWS access."""
import ast
from copy import deepcopy
import re
import hashlib
import json
import tempfile
from pathlib import Path
import sys
import unittest

VALIDATION = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(VALIDATION))
import render_acceptance as sql
import review_run as review
import prepare_queries as prepare


def fixture(dataset):
    days, spec = review.dates(dataset), sql.SPECS[dataset]
    source = {k: str(i)*64 for i, k in enumerate(sorted(review.FINGERPRINTS), 1)}
    manifest = dict(manifest_kind="full_sequence_generation_handoff", state="STAGED_VALIDATED",
        published=False, catalog_updated=False, publication_required=True, rule_version="v1",
        dataset=dataset, run_id="fixture_run", attempt_id="fixture_attempt", owner_token="fixture_owner",
        owner_dates=days, source_dates=days, source=source, source_after=deepcopy(source),
        counts={"2":1,"3":0}, row_count=1,
        source_validation=dict(row_count=spec["rows"], laundering_rows=spec["positive"],
            identity_check_passed=True, full_selected_snapshot_checked=True,
            checked_dates=days, source=deepcopy(source)),
        batches=[dict(start_date=d, counts={"2":int(i==0),"3":0}) for i,d in enumerate(days)])
    version = review.ROOT+"dataset="+dataset+"/versions/fixture_run/fixture_attempt/"
    publication = dict(manifest_kind="full_sequence_publication", state="PUBLISHED",
        published=True, catalog_updated=True, verified=True,
        binding=dict(identity={k:manifest[k] for k in ("dataset","run_id","attempt_id","owner_token")},
            source=deepcopy(source), counts=deepcopy(manifest["counts"]), row_count=1,
            version_root=version, generation_fingerprint=review.digest(manifest)))
    partitions = [dict(Values=[dataset,d], StorageDescriptor=dict(
        Location=version+"data/dataset="+dataset+"/start_date="+d+"/")) for d in days]
    return manifest, publication, partitions


class EvidenceBindingTests(unittest.TestCase):
    def test_both_full_scopes_and_explicit_empty_dates(self):
        for dataset in sql.SPECS:
            with self.subTest(dataset=dataset):
                result = review.review(*fixture(dataset))
                self.assertEqual(result["owner_dates"],sql.SPECS[dataset]["days"])
                self.assertEqual(result["live_athena_acceptance"],"NOT_ESTABLISHED_BY_THIS_REVIEW")

    def test_missing_empty_output_date_rejected(self):
        m,p,parts = fixture("HI-Medium")
        m["batches"].pop()
        with self.assertRaisesRegex(ValueError,"Every owner date"):
            review.review(m,p,parts)

    def test_changed_source_rejected(self):
        m,p,parts=fixture("HI-Medium")
        m["source_after"]["source_fingerprint"]="f"*64
        with self.assertRaisesRegex(ValueError,"snapshot changed"):
            review.review(m,p,parts)

    def test_unrelated_source_profile_rejected(self):
        m,p,parts=fixture("LI-Medium")
        m["source_validation"]["row_count"]=1
        with self.assertRaisesRegex(ValueError,"profile"):
            review.review(m,p,parts)

    def test_mixed_partition_version_rejected(self):
        m,p,parts=fixture("HI-Medium")
        parts[-1]["StorageDescriptor"]["Location"]=parts[-1]["StorageDescriptor"]["Location"].replace("fixture_run","stale_run")
        with self.assertRaisesRegex(ValueError,"partition version"):
            review.review(m,p,parts)

    def test_duplicate_partition_rejected(self):
        m,p,parts=fixture("LI-Medium")
        parts[-1]=deepcopy(parts[0])
        with self.assertRaisesRegex(ValueError,"Catalog dates"):
            review.review(m,p,parts)

    def test_missing_partition_page_rejected(self):
        m,p,parts=fixture("HI-Medium")
        with self.assertRaisesRegex(ValueError,"Catalog dates"):
            review.review(m,p,parts[:20])

    def test_reversed_partition_keys_rejected(self):
        m,p,parts=fixture("LI-Medium")
        parts[0]["Values"].reverse()
        with self.assertRaisesRegex(ValueError,"Catalog dates"):
            review.review(m,p,parts)

    def test_unverified_publication_rejected(self):
        m,p,parts=fixture("HI-Medium")
        p["verified"]=False
        with self.assertRaisesRegex(ValueError,"PUBLISHED"):
            review.review(m,p,parts)

    def test_changed_manifest_digest_rejected(self):
        m,p,parts=fixture("HI-Medium")
        m["builder"]={"version":"different_release"}
        with self.assertRaisesRegex(ValueError,"different run/source"):
            review.review(m,p,parts)

    def test_batch_totals_disagreement_rejected(self):
        m,p,parts=fixture("LI-Medium")
        m["batches"][0]["counts"]["2"]=2
        with self.assertRaisesRegex(ValueError,"Batch/run"):
            review.review(m,p,parts)

    def test_boolean_count_rejected(self):
        m,p,parts=fixture("LI-Medium")
        m["batches"][0]["counts"]["2"]=True
        with self.assertRaisesRegex(ValueError,"Invalid sequence counts"):
            review.review(m,p,parts)

    def test_other_dataset_is_not_replaced_or_combined(self):
        m,p,parts=fixture("HI-Medium")
        parts += fixture("LI-Medium")[2]
        self.assertEqual(review.review(m,p,parts)["owner_dates"],28)

    def test_invalid_partition_shape_rejected(self):
        m,p,parts=fixture("HI-Medium")
        parts[0]["Values"]=[]
        with self.assertRaisesRegex(ValueError,"Malformed"):
            review.review(m,p,parts)


class QueryFilePreparationTests(unittest.TestCase):
    def test_li_worksheet_matches_source_and_schema_contract(self):
        value=json.loads((VALIDATION/'LI_ARGUMENTS.template.json').read_text())
        self.assertEqual(value['dataset'],'LI-Medium')
        source=value['source_contract']; output=value['output_contract']
        self.assertEqual(source['reported_cleaned_transactions'],sql.SPECS['LI-Medium']['rows'])
        self.assertEqual(source['reported_positive_transactions'],sql.SPECS['LI-Medium']['positive'])
        self.assertEqual((source['first_owner_date'],source['last_owner_date'],source['owner_dates']),
                         ('2022-09-01','2022-09-27',27))
        self.assertEqual((output['rule_version'],output['logical_columns'],output['parquet_data_columns']),('v1',21,19))
        self.assertEqual(output['partition_keys'],['dataset','start_date'])

    def test_li_worksheet_does_not_authorise_execution_or_publication(self):
        value=json.loads((VALIDATION/'LI_ARGUMENTS.template.json').read_text())
        self.assertFalse(value['output_contract']['production_publication_authorised'])
        self.assertFalse(value['shared_release']['staging_only_interface_verified'])
        args=value['existing_interface_reference_only']
        for flag in ('--ALLOW_AWS_WRITES','--ALLOW_PUBLICATION','--READERS_PAUSED','--NO_REFRESH_ACKNOWLEDGED'):
            self.assertEqual(args[flag],'false')
        self.assertTrue(all(v is False for k,v in value['approvals'].items() if k.endswith('_approved')))
        self.assertEqual(value['approvals']['approval_references'],[])

    def test_li_worksheet_does_not_invent_fingerprints_limits_or_release(self):
        value=json.loads((VALIDATION/'LI_ARGUMENTS.template.json').read_text())
        self.assertIsNone(value['shared_release']['bounded_generator_commit'])
        for k,v in value['existing_interface_reference_only'].items():
            if k.startswith('--MAX_') or k.endswith('_FINGERPRINT'):
                self.assertIsNone(v)
        source=value['source_contract']
        self.assertTrue(all(source[k] is None for k in ('actual_source_fingerprint',
                            'actual_table_fingerprint','actual_partitions_fingerprint')))

    def test_plan_has_75_unique_files_with_correct_hashes(self):
        plan=prepare.query_plan()
        self.assertEqual(len(plan),75)
        self.assertEqual(len({q['path'] for q in plan}),75)
        self.assertTrue(all(q['sha256']==hashlib.sha256(q['sql'].encode('utf-8')).hexdigest() for q in plan))

    def test_only_full_source_queries_have_prepublication_source_phase(self):
        source=[q for q in prepare.query_plan() if q['phase']=='source']
        self.assertEqual(len(source),4)
        self.assertTrue(all('transactions_clean' in q['sql'] and 'bq2_sequences' not in q['sql'] for q in source))

    def test_every_owner_date_has_one_lineage_file(self):
        for dataset in sql.SPECS:
            items=[q for q in prepare.query_plan() if q['dataset']==dataset and q['phase']=='lineage_date']
            self.assertEqual([q['owner_date'] for q in items],review.dates(dataset))
            self.assertTrue(all(len(prepare.split_sql(q['sql']))==1 for q in items))

    def test_split_preserves_quoted_and_comment_semicolons(self):
        source="SELECT 'a;''b', \"name;here\"; -- comment;\n/* note; */ SELECT 2;"
        statements=prepare.split_sql(source)
        self.assertEqual(len(statements),2)
        self.assertIn("'a;''b'",statements[0])
        self.assertIn('SELECT 2',statements[1])

    def test_comment_only_input_is_not_a_query(self):
        self.assertEqual(prepare.split_sql('-- only a note;\n/* another; */'),[])

    def test_incomplete_quotes_and_comments_rejected(self):
        for source in ("SELECT 'unfinished",'SELECT "unfinished','/* unfinished'):
            with self.assertRaises(ValueError): prepare.split_sql(source)

    def test_preparation_writes_local_files_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            output=Path(folder)/'queries'
            self.assertEqual(prepare.write_plan(output),75)
            self.assertEqual(len(list(output.rglob('*.sql'))),75)
            import json
            manifest=json.loads((output/'query-plan.json').read_text())
            self.assertEqual(manifest['status'],'PREPARED_NOT_EXECUTED')
            with self.assertRaises(FileExistsError): prepare.write_plan(output)


class SQLPreparationTests(unittest.TestCase):
    def test_all_templates_render_both_datasets_without_write_statements(self):
        for dataset in sql.SPECS:
            for query in sql.QUERIES:
                with self.subTest(dataset=dataset,query=query):
                    rendered=sql.render(dataset,query)
                    self.assertNotIn("__",rendered)
                    code=re.sub(r"--[^\n]*","",rendered)
                    self.assertNotRegex(code,r"(?i)\b(CREATE|ALTER|DROP|INSERT|DELETE|UPDATE|UNLOAD|MSCK)\b")
                    self.assertIn("dataset='"+dataset+"'",rendered)

    def test_reject_dataset_alias_sample_and_injection(self):
        for bad in ("sample","HI","LI","HI-Medium'; DROP TABLE x;--"):
            with self.assertRaises(ValueError): sql.render(bad,sql.QUERIES[0])

    def test_reject_path_traversal(self):
        with self.assertRaises(ValueError): sql.render("HI-Medium","../../run_graph.py")

    def test_lineage_chunk_uses_owner_and_two_day_lookahead(self):
        rendered=sql.render('HI-Medium','04_lineage.sql','2022-09-03')
        self.assertIn("q.start_date='2022-09-03'",rendered)
        self.assertIn("txn_date BETWEEN '2022-09-03' AND '2022-09-05'",rendered)
        self.assertIn("'2022-09-03' AS owner_scope",rendered)

    def test_lineage_boundary_chunks_stop_at_extract_end(self):
        for dataset in sql.SPECS:
            last=sql.SPECS[dataset]['last']
            for day in review.dates(dataset)[-2:]:
                rendered=sql.render(dataset,'04_lineage.sql',day)
                self.assertIn("txn_date BETWEEN '"+day+"' AND '"+last+"'",rendered)

    def test_lineage_rejects_invalid_or_outside_owner_dates(self):
        for dataset,day in [('HI-Medium','2022-08-31'),('HI-Medium','2022-09-29'),
                            ('LI-Medium','2022-09-28'),('LI-Medium','2022-9-1'),
                            ('HI-Medium',"2022-09-01' OR true--")]:
            with self.assertRaises(ValueError): sql.render(dataset,'04_lineage.sql',day)

    def test_owner_date_cannot_narrow_source_baselines_or_other_checks(self):
        for query in sql.QUERIES:
            if query!='04_lineage.sql':
                with self.assertRaises(ValueError): sql.render('HI-Medium',query,'2022-09-01')

    def test_default_lineage_still_covers_full_dataset(self):
        rendered=sql.render('LI-Medium','04_lineage.sql')
        self.assertNotIn('q.start_date=',rendered)
        self.assertIn("txn_date BETWEEN '2022-09-01' AND '2022-09-27'",rendered)
        self.assertIn("'ALL' AS owner_scope",rendered)

    def test_all_55_lineage_chunks_render_without_placeholders(self):
        rendered=[sql.render(dataset,'04_lineage.sql',day)
                  for dataset in sql.SPECS for day in review.dates(dataset)]
        self.assertEqual(len(set(rendered)),55)
        self.assertTrue(all('__' not in text for text in rendered))

    def test_baselines_match_generation_source_contract_without_importing_spark(self):
        tree=ast.parse((VALIDATION.parent/'sequence_full_source.py').read_text(encoding='utf-8'))
        datasets=next(n.value for n in tree.body if isinstance(n,ast.Assign)
                      and any(isinstance(t,ast.Name) and t.id=='DATASETS' for t in n.targets))
        for k,v in zip(datasets.keys,datasets.values):
            fields={kw.arg:ast.literal_eval(kw.value) for kw in v.keywords}
            spec=sql.SPECS[ast.literal_eval(k)]
            self.assertEqual((spec['rows'],spec['positive'],spec['last']),
                             (fields['rows'],fields['laundering'],fields['last']))
            self.assertEqual(fields['first'],'2022-09-01')
            self.assertEqual(len(review.dates(ast.literal_eval(k))),spec['days'])

    def test_sql_schema_matches_all_21_approved_fields_in_order(self):
        tree=ast.parse((VALIDATION.parent/'sequence_staging.py').read_text(encoding='utf-8'))
        assignment=next(n for n in tree.body if isinstance(n,ast.Assign)
                        and any(isinstance(t,ast.Name) and t.id=='SEQUENCE_SCHEMA' for t in n.targets))
        def type_name(n):
            names={'StringType':'varchar','IntegerType':'integer','LongType':'bigint','TimestampType':'timestamp'}
            if n.func.id=='ArrayType': return 'array('+type_name(n.args[0])+')'
            return names[n.func.id]
        fields=[(ast.literal_eval(n.elts[0]),type_name(n.elts[1]))
                for n in assignment.value.args[0].generators[0].iter.elts]
        expected=re.findall(r"\((\d+),'([^']+)','([^']+)'\)",
                            (VALIDATION/'00_schema.sql').read_text(encoding='utf-8'))
        self.assertEqual(len(expected),21)
        self.assertEqual([int(p) for p,_,_ in expected],list(range(1,22)))
        self.assertEqual([(n,t) for _,n,t in expected],fields)

    def test_55_expected_dates_and_known_extract_boundaries(self):
        self.assertEqual(sum(len(review.dates(d)) for d in sql.SPECS),55)
        self.assertEqual(review.dates('HI-Medium')[-2:],['2022-09-27','2022-09-28'])
        self.assertEqual(review.dates('LI-Medium')[-2:],['2022-09-26','2022-09-27'])


if __name__=='__main__':
    unittest.main()
