"""Offline validation tests; do not connect to AWS or Neo4j."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import sys

spec = importlib.util.spec_from_file_location('run_graph', Path(__file__).with_name('run_graph.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ValidationTests(unittest.TestCase):
    def fixture(self):
        tx = [
            dict(zip(runner.TX_HEADER, ['-1','0','A','2','B','2022-09-01T01:00:00','10.20','',''])),
            dict(zip(runner.TX_HEADER, ['2','2','B','0','A','2022-09-01T02:00:00','2.30','',''])),
            dict(zip(runner.TX_HEADER, ['3','2','B','2','C','2022-09-01T03:00:00','1.00','','']))]
        bank = [dict(zip(runner.BANK_HEADER, ['0','2','1','10.20'])),
                dict(zip(runner.BANK_HEADER, ['2','0','1','2.30']))]
        baseline = dict(evidence_transactions='3', evidence_usd='13.50',
                        cross_bank_transactions='2', cross_bank_usd='12.50')
        return bank, tx, baseline

    def test_same_bank_and_missing_attempt_are_retained(self):
        totals = runner.check_inputs(*self.fixture())
        self.assertEqual(totals['evidence_transactions'], 3)
        self.assertEqual(totals['cross_bank_transactions'], 2)
        self.assertEqual(totals['banks'], 2)

    def test_duplicate_transaction_stops_run(self):
        bank, tx, baseline = self.fixture()
        tx[1]['txn_id'] = '-1'
        with self.assertRaisesRegex(RuntimeError, 'Duplicate transaction'):
            runner.check_inputs(bank, tx, baseline)

    def test_wrong_corridor_sum_stops_run(self):
        bank, tx, baseline = self.fixture()
        bank[0]['laundering_usd'] = '999'
        with self.assertRaisesRegex(RuntimeError, 'USD mismatch'):
            runner.check_inputs(bank, tx, baseline)

    def test_truncated_export_stops_run(self):
        bank, tx, baseline = self.fixture()
        with self.assertRaises(RuntimeError):
            runner.check_inputs(bank, tx[:-1], baseline)

    def test_missing_account_stops_run(self):
        bank, tx, baseline = self.fixture()
        tx[0]['from_account'] = ' '
        with self.assertRaisesRegex(RuntimeError, 'Missing graph key'):
            runner.check_inputs(bank, tx, baseline)

    def test_twenty_million_boundary_is_excluded(self):
        bank, tx, baseline = self.fixture()
        tx[0]['amount_paid_usd'] = '20000000'
        with self.assertRaisesRegex(RuntimeError, 'Unexpected amount'):
            runner.check_inputs(bank, tx, baseline)

    def test_duplicate_features_and_nonfinite_scores_rejected(self):
        records = [dict(bank_id='0',betweenness=1.0,community_id=1)] * 2
        with self.assertRaisesRegex(RuntimeError, 'Duplicate bank'):
            runner.validate_features(records, {'banks':2})
        records = [dict(bank_id='0',betweenness=float('nan'),community_id=1)]
        with self.assertRaisesRegex(RuntimeError, 'Invalid|invalid'):
            runner.validate_features(records, {'banks':1})

    def test_full_export_is_not_browser_limited(self):
        records = [dict(bank_id=str(i),betweenness=0.0,community_id=i%113) for i in range(5159)]
        runner.validate_features(records, {'banks':5159})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bank_features.csv'
            runner.write_csv(path, records, runner.FEATURE_HEADER)
            self.assertEqual(len(runner.rows(path, runner.FEATURE_HEADER)),5159)

    def test_source_files_parse_without_browser_commands(self):
        exports = runner.statements(runner.ROOT / 'export_edges.sql')
        imports = runner.statements(runner.ROOT / 'import.cypher')
        self.assertEqual(len(exports),3)
        self.assertEqual(sum(s.startswith('LOAD CSV') for s in imports),2)
        self.assertTrue(imports[-1].startswith('CALL {'))
        self.assertFalse(any(s.startswith(':param') for s in imports))

    def simulate_pipeline(self, truncate=False):
        bank, tx, baseline = self.fixture()
        totals = runner.check_inputs(bank, tx, baseline)
        publishes = []
        calls = []
        class FakeDB:
            def __init__(self, *_): pass
            def query(self, sql, params=None):
                calls.append(sql)
                if 'RETURN versions,edition' in sql:
                    return [{'versions':['5.26.32'],'edition':'community'}]
                if 'gds.version()' in sql: return [{'version':'test'}]
                if 'AS scopes' in sql: return [{'scopes':['auto-test']}]
                if 'RETURN $scope AS run_id' in sql:
                    return [{**totals,'run_id':'auto-test'}]
                if 'RETURN nodeCount,relationshipCount' in sql:
                    return [{'nodeCount':2,'relationshipCount':2}]
                if 'RETURN bytesMax' in sql: return [{'bytesMax':1000}]
                if 'gds.betweenness.write(' in sql:
                    return [{'nodePropertiesWritten':2}]
                if 'gds.louvain.write(' in sql:
                    return [{'communityCount':2,'modularity':.3,'nodePropertiesWritten':2}]
                if 'AS betweenness,b.community_id' in sql:
                    return [dict(bank_id=str(i),betweenness=1.,community_id=i)
                            for i in ([0] if truncate else [0,2])]
                if 'count(*) AS bank_count' in sql:
                    return [dict(community_id=i,bank_count=1) for i in [0,2]]
                return []
        def fake_athena(sql, args, work, download=True):
            if not download: return 'ddl-id', None
            path = work / ('query-' + str(len(list(work.glob('query-*')))) + '.csv')
            if 'AS bad ' in sql:
                records = [dict(banks='2',unique_banks='2',communities='2',bad='0')]
                header = list(records[0])
            elif 'AS evidence_transactions' in sql:
                records, header = [baseline], list(baseline)
            elif 'GROUP BY from_bank' in sql:
                records, header = bank, runner.BANK_HEADER
            else:
                records, header = tx, runner.TX_HEADER
            runner.write_csv(path,records,header)
            return path.stem,path
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory); (home/'import').mkdir()
            args = SimpleNamespace(role4_home=str(home),run_id='auto-test',dataset='HI-Medium',
                region='us-east-1',bucket='test-bucket',query_timeout=5,memory_budget_mib=128)
            fake_lock = SimpleNamespace(LOCK_EX=1,LOCK_NB=2,flock=lambda *_:None)
            with patch.dict(sys.modules, {'fcntl':fake_lock}), \
                 patch.object(runner,'Neo4j',FakeDB), \
                 patch.object(runner,'query_athena',fake_athena), \
                 patch.object(runner,'aws_json',return_value={}), \
                 patch.object(runner,'publish',side_effect=lambda a,p,k:publishes.append(k)):
                if truncate:
                    with self.assertRaisesRegex(RuntimeError,'Incomplete feature export'):
                        runner.run(args)
                else:
                    runner.run(args)
        return publishes,calls

    def test_success_receipt_published_last_after_full_pipeline_checks(self):
        publishes,calls = self.simulate_pipeline()
        self.assertEqual(publishes[-1],'bq1_graph_results/auto-test/success.json')
        self.assertIn('bq1_graph_results/auto-test/bank_features/bank_features.csv',publishes)
        self.assertTrue(any('file:///auto-auto-test/' in sql for sql in calls))

    def test_truncated_features_cannot_publish_success_or_features(self):
        publishes,_ = self.simulate_pipeline(truncate=True)
        self.assertFalse(any(k.endswith('success.json') or '/bank_features/' in k for k in publishes))


if __name__ == '__main__':
    unittest.main()
