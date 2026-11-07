"""Execute portable analytical SQL. Athena array validation/storage need real-engine checks."""
from pathlib import Path
import re
import sqlite3
import unittest

SQL_DIR = Path(__file__).resolve().parents[1]


class AnalyticsSQLTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function('split_part', 3, lambda s, sep, i: s.split(sep)[i - 1])
        self.db.executescript('''
            CREATE TABLE bq2_sequences (
                dataset TEXT, sequence_id TEXT, method_sequence TEXT,
                sequence_length INT, any_laundering INT,
                final_txn_id INT, final_is_laundering INT);
            CREATE TABLE transactions_clean (
                dataset TEXT, txn_id INT, is_laundering INT,
                txn_date TEXT DEFAULT '2022-09-01', usd_missing INT DEFAULT 0,
                PRIMARY KEY (dataset, txn_id));
        ''')

    def run_sql(self, filename):
        sql = (SQL_DIR / filename).read_text()
        sql = re.sub(r'WITH\s*\(\s*format\s*=.*?\)\s*AS', 'AS', sql, count=1, flags=re.S)
        self.db.executescript(sql.replace('is459_g2t1_aml.', ''))

    def add_pattern(self, dataset, pattern, count, positive, final_id, final_label,
                    length=2, start=1000):
        self.db.execute('INSERT OR IGNORE INTO transactions_clean (dataset, txn_id, is_laundering) VALUES (?, ?, ?)',
                        (dataset, final_id, final_label))
        for i in range(count):
            sid = f'{pattern}:{length}:{i}'
            self.db.execute('INSERT INTO bq2_sequences VALUES (?, ?, ?, ?, ?, ?, ?)',
                            (dataset, sid, pattern, length, positive, final_id, final_label))
            self.db.execute('INSERT INTO transactions_clean (dataset, txn_id, is_laundering) VALUES (?, ?, ?)',
                            (dataset, start + i, positive if not final_label else 0))
            if length == 3:
                self.db.execute('INSERT INTO transactions_clean (dataset, txn_id, is_laundering) VALUES (?, ?, 0)',
                                (dataset, start + count + i))

    def fixture(self):
        # Distinct incoming chains and patterns share final 1; [1,0] chains are positive.
        self.add_pattern('HI-Medium', 'Cash -> Wire', 100, 1, 1, 0, start=1000)
        self.add_pattern('HI-Medium', 'ACH -> Wire', 100, 1, 1, 0, start=2000)
        self.add_pattern('HI-Medium', 'Wire -> ACH', 100, 0, 2, 0, start=3000)
        self.add_pattern('HI-Medium', 'Rare -> Cash', 1, 1, 3, 1, start=4000)
        self.add_pattern('sample', 'Sample -> Sample', 100, 1, 1, 1, start=5000)
        self.add_pattern('LI-Medium', 'Cash -> Wire', 1, 0, 1, 0, start=1000)
        self.add_pattern('LI-Medium', 'Bitcoin -> ACH', 100, 1, 2, 1, start=2000)

    def policy(self, dataset='HI-Medium', length=2, cutoff=100, n=3):
        return self.db.execute('''SELECT * FROM bq2_tradeoff
            WHERE dataset=? AND sequence_length=? AND min_support=? AND top_n=?''',
                               (dataset, length, cutoff, n)).fetchone()

    def test_rare_patterns_remain_in_baseline_and_sample_is_excluded(self):
        self.fixture()
        self.run_sql('bq2_lift.sql')
        rows = list(self.db.execute("SELECT * FROM bq2_lift WHERE dataset='HI-Medium' AND min_support=100"))
        self.assertEqual(len(rows), 4)
        winner = next(r for r in rows if r['risk_rank'] == 1)
        self.assertEqual(winner['total_sequences'], 301)
        self.assertAlmostEqual(winner['baseline_rate'], 201 / 301)
        self.assertAlmostEqual(winner['lift'], 301 / 201)
        self.assertIsNone(next(r for r in rows if r['support_count'] == 1)['risk_rank'])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM bq2_lift WHERE dataset='sample'").fetchone()[0], 0)

    def test_final_only_distinct_checks_use_original_label_and_full_denominator(self):
        self.fixture()
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        row = self.policy()
        self.assertEqual(row['actual_patterns'], 3)
        self.assertEqual(row['reviewed'], 2)  # NOT 300 chains, nor all constituent IDs.
        self.assertEqual(row['laundering_flagged'], 0)  # Positive chains end in label 0.
        self.assertEqual(row['legitimate_reviewed'], 2)
        self.assertEqual(row['total_transactions'], 304)
        self.assertEqual(row['total_laundering'], 201)
        self.assertAlmostEqual(row['legitimate_review_rate'], 2 / 103)
        self.assertEqual(self.policy(n=0)['laundering_catch_rate'], 1)
        self.assertEqual(self.policy(n=0)['legitimate_review_rate'], 1)

    def test_li_selection_is_frozen_even_when_pattern_is_below_li_support(self):
        self.fixture()
        for f in ('bq2_lift.sql', 'tradeoff.sql', 'robustness.sql'):
            self.run_sql(f)
        row = self.policy(dataset='LI-Medium')
        self.assertEqual(row['reviewed'], 1)
        self.assertEqual(row['laundering_flagged'], 0)
        rows = {r['method_sequence']: r for r in self.db.execute('SELECT * FROM bq2_robustness WHERE min_support=100')}
        self.assertEqual(rows['Cash -> Wire']['li_status'], 'Below LI support cutoff')
        self.assertEqual(rows['ACH -> Wire']['li_status'], 'Absent on LI')
        self.assertIsNone(rows['ACH -> Wire']['li_lift'])

    def test_lengths_have_separate_baselines_and_final_id_policies(self):
        self.add_pattern('HI-Medium', 'Cash -> Wire', 100, 1, 1, 1, start=1000)
        self.add_pattern('HI-Medium', 'Cash -> Wire -> ACH', 100, 0, 2, 0, length=3, start=2000)
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        rows = list(self.db.execute('SELECT * FROM bq2_lift WHERE min_support=100 ORDER BY sequence_length'))
        self.assertEqual([r['baseline_rate'] for r in rows], [1, 0])
        self.assertEqual(rows[0]['risk_rank'], 1)
        self.assertIsNone(rows[1]['lift'])
        self.assertEqual(self.policy(length=2)['reviewed'], 1)
        self.assertEqual(self.policy(length=3)['reviewed'], 0)

    def test_support_boundaries_50_100_500_and_fewer_than_n(self):
        self.add_pattern('HI-Medium', 'A -> B', 50, 1, 1, 1, start=1000)
        self.add_pattern('HI-Medium', 'B -> C', 100, 1, 2, 1, start=2000)
        self.add_pattern('HI-Medium', 'C -> D', 500, 1, 3, 1, start=3000)
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        for cutoff, expected in ((50, 3), (100, 2), (500, 1)):
            self.assertEqual(self.policy(cutoff=cutoff, n=10)['actual_patterns'], expected)
            self.assertEqual(self.policy(cutoff=cutoff, n=10)['reviewed'], expected)

    def test_top_n_and_ties_are_deterministic(self):
        for i in range(11):
            self.add_pattern('HI-Medium', f'Method{i:02d} -> Wire', 100, 1, i + 1, 1, start=1000 + i * 100)
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        self.assertEqual(self.db.execute('SELECT method_sequence FROM bq2_lift WHERE min_support=100 AND risk_rank=1').fetchone()[0], 'Method00 -> Wire')
        for n in (3, 5, 10):
            self.assertEqual(self.policy(n=n)['reviewed'], n)
            self.assertEqual(self.policy(n=n)['actual_patterns'], n)

    def test_zero_positive_and_zero_review_denominators_are_null(self):
        self.add_pattern('HI-Medium', 'Cash -> Wire', 100, 0, 1, 0)
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        self.assertIsNone(self.db.execute('SELECT lift FROM bq2_lift LIMIT 1').fetchone()[0])
        self.assertIsNone(self.policy()['laundering_catch_rate'])
        self.assertIsNone(self.policy()['review_precision'])

    def test_sankey_does_not_triple_counts_for_support_policies(self):
        self.fixture()
        self.add_pattern('HI-Medium', 'Wire -> Wire -> Bitcoin', 1, 1, 9, 1, length=3, start=6000)
        self.run_sql('bq2_lift.sql')
        self.run_sql('sankey.sql')
        self.assertEqual(self.db.execute("SELECT SUM(sequence_count) FROM bq2_sankey WHERE dataset='HI-Medium'").fetchone()[0], 302)
        row = self.db.execute('SELECT * FROM bq2_sankey WHERE sequence_length=3').fetchone()
        self.assertEqual((row['method_1'], row['method_2'], row['method_3']), ('Wire', 'Wire', 'Bitcoin'))
        self.assertEqual(row['laundering_sequence_count'], 1)

    def test_empty_inputs_do_not_invent_results(self):
        for f in ('bq2_lift.sql', 'tradeoff.sql', 'robustness.sql', 'sankey.sql'):
            self.run_sql(f)
        for table in ('bq2_lift', 'bq2_tradeoff', 'bq2_robustness', 'bq2_sankey'):
            self.assertEqual(self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)

    def test_frequency_query_does_not_depend_on_labels(self):
        self.fixture()
        sql = (SQL_DIR / 'frequency.sql').read_text().replace('is459_g2t1_aml.', '')
        self.assertNotIn('any_laundering', sql)
        self.assertEqual(sum(r['support_count'] for r in self.db.execute(sql)), 301)

    def test_generated_development_queries_keep_sample_partition_and_full_denominators(self):
        import runpy
        generated = runpy.run_path(str(SQL_DIR / 'prepare_development.py'))['build_queries']()
        for name, sql in generated.items():
            self.assertEqual((SQL_DIR / 'development' / name).read_text(), sql)
            self.assertNotIn("'HI-Medium'", sql)
            self.assertNotIn("'LI-Medium'", sql)
        self.fixture()
        self.add_pattern('sample', 'Excluded -> Sequence', 51, 1, 999, 0, start=8000)
        self.db.executescript("""
            ALTER TABLE bq2_sequences ADD COLUMN start_date TEXT DEFAULT '2022-09-01';
            UPDATE bq2_sequences SET start_date='2022-09-02' WHERE method_sequence='Excluded -> Sequence';
            ALTER TABLE bq2_sequences RENAME TO bq2_sequences_dev;
            CREATE TABLE bq2_lift (sentinel INT);
            INSERT INTO bq2_lift VALUES (42);
        """)
        for name in ('03_lift.sql', '04_tradeoff.sql', '05_sankey.sql'):
            sql = re.sub(r'WITH\s*\(\s*format\s*=.*?\)\s*AS', 'AS', generated[name], count=1, flags=re.S)
            self.db.executescript(sql.replace('is459_g2t1_aml.', ''))
        self.assertEqual(self.db.execute('SELECT sentinel FROM bq2_lift').fetchone()[0], 42)
        row = self.db.execute('SELECT * FROM bq2_lift_dev WHERE min_support=100').fetchone()
        self.assertEqual(row['dataset'], 'sample')
        self.assertEqual(row['total_sequences'], 100)
        row = self.db.execute('SELECT * FROM bq2_tradeoff_dev WHERE min_support=100 AND sequence_length=2 AND top_n=3').fetchone()
        self.assertEqual(row['reviewed'], 1)
        self.assertEqual(row['laundering_flagged'], 1)
        self.assertEqual(row['total_transactions'], 153)  # Includes sample transactions outside start partition.
        self.assertEqual(row['total_laundering'], 52)
        self.assertEqual(self.db.execute('SELECT SUM(sequence_count) FROM bq2_sankey_dev').fetchone()[0], 100)

    def test_start_now_queries_work_without_sequence_tables(self):
        self.fixture()
        self.db.executescript('''
            ATTACH DATABASE ':memory:' AS information_schema;
            CREATE TABLE information_schema.columns (table_schema TEXT, table_name TEXT,
                column_name TEXT, data_type TEXT, ordinal_position INT);
            CREATE TABLE information_schema.tables (table_schema TEXT, table_name TEXT);
            INSERT INTO information_schema.tables VALUES ('is459_g2t1_aml', 'transactions_clean');
            DROP TABLE bq2_sequences;
        ''')
        sql = '\n'.join(l for l in (SQL_DIR / 'start_now.sql').read_text().splitlines() if not l.lstrip().startswith('--'))
        results = [list(self.db.execute(s.replace('is459_g2t1_aml.', ''))) for s in sql.split(';') if s.strip()]
        self.assertEqual(len(results), 5)
        self.assertEqual(results[3][0]['transactions'], 304)
        self.assertEqual(results[3][0]['laundering_transactions'], 201)


if __name__ == '__main__':
    unittest.main()
