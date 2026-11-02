"""Arithmetic checks only; Athena CTAS/storage need real-engine checks."""
from pathlib import Path
import re
import sqlite3
import unittest

SQL_DIR = Path(__file__).resolve().parents[1]


class AnalyticsSQLTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.create_function('split_part', 3, lambda text, delimiter, index: text.split(delimiter)[index - 1])
        self.db.executescript('''
            CREATE TABLE bq2_sequences (
                dataset TEXT, sequence_id TEXT, method_sequence TEXT,
                sequence_length INT, is_laundering INT);
            CREATE TABLE bq2_sequence_transactions (
                dataset TEXT, sequence_id TEXT, hop_index INT, txn_id INT);
            CREATE TABLE transactions_clean (dataset TEXT, txn_id INT, is_laundering INT);
        ''')

    def run_sql(self, filename):
        # Strip only Athena storage properties and database qualifiers.
        sql = (SQL_DIR / filename).read_text()
        sql = re.sub(r'WITH\s*\(\s*format\s*=.*?\)\s*AS', 'AS', sql, count=1, flags=re.S)
        self.db.executescript(sql.replace('is459_g2t1_aml.', ''))

    def add_pattern(self, dataset, pattern, count, label, anchor, first_id):
        self.db.execute('INSERT OR IGNORE INTO transactions_clean VALUES (?, ?, ?)',
                        (dataset, anchor, label))
        for i in range(count):
            sid, tid = f'{pattern}:{i}', first_id + i
            self.db.execute('INSERT INTO bq2_sequences VALUES (?, ?, ?, 2, ?)',
                            (dataset, sid, pattern, label))
            self.db.execute('INSERT INTO transactions_clean VALUES (?, ?, 0)', (dataset, tid))
            self.db.executemany('INSERT INTO bq2_sequence_transactions VALUES (?, ?, ?, ?)',
                                [(dataset, sid, 1, anchor), (dataset, sid, 2, tid)])

    def fixture(self):
        # Many unique chains share one incoming transfer, modelling account fan-out.
        self.add_pattern('HI-Medium', 'Cash -> Wire', 100, 1, 1, 1000)
        self.add_pattern('HI-Medium', 'Wire -> ACH', 100, 0, 2, 2000)
        self.add_pattern('HI-Medium', 'Rare -> Cash', 1, 1, 3, 3000)
        self.add_pattern('sample', 'Sample -> Sample', 150, 1, 1, 4000)
        # LI has a different winner and an unsupported HI pattern.
        self.add_pattern('LI-Medium', 'Cash -> Wire', 1, 0, 1, 1000)
        self.add_pattern('LI-Medium', 'Bitcoin -> ACH', 100, 1, 2, 2000)

    def test_baseline_keeps_rare_and_excludes_sample(self):
        self.fixture()
        self.run_sql('bq2_lift.sql')
        row = self.db.execute("SELECT * FROM bq2_lift WHERE dataset='HI-Medium' AND risk_rank=1").fetchone()
        self.assertEqual(row['method_sequence'], 'Cash -> Wire')
        self.assertEqual(row['total_sequences'], 201)
        self.assertAlmostEqual(row['baseline_rate'], 101 / 201)
        self.assertAlmostEqual(row['lift'], 201 / 101)
        rare = self.db.execute("SELECT * FROM bq2_lift WHERE method_sequence='Rare -> Cash'").fetchone()
        self.assertEqual(rare['eligible'], 0)
        self.assertIsNone(rare['risk_rank'])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM bq2_lift WHERE dataset='sample'").fetchone()[0], 0)

    def test_overlap_is_deduplicated_and_unmatched_txns_stay_in_denominator(self):
        self.fixture()
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        row = self.db.execute("SELECT * FROM bq2_tradeoff WHERE dataset='HI-Medium' AND top_n=3").fetchone()
        self.assertEqual(row['actual_patterns'], 2)
        self.assertEqual(row['reviewed'], 202)
        self.assertEqual(row['laundering_flagged'], 1)
        self.assertEqual(row['legitimate_reviewed'], 201)
        self.assertEqual(row['total_transactions'], 204)
        self.assertAlmostEqual(row['laundering_catch_rate'], 1 / 2)
        self.assertAlmostEqual(row['legitimate_review_rate'], 201 / 202)
        broad = self.db.execute("SELECT * FROM bq2_tradeoff WHERE dataset='HI-Medium' AND top_n=0").fetchone()
        self.assertEqual(broad['reviewed'], 204)
        self.assertEqual(broad['laundering_catch_rate'], 1)
        self.assertEqual(broad['legitimate_review_rate'], 1)

    def test_li_rules_are_frozen_not_reranked(self):
        self.fixture()
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        self.run_sql('robustness.sql')
        row = self.db.execute("SELECT * FROM bq2_tradeoff WHERE dataset='LI-Medium' AND top_n=3").fetchone()
        self.assertEqual(row['reviewed'], 2)
        self.assertEqual(row['laundering_flagged'], 0)
        rows = {r['method_sequence']: r for r in self.db.execute('SELECT * FROM bq2_robustness')}
        self.assertEqual(rows['Cash -> Wire']['li_status'], 'Below LI support cutoff')
        self.assertEqual(rows['Wire -> ACH']['li_status'], 'Absent on LI')
        self.assertIsNone(rows['Wire -> ACH']['li_lift'])
        self.assertEqual(rows['Wire -> ACH']['li_support'], 0)

    def test_no_laundering_has_undefined_lift_and_catch(self):
        self.add_pattern('HI-Medium', 'Cash -> Wire', 100, 0, 1, 1000)
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        row = self.db.execute('SELECT * FROM bq2_lift').fetchone()
        self.assertIsNone(row['lift'])
        self.assertIsNone(row['risk_rank'])
        row = self.db.execute('SELECT * FROM bq2_tradeoff WHERE top_n=3').fetchone()
        self.assertEqual(row['reviewed'], 0)
        self.assertIsNone(row['laundering_catch_rate'])
        self.assertIsNone(row['review_precision'])

    def test_top_n_and_ties_are_deterministic(self):
        for i in range(11):
            self.add_pattern('HI-Medium', f'Method{i:02d} -> Wire', 100, 1, i + 1, 1000 + 100 * i)
        self.run_sql('bq2_lift.sql')
        self.run_sql('tradeoff.sql')
        ordered = list(self.db.execute('SELECT method_sequence FROM bq2_lift ORDER BY risk_rank'))
        self.assertEqual(ordered[0][0], 'Method00 -> Wire')
        for n in (3, 5, 10):
            row = self.db.execute('SELECT * FROM bq2_tradeoff WHERE top_n=?', (n,)).fetchone()
            self.assertEqual(row['actual_patterns'], n)
            self.assertEqual(row['reviewed'], n * 101)
            self.assertEqual(row['laundering_flagged'], n)

    def test_sankey_preserves_complete_patterns_without_edge_double_count(self):
        self.fixture()
        self.db.execute("INSERT INTO bq2_sequences VALUES ('HI-Medium', 'triple', 'Wire -> Wire -> Bitcoin', 3, 1)")
        self.run_sql('bq2_lift.sql')
        self.run_sql('sankey.sql')
        row = self.db.execute("SELECT * FROM bq2_sankey WHERE sequence_length=3").fetchone()
        self.assertEqual((row['method_1'], row['method_2'], row['method_3']), ('Wire', 'Wire', 'Bitcoin'))
        self.assertEqual(row['laundering_sequence_count'], 1)
        self.assertEqual(self.db.execute("SELECT SUM(sequence_count) FROM bq2_sankey WHERE dataset='HI-Medium'").fetchone()[0], 202)
        self.assertIsNone(self.db.execute("SELECT method_3 FROM bq2_sankey WHERE sequence_length=2 LIMIT 1").fetchone()[0])

    def test_empty_input_does_not_invent_results(self):
        for filename in ('bq2_lift.sql', 'tradeoff.sql', 'robustness.sql', 'sankey.sql'):
            self.run_sql(filename)
        for table in ('bq2_lift', 'bq2_tradeoff', 'bq2_robustness', 'bq2_sankey'):
            self.assertEqual(self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
