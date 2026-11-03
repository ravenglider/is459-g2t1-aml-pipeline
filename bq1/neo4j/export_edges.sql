-- Role 4: HI-Medium graph inputs, manual run hi-01.
-- Run each SELECT separately in Athena; download the complete result CSV.
-- Source: is459_g2t1_aml.transactions_clean. Region: us-east-1.
-- Match Role 3's < $20M convention; this removes rows from counts AND sums.
-- Keep signed IDs as strings; do not round-trip CSV through Excel.
-- Reconcile exports before loading. Changed inputs require a new run/scope.

-- Save as bank_edges.csv; upload to processed bucket/bq1_graph_edges/hi-01/.
SELECT CAST(from_bank AS VARCHAR) AS from_bank,
       CAST(to_bank AS VARCHAR) AS to_bank,
       COUNT(*) AS laundering_txns,
       SUM(amount_paid_usd) AS laundering_usd
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND is_laundering = 1
  AND cross_bank = 1
  AND amount_paid_usd < 20000000
GROUP BY from_bank, to_bank
ORDER BY from_bank, to_bank;

-- Save as transactions.csv in the same run folder.
-- Include same-bank transfers: they may connect an account-level path.
-- attempt_id is optional; missing annotations must not exclude these rows.
SELECT CAST(txn_id AS VARCHAR) AS txn_id,
       CAST(from_bank AS VARCHAR) AS from_bank,
       from_account,
       CAST(to_bank AS VARCHAR) AS to_bank,
       to_account,
       REPLACE(CAST(txn_ts AS VARCHAR), ' ', 'T') AS txn_ts,
       amount_paid_usd,
       COALESCE(CAST(attempt_id AS VARCHAR), '') AS attempt_id,
       COALESCE(pattern_type, '') AS pattern_type
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND is_laundering = 1
  AND amount_paid_usd < 20000000
ORDER BY txn_ts, txn_id;

-- Source reconciliation: expected hi-01 values documented in findings.md.
SELECT COUNT(*) AS evidence_transactions,
       ROUND(SUM(amount_paid_usd), 2) AS evidence_usd,
       COUNT_IF(cross_bank = 1) AS cross_bank_transactions,
       ROUND(SUM(CASE WHEN cross_bank = 1
                      THEN amount_paid_usd ELSE 0 END), 2) AS cross_bank_usd
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND is_laundering = 1
  AND amount_paid_usd < 20000000;
