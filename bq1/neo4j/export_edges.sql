-- Role 4: HI-Medium graph inputs, manual run hi-01.
-- Run each SELECT separately in Athena; download the complete result CSV.
-- Source: is459_g2t1_aml.transactions_clean. Region: us-east-1.
-- Match Role 3's < $20M convention; this removes rows from counts AND sums.
-- Keep signed IDs as strings; do not round-trip CSV through Excel.
-- Reconcile exports before loading. Changed inputs require a new run/scope.
-- Sample smoke test: substitute dataset 'sample'; original sample CSVs were
-- staged directly in import/ (file:///bank_edges.csv and transactions.csv).
-- LI manual comparison: use these same three SELECTs, replacing the dataset
-- literal 'HI-Medium' with 'LI-Medium' in Athena. Save outputs separately under
-- li-01/ and upload to processed bucket/bq1_graph_edges/li-01/.
-- Keep this checked-in HI template unchanged for the automation runner.
-- Earlier LI baseline: 13724 corridors, 15782 cross-bank transactions,
-- cross-bank USD 908116109.62; 15892 account-evidence rows, USD 908804372.35.
-- Recheck against the current source before accepting a new LI import.
-- HI cutoff comparison: copy the FIRST SELECT into Athena and omit ONLY
-- AND amount_paid_usd < 20000000. Keep HI-Medium,label1,cross_bank1 unchanged.
-- Save only bank_edges.csv under hi-uncapped-01/ (a separate bank-only scope).
-- Export query 7c2e2f42-8bd3-4f32-b924-ade41416f735 returned 29660 corridors.
-- Do not change the runner's three-SELECT template or accepted hi-01 inputs.

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
