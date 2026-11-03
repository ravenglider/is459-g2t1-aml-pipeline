-- Role 4 input/source checks. Run one statement at a time in Athena.
-- Historical results are documented in the guide/findings; rerun after refreshes.
-- Change dataset consistently for LI robustness; do not infer LI quality from HI.

SHOW TABLES IN is459_g2t1_aml;

DESCRIBE is459_g2t1_aml.bq1_corridors_ranked;

SHOW CREATE TABLE is459_g2t1_aml.bq1_corridors_ranked;

SELECT dataset, COUNT(*) AS rows_clean,
       COUNT_IF(is_laundering = 1) AS laundering_rows,
       COUNT_IF(amount_paid_usd IS NULL) AS missing_usd,
       MIN(txn_ts) AS first_time, MAX(txn_ts) AS last_time
FROM is459_g2t1_aml.transactions_clean
WHERE dataset IN ('sample', 'HI-Medium', 'LI-Medium')
GROUP BY dataset ORDER BY dataset;

-- Measure the cutoff before applying it; it removes counts AND sums.
SELECT 'HI-Medium' AS dataset,
       COUNT(*) AS labelled_rows,
       COUNT_IF(amount_paid_usd >= 20000000) AS excluded_large_rows,
       COUNT_IF(amount_paid_usd IS NULL) AS null_usd_rows,
       COUNT_IF(amount_paid_usd < 20000000) AS retained_rows,
       COUNT_IF(amount_paid_usd < 20000000 AND cross_bank = 1) AS retained_cross_bank_rows,
       SUM(amount_paid_usd) AS unfiltered_usd,
       SUM(CASE WHEN amount_paid_usd >= 20000000 THEN amount_paid_usd ELSE 0 END) AS excluded_usd,
       SUM(CASE WHEN amount_paid_usd < 20000000 THEN amount_paid_usd ELSE 0 END) AS retained_usd
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium' AND is_laundering = 1;

-- All four counts must be zero or investigated before accepting graph inputs.
SELECT 'HI-Medium' AS dataset,
       COUNT_IF(from_bank IS NULL OR to_bank IS NULL) AS bad_banks,
       COUNT_IF(from_account IS NULL OR to_account IS NULL
                OR TRIM(from_account) = '' OR TRIM(to_account) = '') AS bad_accounts,
       COUNT_IF(txn_ts IS NULL) AS bad_times,
       COUNT(*) - COUNT(DISTINCT txn_id) AS duplicate_txn_ids
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium' AND is_laundering = 1
  AND amount_paid_usd < 20000000;

-- Two-hop candidate: source-verified account continuity and timestamps.
-- Query ID was not supplied for this execution; do not invent one.
SELECT txn_id, txn_ts, from_bank, from_account, to_bank, to_account,
       amount_paid_usd, is_laundering, attempt_id
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND txn_id IN (9173283034951331860, -1430906872139732606)
ORDER BY txn_ts;
