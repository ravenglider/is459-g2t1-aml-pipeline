-- Role 6 pre-Role-5 checks. Run ONE numbered statement at a time.
-- Reads only; SELECT results use the existing Athena staging location.
-- No sequence table is queried and no project table is created or replaced.

-- 1. Confirm the cleaned schema and types before querying data.
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'is459_g2t1_aml'
  AND table_name = 'transactions_clean'
ORDER BY ordinal_position;

-- 2. Discover whether Role 5 has published either proposed input yet.
-- An absent row means not visible to this login/catalog, not proof the job is unfinished.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'is459_g2t1_aml'
  AND table_name IN ('transactions_clean', 'bq2_sequences', 'bq2_sequences_dev')
ORDER BY table_name;

-- 3. Small HI date-partition check before full-data queries.
-- This is one day only: do not compare its counts with the full HI reference.
SELECT COUNT(*) AS transactions,
       SUM(CASE WHEN is_laundering = 1 THEN 1 ELSE 0 END) AS laundering_transactions,
       SUM(CASE WHEN is_laundering IS NULL OR is_laundering NOT IN (0, 1) THEN 1 ELSE 0 END) AS invalid_labels,
       SUM(CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END) AS null_txn_ids,
       SUM(CASE WHEN usd_missing IS NULL OR usd_missing <> 0 THEN 1 ELSE 0 END) AS usd_flag_issues
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium' AND txn_date = '2022-09-01';

-- 4. Full HI baseline. No date filter because this reconciles the complete dataset.
-- Team reference: 31,898,218 transactions and 35,230 laundering transactions.
SELECT COUNT(*) AS transactions,
       SUM(CASE WHEN is_laundering = 1 THEN 1 ELSE 0 END) AS laundering_transactions,
       SUM(CASE WHEN is_laundering = 0 THEN 1 ELSE 0 END) AS legitimate_transactions,
       CAST(SUM(CASE WHEN is_laundering = 1 THEN 1 ELSE 0 END) AS DOUBLE)
         / NULLIF(COUNT(*), 0) AS transaction_laundering_rate,
       MIN(txn_date) AS first_date, MAX(txn_date) AS last_date
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium';

-- 5. Full LI baseline, kept separate from HI.
-- Repository ETL reference: 31,251,469 transactions and 16,041 laundering transactions.
SELECT COUNT(*) AS transactions,
       SUM(CASE WHEN is_laundering = 1 THEN 1 ELSE 0 END) AS laundering_transactions,
       SUM(CASE WHEN is_laundering = 0 THEN 1 ELSE 0 END) AS legitimate_transactions,
       CAST(SUM(CASE WHEN is_laundering = 1 THEN 1 ELSE 0 END) AS DOUBLE)
         / NULLIF(COUNT(*), 0) AS transaction_laundering_rate,
       MIN(txn_date) AS first_date, MAX(txn_date) AS last_date
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'LI-Medium';
