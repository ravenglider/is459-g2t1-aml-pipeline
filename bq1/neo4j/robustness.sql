-- Role 4 sensitivity sources. All six SELECTs executed successfully in Athena.
-- Query 1 ID: 679b8d6e-083d-4d27-90ab-d72cc0d72d1c.
-- Query 2: all four population counts match; all quality counts zero.
-- Query 2 ID: cf3afa7b-c85c-4f3b-883a-f34dc08d1543.
-- Query 3 ID: 3bbfb6b1-d294-478a-8e94-c76256630cbb.
-- Query 3: HI leading rows inspected; separate LI preview executed below.
-- Whole-corridor totals checked in query 5; evidence 10f-hi-li-corridor-totals.png.
-- Run one SELECT at a time, record dataset/query IDs and retain results in S3.
-- Never overwrite accepted hi-01. New graph populations need new scopes/files.
-- These checks describe labelled graph inputs, not Role 3's attempt-pair sums.

-- 1. Measure the $20M convention on HI and LI, including its value impact.
SELECT dataset,
       COUNT(*) AS labelled_rows,
       COUNT_IF(amount_paid_usd IS NULL) AS missing_usd,
       COUNT_IF(amount_paid_usd >= 20000000) AS excluded_rows,
       ROUND(100.0 * COUNT_IF(amount_paid_usd >= 20000000)
             / NULLIF(COUNT(*),0),4) AS excluded_row_percent,
       COUNT_IF(amount_paid_usd < 20000000) AS retained_rows,
       COUNT_IF(amount_paid_usd < 20000000 AND cross_bank=1) AS retained_cross_bank_rows,
       SUM(amount_paid_usd) AS unfiltered_usd,
       SUM(CASE WHEN amount_paid_usd >= 20000000 THEN amount_paid_usd ELSE 0 END) AS excluded_usd,
       SUM(CASE WHEN amount_paid_usd < 20000000 THEN amount_paid_usd ELSE 0 END) AS retained_usd,
       MIN(txn_ts) AS first_time,MAX(txn_ts) AS last_time
FROM is459_g2t1_aml.transactions_clean
WHERE dataset IN ('HI-Medium','LI-Medium') AND is_laundering=1
GROUP BY dataset ORDER BY dataset;

-- 2. Check both filtered and unfiltered populations before any sensitivity import.
-- No intentional row dropping for bad keys. Investigate nonzero missing keys/IDs.
WITH populations AS (
    SELECT t.*,p.population
    FROM is459_g2t1_aml.transactions_clean t
    CROSS JOIN UNNEST(ARRAY['below_20m','unfiltered']) AS p(population)
    WHERE dataset IN ('HI-Medium','LI-Medium') AND is_laundering=1
      AND (p.population='unfiltered' OR amount_paid_usd<20000000)
)
SELECT dataset,population,COUNT(*) AS rows_selected,
       COUNT_IF(from_bank IS NULL OR to_bank IS NULL) AS bad_banks,
       COUNT_IF(from_account IS NULL OR to_account IS NULL
                OR TRIM(from_account)='' OR TRIM(to_account)='') AS bad_accounts,
       COUNT_IF(txn_ts IS NULL) AS bad_times,
       COUNT_IF(amount_paid_usd IS NULL) AS missing_usd,
       COUNT(*)-COUNT(DISTINCT txn_id) AS duplicate_txn_ids
FROM populations
GROUP BY dataset,population ORDER BY dataset,population;

-- 3. Compare corridor-level volume distributions with and without the cutoff.
-- This is a value/ranking sensitivity check, not a replacement for graph reruns.
WITH pairs AS (
    SELECT dataset,from_bank,to_bank,
           COUNT(*) AS unfiltered_txns,
           SUM(amount_paid_usd) AS unfiltered_usd,
           COUNT_IF(amount_paid_usd<20000000) AS filtered_txns,
           SUM(CASE WHEN amount_paid_usd<20000000 THEN amount_paid_usd ELSE 0 END) AS filtered_usd
    FROM is459_g2t1_aml.transactions_clean
    WHERE dataset IN ('HI-Medium','LI-Medium') AND is_laundering=1 AND cross_bank=1
    GROUP BY dataset,from_bank,to_bank
)
SELECT dataset,CAST(from_bank AS VARCHAR) AS from_bank,
       CAST(to_bank AS VARCHAR) AS to_bank,
       unfiltered_txns,filtered_txns,unfiltered_usd,filtered_usd,
       unfiltered_usd-filtered_usd AS excluded_usd
FROM pairs
ORDER BY dataset,excluded_usd DESC,from_bank,to_bank;

-- 4. LI-only leading affected corridors, executed successfully.
-- Query ID: ff1ff99e-ada4-454c-b54e-2f4597c55d3e.
WITH pairs AS (
    SELECT from_bank,to_bank,
           COUNT(*) AS unfiltered_txns,
           COUNT_IF(amount_paid_usd < 20000000) AS filtered_txns,
           SUM(amount_paid_usd) AS unfiltered_usd,
           SUM(CASE WHEN amount_paid_usd < 20000000
                    THEN amount_paid_usd ELSE 0 END) AS filtered_usd
    FROM is459_g2t1_aml.transactions_clean
    WHERE dataset = 'LI-Medium' AND is_laundering = 1 AND cross_bank = 1
    GROUP BY from_bank,to_bank
)
SELECT 'LI-Medium' AS dataset,
       CAST(from_bank AS VARCHAR) AS from_bank,
       CAST(to_bank AS VARCHAR) AS to_bank,
       unfiltered_txns,filtered_txns,unfiltered_usd,filtered_usd,
       unfiltered_usd-filtered_usd AS excluded_usd
FROM pairs
ORDER BY excluded_usd DESC,from_bank,to_bank
LIMIT 5;

-- 5. Whole-corridor totals. Executed successfully.
-- Query ID: 77ffcf9b-e438-41f6-b759-74aadb63c17d.
-- HI: 29660/29374 corridors,34887/34552 transactions (unfiltered/filtered).
-- LI: 13858/13724 corridors,15930/15782 transactions (unfiltered/filtered).
-- Cross-bank only: do not compare these USD totals to all-row totals that
-- include same-bank transfers. No local result CSV download is required.
WITH pairs AS (
    SELECT dataset,from_bank,to_bank,
           COUNT(*) AS unfiltered_txns,
           COUNT_IF(amount_paid_usd < 20000000) AS filtered_txns,
           SUM(amount_paid_usd) AS unfiltered_usd,
           SUM(CASE WHEN amount_paid_usd < 20000000
                    THEN amount_paid_usd ELSE 0 END) AS filtered_usd
    FROM is459_g2t1_aml.transactions_clean
    WHERE dataset IN ('HI-Medium','LI-Medium')
      AND is_laundering = 1 AND cross_bank = 1
    GROUP BY dataset,from_bank,to_bank
)
SELECT dataset,
       COUNT(*) AS unfiltered_corridors,
       COUNT_IF(filtered_txns > 0) AS filtered_corridors,
       SUM(unfiltered_txns) AS unfiltered_cross_bank_txns,
       SUM(filtered_txns) AS filtered_cross_bank_txns,
       ROUND(SUM(unfiltered_usd),2) AS unfiltered_cross_bank_usd,
       ROUND(SUM(filtered_usd),2) AS filtered_cross_bank_usd,
       ROUND(SUM(unfiltered_usd-filtered_usd),2) AS excluded_cross_bank_usd
FROM pairs
GROUP BY dataset ORDER BY dataset;

-- For LI graph rerun: use export_edges.sql with dataset LI-Medium, a fresh li
-- input prefix/scope and the same <20M convention. Do not mix exports/runs.
-- The completed uncapped comparison is BANK-ONLY; no account CSV is needed.
-- The current Python runner intentionally enforces <20M and must NOT be used
-- for an uncapped run without a separately reviewed/tested implementation.

-- 6. Actual uncapped HI bank export. Executed; 29660 rows.
-- Query ID: 7c2e2f42-8bd3-4f32-b924-ade41416f735.
-- Save complete CSV as bq1_graph_edges/hi-uncapped-01/bank_edges.csv.
SELECT CAST(from_bank AS VARCHAR) AS from_bank,
       CAST(to_bank AS VARCHAR) AS to_bank,
       COUNT(*) AS laundering_txns,
       SUM(amount_paid_usd) AS laundering_usd
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND is_laundering = 1
  AND cross_bank = 1
GROUP BY from_bank, to_bank
ORDER BY from_bank, to_bank;
