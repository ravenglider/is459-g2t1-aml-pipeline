-- Role 3 (BQ1 Corridors): bank-pair aggregation, Blindspot Gap, Extra Laundering Value
-- Database: is459_g2t1_aml | Source: transactions_clean (dataset = 'HI-Medium')
--
-- Data quality note: individual transactions with amount_paid_usd >= $20,000,000
-- are excluded from every SUM below. The real data (median $6,014, 90th percentile
-- $17,318, 99th percentile $17.4M) stops being plausible right around there; past it,
-- values climb to a max of $15.9B, which are synthetic-data artifacts, not real
-- transactions. ~1% of laundering rows are affected. See metrics.md for detail.
--
-- Output location: every table is written to its own folder in the processed bucket
-- (external_location). Each folder must be EMPTY before its CREATE TABLE runs, so
-- on a rerun: run the DROP statements, then delete the four S3 folders' contents.
--
-- Athena runs one statement at a time: run each statement separately, in order.

-- ===== Step 0: drop old tables (reverse dependency order) =====
DROP TABLE IF EXISTS is459_g2t1_aml.bq1_corridors_ranked;
DROP TABLE IF EXISTS is459_g2t1_aml.bq1_shared_attempt_value;
DROP TABLE IF EXISTS is459_g2t1_aml.bq1_attempt_banks;
DROP TABLE IF EXISTS is459_g2t1_aml.bq1_direct_corridors;

-- ===== Step 1: direct cross-bank corridor totals =====
CREATE TABLE is459_g2t1_aml.bq1_direct_corridors
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq1_direct_corridors/'
) AS
SELECT
  from_bank,
  to_bank,
  SUM(CASE WHEN is_laundering = 1 THEN amount_paid_usd ELSE 0 END) AS direct_laundered_usd,
  SUM(amount_paid_usd) AS direct_total_usd,
  COUNT(CASE WHEN is_laundering = 1 THEN 1 END) AS direct_laundering_txns,
  COUNT(*) AS direct_total_txns
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND cross_bank = 1
  AND amount_paid_usd < 20000000
GROUP BY from_bank, to_bank;

-- ===== Step 2a: per-attempt totals and which banks each attempt touched =====
CREATE TABLE is459_g2t1_aml.bq1_attempt_banks
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq1_attempt_banks/'
) AS
SELECT
  attempt_id,
  SUM(amount_paid_usd) AS attempt_total_usd,
  ARRAY_DISTINCT(ARRAY_AGG(DISTINCT from_bank) || ARRAY_AGG(DISTINCT to_bank)) AS banks_touched
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND is_laundering = 1
  AND attempt_id IS NOT NULL
  AND amount_paid_usd < 20000000
GROUP BY attempt_id;

-- ===== Step 2b: explode each attempt's bank list into pairs, sum shared value per pair =====
-- Depends on: bq1_attempt_banks
CREATE TABLE is459_g2t1_aml.bq1_shared_attempt_value
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq1_shared_attempt_value/'
) AS
SELECT
  a.bank_x AS from_bank,
  b.bank_y AS to_bank,
  SUM(t.attempt_total_usd) AS shared_attempt_value_usd
FROM is459_g2t1_aml.bq1_attempt_banks t
CROSS JOIN UNNEST(t.banks_touched) AS a(bank_x)
CROSS JOIN UNNEST(t.banks_touched) AS b(bank_y)
WHERE a.bank_x != b.bank_y
GROUP BY a.bank_x, b.bank_y;

-- ===== Step 3: final ranked output (Rank | From Bank | To Bank | Extra Laundering Value | Blindspot Gap) =====
-- Depends on: bq1_direct_corridors, bq1_shared_attempt_value
-- Note: CTAS does not guarantee row order in the stored files, so sort when you query.
CREATE TABLE is459_g2t1_aml.bq1_corridors_ranked
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq1_corridors_ranked/'
) AS
SELECT
  d.from_bank,
  d.to_bank,
  d.direct_laundered_usd,
  COALESCE(s.shared_attempt_value_usd, 0) AS total_laundering_in_path_usd,
  GREATEST(COALESCE(s.shared_attempt_value_usd, 0) - d.direct_laundered_usd, 0) AS extra_laundering_value_usd,
  CASE WHEN COALESCE(s.shared_attempt_value_usd, 0) > 0
    THEN GREATEST(COALESCE(s.shared_attempt_value_usd, 0) - d.direct_laundered_usd, 0)
         / CAST(s.shared_attempt_value_usd AS DOUBLE)
    ELSE 0 END AS blindspot_gap
FROM is459_g2t1_aml.bq1_direct_corridors d
LEFT JOIN is459_g2t1_aml.bq1_shared_attempt_value s
  ON d.from_bank = s.from_bank AND d.to_bank = s.to_bank;

-- ===== Check =====
-- SELECT * FROM is459_g2t1_aml.bq1_corridors_ranked ORDER BY extra_laundering_value_usd DESC LIMIT 10;