-- Confirm input_contract.md and validation.sql first.
-- Provisional support >= 100; rare patterns remain in the baseline.
CREATE TABLE is459_g2t1_aml.bq2_lift
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_lift/'
) AS
WITH counts AS (
  SELECT dataset, method_sequence, sequence_length,
         COUNT(*) AS support_count, SUM(is_laundering) AS laundering_count
  FROM is459_g2t1_aml.bq2_sequences
  WHERE dataset IN ('HI-Medium', 'LI-Medium')
  GROUP BY dataset, method_sequence, sequence_length
), totals AS (
  SELECT dataset, SUM(support_count) AS total_sequences,
         SUM(laundering_count) AS total_laundering_sequences
  FROM counts GROUP BY dataset
), rates AS (
  SELECT c.*, t.total_sequences, t.total_laundering_sequences,
         CAST(c.laundering_count AS DOUBLE) / c.support_count AS laundering_rate,
         CAST(t.total_laundering_sequences AS DOUBLE) / t.total_sequences AS baseline_rate,
         CAST(c.support_count AS DOUBLE) / t.total_sequences AS support_share,
         CASE WHEN c.support_count >= 100 THEN 1 ELSE 0 END AS eligible
  FROM counts c JOIN totals t ON c.dataset = t.dataset
), scored AS (
  SELECT *, laundering_rate / NULLIF(baseline_rate, 0) AS lift FROM rates
)
SELECT *, CASE WHEN eligible = 1 AND lift IS NOT NULL THEN
  ROW_NUMBER() OVER (
    PARTITION BY dataset
    ORDER BY eligible DESC, lift DESC, laundering_count DESC,
             support_count DESC, method_sequence, sequence_length
  ) END AS risk_rank
FROM scored;
