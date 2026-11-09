CREATE TABLE is459_g2t1_aml.bq2_tradeoff
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_tradeoff/'
) AS
WITH policies (policy, top_n) AS (
  VALUES ('Targeted top-3', 3), ('Targeted top-5', 5),
         ('Targeted top-10', 10), ('Broad: review all', 0)
), lengths (sequence_length) AS (VALUES (2), (3)),
cutoffs (min_support) AS (VALUES (50), (100), (500)),
selected AS (
  SELECT p.policy, p.top_n, l.min_support, l.method_sequence, l.sequence_length
  FROM policies p JOIN is459_g2t1_aml.bq2_lift l
    ON l.dataset = 'HI-Medium' AND l.risk_rank <= p.top_n
), selected_counts AS (
  SELECT policy, sequence_length, min_support, COUNT(*) AS actual_patterns
  FROM selected GROUP BY policy, sequence_length, min_support
), flagged AS (
  SELECT dataset, sequence_length, min_support, final_txn_id, MIN(best_rank) AS best_rank
  FROM is459_g2t1_aml.bq2_tradeoff_flagged
  GROUP BY dataset, sequence_length, min_support, final_txn_id
), by_rank AS (
  SELECT f.dataset, f.sequence_length, f.min_support, f.best_rank,
         COUNT(*) AS n, SUM(t.is_laundering) AS n_laundering, SUM(1 - t.is_laundering) AS n_legitimate
  FROM flagged f JOIN is459_g2t1_aml.transactions_clean t
    ON t.dataset = f.dataset AND t.txn_id = f.final_txn_id
  WHERE t.dataset IN ('HI-Medium', 'LI-Medium')
  GROUP BY f.dataset, f.sequence_length, f.min_support, f.best_rank
), reviewed AS (
  SELECT b.dataset, p.policy, b.sequence_length, b.min_support,
         SUM(b.n) AS reviewed, SUM(b.n_laundering) AS laundering_flagged,
         SUM(b.n_legitimate) AS legitimate_reviewed
  FROM by_rank b JOIN policies p ON p.top_n > 0 AND b.best_rank <= p.top_n
  GROUP BY b.dataset, p.policy, b.sequence_length, b.min_support
), population AS (
  SELECT dataset, COUNT(*) AS total_transactions,
         SUM(is_laundering) AS total_laundering,
         SUM(1 - is_laundering) AS total_legitimate
  FROM is459_g2t1_aml.transactions_clean
  WHERE dataset IN ('HI-Medium', 'LI-Medium') GROUP BY dataset
), measured AS (
  SELECT t.*, p.policy, p.top_n, l.sequence_length, k.min_support,
         COALESCE(c.actual_patterns, 0) AS actual_patterns,
         CASE WHEN p.top_n = 0 THEN t.total_transactions ELSE COALESCE(r.reviewed, 0) END AS reviewed,
         CASE WHEN p.top_n = 0 THEN t.total_laundering ELSE COALESCE(r.laundering_flagged, 0) END AS laundering_flagged,
         CASE WHEN p.top_n = 0 THEN t.total_legitimate ELSE COALESCE(r.legitimate_reviewed, 0) END AS legitimate_reviewed
  FROM population t CROSS JOIN policies p CROSS JOIN lengths l CROSS JOIN cutoffs k
  LEFT JOIN reviewed r ON r.dataset = t.dataset AND r.policy = p.policy
    AND r.sequence_length = l.sequence_length AND r.min_support = k.min_support
  LEFT JOIN selected_counts c ON c.policy = p.policy
    AND c.sequence_length = l.sequence_length AND c.min_support = k.min_support
)
SELECT *,
       CAST(laundering_flagged AS DOUBLE) / NULLIF(total_laundering, 0) AS laundering_catch_rate,
       CAST(legitimate_reviewed AS DOUBLE) / NULLIF(total_legitimate, 0) AS legitimate_review_rate,
       CAST(reviewed AS DOUBLE) / total_transactions AS review_share,
       CAST(laundering_flagged AS DOUBLE) / NULLIF(reviewed, 0) AS review_precision
FROM measured;
