-- Freeze HI rules, apply unchanged to both datasets.
-- Review all constituent transactions of matched chains, each counted once.
-- Retrospective flagging, not proof of stopping a transfer in time.
CREATE TABLE is459_g2t1_aml.bq2_tradeoff
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_tradeoff/'
) AS
WITH policies (policy, top_n) AS (
  VALUES ('Targeted top-3', 3), ('Targeted top-5', 5),
         ('Targeted top-10', 10), ('Broad: review all', 0)
), selected AS (
  SELECT p.policy, p.top_n, l.method_sequence, l.sequence_length
  FROM policies p JOIN is459_g2t1_aml.bq2_lift l
    ON l.dataset = 'HI-Medium' AND l.risk_rank <= p.top_n
), selected_counts AS (
  SELECT policy, COUNT(*) AS actual_patterns FROM selected GROUP BY policy
), flagged AS (
  SELECT DISTINCT s.dataset, r.policy, m.txn_id
  FROM selected r
  JOIN is459_g2t1_aml.bq2_sequences s
    ON s.method_sequence = r.method_sequence AND s.sequence_length = r.sequence_length
  JOIN is459_g2t1_aml.bq2_sequence_transactions m
    ON m.dataset = s.dataset AND m.sequence_id = s.sequence_id
  WHERE s.dataset IN ('HI-Medium', 'LI-Medium')
), population AS (
  SELECT dataset, txn_id, is_laundering
  FROM is459_g2t1_aml.transactions_clean
  WHERE dataset IN ('HI-Medium', 'LI-Medium')
), measured AS (
  SELECT t.dataset, p.policy, p.top_n,
         COUNT(*) AS total_transactions,
         SUM(t.is_laundering) AS total_laundering,
         SUM(1 - t.is_laundering) AS total_legitimate,
         SUM(CASE WHEN p.top_n = 0 OR f.txn_id IS NOT NULL THEN 1 ELSE 0 END) AS reviewed,
         SUM(CASE WHEN p.top_n = 0 OR f.txn_id IS NOT NULL THEN t.is_laundering ELSE 0 END) AS laundering_flagged,
         SUM(CASE WHEN p.top_n = 0 OR f.txn_id IS NOT NULL THEN 1 - t.is_laundering ELSE 0 END) AS legitimate_reviewed
  FROM population t CROSS JOIN policies p
  LEFT JOIN flagged f ON f.dataset = t.dataset AND f.txn_id = t.txn_id AND f.policy = p.policy
  GROUP BY t.dataset, p.policy, p.top_n
)
SELECT m.*, COALESCE(c.actual_patterns, 0) AS actual_patterns,
       CAST(laundering_flagged AS DOUBLE) / NULLIF(total_laundering, 0) AS laundering_catch_rate,
       CAST(legitimate_reviewed AS DOUBLE) / NULLIF(total_legitimate, 0) AS legitimate_review_rate,
       CAST(reviewed AS DOUBLE) / total_transactions AS review_share,
       CAST(laundering_flagged AS DOUBLE) / NULLIF(reviewed, 0) AS review_precision
FROM measured m LEFT JOIN selected_counts c ON c.policy = m.policy;
