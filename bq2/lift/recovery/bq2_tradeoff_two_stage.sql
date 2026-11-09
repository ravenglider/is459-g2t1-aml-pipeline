-- bq2_tradeoff in two stages (Role 6 logic unchanged). Run each statement in order.

-- Stage 1: each final transaction once per (dataset, length, cutoff), with the best HI rank
-- of any selected pattern it ends. Folder s3://is459-g2t1-aml-processed-bucket/bq2_tradeoff_flagged/ must be empty first.
DROP TABLE IF EXISTS is459_g2t1_aml.bq2_tradeoff_flagged;

-- HI-Medium, length 2
CREATE TABLE is459_g2t1_aml.bq2_tradeoff_flagged
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_tradeoff_flagged/'
) AS
WITH ranks AS (
  SELECT min_support, method_sequence, MIN(risk_rank) AS risk_rank
  FROM is459_g2t1_aml.bq2_lift
  WHERE dataset = 'HI-Medium' AND sequence_length = 2 AND risk_rank <= 10
  GROUP BY min_support, method_sequence
), patterns AS (
  SELECT DISTINCT method_sequence FROM ranks
), matched AS (
  SELECT DISTINCT s.method_sequence, s.final_txn_id
  FROM is459_g2t1_aml.bq2_sequences s
  JOIN patterns p ON p.method_sequence = s.method_sequence
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 2
)
SELECT 'HI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 3
INSERT INTO is459_g2t1_aml.bq2_tradeoff_flagged
WITH ranks AS (
  SELECT min_support, method_sequence, MIN(risk_rank) AS risk_rank
  FROM is459_g2t1_aml.bq2_lift
  WHERE dataset = 'HI-Medium' AND sequence_length = 3 AND risk_rank <= 10
  GROUP BY min_support, method_sequence
), patterns AS (
  SELECT DISTINCT method_sequence FROM ranks
), matched AS (
  SELECT DISTINCT s.method_sequence, s.final_txn_id
  FROM is459_g2t1_aml.bq2_sequences s
  JOIN patterns p ON p.method_sequence = s.method_sequence
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 3
)
SELECT 'HI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 2
INSERT INTO is459_g2t1_aml.bq2_tradeoff_flagged
WITH ranks AS (
  SELECT min_support, method_sequence, MIN(risk_rank) AS risk_rank
  FROM is459_g2t1_aml.bq2_lift
  WHERE dataset = 'HI-Medium' AND sequence_length = 2 AND risk_rank <= 10
  GROUP BY min_support, method_sequence
), patterns AS (
  SELECT DISTINCT method_sequence FROM ranks
), matched AS (
  SELECT DISTINCT s.method_sequence, s.final_txn_id
  FROM is459_g2t1_aml.bq2_sequences s
  JOIN patterns p ON p.method_sequence = s.method_sequence
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 2
)
SELECT 'LI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 3
INSERT INTO is459_g2t1_aml.bq2_tradeoff_flagged
WITH ranks AS (
  SELECT min_support, method_sequence, MIN(risk_rank) AS risk_rank
  FROM is459_g2t1_aml.bq2_lift
  WHERE dataset = 'HI-Medium' AND sequence_length = 3 AND risk_rank <= 10
  GROUP BY min_support, method_sequence
), patterns AS (
  SELECT DISTINCT method_sequence FROM ranks
), matched AS (
  SELECT DISTINCT s.method_sequence, s.final_txn_id
  FROM is459_g2t1_aml.bq2_sequences s
  JOIN patterns p ON p.method_sequence = s.method_sequence
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 3
)
SELECT 'LI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- Stage 2: the trade-off table (same columns and values as the original single query).
-- Folder s3://is459-g2t1-aml-processed-bucket/bq2_tradeoff/ must be empty first.
DROP TABLE IF EXISTS is459_g2t1_aml.bq2_tradeoff;

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
