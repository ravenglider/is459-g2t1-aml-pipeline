-- Stage 1 FALLBACK (use instead of the 4-statement stage 1): one start-date week per statement.
-- Folder s3://is459-g2t1-aml-processed-bucket/bq2_tradeoff_flagged/ must be empty first.
DROP TABLE IF EXISTS is459_g2t1_aml.bq2_tradeoff_flagged;

-- HI-Medium, length 2, start dates 2022-09-01 to 2022-09-07
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-01' AND '2022-09-07'
)
SELECT 'HI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 2, start dates 2022-09-08 to 2022-09-14
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-08' AND '2022-09-14'
)
SELECT 'HI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 2, start dates 2022-09-15 to 2022-09-21
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-15' AND '2022-09-21'
)
SELECT 'HI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 2, start dates 2022-09-22 to 2022-09-28
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-22' AND '2022-09-28'
)
SELECT 'HI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 3, start dates 2022-09-01 to 2022-09-07
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-01' AND '2022-09-07'
)
SELECT 'HI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 3, start dates 2022-09-08 to 2022-09-14
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-08' AND '2022-09-14'
)
SELECT 'HI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 3, start dates 2022-09-15 to 2022-09-21
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-15' AND '2022-09-21'
)
SELECT 'HI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- HI-Medium, length 3, start dates 2022-09-22 to 2022-09-28
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
  WHERE s.dataset = 'HI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-22' AND '2022-09-28'
)
SELECT 'HI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 2, start dates 2022-09-01 to 2022-09-07
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-01' AND '2022-09-07'
)
SELECT 'LI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 2, start dates 2022-09-08 to 2022-09-14
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-08' AND '2022-09-14'
)
SELECT 'LI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 2, start dates 2022-09-15 to 2022-09-21
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-15' AND '2022-09-21'
)
SELECT 'LI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 2, start dates 2022-09-22 to 2022-09-27
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 2 AND s.start_date BETWEEN '2022-09-22' AND '2022-09-27'
)
SELECT 'LI-Medium' AS dataset, CAST(2 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 3, start dates 2022-09-01 to 2022-09-07
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-01' AND '2022-09-07'
)
SELECT 'LI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 3, start dates 2022-09-08 to 2022-09-14
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-08' AND '2022-09-14'
)
SELECT 'LI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 3, start dates 2022-09-15 to 2022-09-21
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-15' AND '2022-09-21'
)
SELECT 'LI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;

-- LI-Medium, length 3, start dates 2022-09-22 to 2022-09-27
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
  WHERE s.dataset = 'LI-Medium' AND s.sequence_length = 3 AND s.start_date BETWEEN '2022-09-22' AND '2022-09-27'
)
SELECT 'LI-Medium' AS dataset, CAST(3 AS integer) AS sequence_length, r.min_support,
       m.final_txn_id, MIN(r.risk_rank) AS best_rank
FROM matched m JOIN ranks r ON r.method_sequence = m.method_sequence
GROUP BY r.min_support, m.final_txn_id;
