-- One row per complete pattern: use method_1/2/3 as Tableau Sankey levels.
-- Reuse lift aggregates, including rare patterns, instead of rescanning chains.
-- Weights are chain counts, not USD or distinct transaction counts.
CREATE TABLE is459_g2t1_aml.bq2_sankey
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_sankey/'
) AS
SELECT dataset, method_sequence, sequence_length,
       split_part(method_sequence, ' -> ', 1) AS method_1,
       split_part(method_sequence, ' -> ', 2) AS method_2,
       CASE WHEN sequence_length = 3
         THEN split_part(method_sequence, ' -> ', 3) END AS method_3,
       support_count AS sequence_count,
       laundering_count AS laundering_sequence_count,
       support_count - laundering_count AS legitimate_sequence_count
FROM is459_g2t1_aml.bq2_lift
-- Counts are identical across support policies; select exactly one copy.
WHERE min_support = 50;
