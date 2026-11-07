-- Run the three statements separately before 01_validation.sql.
-- Expected: 184449 pairs and 33931 triples. Count sum: 218380.
SELECT sequence_length, COUNT(*) AS sequences
FROM is459_g2t1_aml.bq2_sequences_dev
WHERE dataset = 'sample' AND start_date = '2022-09-01'
GROUP BY sequence_length ORDER BY sequence_length;

-- Expect the exact 21 logical columns/types in Role 5's v1 schema, including partitions.
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'is459_g2t1_aml' AND table_name = 'bq2_sequences_dev'
ORDER BY ordinal_position;

-- Inspect table definition. Partition-level locations may override the table root.
-- Check the exact partition Location in Glue against Ji Ning's immutable publication.
SHOW CREATE TABLE is459_g2t1_aml.bq2_sequences_dev;
