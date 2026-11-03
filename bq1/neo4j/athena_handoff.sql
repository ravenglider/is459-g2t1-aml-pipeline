-- Role 4: manual HI feature registration and Role 3 join preview.
-- Run ONE statement at a time. Does not rebuild Role 3 tables.
-- CSV contract: bank_id,betweenness,community_id (exactly three columns).
-- Run identity is in the S3 prefix/table name, not a fourth CSV column.
-- Disable result reuse when checking a replaced CSV.

CREATE EXTERNAL TABLE IF NOT EXISTS
is459_g2t1_aml.bq1_graph_bank_features_hi01 (
    bank_id string,
    betweenness double,
    community_id bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION 's3://is459-g2t1-aml-processed-bucket/bq1_graph_results/hi-01/bank_features/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 09a: 5159, 5159, 113, 0.
-- Query ID: 17d50c17-5c66-4ce9-b996-55a98196f19a
SELECT COUNT(*) AS bank_rows,
       COUNT(DISTINCT bank_id) AS unique_banks,
       COUNT(DISTINCT community_id) AS communities,
       COUNT_IF(bank_id IS NULL OR betweenness IS NULL
                OR community_id IS NULL) AS incomplete_rows
FROM is459_g2t1_aml.bq1_graph_bank_features_hi01;

-- 09b: preview only; preserves Ryan's metric order, no new priority weights.
-- Query ID: 47187dbd-4b2e-4948-9836-75462cd0f393
-- LIMIT 20 is illustrative, not the full Priority Sharing List.
-- Coordinate source freshness before using these features after an ETL refresh.
SELECT r.*,
       f.betweenness AS from_betweenness,
       t.betweenness AS to_betweenness,
       f.community_id AS from_community,
       t.community_id AS to_community,
       CASE WHEN f.community_id IS NULL OR t.community_id IS NULL THEN NULL
            ELSE f.community_id <> t.community_id END AS cross_community
FROM is459_g2t1_aml.bq1_corridors_ranked r
LEFT JOIN is459_g2t1_aml.bq1_graph_bank_features_hi01 f
    ON CAST(r.from_bank AS VARCHAR) = f.bank_id
LEFT JOIN is459_g2t1_aml.bq1_graph_bank_features_hi01 t
    ON CAST(r.to_bank AS VARCHAR) = t.bank_id
ORDER BY r.extra_laundering_value_usd DESC,
         r.blindspot_gap DESC, r.from_bank, r.to_bank
LIMIT 20;

-- 09c: original_rows = joined_rows = 1697382.
-- Query ID: 7a8f1d2a-8858-4741-8412-fdd92a26c5b7
WITH joined AS (
    SELECT r.from_bank
    FROM is459_g2t1_aml.bq1_corridors_ranked r
    LEFT JOIN is459_g2t1_aml.bq1_graph_bank_features_hi01 f
        ON CAST(r.from_bank AS VARCHAR) = f.bank_id
    LEFT JOIN is459_g2t1_aml.bq1_graph_bank_features_hi01 t
        ON CAST(r.to_bank AS VARCHAR) = t.bank_id
)
SELECT (SELECT COUNT(*) FROM is459_g2t1_aml.bq1_corridors_ranked) AS original_rows,
       COUNT(*) AS joined_rows
FROM joined;

-- 08g: source check for the account/time-supported three-hop candidate.
-- Query ID: 1a3a3486-6c80-42a4-b9a2-16e247fe20eb
-- attempt_id values 670,2117,2117: not one common annotated attempt.
SELECT txn_id, txn_ts, from_bank, from_account, to_bank, to_account,
       amount_paid_usd, is_laundering, attempt_id
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium'
  AND txn_id IN (7394026252962401598, -2980862507599786311, 606750438349263592)
ORDER BY txn_ts;
