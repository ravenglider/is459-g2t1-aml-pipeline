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

-- Task 11: independent full-HI automated feature check executed:5159,5159,113,0.
-- Query ID:5fb8a8a9-d2e4-4acf-9ac9-7a75413a5676; evidence11g.
-- Sample equivalent used bq1_graph_bank_features_sample_auto_20261106_01
-- and returned42,42,8,0; its query ID is not yet supplied (evidence11d).
-- Repeat HI validation used the SAME SELECT with the table suffix changed to
-- hi_auto_20261106_02;returned5159,5159,113,0.
-- Repeat query ID:d55dc6f8-0a52-467e-9bc0-dc286429be15.
SELECT COUNT(*) AS bank_rows,
       COUNT(DISTINCT bank_id) AS unique_banks,
       COUNT(DISTINCT community_id) AS communities,
       COUNT_IF(bank_id IS NULL OR TRIM(bank_id) = ''
                OR betweenness IS NULL OR betweenness < 0
                OR community_id IS NULL) AS invalid_rows
FROM is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_01;

-- Task 11: manual/automated feature comparison executed:
-- matched5159,manual_only0,auto_only0,score_mismatches0,
-- max_score_delta1.1641532182693481E-10.
-- Query ID:a465ff5f-5561-4df1-9394-9f1b06202cbf.
-- FULL OUTER JOIN exposes missing/extra bank IDs in either population.
-- Score tolerance:max(1e-6 absolute,1e-9 relative to manual score), allowing
-- floating-point accumulation differences. This is not an accuracy guarantee.
-- Community labels are arbitrary: compare their membership separately, not IDs.
WITH paired AS (
    SELECT m.bank_id AS manual_id,a.bank_id AS auto_id,
           m.betweenness AS manual_score,a.betweenness AS auto_score
    FROM is459_g2t1_aml.bq1_graph_bank_features_hi01 m
    FULL OUTER JOIN is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_01 a
      ON m.bank_id = a.bank_id
)
SELECT COUNT_IF(manual_id IS NOT NULL AND auto_id IS NOT NULL) AS matched_banks,
       COUNT_IF(manual_id IS NOT NULL AND auto_id IS NULL) AS manual_only,
       COUNT_IF(manual_id IS NULL AND auto_id IS NOT NULL) AS auto_only,
       COUNT_IF(manual_id IS NOT NULL AND auto_id IS NOT NULL
                AND ABS(auto_score-manual_score) >
                    GREATEST(0.000001,ABS(manual_score)*0.000000001)) AS score_mismatches,
       MAX(ABS(auto_score-manual_score)) AS max_score_delta
FROM paired;

-- Sharing-case checks: read-only SELECTs; run between shared pipeline executions.
-- CASE A: verify three account-connected transfers against the cleaned source.
-- Checked query: d084c4cc-c9af-44dc-84f4-10ec1e74294c.
-- The executed projection omitted the derived cutoff column; all amounts <20M.
SELECT txn_id,txn_ts,from_bank,from_account,to_bank,to_account,
       amount_paid_usd,is_laundering,attempt_id,pattern_type,
       amount_paid_usd<20000000 AS retained_under_primary_cutoff
FROM is459_g2t1_aml.transactions_clean
WHERE dataset='HI-Medium'
  AND txn_date BETWEEN '2022-09-16' AND '2022-09-17'
  AND txn_id IN (1923680745310817531,-5560620630767630164,
                 -2118989140943353923)
ORDER BY txn_ts;

-- CASE B: current priority context for 260802 -> 151989; checked live.
-- Query ID not supplied. This is not a new risk score or full snapshot comparison.
SELECT from_bank,to_bank,direct_laundered_usd,
       total_laundering_in_path_usd,extra_laundering_value_usd,blindspot_gap,
       from_bank_betweenness,from_bank_community,
       to_bank_betweenness,to_bank_community,cross_community_flag
FROM is459_g2t1_aml.bq1_priority_sharing_list
WHERE from_bank=260802 AND to_bank=151989;

-- CASE C: optional full-attempt context; not yet checked live in Athena.
-- Local retained records: 10 transactions, USD96,840,838.84.
-- The three-transfer sharing example is only a subpath, not unique proceeds.
SELECT attempt_id,COUNT(*) AS retained_annotated_transactions,
       ROUND(SUM(amount_paid_usd),2) AS attempt_transaction_value_usd,
       MIN(txn_ts) AS first_time,MAX(txn_ts) AS last_time
FROM is459_g2t1_aml.transactions_clean
WHERE dataset='HI-Medium' AND is_laundering=1
  AND amount_paid_usd<20000000 AND attempt_id=1885
GROUP BY attempt_id;

-- Task 11: fresh-ID run01/run02 partition equivalence.
-- Executed:5159 matched,113 groups each,0 run01 splits,0 run02 merges.
-- Query ID:189b5da1-06ce-409b-b50b-d463661a759c.
-- IDs/uniqueness/coverage are checked separately.
-- Both split/merge counts zero means equivalent partitions of matched banks.
WITH overlaps AS (
    SELECT f.community_id AS first_community,
           s.community_id AS second_community,
           COUNT(*) AS shared_banks
    FROM is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_01 f
    JOIN is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_02 s
      ON f.bank_id = s.bank_id
    GROUP BY f.community_id,s.community_id
), first_groups AS (
    SELECT first_community,COUNT(*) AS mapped_groups
    FROM overlaps GROUP BY first_community
), second_groups AS (
    SELECT second_community,COUNT(*) AS mapped_groups
    FROM overlaps GROUP BY second_community
)
SELECT (SELECT SUM(shared_banks) FROM overlaps) AS matched_banks,
       (SELECT COUNT(*) FROM first_groups) AS run01_communities,
       (SELECT COUNT(*) FROM second_groups) AS run02_communities,
       (SELECT COUNT_IF(mapped_groups > 1) FROM first_groups) AS run01_splits,
       (SELECT COUNT_IF(mapped_groups > 1) FROM second_groups) AS run02_merges;

-- Task 11: manual/automated partition equivalence, ignoring numeric labels.
-- Executed:5159 matched,113 manual groups,113 auto groups,0 splits,0 merges.
-- Query ID:ee12264a-1031-460c-8245-8e0aaafa6b40.
-- Bank-ID uniqueness/complete matching were checked
-- above. Zero manual_splits AND zero auto_merges establish equivalent groups
-- over the matched banks, even if community ID numbers differ.
WITH overlaps AS (
    SELECT m.community_id AS manual_community,
           a.community_id AS auto_community,
           COUNT(*) AS shared_banks
    FROM is459_g2t1_aml.bq1_graph_bank_features_hi01 m
    JOIN is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_01 a
      ON m.bank_id = a.bank_id
    GROUP BY m.community_id,a.community_id
), manual_groups AS (
    SELECT manual_community,COUNT(*) AS mapped_groups
    FROM overlaps GROUP BY manual_community
), auto_groups AS (
    SELECT auto_community,COUNT(*) AS mapped_groups
    FROM overlaps GROUP BY auto_community
)
SELECT (SELECT SUM(shared_banks) FROM overlaps) AS matched_banks,
       (SELECT COUNT(*) FROM manual_groups) AS manual_communities,
       (SELECT COUNT(*) FROM auto_groups) AS auto_communities,
       (SELECT COUNT_IF(mapped_groups > 1) FROM manual_groups) AS manual_splits,
       (SELECT COUNT_IF(mapped_groups > 1) FROM auto_groups) AS auto_merges;

-- Task 12: independent SSM sample feature validation passed on 7 November 2026.
-- Observed:42 bank_rows,42 unique_banks,8 communities,0 invalid_rows.
-- Query ID:d40eb381-bdfd-40c7-a12a-b5a58ccc991e.
-- Command ID:fe553b23-2f76-48a4-bf58-dd59f06b1997.
SELECT COUNT(*) AS bank_rows,
       COUNT(DISTINCT bank_id) AS unique_banks,
       COUNT(DISTINCT community_id) AS communities,
       COUNT_IF(bank_id IS NULL OR TRIM(bank_id) = ''
                OR betweenness IS NULL OR NOT is_finite(betweenness)
                OR betweenness < 0 OR community_id IS NULL) AS invalid_rows
FROM is459_g2t1_aml.bq1_graph_bank_features_sample_ssm_20261107_01;

-- Task 11: fresh-ID repeat feature comparison (run01 versus run02).
-- Executed:5159 matched,0 run01-only,0 run02-only,0 score mismatches;
-- maximum score delta2.3283064365386963E-10. Same tolerance as manual/auto.
-- Query ID:1fa74ea8-81ad-453d-8f6f-2feff5199312.
WITH paired AS (
    SELECT f.bank_id AS first_id,s.bank_id AS second_id,
           f.betweenness AS first_score,s.betweenness AS second_score
    FROM is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_01 f
    FULL OUTER JOIN is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_02 s
      ON f.bank_id = s.bank_id
)
SELECT COUNT_IF(first_id IS NOT NULL AND second_id IS NOT NULL) AS matched_banks,
       COUNT_IF(first_id IS NOT NULL AND second_id IS NULL) AS run01_only,
       COUNT_IF(first_id IS NULL AND second_id IS NOT NULL) AS run02_only,
       COUNT_IF(first_id IS NOT NULL AND second_id IS NOT NULL
                AND ABS(second_score-first_score) >
                    GREATEST(0.000001,ABS(first_score)*0.000000001)) AS score_mismatches,
       MAX(ABS(second_score-first_score)) AS max_score_delta
FROM paired;
