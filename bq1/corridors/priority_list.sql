-- priority_list.sql
-- Builds the final BQ1 priority sharing list: joins corridor rankings (bq1_corridors_ranked)
-- with Role 4's graph features (bq1_graph_bank_features_latest, a view the pipeline points
-- at the most recent validated graph run) to add betweenness, community membership,
-- and a cross-community flag for each corridor.
-- cross_community_flag is NULL when either bank has no graph features (missing, not "no risk").
--
-- In the pipeline: runs after the Analytics step (needs both the corridor tables and the
-- graph run). The reset Lambda drops this table and empties its S3 folder before each run.
-- Manual rerun: run the DROP, then empty
-- s3://is459-g2t1-aml-processed-bucket/bq1_priority_sharing_list/ before the CREATE.

DROP TABLE IF EXISTS is459_g2t1_aml.bq1_priority_sharing_list;

CREATE TABLE is459_g2t1_aml.bq1_priority_sharing_list
WITH (format = 'PARQUET', external_location = 's3://is459-g2t1-aml-processed-bucket/bq1_priority_sharing_list/') AS
SELECT
  r.from_bank,
  r.to_bank,
  r.direct_laundered_usd,
  r.total_laundering_in_path_usd,
  r.extra_laundering_value_usd,
  r.blindspot_gap,
  fb.betweenness AS from_bank_betweenness,
  fb.community_id AS from_bank_community,
  tb.betweenness AS to_bank_betweenness,
  tb.community_id AS to_bank_community,
  CASE WHEN fb.community_id IS NULL OR tb.community_id IS NULL THEN NULL
       WHEN fb.community_id != tb.community_id THEN 1 ELSE 0 END AS cross_community_flag
FROM is459_g2t1_aml.bq1_corridors_ranked r
LEFT JOIN is459_g2t1_aml.bq1_graph_bank_features_latest fb ON r.from_bank = CAST(fb.bank_id AS BIGINT)
LEFT JOIN is459_g2t1_aml.bq1_graph_bank_features_latest tb ON r.to_bank = CAST(tb.bank_id AS BIGINT);