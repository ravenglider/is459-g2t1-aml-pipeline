-- Compare observed paths to publication receipt and Glue partition locations.
-- All files for each dataset must belong to the SAME accepted run/attempt.
-- No row count can establish absence/presence of files for a zero-output date.
SELECT dataset,start_date,"$path" AS observed_path,count(*) AS sequence_rows,
       regexp_extract("$path",'/versions/([^/]+)/([^/]+)/data/',1) AS run_id,
       regexp_extract("$path",'/versions/([^/]+)/([^/]+)/data/',2) AS attempt_id,
       NOT coalesce(regexp_like("$path",concat(
         '^s3://is459-g2t1-aml-processed-bucket/bq2_sequences/dataset=',dataset,
         '/versions/[A-Za-z0-9][A-Za-z0-9_-]{0,63}/[A-Za-z0-9][A-Za-z0-9_-]{0,63}',
         '/data/dataset=',dataset,'/start_date=',start_date,'/[^/]+[.]parquet$')),false)
         AS bad_path_shape
FROM is459_g2t1_aml.bq2_sequences WHERE dataset='__DATASET__'
GROUP BY dataset,start_date,"$path"
ORDER BY start_date,observed_path;
