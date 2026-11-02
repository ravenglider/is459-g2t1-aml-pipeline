-- Frozen HI top 10 evaluated on LI; independent LI rank is descriptive only.
-- Missing patterns retain zero counts and undefined rates/lift.
CREATE TABLE is459_g2t1_aml.bq2_robustness
WITH (
  format = 'PARQUET',
  external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_robustness/'
) AS
SELECT hi.method_sequence, hi.sequence_length, hi.risk_rank AS hi_rank,
       hi.support_count AS hi_support, hi.lift AS hi_lift,
       COALESCE(li.support_count, 0) AS li_support,
       COALESCE(li.laundering_count, 0) AS li_laundering_count,
       li.laundering_rate AS li_laundering_rate, li.lift AS li_lift,
       li.risk_rank AS li_rank,
       CASE WHEN li.method_sequence IS NULL THEN 'Absent on LI'
            WHEN li.eligible = 0 THEN 'Below LI support cutoff'
            WHEN li.lift IS NULL THEN 'LI baseline has no laundering'
            ELSE 'Supported on LI' END AS li_status
FROM is459_g2t1_aml.bq2_lift hi
LEFT JOIN is459_g2t1_aml.bq2_lift li
  ON li.dataset = 'LI-Medium' AND li.method_sequence = hi.method_sequence
     AND li.sequence_length = hi.sequence_length
WHERE hi.dataset = 'HI-Medium' AND hi.risk_rank <= 10;
