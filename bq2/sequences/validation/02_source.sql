-- Render for each dataset. Source totals are transactions, never sequence counts.
-- No date filter on this first scan: catch unexpected coverage, not just expected rows.
SELECT '__DATASET__' AS dataset, count(*) AS source_transactions,
       count_if(is_laundering=1) AS positive_transactions,
       count_if(is_laundering=0) AS legitimate_transactions,
       count(DISTINCT txn_date) AS observed_dates,
       min(txn_ts) AS first_timestamp,max(txn_ts) AS last_timestamp,
       count_if(txn_id IS NULL) AS null_txn_ids,
       count(txn_id)-count(DISTINCT txn_id) AS duplicate_txn_ids,
       count_if(NOT coalesce(is_laundering IN (0,1),false)) AS bad_labels,
       count_if(NOT coalesce(txn_date=cast(cast(txn_ts AS date) AS varchar)
         AND txn_date BETWEEN '2022-09-01' AND '__LAST_DATE__',false)) AS bad_dates,
       count_if(NOT coalesce(txn_ts=date_trunc('minute',txn_ts),false)) AS bad_timestamp_precision,
       count_if(NOT coalesce(from_bank IS NOT NULL AND to_bank IS NOT NULL
         AND length(trim(from_account))>0 AND length(trim(to_account))>0
         AND length(trim(payment_format))>0 AND strpos(payment_format,' -> ')=0
         AND length(trim(payment_currency))>0 AND strpos(payment_currency,' -> ')=0
         AND length(trim(receiving_currency))>0 AND strpos(receiving_currency,' -> ')=0,
         false)) AS bad_link_or_token_fields,
       count(*)=__SOURCE_ROWS__ AND count_if(is_laundering=1)=__POSITIVE_ROWS__
         AS reported_baseline_matches
FROM is459_g2t1_aml.transactions_clean
WHERE dataset='__DATASET__';

-- Every expected source date, including an explicit zero if no rows are observed.
WITH expected AS (
    SELECT cast(day AS varchar) AS txn_date
    FROM UNNEST(sequence(DATE '2022-09-01',DATE '__LAST_DATE__',INTERVAL '1' DAY)) AS u(day)
), actual AS (
    SELECT txn_date,count(*) AS source_transactions,
           count_if(is_laundering=1) AS positive_transactions,
           count(DISTINCT "$path") AS observed_files,min(txn_ts) AS first_timestamp,
           max(txn_ts) AS last_timestamp
    FROM is459_g2t1_aml.transactions_clean WHERE dataset='__DATASET__'
    GROUP BY txn_date
)
SELECT '__DATASET__' AS dataset,e.txn_date,
       coalesce(a.source_transactions,0) AS source_transactions,
       coalesce(a.positive_transactions,0) AS positive_transactions,
       coalesce(a.observed_files,0) AS observed_files,a.first_timestamp,a.last_timestamp
FROM expected e LEFT JOIN actual a ON e.txn_date=a.txn_date ORDER BY e.txn_date;
