-- One published dataset at a time; the other dataset need not be ready yet.
-- Expect __OWNER_DAYS__ PASS rows, including empty dates. Check locations separately.
WITH expected AS (
    SELECT cast(day AS varchar) AS start_date
    FROM UNNEST(sequence(DATE '2022-09-01',DATE '__LAST_DATE__',INTERVAL '1' DAY)) AS u(day)
), actual AS (
    SELECT start_date,count(*) AS registrations
    FROM is459_g2t1_aml."bq2_sequences$partitions"
    WHERE dataset='__DATASET__'
    GROUP BY start_date
)
SELECT '__DATASET__' AS dataset,coalesce(e.start_date,a.start_date) AS start_date,
       coalesce(a.registrations,0) AS registrations,
       CASE WHEN e.start_date IS NULL THEN 'FAIL_EXTRA'
            WHEN a.start_date IS NULL THEN 'FAIL_MISSING'
            WHEN a.registrations<>1 THEN 'FAIL_DUPLICATE' ELSE 'PASS' END AS status
FROM expected e FULL OUTER JOIN actual a ON e.start_date=a.start_date
ORDER BY start_date;
