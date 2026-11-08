-- Includes empty dates. Expect 55 rows, each registrations=1 and status=PASS.
-- Glue metadata export must independently verify ordered keys and version locations.
WITH expected AS (
    SELECT dataset,cast(day AS varchar) AS start_date
    FROM (VALUES ('HI-Medium',DATE '2022-09-28'),
                 ('LI-Medium',DATE '2022-09-27')) AS d(dataset,last_date)
    CROSS JOIN UNNEST(sequence(DATE '2022-09-01',last_date,INTERVAL '1' DAY)) AS u(day)
), actual AS (
    SELECT dataset,start_date,count(*) AS registrations
    FROM is459_g2t1_aml."bq2_sequences$partitions"
    GROUP BY dataset,start_date
)
SELECT coalesce(e.dataset,a.dataset) AS dataset,
       coalesce(e.start_date,a.start_date) AS start_date,
       coalesce(a.registrations,0) AS registrations,
       CASE WHEN e.dataset IS NULL THEN 'FAIL_EXTRA'
            WHEN a.dataset IS NULL THEN 'FAIL_MISSING'
            WHEN a.registrations<>1 THEN 'FAIL_DUPLICATE' ELSE 'PASS' END AS status
FROM expected e FULL OUTER JOIN actual a
  ON e.dataset=a.dataset AND e.start_date=a.start_date
ORDER BY dataset,start_date;
