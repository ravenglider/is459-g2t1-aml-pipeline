# Cost and performance evidence (X-factor)

## 1. CSV vs Parquet in Athena
Same question asked of the raw CSV table and of transactions_clean. Copy "Data scanned" and "Run time" from Athena (shown under the results, and in Query history).

```sql
-- raw CSV
SELECT payment_format, SUM(amount_paid) FROM is459_g2t1_aml.raw_hi_medium GROUP BY 1;
-- Parquet
SELECT payment_format, SUM(amount_paid) FROM is459_g2t1_aml.transactions_clean WHERE dataset = 'HI-Medium' GROUP BY 1;
-- Parquet, one day only (shows the date folders working)
SELECT payment_format, SUM(amount_paid) FROM is459_g2t1_aml.transactions_clean WHERE dataset = 'HI-Medium' AND txn_date = '2022-09-05' GROUP BY 1;
```

| Query | Data scanned | Run time | Cost at $5/TB |
|---|---|---|---|
| Raw CSV | 2.82 GB | 1.414 s | ~$0.014 |
| Parquet, all days | 186.08 MB | 1.332 s | ~$0.0009 |
| Parquet, one day | 11.18 MB | 710 ms | ~$0.00005 |

Result: Parquet scanned about **15× less** data than CSV for the same question, and about **250× less** when filtered to one day, because Athena reads only the columns and date folders it needs. At 32M rows the time difference is small, but cost in Athena is per byte scanned, so every BQ1/BQ2 query and Tableau refresh is about 15× cheaper. Screenshots 13a, 13b, 13c.

## 2. Storage size
S3 console > select the folder > Actions > Calculate total size.

| Data | Size |
|---|---|
| Raw HI-Medium CSV (`is459-g2t1-aml-bucket/ibm/hi_medium/`) | 2.8 GiB in 1 file |
| Parquet (`transactions_clean/dataset=HI-Medium/`) | 1.5 GiB in 28 files (one per day), even with 7 extra columns added |

## 3. Glue run cost
From the job's Runs tab (run time, DPU-hours). Flex is about $0.29 per DPU-hour, Standard about $0.44 (check current AWS pricing).

| Run | Workers | Run time | DPU-hours | Approx. cost |
|---|---|---|---|---|
| sample | 2 | 2 m 18 s | ~0.08 | ~$0.02 |
| HI-Medium | 10 | 17 m 55 s | ~3.0 | ~$0.87 |
| LI-Medium | 10 | 9 m 40 s | ~1.6 | ~$0.47 |
| **Total** | | | | **~$1.36** of the $250 budget |

Screenshots 14a, 14b (sizes) and 06, 10, 11 (runs).
