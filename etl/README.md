# etl/ (Role 2: Shared Spark ETL)

Owner: Amareet

## What it does
A Glue Spark job that turns the raw IBM transactions into **transactions_clean**: typed and de-duplicated, every amount converted to USD (Ruble rate derived from the data), laundering rows linked to their attempt and shape from IBM's patterns file, quality-checked, and stored as Parquet split by dataset and date. Every BQ1 and BQ2 job reads this table.

Files:
- [etl_transactions_clean.py](etl_transactions_clean.py): the job script
- [schema_transactions_clean.md](schema_transactions_clean.md): columns for the rest of the team
- [job_settings.md](job_settings.md): Glue settings and run history
- [data_quality.md](data_quality.md): checks, Ruble method, results
- [cost_evidence.md](cost_evidence.md): CSV vs Parquet and Glue cost numbers

## AWS resources

| Resource | Exact name |
|---|---|
| Glue job | `is459-g2t1-etl-transactions-clean` |
| IAM role (Sonia's, shared) | `AWSGlueServiceRole-is459g2t1` |
| Script | `s3://is459-g2t1-aml-athena-bucket/scripts/etl_transactions_clean.py` |
| Output folder | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/` |
| Output table | `is459_g2t1_aml.transactions_clean` (created and updated by the job itself, no crawler needed) |

## Inputs and outputs

| | Table / file | S3 path |
|---|---|---|
| Reads | raw_sample / raw_hi_medium / raw_li_medium (CSV files) | `s3://is459-g2t1-aml-bucket/ibm/sample/`, `ibm/hi_medium/`, `ibm/li_medium/` |
| Reads | raw_fx, raw_btc | `s3://is459-g2t1-aml-bucket/fx/`, `btc/` |
| Reads | patterns file | `s3://is459-g2t1-aml-bucket/patterns/HI-Medium_Patterns.txt` |
| Writes | transactions_clean | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/dataset=<DATASET>/txn_date=<date>/` |

## How to run
1. AWS console, region **us-east-1**. Open **AWS Glue > ETL jobs**.
2. Open `is459-g2t1-etl-transactions-clean` (settings in [job_settings.md](job_settings.md)).
3. **Job details > Advanced properties > Job parameters:** set `--DATASET` to `sample`, `HI-Medium` or `LI-Medium`. Use 2 workers for sample, 10 for the full files.
4. Click **Save**, then **Run**.
5. **Runs** tab: wait for **Succeeded**. Open **Output logs** and find the `DQ |` line.
6. Athena, database `is459_g2t1_aml`:
   `SELECT dataset, COUNT(*), SUM(is_laundering), SUM(usd_missing) FROM transactions_clean GROUP BY 1;`

Re-running a dataset replaces only that dataset's folder.

## Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | Glue Spark job (`arn:aws:states:::glue:startJobRun.sync`) |
| Exact name | `is459-g2t1-etl-transactions-clean` |
| Arguments | `{"--DATASET": "HI-Medium"}` (a second run with `LI-Medium` if the pipeline should also refresh LI) |
| Output table | `transactions_clean` |
| Output S3 path | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/` |
| Depends on | raw_fx and raw_btc (FetchRates + rates crawler, already before it), raw transaction files, patterns file |
| Typical run time on full data | About 18 minutes for HI-Medium (17 m 55 s, 10 workers, Flex); about 10 minutes for LI-Medium (9 m 40 s) |
| Fails when | Timestamps unreadable or more than 0.1% of rows not convertible to USD (Step Functions then stops before BQ1/BQ2) |

## Contribution summary (for the Week 13 slide)
Built the shared Glue Spark ETL producing transactions_clean: cleaning, USD conversion (including a Ruble rate derived from the data), linking laundering attempts from IBM's patterns file, automated quality checks, and partitioned Parquet that cut Athena data scanned by about 15× (about 250× when filtering to one day).

Results: 31,898,218 clean HI-Medium rows (20 duplicates removed, 0 rows missing USD), 35,230 laundering rows (matches IBM's published figure), 22,743 of them linked to a laundering attempt. All three Glue runs together cost about $1.36.

Evidence screenshots: [screenshots/](screenshots/) (list in [data_quality.md](data_quality.md#screenshots)).
