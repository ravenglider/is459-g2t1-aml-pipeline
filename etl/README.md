# etl: Shared Spark ETL

**Owner:** Role 2 (name: ____________)

Glue Spark job that turns the raw IBM tables into `transactions_clean`: cleaned, typed, converted to USD and stored as partitioned Parquet. **Everyone depends on this table, so publish the schema first.**

## What to commit here

| File | Contents |
|---|---|
| `etl_transactions_clean.py` | Glue Spark job script |
| `job_settings.md` | Glue version, worker type and count, IAM role, job arguments |
| `schema_transactions_clean.md` | Final column names, types, partition keys (share with the team early) |
| `data_quality.md` | Row counts before and after, nulls, duplicates removed, Ruble handling |
| `cost_evidence.md` | Athena bytes scanned and run time, CSV vs Parquet (X-factor evidence) |

## Notes for this role

- Read from `raw_sample` while developing, then `raw_hi_medium` and `raw_li_medium`.
- Join `raw_fx` and `raw_btc` for USD conversion: `amount_usd = amount / units_per_usd` on currency name and date.
- Ruble has no external rate: derive an implied rate from the dataset and document how.
- Write to `s3://is459-g2t1-aml-processed-bucket/transactions_clean/` and register the table in `is459_g2t1_aml`.
- Use IAM role `AWSGlueServiceRole-is459g2t1`. Keep workers at 2 to 5 while developing.

---

## README template (fill in as you build)

### What it does

_2 to 3 lines._

### AWS resources

| Resource | Exact name |
|---|---|
| | |

### Inputs and outputs

| | Table | S3 path |
|---|---|---|
| Reads | | |
| Writes | | |

### How to run

1. 
2. 

### Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | |
| Exact name | |
| Output table | |
| Output S3 path | |
| Depends on | |
| Typical run time on full data | |

### Contribution summary (for the Week 13 slide)

_1 to 2 lines._