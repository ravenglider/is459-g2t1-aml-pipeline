# IS459 G2T1 AML Project: Ingestion & Orchestration Handoff

**Status:** Ingestion complete. Step Functions skeleton running end to end. Waiting on Roles 2 to 6 for job names.

---

## 1. Quick reference

| Item                           | Value                                                                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| AWS region                     | `us-east-1` (N. Virginia)                                                                                                      |
| Naming prefix                  | `is459-g2t1-` (hyphens) / `is459_g2t1_` (underscores, for Glue databases)                                                      |
| Glue database (data catalog)   | `is459_g2t1_aml`                                                                                                               |
| Athena query result location   | `s3://is459-g2t1-aml-athena-bucket/results/`                                                                                   |
| Athena CTAS                    | Always set `external_location` to your own folder in the processed bucket. The folder must be empty before each run.           |
| Glue role to use for your jobs | `AWSGlueServiceRole-is459g2t1`                                                                                                 |
| Budget                         | `is459-g2t1-budget`, USD 250 for the whole duration. Budget limit set at USD 230, with alerts to our SMU emails at 50% and 80% |

**Everything must be created in `us-east-1`.** Resources in other regions will not be visible to the pipeline.

---

## 2. Resource inventory

### 2.1 S3 buckets

| Bucket                            | Purpose                                              | Who writes   |
| --------------------------------- | ---------------------------------------------------- | ------------ |
| `is459-g2t1-aml-bucket`           | Raw zone. Original data, never modified              | Role 1 only  |
| `is459-g2t1-aml-processed-bucket` | Processed zone. Cleaned Parquet and analysis outputs | Roles 2 to 6 |
| `is459-g2t1-aml-athena-bucket`    | Athena query results and job scripts                 | Everyone     |

**Raw bucket layout**

```
is459-g2t1-aml-bucket/
  ibm/hi_medium/            HI-Medium_Trans.csv        (2.8 GiB)
  ibm/li_medium/            LI-Medium_Trans.csv        (2.8 GiB)
  ibm/sample/               hi_medium_sample.csv       (48 MiB, first 500,000 rows of HI-Medium)
  ibm/hi_medium_accounts/   HI-Medium_accounts.csv     (138 MiB)
  patterns/                 HI-Medium_Patterns.txt     (2.2 MiB, not crawled)
  fx/                       fx_rates_daily.csv         (written by Lambda)
  btc/                      btc_usd_daily.csv          (written by Lambda)
```

**Athena bucket layout**

```
is459-g2t1-aml-athena-bucket/
  results/     Athena query output
  scripts/     Glue job scripts
```

**Processed bucket:** each role creates its own folder (CREATE YOUR OWN I DID NOT CREATE IT YET). Proposed names:

```
is459-g2t1-aml-processed-bucket/
  transactions_clean/   (Role 2)
  bq1_corridors/        (Role 3)
  bq1_graph_edges/      (Role 4)
  bq2_sequences/        (Role 5)
  bq2_lift/             (Role 6)
```

### 2.2 Glue Data Catalog

**Database:** `is459_g2t1_aml`

| Table                    | Source path               | Rows       | Notes                              |
| ------------------------ | ------------------------- | ---------- | ---------------------------------- |
| `raw_hi_medium`          | `ibm/hi_medium/`          | 31,898,238 | Main dataset                       |
| `raw_li_medium`          | `ibm/li_medium/`          |            | For BQ2 robustness check           |
| `raw_sample`             | `ibm/sample/`             | 500,000    | **Use this for development**       |
| `raw_hi_medium_accounts` | `ibm/hi_medium_accounts/` |            | Account to bank and entity mapping |
| `raw_fx`                 | `fx/`                     |            | Daily FX rates per currency        |
| `raw_btc`                | `btc/`                    |            | Daily BTC rate                     |

### 2.3 Glue crawlers

| Crawler                          | Data sources                                                                 | Classifier                   | Table prefix | In Step Functions?                |
| -------------------------------- | ---------------------------------------------------------------------------- | ---------------------------- | ------------ | --------------------------------- |
| `is459-g2t1-raw-crawler`         | `ibm/hi_medium/`, `ibm/li_medium/`, `ibm/sample/`, `ibm/hi_medium_accounts/` | `is459-g2t1-trans-csv`       | `raw_`       | No (data is static, run manually) |
| `is459-g2t1-fxbtc-rates-crawler` | `fx/`, `btc/`                                                                | `is459-g2t1-fxbtc-rates-csv` | `raw_`       | Yes                               |

### 2.4 Glue custom classifiers

| Classifier                   | Type                                   | Custom header                                                                                                                                      |
| ---------------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `is459-g2t1-trans-csv`       | CSV, comma, double quote, has headings | `timestamp,from_bank,from_account,to_bank,to_account,amount_received,receiving_currency,amount_paid,payment_currency,payment_format,is_laundering` |
| `is459-g2t1-fxbtc-rates-csv` | CSV, comma, double quote, has headings | `date,currency,currency_code,units_per_usd,source`                                                                                                 |

### 2.5 Lambda

**Rates Lambda**

| Item               | Value                                                                              |
| ------------------ | ---------------------------------------------------------------------------------- |
| Function           | `is459-g2t1-fetch-fxbtc-rates`                                                     |
| Runtime            | Python 3.14 (standard library and boto3 only, no layers)                           |
| Timeout            | 1 minute                                                                           |
| Execution role     | `is459-g2t1-fetch-fxbtc-rates-role`, with `AmazonS3FullAccess` attached            |
| Output             | `s3://is459-g2t1-aml-bucket/fx/fx_rates_daily.csv` and `.../btc/btc_usd_daily.csv` |
| Default date range | 2022-08-25 to 2022-09-30 (starts early so weekend forward-fill has a seed)         |

**Reset outputs Lambda**

| Item           | Value                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------- |
| Function       | `is459-g2t1-reset-outputs`                                                                           |
| Runtime        | Python 3.14 (boto3 only, no layers)                                                                  |
| Timeout        | 1 minute                                                                                             |
| Execution role | `is459-g2t1-reset-outputs-role-*`, with `AmazonS3FullAccess` and `AWSGlueConsoleFullAccess` attached |
| Purpose        | Before Athena CTAS steps rerun: deletes the Glue tables and empties their S3 folders                 |
| Safety         | Only works on `is459-g2t1-aml-processed-bucket`, and only on folder prefixes ending in `/`           |
| Used by        | `BQ1_ResetOutputs` step (Role 3 tables). Reusable for Role 6                                         |

### 2.6 Step Functions

| Item           | Value                                                                                                                   |
| -------------- | ----------------------------------------------------------------------------------------------------------------------- |
| State machine  | `is459-g2t1-pipeline` (Standard)                                                                                        |
| Execution role | Auto-created, with `AWSGlueConsoleFullAccess`, `AWSLambdaRole`, `AmazonAthenaFullAccess`, `AmazonS3FullAccess` attached |
| Input          | `{}`                                                                                                                    |
| Last run       | 2 Nov 2026, Succeeded (ETL and BQ1 live)                                                                                |

### 2.7 EC2 (ingestion only)

| Item           | Value                                                                          |
| -------------- | ------------------------------------------------------------------------------ |
| Instance       | `is459-g2t1-ingest` (Amazon Linux 2023, 50 GiB gp3)                            |
| Security group | `sg-05691ad89519d37c6 - launch-wizard-6` (inbound SSH 22 from owner’s IP only) |
| IAM role       | `is459-g2t1-ec2-s3-role` (`AmazonS3FullAccess`)                                |
| State          | **Stopped.** Only restart to re-download data or record the setup video        |

### 2.8 IAM roles summary

| Role                                | Used by                                     | Policies                                                                                    |
| ----------------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `AWSGlueServiceRole-is459g2t1`      | Glue crawlers, **and teammates’ Glue jobs** | Glue-generated policy, `AmazonS3ReadOnlyAccess`, `AmazonS3FullAccess`                       |
| `is459-g2t1-fetch-fxbtc-rates-role` | Lambda                                      | Basic execution, `AmazonS3FullAccess`                                                       |
| `is459-g2t1-ec2-s3-role`            | EC2 ingest instance                         | `AmazonS3FullAccess`                                                                        |
| `is459-g2t1-reset-outputs-role-*`   | Reset outputs Lambda                        | Basic execution, `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`                           |
| Step Functions auto-created role    | State machine                               | `AWSGlueConsoleFullAccess`, `AWSLambdaRole`, `AmazonAthenaFullAccess`, `AmazonS3FullAccess` |

> **Note for Roles 2 to 6:** `AWSGlueServiceRole-is459g2t1` can read and write S3. Use it as the IAM role for your Glue jobs. Write outputs only to `is459-g2t1-aml-processed-bucket`.

---

## 3. Data facts

### 3.1 Transactions (`raw_hi_medium`, `raw_li_medium`, `raw_sample`)

| Column               | Type   | Notes                                                            |
| -------------------- | ------ | ---------------------------------------------------------------- |
| `timestamp`          | string | Format `YYYY/MM/DD HH:MM`, e.g. `2022/09/01 00:20`. Parse in ETL |
| `from_bank`          | bigint | Originating bank ID                                              |
| `from_account`       | string | Originating account (was the first `Account` column)             |
| `to_bank`            | bigint | Receiving bank ID                                                |
| `to_account`         | string | Receiving account (was the second `Account` column, `Account.1`) |
| `amount_received`    | double | In `receiving_currency`                                          |
| `receiving_currency` | string | Currency name, see 3.3                                           |
| `amount_paid`        | double | In `payment_currency`                                            |
| `payment_currency`   | string | Currency name, see 3.3                                           |
| `payment_format`     | string | Cheque, Credit Card, ACH, Wire, Cash, Bitcoin, Reinvestment      |
| `is_laundering`      | bigint | 1 = laundering, 0 = legitimate                                   |

- **HI-Medium date range:** 2022-09-01 00:00 to 2022-09-28 15:58
- **HI-Medium rows:** 31,898,238
- **Sample:** first 500,000 rows of HI-Medium, a contiguous slice in time so 24h chains and multi-hop paths stay intact

### 3.2 Rates (`raw_fx`, `raw_btc`)

| Column          | Type   | Notes                                                      |
| --------------- | ------ | ---------------------------------------------------------- |
| `date`          | string | `YYYY-MM-DD`, every calendar day (weekends forward-filled) |
| `currency`      | string | Matches the dataset’s currency names exactly               |
| `currency_code` | string | ISO code                                                   |
| `units_per_usd` | double | Units of that currency per 1 USD                           |
| `source`        | string | ECB via Frankfurter / base / fixed peg / Coinbase          |

**Conversion to USD**

```sql
amount_usd = amount / units_per_usd
-- join on: currency name = receiving_currency (or payment_currency)
--          date = date_format(date_parse(timestamp, '%Y/%m/%d %H:%i'), '%Y-%m-%d')
```

`date` is a reserved word in Athena: write it as `"date"`.

### 3.3 Currency coverage

| Source                       | Currencies                                                                                                                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ECB via Frankfurter          | Euro, Yuan, Yen, Rupee, UK Pound, Canadian Dollar, Australian Dollar, Swiss Franc, Mexican Peso, Brazil Real, Shekel                                                             |
| Base                         | US Dollar (1.0)                                                                                                                                                                  |
| Fixed peg                    | Saudi Riyal (3.75 per USD)                                                                                                                                                       |
| Coinbase BTC-USD daily close | Bitcoin                                                                                                                                                                          |
| **Not covered**              | **Ruble.** ECB stopped publishing it in March 2022. Role 2 to derive an implied rate from the dataset (transactions where one side is Ruble and the other is a covered currency) |

## 4. Pipeline (Step Functions)

```
FetchRates (Lambda)
  → StartRatesCrawler → Wait 20s → GetCrawlerState → loop until READY
  → ETL_TransactionsClean            [LIVE: Glue is459-g2t1-etl-transactions-clean, ~18 min, stops pipeline on DQ failure]
  → Parallel:
       BQ1: ResetOutputs (Lambda) → [Step 1 ‖ Step 2a] → Step 2b → Step 3   [LIVE: Role 3 Athena CTAS]
            → BQ1_Neo4jLoad          [Role 4 placeholder]
       BQ2_SequenceBuilder → BQ2_LiftAnalysis  [Role 5, Role 6 placeholders]
  → PipelineComplete
```

Step Functions has no `.sync` integration for crawlers, so the crawler is polled with a Wait and Choice loop.

Athena CTAS steps run as the state machine role, so that role needs S3 access to read `transactions_clean` and write outputs. Before each CTAS rerun, `is459-g2t1-reset-outputs` drops the tables and empties their folders.

### How your job gets added

Send Sonia:

1. **Glue job name** (or the Athena SQL / named query, or the Neo4j import command)
2. **Output table name** and **S3 path**
3. Whether it depends on anything other than `transactions_clean`

Your placeholder is then replaced with:

| Job type            | Step Functions resource                                                            |
| ------------------- | ---------------------------------------------------------------------------------- |
| Glue Spark job      | `arn:aws:states:::glue:startJobRun.sync`                                           |
| Athena query        | `arn:aws:states:::athena:startQueryExecution.sync`                                 |
| Neo4j import on EC2 | `arn:aws:states:::aws-sdk:ssm:sendCommand` (instance must have SSM agent and role) |

---

## 5. Rules for everyone

1. **Develop on `raw_sample`.** Only run on `raw_hi_medium` once the logic works.
2. **Glue jobs:** set **Number of workers** low (2 to 5) while developing. Default settings burn budget fast.
3. **Athena on raw CSV scans the whole file every time.** Query `transactions_clean` (Parquet) once Role 2 publishes it.
4. **Stop EC2 instances when idle,** especially Neo4j.
5. **Never commit secrets:** no `.pem` files, AWS keys or Kaggle tokens in GitHub. Add `*.pem` and `.env` to `.gitignore`.
6. **Write only to the processed bucket.** The raw bucket is read-only for everyone except Role 1.
7. **Tell Sonia before creating new IAM roles** so the inventory stays complete for cleanup.
