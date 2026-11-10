# IS459 G2T1 AML Project: Ingestion & Orchestration Handoff

**Status (10 November 2026):** Complete. Every role's step is live in the Step Functions pipeline, and the latest full run (`demo_run`, 9 November 2026) succeeded in 92 minutes with all 114 data checks passing ([evidence](../bq2/sequences/athena/evidence/pipeline_demo_run.md)). Tableau dashboards read the final tables live from Athena.

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

**Processed bucket layout** (one folder per table; the pipeline writes all of these)

```
is459-g2t1-aml-processed-bucket/
  transactions_clean/                     Role 2   cleaned Parquet, partitioned by dataset
  bq1_direct_corridors/                   Role 3   BQ1 step 1
  bq1_attempt_banks/                      Role 3   BQ1 step 2a
  bq1_shared_attempt_value/               Role 3   BQ1 step 2b
  bq1_corridors_ranked/                   Role 3   ranked corridors (Tableau)
  bq1_graph_edges/, bq1_graph_results/    Role 4   Neo4j input edges and graph features
  bq1_priority_sharing_list/              Role 3   corridors + graph features
  bq2_sequences/, bq2_sequences_b/        Role 5   the two sequence copies (A and B)
  bq2_lift/                               Role 6   lift by pattern (Tableau)
  bq2_tradeoff_flagged/                   Role 6   trade-off helper
  bq2_tradeoff/, bq2_robustness/, bq2_sankey/   Role 6   (Tableau)
```

Folders ending in `_dev` and other `bq2_sequences_*` folders hold the sample and development builds from earlier testing. The pipeline does not use them.

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
| Used by        | `BQ1_ResetOutputs` and `BQ1_PriorityReset` (Role 3 tables), `BQ2_Lift_Reset` (Role 6 tables)         |

BQ2 never drops live data: it builds the copy that `bq2_sequences` does not point to, and switches the view only after every gate passes. A failed run leaves the live copy untouched. See `bq2/sequences/athena/README.md`.

**BQ2 control Lambda**

| Item           | Value                                                                                                                                             |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Function       | `is459-g2t1-bq2-control`                                                                                                                          |
| Runtime        | Python 3.12, handler `lambda_function.lambda_handler`, zip of `lambda_function.py` and `generate_sql.py`                                          |
| Timeout        | 5 minutes                                                                                                                                         |
| Execution role | `is459-g2t1-bq2-control-role-*`, basic execution plus inline `is459-g2t1-bq2-control-policy`                                                      |
| Actions        | `lock` (one pipeline run at a time), `plan` (which copy is live, build the other), `reset` (empty the build copy), `render` (SQL per dataset-day) |
| Safety         | Refuses to reset the live copy. Can only drop `bq2_sequences_a`/`_b` and empty `bq2_sequences/` or `bq2_sequences_b/`                             |
| Code           | `bq2/sequences/athena/control_lambda/` (policy in `policy.json`)                                                                                  |

### 2.6 Step Functions

| Item           | Value                                                                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------------------------- |
| State machine  | `is459-g2t1-pipeline` (Standard)                                                                                          |
| Execution role | Auto-created, with `AWSGlueConsoleFullAccess`, `AWSLambdaRole`, `AmazonAthenaFullAccess`, `AmazonS3FullAccess` attached   |
| Input          | `{}`                                                                                                                      |
| Last run       | 9 Nov 2026, `demo_run`: Succeeded in 92 min, all steps of every role, 114/114 data checks passed                          |
| Concurrency    | `PipelineLock` (first state) stops a run while an earlier execution is still running                                      |
| BQ2 output     | `bq2_sequences` is a **view** on `bq2_sequences_a` or `bq2_sequences_b`. Query the view, never write to `_a`/`_b` by hand |

### 2.7 EC2 (ingestion, shared Neo4j host)

| Item                  | Value                                                                                                                            |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Instance              | `is459-g2t1-ingest` / `i-0125056b848e5634a` (Amazon Linux 2023, 50 GiB gp3)                                                      |
| Instance type         | `t3.medium` (resized from `t3.small`)                                                                                            |
| Used by               | Role 1: one-off Kaggle download. Role 4: Neo4j graph analysis                                                                    |
| Neo4j                 | Docker container `role4-neo4j`, Neo4j 5.26.32 Community with GDS, persistent volumes `role4-data`, `role4-logs`, `role4-plugins` |
| Role 4 work directory | `/home/ssm-user/role4/` (credentials in a protected `.env`, never in Git)                                                        |
| Security group        | `sg-05691ad89519d37c6 - launch-wizard-6`: SSH 22 from Sonia's IP; Neo4j 7474 and 7687 from Role 4's IP only                      |
| Access                | Session Manager (no shared `.pem` needed)                                                                                        |
| IAM role              | `is459-g2t1-ec2-s3-role` (see 2.8)                                                                                               |
| State                 | **Stop when idle.** Coordinate with Role 4 before starting or stopping.                                                          |

### 2.8 IAM roles summary

| Role                                | Used by                                     | Policies                                                                                                                                                                              |
| ----------------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AWSGlueServiceRole-is459g2t1`      | Glue crawlers, **and teammates’ Glue jobs** | Glue-generated policy, `AmazonS3ReadOnlyAccess`, `AmazonS3FullAccess`                                                                                                                 |
| `is459-g2t1-fetch-fxbtc-rates-role` | Lambda                                      | Basic execution, `AmazonS3FullAccess`                                                                                                                                                 |
| `is459-g2t1-ec2-s3-role`            | EC2 instance (ingest, now Neo4j)            | `AmazonS3FullAccess`, `AmazonSSMManagedInstanceCore`, inline `is459-g2t1-neo4j-athena-glue` (Athena on primary workgroup; Glue read, create, update, delete tables in is459_g2t1_aml) |
| `is459-g2t1-reset-outputs-role-*`   | Reset outputs Lambda                        | Basic execution, `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`                                                                                                                     |
| Step Functions auto-created role    | State machine                               | `AWSGlueConsoleFullAccess`, `AWSLambdaRole`, `AmazonAthenaFullAccess`, `AmazonS3FullAccess`                                                                                           |
| `is459-g2t1-bq2-control-role-*`     | BQ2 control Lambda                          | Basic execution, inline `is459-g2t1-bq2-control-policy` (Glue get/delete on the BQ2 tables, S3 list/delete on the two copy folders, `states:ListExecutions` on the pipeline)          |

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
- **HI-Medium rows:** 31,898,238 raw, 31,898,218 in `transactions_clean`
- **LI-Medium:** 31,251,469 rows in `transactions_clean`, 2022-09-01 to 2022-09-27; used for the BQ2 robustness check
- **Sample:** first 500,000 rows of HI-Medium, a contiguous slice in time so 24h chains and multi-hop paths stay intact
- `transactions_clean` is partitioned by `dataset`; an ETL run replaces only its own dataset's partition (verified 7 Nov: HI rewrite left LI from 1 Nov intact).

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
PipelineLock (Lambda: stop if an earlier execution is running)
  → FetchRates (Lambda)
  → StartRatesCrawler → Wait 20s → GetCrawlerState → loop until READY
  → ETL_TransactionsClean            [LIVE: Glue is459-g2t1-etl-transactions-clean, stops pipeline on DQ failure]
  → Parallel:
       BQ1: ResetOutputs → [DirectCorridors ‖ AttemptBanks] → SharedAttemptValue → CorridorsRanked   [LIVE: Role 3 Athena]
       Graph: start EC2 if needed → SSM run_graph (Neo4j) → check receipt → update _latest view → stop EC2   [LIVE: Role 4]
       BQ2: Plan → source gate → reset build copy → create → Map 55 dataset-days (pairs, triples, day gate)
            → structure gate → source gate (unchanged) → publish view   [LIVE: Role 5 Athena]
            → reset → lift → trade-off stage 1 and 2 → robustness → Sankey → lift gate   [LIVE: Role 6 Athena]
  → BQ1_PriorityReset → BQ1_PriorityList   [LIVE: Role 3 + Role 4 features]
  → PipelineComplete
```

Step Functions has no `.sync` integration for crawlers, so the crawler is polled with a Wait and Choice loop.

Athena CTAS steps run as the state machine role, so that role needs S3 access to read `transactions_clean` and write outputs. Before each CTAS rerun, `is459-g2t1-reset-outputs` drops the tables and empties their folders.

### How your job gets added

All placeholders have been replaced (9 November 2026). This section is kept as the guide for adding a new step.

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
