# How to contribute (all roles)

Everyone commits their own code and notes to this repo

## 1. Where your work goes

| Role | Folder | Commit |
|---|---|---|
| 1 Ingestion and orchestration | `ingestion/` | Lambda, Step Functions definition, Glue classifiers |
| 2 Shared Spark ETL | `etl/` | Glue job script, job settings, `transactions_clean` schema, cost evidence |
| 3 BQ1 corridors | `bq1/corridors/` | Spark or SQL for corridors and blindspot gap, priority list query |
| 4 BQ1 graph | `bq1/neo4j/` | EC2 and Neo4j setup notes, edge export, import script, Cypher queries |
| 5 BQ2 sequence builder | `bq2/sequences/` | Glue job script building `bq2_sequences` |
| 6 BQ2 analytics | `bq2/lift/` | Athena SQL for lift, top-N trade-off, LI robustness |
| 3 and 6 Tableau | `viz/` | `.twb` workbooks and dashboard screenshots |

Each folder has a README listing exactly what to add.

## 2. Every folder README must have

Fill in the template in your folder's README:

1. **What it does** (2 to 3 lines)
2. **AWS resources** you created: exact names of Glue jobs, tables, S3 paths, EC2 instances, IAM roles
3. **Inputs and outputs:** which table you read, which table and S3 path you write
4. **How to run it**, step by step (this becomes the setup and run video script)
5. **Step Functions info for Sonia** (see below)
6. **Contribution summary:** 1 to 2 lines for the Week 13 contributions slide

## 3. Getting your job into the pipeline

When your job works on `raw_sample`, add to your README:

| Field | Example |
|---|---|
| Job type | Glue Spark job / Athena query / Neo4j import via SSM |
| Exact name | `is459-g2t1-etl-transactions-clean` |
| Output table | `transactions_clean` |
| Output S3 path | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/` |
| Depends on | `transactions_clean` (or something else) |
| Typical run time on full data | ~6 minutes |

## 4. Naming conventions

- AWS resources: `is459-g2t1-<what>`, e.g. `is459-g2t1-bq2-sequence-builder`
- Glue tables: lowercase with underscores, e.g. `bq1_corridors`
- Output folders in the processed bucket match the table name
- Everything in `us-east-1`