# IS459 G2T1: Cross-Bank & Cross-Channel AML Analytics

Big data pipeline on AWS using the IBM Transactions for Anti Money Laundering dataset (HI-Medium, with LI-Medium as a robustness check).

- **BQ1:** Which pairs of banks are most involved in laundering, and which should prioritise information sharing?
- **BQ2:** Which payment-method sequences are most associated with money laundering, and how can banks use these patterns to target checks while minimising unnecessary reviews of legitimate transactions?

## Repository layout

| Folder | Owner | Contents |
|---|---|---|
| `ingestion/` | Role 1 | S3 ingestion, FX/BTC Lambda, Glue crawlers, Step Functions |
| `etl/` | Role 2 | Glue Spark job producing `transactions_clean` |
| `bq1/` | Roles 3, 4 | Bank corridor analysis, Neo4j graph (Cypher) |
| `bq2/` | Roles 5, 6 | Sequence builder, lift and targeting analysis |
| `viz/` | Roles 3, 6 | Tableau workbooks and screenshots |
| `docs/` | All | Handoff notes, schema, architecture diagram |

## Pipeline status

Everything runs from one Step Functions state machine, `is459-g2t1-pipeline` (definition: [`ingestion/stepfunctions/`](ingestion/stepfunctions/is459-g2t1-pipeline.asl.json)): rates Lambda, crawler, Glue ETL, then BQ1 corridors, the Neo4j graph and BQ2 (sequences, then lift) in parallel, then the BQ1 priority list. Tableau reads the final tables live from Athena.

Latest full run: `demo_run`, 9 November 2026, **Succeeded** in 92 minutes with all 114 data checks passing ([evidence](bq2/sequences/athena/evidence/pipeline_demo_run.md)). Architecture and pipeline diagrams: [`docs/diagrams/`](docs/diagrams/).

## Start here

Read [`docs/ingestion-handoff.md`](docs/ingestion-handoff.md) for bucket names, table schemas, IAM roles and team rules.

## Rules

- Never commit secrets (`.pem`, AWS keys, Kaggle tokens) or data files. `.gitignore` blocks the common ones.
- Put your code in your own folder and commit often.
