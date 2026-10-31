# IS459 G2T1: Cross-Bank & Cross-Channel AML Analytics

Big data pipeline on AWS using the IBM Transactions for Anti Money Laundering dataset (HI-Medium).

- **BQ1:** Which pairs of banks are most involved in laundering, and which should prioritise information sharing?
- **BQ2:** Which payment-method and currency sequences are most associated with laundering, and where should checks be targeted?

## Repository layout

| Folder | Owner | Contents |
|---|---|---|
| `ingestion/` | Role 1 | S3 ingestion, FX/BTC Lambda, Glue crawlers, Step Functions |
| `etl/` | Role 2 | Glue Spark job producing `transactions_clean` |
| `bq1/` | Roles 3, 4 | Bank corridor analysis, Neo4j graph (Cypher) |
| `bq2/` | Roles 5, 6 | Sequence builder, lift and targeting analysis |
| `viz/` | Roles 3, 6 | Tableau workbooks and screenshots |
| `docs/` | All | Handoff notes, schema, architecture diagram |

## Start here

Read [`docs/ingestion-handoff.md`](docs/ingestion-handoff.md) for bucket names, table schemas, IAM roles and team rules.

## Rules

- Never commit secrets (`.pem`, AWS keys, Kaggle tokens) or data files. `.gitignore` blocks the common ones.
- Put your code in your own folder and commit often.
