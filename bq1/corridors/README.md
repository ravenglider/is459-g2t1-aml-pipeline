# bq1/corridors: Bank-pair corridors

**Owner:** Role 3 (name: Ryan)

Aggregates cross-bank laundering activity by bank pair, computes the Blindspot Gap and Extra Laundering Value using IBM's laundering-attempt links, ranks corridors by priority for information sharing, and feeds the Tableau heatmap/ranked table.

## What to commit here

| File | Contents |
|---|---|
| `bq1_corridors.sql` | Athena queries building `bq1_direct_corridors`, `bq1_attempt_banks`, `bq1_shared_attempt_value`, `bq1_corridors_ranked` |
| `metrics.md` | Exact formulas for Blindspot Gap and Extra Laundering Value, with a worked example |
| `priority_list.sql` | Query producing the ranked priority sharing list (pending Role 4's Neo4j findings) |
| `results.md` | Top 10 corridors and key findings (numbers for the slides) |

## Notes for this role

- Reads from `transactions_clean` (Role 2, both datasets — filtered to `dataset = 'HI-Medium'` here).
- Writes: see "Inputs and outputs" below — all four tables write to the processed bucket.
- Merge Role 4's graph findings (intermediary banks, clusters) into the priority list once available.
- Tableau heatmap and ranked table go in `viz/`.

## What it does

Reads `transactions_clean`, aggregates laundering value by bank pair (direct and via shared multi-bank laundering attempts), computes Extra Laundering Value and Blindspot Gap per pair, and ranks corridors by priority for information sharing.

## AWS resources

| Resource | Exact name |
|---|---|
| Athena workgroup | primary |
| Glue database | is459_g2t1_aml |
| Output tables | bq1_direct_corridors, bq1_attempt_banks, bq1_shared_attempt_value, bq1_corridors_ranked |

## Inputs and outputs

|  | Table | S3 path |
|---|---|---|
| Reads | is459_g2t1_aml.transactions_clean | s3://is459-g2t1-aml-processed-bucket/transactions_clean/ |
| Writes | is459_g2t1_aml.bq1_direct_corridors | s3://is459-g2t1-aml-processed-bucket/bq1_direct_corridors/ |
| Writes | is459_g2t1_aml.bq1_attempt_banks | s3://is459-g2t1-aml-processed-bucket/bq1_attempt_banks/ |
| Writes | is459_g2t1_aml.bq1_shared_attempt_value | s3://is459-g2t1-aml-processed-bucket/bq1_shared_attempt_value/ |
| Writes | is459_g2t1_aml.bq1_corridors_ranked | s3://is459-g2t1-aml-processed-bucket/bq1_corridors_ranked/ |

## How to run

1. Open `bq1_corridors.sql` in the Athena query editor, database `is459_g2t1_aml`.
2. Run each statement separately, in order — Athena only runs one statement per click.
3. On a rerun: run the 4 `DROP TABLE IF EXISTS` statements first, then **delete the contents of all four S3 folders above** before running the `CREATE TABLE` statements again — each table writes to a fixed folder now, and `DROP TABLE` does not delete the underlying files.
4. Query `bq1_corridors_ranked` for the final ranked list.

## Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | Athena (`arn:aws:states:::athena:startQueryExecution.sync`) |
| Exact name | `bq1_corridors.sql` (4 chained CREATE TABLE statements) |
| Output table | `is459_g2t1_aml.bq1_corridors_ranked` |
| Output S3 path | `s3://is459-g2t1-aml-processed-bucket/bq1_corridors_ranked/` |
| Depends on | `is459_g2t1_aml.transactions_clean` (Role 2) only |
| Typical run time on full data | All 4 queries complete in under 5 seconds combined on HI-Medium |

## Contribution summary (for the Week 13 slide)

Built the BQ1 bank-pair corridor aggregation, Blindspot Gap, and Extra Laundering Value pipeline in Athena SQL; identified and filtered synthetic data-quality outliers (~1% of laundering rows) that were distorting the ranking; produced the ranked priority-sharing list.
