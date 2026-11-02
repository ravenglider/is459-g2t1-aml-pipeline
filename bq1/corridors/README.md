# bq1/corridors: Bank-pair corridors

**Owner:** Role 3 (name: Ryan)

Aggregates cross-bank laundering activity by bank pair, computes the Blindspot Gap and Extra Laundering Value using IBM's laundering-attempt links, ranks corridors by priority for information sharing, and feeds the Tableau heatmap/ranked table.

## What to commit here

| File | Contents |
|---|---|
| `bq1_corridors.sql` | Athena queries building `bq1_direct_corridors`, `bq1_attempt_banks`, `bq1_shared_attempt_value`, `bq1_corridors_ranked` |
| `metrics.md` | Exact formulas for Blindspot Gap and Extra Laundering Value, with a worked example |
| `priority_list.sql` | Query producing the ranked priority sharing list (pending Le Xin's Neo4j findings) |
| `results.md` | Top 10 corridors and key findings (numbers for the slides) |

## Notes for this role

- Reads from `transactions_clean` (Role 2, both datasets — filtered to `dataset = 'HI-Medium'` here).
- Writes: see "Inputs and outputs" below — not in the processed bucket as originally planned (see note).
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
| Writes | is459_g2t1_aml.bq1_corridors_ranked | s3://is459-g2t1-aml-athena-bucket/results/tables/8c7b8406-1b52-48e2-aca9-2fbacfd51185/ |

**Note:** the shared Athena workgroup enforces a centralized output location, so these tables did not write to `processed-bucket/bq1_corridors/` as originally planned in the handoff doc — they're under the Athena results bucket instead, in an auto-generated per-table folder. Flagged to Sonia; may need a workgroup setting change if Role 4's Neo4j load needs a predictable path.

## How to run

1. Open `bq1_corridors.sql` in the Athena query editor, database `is459_g2t1_aml`.
2. Run the 4 `CREATE TABLE` statements in order (if re-running, `DROP TABLE` each one first).
3. Query `bq1_corridors_ranked` for the final ranked list.

## Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | Athena (`arn:aws:states:::athena:startQueryExecution.sync`) |
| Exact name | `bq1_corridors.sql` (4 chained CREATE TABLE statements) |
| Output table | `is459_g2t1_aml.bq1_corridors_ranked` |
| Output S3 path | `s3://is459-g2t1-aml-athena-bucket/results/tables/8c7b8406-1b52-48e2-aca9-2fbacfd51185/` (see note above — not the processed bucket) |
| Depends on | `is459_g2t1_aml.transactions_clean` (Role 2) only |
| Typical run time on full data | All 4 queries complete in under 5 seconds combined on HI-Medium |

## Contribution summary (for the Week 13 slide)

Built the BQ1 bank-pair corridor aggregation, Blindspot Gap, and Extra Laundering Value pipeline in Athena SQL; identified and filtered synthetic data-quality outliers (~1% of laundering rows) that were distorting the ranking; produced the ranked priority-sharing list.
