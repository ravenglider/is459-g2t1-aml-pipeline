# bq1/corridors: Bank-pair corridors

**Owner:** Role 3 (name: Ryan Wu)

Aggregates laundering activity by bank pair, computes the blindspot gap and extra laundering value, ranks corridors and produces the priority sharing list.

## What to commit here

| File | Contents |
|---|---|
| `bq1_corridors.py` or `bq1_corridors.sql` | Glue Spark job or Athena query building `bq1_corridors` |
| `metrics.md` | Exact formulas for blindspot gap and extra laundering value, with a worked example |
| `priority_list.sql` | Query producing the ranked priority sharing list |
| `results.md` | Top 10 corridors and key findings (numbers for the slides) |

## Notes for this role

- Read from `transactions_clean` once Role 2 publishes it (use `raw_sample` before that).
- Write to `s3://is459-g2t1-aml-processed-bucket/bq1_corridors/`.
- Merge Role 4's graph findings (intermediary banks, clusters) into the priority list.
- Tableau heatmap and ranked table go in `viz/`.

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
