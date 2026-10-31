# bq2/lift: Lift and targeting analysis

**Owner:** Role 6 (name: ____________)

Athena analysis of `bq2_sequences`: lift per sequence, top-N targeted checks vs broad checks, and the LI-Medium robustness check.

## What to commit here

| File | Contents |
|---|---|
| `bq2_lift.sql` | Lift per sequence with the minimum-support cutoff |
| `tradeoff.sql` | Top 3/5/10 sequences: % laundering caught vs % legitimate reviewed |
| `robustness.sql` | Same ranking on LI-Medium, overlap with HI-Medium top sequences |
| `results.md` | Final ranked list, trade-off numbers, targeted check list |

## Notes for this role

- Athena result location is set at the workgroup level: `s3://is459-g2t1-aml-athena-bucket/results/`.
- Use `CREATE TABLE AS` (CTAS) writing Parquet to `s3://is459-g2t1-aml-processed-bucket/bq2_lift/` so Tableau and Step Functions can use the output.
- Send Sonia the exact SQL for each Step Functions step.
- Sankey and trade-off charts go in `viz/`.

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