# bq2/sequences: Payment sequence builder

**Owner:** Role 5 (name: ____________)

Glue Spark job that chains hops (money received by an account, then sent on within 24h) into payment-method and currency sequences with laundering labels.

## What to commit here

| File | Contents |
|---|---|
| `bq2_sequence_builder.py` | Glue Spark job script |
| `job_settings.md` | Worker type and count, IAM role, job arguments |
| `sequence_rules.md` | Exact hop rule (24h window), max sequence length, how a sequence is labelled laundering |
| `example_chain.md` | One real chain from the data, traced step by step (for the demo) |

## Notes for this role

- Read from `transactions_clean` (use `raw_sample` before it is ready; the sample is a contiguous time slice so chains stay intact).
- Run on both HI-Medium and LI-Medium for Role 6's robustness check.
- Write to `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/`.
- This is the heaviest Spark job: test thoroughly on the sample before running on full data.

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