# bq2/sequences/athena: full HI/LI sequence build in Athena SQL

Builds `is459_g2t1_aml.bq2_sequences` for HI-Medium and LI-Medium with plain Athena SQL, producing the **same approved 21-column v1 schema** as the sample builder (`schema_bq2_sequences.md`), so Role 6's queries work unchanged. No Glue job, packaging or SDK wheels.

## Why Athena

A sequence is a self-join of `transactions_clean`: money arrives in an account, then leaves it within 24 hours. Athena does this join directly on the cleaned Parquet. Each transaction is matched only against its own day and the next day (a valid next hop can never be later), which keeps the join small and still finds every valid link exactly once.

## Scale

Ji Ning's Glue run for HI-Medium owner date 2022-09-01 expected **14,679,196 pairs and 36,965,485 triples for that one day**. Across 28 HI days and 27 LI days the table is likely around a billion rows. That is too much for one Athena statement (30 minute limit), so the build runs **one start date per statement**: 111 statements, run by `run_athena.py`. Athena charges for data read, not written, and each statement reads only one to three days of `transactions_clean`, so the cost stays low. The runner prints the total.

## Files

| File | What it does |
|---|---|
| `generate_sql.py` | One template that writes every `.sql` file below. Edit this, not the `.sql` files |
| `test_generated_sql.py` | Local test: runs the generated SQL in DuckDB on synthetic edge-case data and compares every row with a brute-force implementation of `sequence_rules.md`, including the day-by-day build |
| `run_athena.py` | Runs a `.sql` file's statements in order in Athena, refuses to start while other queries are running, stops at the first failure, resumes with `--start N` |
| `00_preflight.sql` | Source ID uniqueness, sample size, and one full HI day counted against Ji Ning's Glue numbers |
| `01_validate_on_sample.sql` | Rebuilds the sample into `bq2_sequences_check` and compares every column with the accepted `bq2_sequences_dev` |
| `02_build_daily.sql` | The production build: DROP, then HI and LI, pairs and triples, one start date per statement |
| `03_acceptance.sql` | Counts, structure checks and a one-day lineage check on the built table |

## Run order (AWS CloudShell, us-east-1)

Open CloudShell from the console toolbar, upload this folder (Actions, Upload file) or clone the repo, then:

1. `python3 run_athena.py 00_preflight.sql`
   `duplicate_ids` must be 0 for every dataset. Sample: 184,449 pairs, 33,931 triples. HI 2022-09-01: **14,679,196 pairs, 36,965,485 triples** (Ji Ning's numbers).
2. Empty `s3://is459-g2t1-aml-processed-bucket/bq2_sequences_check/`, then `python3 run_athena.py 01_validate_on_sample.sql`
   Expected: `only_in_new = 0`, `only_in_dev = 0`, `new_pairs = 184449`, `new_triples = 33931`.
3. Empty `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/`, then `python3 run_athena.py 02_build_daily.sql`
   If it stops, it prints the statement number; rerun with `--start N`. Do not run the pipeline or the ETL job while it runs.
4. `python3 run_athena.py 03_acceptance.sql`
   Every structure and lineage count must be 0. The HI 2022-09-01 counts must match step 1.

CloudShell stops after about 20 minutes without keyboard input. If the build takes longer, run it as `nohup python3 run_athena.py 02_build_daily.sql > build.log 2>&1 &` and check with `tail build.log`.

## Local test

```bash
pip install sqlglot duckdb
python3 test_generated_sql.py   # prints ALL PASS
```
