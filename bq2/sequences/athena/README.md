# bq2/sequences/athena: production BQ2 sequence build (Athena SQL, Step Functions)

Builds the full HI-Medium and LI-Medium sequences in the **same approved 21-column v1 schema** as the sample builder (`../schema_bq2_sequences.md`, rules in `../sequence_rules.md`). Everyone queries **`is459_g2t1_aml.bq2_sequences`**, exactly as before.

## How it works

A sequence is a self-join of `transactions_clean`: money arrives in an account, then leaves it within 24 hours. Each transaction is matched only against its own day and the next day (a valid next hop can never be later), which keeps the join small and still finds every link exactly once. HI is about 50 million chains per day (2.87 billion rows in total, about 236 GB), so the build runs one dataset-day per query.

## Production safety

| Risk | Control |
|---|---|
| A failed or partial build replaces good data | Two copies, `bq2_sequences_a` (folder `bq2_sequences/`) and `bq2_sequences_b` (folder `bq2_sequences_b/`). `bq2_sequences` is a **view** on the live copy. Each run builds the other copy and switches the view only after every gate passes. The previous copy stays as a rollback |
| Retries append duplicate rows | Athena tasks retry only when Athena refuses to start a query (nothing written). Any duplicate from any cause makes the day gate's stored count differ from its recount, so it cannot be published. Each run starts by dropping and emptying the copy it builds |
| Missing or invalid source members | Two gates per dataset-day. Count gate: stored pairs and triples = fresh recount from `transactions_clean`. Lineage gate, one pass with LEFT JOINs: every member exists in the source, its stored timestamp, method, currencies and label match, and the next member leaves the account it paid into within (0, 24h]. Structure gate recomputes every derived column of every row and rejects duplicates. Together: stored chains are exactly the expected set |
| Incomplete or changing source | Source gate before the build: exact row counts (HI 31,898,218, LI 31,251,469), unique `txn_id`, every owner date present (HI 28, LI 27), plus a fingerprint of every source file (path, size, modified time). Repeated after the build: the fingerprint must be identical |
| Two writers at once | `PipelineLock`, the first pipeline state, stops a run while an earlier execution is still running. The pipeline is the only writer: there is no manual production build. The control Lambda refuses to reset the live copy |

## Pipeline steps (BQ2 branch of `is459-g2t1-pipeline`)

`BQ2_Plan` (which copy is live) → `BQ2_SourceBefore` gate → `BQ2_ResetTarget` (the other copy) → `BQ2_CreateTable` (empty, v1 schema) → `BQ2_BuildDays` Map, 5 dataset-days at a time: render SQL, pairs, triples, count gate, lineage gate → `BQ2_Structure` gate → `BQ2_SourceAfter` gate (unchanged) → `BQ2_Publish` (switch the view) → `BQ2_LiftAnalysis` (Role 6).

Any failed gate ends the execution with a named error (`SourceIncomplete`, `DayCountCheckFailed`, `DayLineageCheckFailed`, `StructureCheckFailed`, `SourceChangedDuringBuild`) and the failing result row as the cause. The live view is untouched.

## Files

| File | What it does |
|---|---|
| `generate_sql.py` | One template for every statement: build, gates, manual checks. Edit this, not the `.sql` files |
| `control_lambda/lambda_function.py` | Lambda `is459-g2t1-bq2-control`: `lock`, `plan`, `reset`, `render`. Deploy as a zip with `generate_sql.py` |
| `build_asl.py` | Writes the BQ2 branch and `PipelineLock` into `ingestion/stepfunctions/is459-g2t1-pipeline.asl.json` |
| `migrate_to_slots.py` | One-time switch of the existing table to copy A plus the view (no data copied). Safe to rerun |
| `collect_evidence.py` | Writes `evidence/pipeline_<execution>.md` from a run's history: every gate's full result row and query id |
| `coverage.py` | Per-date evidence for the live view: source transactions, expected chains (recount) and stored chains for all 55 dataset-days. Dates with zero valid chains have no partition |
| `run_athena.py` | Runs a `.sql` file in CloudShell for manual checks, with `--evidence` to record results |
| `00_preflight.sql` | Source checks, sample size, HI 2022-09-01 recount, the source gate |
| `01_validate_on_sample.sql` | Rebuilds the sample into `bq2_sequences_check` and compares every column with `bq2_sequences_dev` |
| `03_acceptance.sql`, `04_day_checks.sql` | The structure gate and the 110 day gates (count and lineage per dataset-day) against the live view, for a manual recheck |
| `test_generated_sql.py` | DuckDB: the SQL equals a brute-force implementation of the rules (day by day too), and every gate blocks 16 kinds of corruption |
| `test_control.py` | Fake AWS: lock, copy selection, reset refusing the live copy, migration with rollback, evidence report, source gate |
| `evidence/` | Build and acceptance records with query ids |

## Setup (once)

1. **Lambda** `is459-g2t1-bq2-control`, Python 3.12 or later, handler `lambda_function.lambda_handler`, timeout 5 minutes, code = zip of `control_lambda/lambda_function.py` and `generate_sql.py` at the top level. Inline policy on its role: `control_lambda/policy.json`.
2. After any pipeline execution in progress finishes: `python3 migrate_to_slots.py` in CloudShell.
3. Deploy the state machine definition (validate first), then run the pipeline.
4. `python3 collect_evidence.py <execution ARN>` and commit the file it writes.

## Local tests

```bash
pip install sqlglot duckdb
python3 test_generated_sql.py   # ALL PASS
python3 test_control.py         # ALL PASS
```

## Cost per pipeline run

Build about 8 GB scanned. Gates: the structure gate reads the whole copy once (about 236 GB), each lineage gate reads its own day once. Roughly USD 2 to 3 per run; check `collect_evidence.py` output for the real figure. Storage for the second copy about USD 5 per month.
