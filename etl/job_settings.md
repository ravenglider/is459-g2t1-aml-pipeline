# Glue job settings

| Setting | Value | Why |
|---|---|---|
| Job name | `is459-g2t1-etl-transactions-clean` | Team naming convention |
| Type / engine | Spark, Python 3 | Distributed processing of ~32M rows |
| Glue version | Glue 5.0 (Spark 3.5) | The script was tested locally on Spark 3.5, so 5.0 runs exactly what was tested. 6.0 costs the same but would need re-testing; upgrading later is a settings change plus one sample run |
| IAM role | `AWSGlueServiceRole-is459g2t1` | Sonia's shared Glue role (reads raw bucket, writes processed bucket, updates the catalog) |
| Worker type | G.1X | Smallest standard worker |
| Number of workers | **2** for `sample`, **10** for HI-Medium / LI-Medium | Keep cost low while testing |
| Execution class | **Flex** | About a third cheaper than Standard; we do not need instant start |
| Job timeout | 60 minutes | Stops a stuck run from burning budget |
| Number of retries | 0 | A failed run should be looked at, not repeated automatically |
| Job bookmark | Disable | Every run processes the whole file |
| Script path | `s3://is459-g2t1-aml-athena-bucket/scripts/` | Team's script location |
| Temporary path | `s3://is459-g2t1-aml-athena-bucket/glue-temp/` | Keeps Glue from creating an extra bucket |
| Spark UI logs | Off | Not needed; saves S3 writes |
| Job parameter | `--DATASET` = `sample` / `HI-Medium` / `LI-Medium` | Picks which raw file to clean |

Script: [etl_transactions_clean.py](etl_transactions_clean.py)

Run history (1 Nov 2026, all from the job's Runs tab):

| Date | --DATASET | Workers | Execution class | Run time | DPU-hours | Status |
|---|---|---|---|---|---|---|
| 1 Nov 2026 | sample | 2 | Flex | 2 m 18 s | ~0.08 | Succeeded |
| 1 Nov 2026 | HI-Medium | 10 | Flex | 17 m 55 s | ~3.0 | Succeeded |
| 1 Nov 2026 | LI-Medium | 10 | Flex | 9 m 40 s | ~1.6 | Succeeded |

After the runs, `--DATASET` was set back to `HI-Medium` and saved, so the job's default is the main dataset.
