# Role 5 sample-only preflight: prepared locally, NOT deployed

This is a validation/candidate-profile job, not ETL, sequence generation, staging
or publication. No deployment, artifact upload, AWS job/resource creation or run
is authorized by this document. Coordinate those actions separately with Sonia.

## Proposed job settings

| Setting | Value |
|---|---|
| Name | `is459-g2t1-bq2-sample-preflight` (proposed) |
| Region | `us-east-1`, confirmed group account ID supplied explicitly |
| Engine | Glue **5.0**, Spark **3.5.4**, Python **3.11**, Java **17** |
| Worker type/count | **G.1X / 2** |
| Execution class | Flex, following Role 2; startup may be delayed |
| Automatic retries | **0** |
| Maximum concurrent runs | **1** |
| Timeout | Proposed **15 minutes**; configure in Glue, not just script arguments |
| Bookmarks | Disabled (`job-bookmark-disable`) |
| Role | Existing `AWSGlueServiceRole-is459g2t1` |
| Script/package location | Existing Athena bucket `scripts/` prefix; upload only after approval |
| Temporary location | Existing Athena bucket `glue-temp/` prefix |
| Source | `is459_g2t1_aml.transactions_clean`, only `dataset=sample` |
| Sample dates | Exactly `2022-09-01` through `2022-09-06` |

Use a standard Spark script job, not a notebook/interactive session. Do not run
Role 2's job or the shared state machine; their defaults target HI-Medium.

## Arguments and artifact preparation

`glue_sample_preflight.py` is the separately selected entry script. The allowlisted
ZIP also includes it for isolated import testing; supply the ZIP through
`--extra-py-files`. Do not package `.venv`, transaction data, tests or credentials,
and do not install another PySpark inside Glue.

Required arguments:

- `--EXPECTED_ACCOUNT_ID`: group account ID, 12 digits, confirmed with Sonia.
- `--RUN_ID`: fresh safe identifier (letters/digits/underscore/hyphen).
- `--TIMEZONE_EVIDENCE`: reference to Amareet's confirmed upstream UTC-compatible
  interpretation. Setting this argument is an attestation, not independent proof.
- `--NO_REFRESH_ACKNOWLEDGED`: explicit `true`; `false` blocks execution.
- `--NO_REFRESH_EVIDENCE`: current coordination reference from Amareet/Sonia.
  No active jobs at one instant does not prevent later manual refreshes.
- `--BUILDER_FINGERPRINT`: SHA-256 of the actual tested module ZIP.

`--DATASET` defaults to `sample` and rejects all other values. `--REGION` defaults
to `us-east-1` and rejects all other values. `--JOB_NAME` must match the dedicated
name. For an approved Glue Console run, enter key `--ALLOW_AWS_READS` with value
`true` (equivalent to `--ALLOW_AWS_READS=true` on the CLI). Missing or `false`
activation stays offline: output explicitly says `NOT_RUN` and
`preflight_completed=false`. If `--JOB_NAME`, `--JOB_RUN_ID` or `--JOB_ID` is supplied, offline
validation exits with code **2**, preventing a misleading green Glue run. Local
offline validation without those injected arguments retains exit code zero.

Only lowercase literal `true` enables reads; malformed/valueless or duplicate
activation arguments fail parsing. The old `--allow-aws-reads` spelling remains
an alias but now also requires an explicit `true`/`false` value. Never supply
both spellings. Duplicate Role 5 configuration values and abbreviated argument
names are rejected. Known Glue engine arguments are allowlisted; known boolean
engine flags support presence or explicit true/false and repeated `--conf` is
allowed. Unknown custom arguments are rejected. If Glue injects an
unsupported engine argument, inspect it and update the allowlist locally; do not
silently ignore all unknown arguments.

## What the future run reads and checks

1. STS account identity and explicit client region; runtime versions and UTC.
2. Exact table schema and every `dataset='sample'` Catalog partition, paginated.
3. Exact sample S3 locations and complete object listing, paginated. Unknown
   artifacts/missing dates fail closed; `_SUCCESS` is not validation evidence.
4. Every inventoried Parquet file's 18 physical column names/types, no ID casts.
5. All 20 logical columns via Catalog pushdown across the six sample dates.
6. Required values, binary labels, minute timestamps and UTC date consistency.
7. Dataset-wide IDs across the entire selected sample, including distant dates.
8. Exactly **500,000 rows** and **35 laundering rows**, plus counts by date and
   observed timestamp range. Baseline changes stop the run for investigation.
9. Conservative pair-candidate upper bound for owner September 1: input dates
   September 1–3; preceding dates September 1–2. Grouped degrees only, no actual
   transaction pairs or triples. Exact triple projection remains deferred.
10. Re-read Catalog and S3 inventories before success. Changes fail the run.

The original identity function continues to report `provided_dataframe_only`
and `dataset_wide_verified=False`. Separate coverage evidence binds that result
to the complete inventoried sample. It does not establish full-HI completeness.
The 500,000-row extract can omit continuations and truncate the final day.

## Evidence output and storage boundary

The script returns a JSON report and prints it prefixed `PREFLIGHT |` for future
CloudWatch logs. It contains runtime versions, source object metadata/fingerprints,
Catalog fingerprints, coverage, counts, identity evidence, candidate bound,
timings and `VALIDATED_READ_ONLY` or `FAILED`. No transaction/account rows are
logged. Failures raise exceptions so Glue cannot report a passed preflight.

The report contains this DESIGN-ONLY destination:

`s3://is459-g2t1-aml-processed-bucket/bq2_sequences_metadata/development/preflight/<run_id>/report.json`

**Report S3 writes are disabled.** There is no report writer or put/copy/delete
call in this increment, even in the opt-in read runtime. `report_storage_enabled`
is always false. A validated read report is not a stored run manifest or a
publication receipt. Glue/CloudWatch themselves may write logs/temp artifacts
during a future actual run. Download/store log evidence only after approval.

Only reused sample data is persisted. Existing caller caches are retained and
owned persistence is released on success/failure. Cleanup failure fails the run.
The adapter never stops a caller-owned SparkSession.

## Remaining prerequisites and stop conditions

- Renew the no-refresh agreement at execution time; retain timezone evidence.
- Verify Glue trust/Catalog reads, logging, scripts/temp access and any KMS or
  Lake Formation restrictions. S3FullAccess alone does not prove effective access.
- Coordinate the proposed job and `iam:PassRole` with Sonia. No IAM changes here.
- Approve artifact upload, job configuration and the paid run separately.
- Stop on account/region/path/schema/coverage/count/identity failures, inventory
  changes, permissions, arithmetic overflow or timeout. Do not silently raise
  workers, retry, cast IDs, drop invalid rows or accept a changed baseline.

There are no final production growth thresholds: this run measures the candidate
bound. It never calls link builders, the anchored batch runner, sequence writers,
publication functions, Athena or Catalog mutations. Real Glue/S3 connector and
commit behavior remain unverified by local fakes; no output commit is tested.
