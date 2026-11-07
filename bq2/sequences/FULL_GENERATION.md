# Full HI/LI generation: local implementation and publication boundary

This increment generates and validates **staging only**. It does not publish
`bq2_sequences`, replace an existing dataset, or update Glue Catalog tables.
Those parts of Sonia's final single-job contract are still required. No new
AWS deployment, permissions, runtime, growth threshold or full output count is
claimed here. The accepted sample development workflow remains unchanged.

## Fixed source scope

| `--DATASET` (exact value) | Owner dates, inclusive | Expected clean transfers | Expected positive labels |
|---|---|---:|---:|
| `HI-Medium` | 2022-09-01 through 2022-09-28 | 31,898,218 | 35,230 |
| `LI-Medium` | 2022-09-01 through 2022-09-27 | 31,251,469 | 16,041 |

One selected dataset per run, all owner dates, both sequence lengths, approved
21-column v1 output. A batch reads only its D/D+1/D+2 window from a selected
dataset cache. The final two dates explicitly record unavailable continuations
after the independently confirmed source boundary. There is no extra 24-hour
whole-chain restriction. No sampling, minimum-support filter or branch truncation
is introduced. Dataset/time boundaries are observed coverage, not proof that
real-world payment activity ended.

## New components

- `sequence_full_source.py`: full selected-dataset Catalog/S3 inventory, exact
  coverage and schemas, three fingerprints, explicit partition pushdown.
- `sequence_full_generation.py`: full-source field/ID validation once before
  daily filtering, all owner dates, existing guarded batch runner, final source
  checks and cleanup, staging handoff completion.
- `sequence_full_staging.py`: exclusive run/attempt reservation, per-date
  Snappy Parquet, existing physical-schema/compression/read-back validators,
  conditional receipts, explicit empty outputs and retained failures.
- `glue_full_generation.py`: reusable HI/LI CLI and injected coordinator;
  uses the existing approved runtime/SDK capability gates before staging writes.
- `package_full_generation.py`: separate deterministic flat 17-module ZIP.
  Entry script is separate; the existing 13-module/sample and nine-module
  preflight packaging profiles are unchanged. SDK wheels remain separate.

Imports start no clients or Spark sessions. Tests use fake AWS clients; real
Parquet tests must run in the existing Ubuntu/PySpark environment.

## Run arguments and safeguards

Use the same Glue 5.0 / Python 3.11 / Java 17 / approved Spark and pinned SDK
environment as the accepted sample job. Bootstrap arguments remain optional in
Python argv, but if provided must match reviewed versioned role-owned artifacts.
SDK versions and actual conditional PutObject capability are checked at runtime.
No Python-side dependency installation is implemented.

Required arguments:

```text
--DATASET                         HI-Medium or LI-Medium (no aliases/default)
--EXPECTED_ACCOUNT_ID             independently verified 12-digit group account
--REGION                          us-east-1
--RUN_ID --ATTEMPT_ID --OWNER_TOKEN fresh safe identifiers (1-64 characters)
--BUILDER_VERSION                 exact reviewed release/source version
--BUILDER_FINGERPRINT             SHA-256 of role5_full_modules.zip
--SOURCE_FINGERPRINT              fresh full selected-dataset object inventory hash
--TABLE_FINGERPRINT               fresh source table metadata hash
--PARTITIONS_FINGERPRINT          fresh selected-dataset partition metadata hash
--TIMEZONE_EVIDENCE               independent upstream UTC verification
--NO_REFRESH_ACKNOWLEDGED         true
--NO_REFRESH_EVIDENCE             current frozen-source coordination reference
--MAX_PAIR_CANDIDATES              positive per-owner-batch bound
--MAX_PAIR_LINKS                   positive continuation-pair bound
--MAX_PROJECTED_TRIPLES            positive anchored-triple bound
--MAX_TRIPLES_PER_MIDDLE           positive maximum middle expansion bound
--MAX_OUTPUT_ROWS                  positive pair+triple batch bound
--LIMITS_EVIDENCE                  measured-profile/review reference
--MAX_RECORDS_PER_FILE             positive explicit file-sizing limit
--MAX_RUN_SECONDS                  positive cooperative elapsed-time budget
--ALLOW_AWS_WRITES                 literal true; otherwise no runtime initialization
```

No sample thresholds or production defaults are supplied. Fresh full fingerprints
must be obtained with `inspect_full_source`, not the sample fingerprint helper.
The generation API supports independently attested empty-date evidence; the CLI
currently supplies an empty evidence map and therefore fails on unexplained
missing partitions/files. Existing zero-row Parquet partitions are scanned and
can produce explicit empty output batches. Do not invent missing-date evidence.

The complete selected input is persisted with `DISK_ONLY` to avoid repeated S3
full reads. Global ID/required-field validation and daily source counts run on
that cache once; existing batch validators still recheck the bounded windows.
The orchestrator releases only its own cache, after daily batch caches exit.
Sufficient worker local disk and shuffle capacity remain deployment prerequisites.

`MAX_RUN_SECONDS` checks between stages, not during in-flight Spark actions.
Also configure an actual Glue job Timeout, zero retries and maximum concurrency
one. Freeze upstream refreshes for the run: matching metadata is change evidence,
not a transactional snapshot or lock. Spark's S3 file committer is protected by
cooperative immutable-prefix ownership, not atomic per-file conditional writes.

## Generation-to-publication interface

Fixed root (never queryable production/development roots):

```text
s3://is459-g2t1-aml-processed-bucket/bq2_sequences_staging/full/
  dataset=<HI-Medium|LI-Medium>/<run_id>/<attempt_id>/
    metadata/run.json
    batches/<YYYY-MM-DD>/
      metadata/status.json
      metadata/validation.json
      data/dataset=<dataset>/start_date=<YYYY-MM-DD>/*.parquet
```

The run manifest has kind `full_sequence_generation_handoff` and only declares
`STAGED_VALIDATED` after all dates, final source consistency and cleanup succeed.
It records dataset/run/attempt/owner, rule/builder version, all three source
fingerprints, complete owner/source coverage, exact schema, source row/label
baselines, source identity check scope, totals, file count/bytes and per-date
receipts. Each batch includes continuation dates, verified-empty/boundary dates,
growth metrics, validated staging URI, files/sizes, inventory fingerprint,
receipt fingerprint, pair/triple counts and validation status. An empty batch
has explicit zero counts and no data files; it is not omitted.

Source identity evidence retains `verification_scope=provided_dataframe_only`
and `dataset_wide_verified=false`. Separately, `full_selected_snapshot_checked`
binds that scan to inspected complete coverage; it does not certify unseen data.

`published=false`, `catalog_updated=false` and `publication_required=true` are
preserved. This is not a publication manifest and must not be relabeled PUBLISHED.
FAILED/CREATED runs are not consumable. On failure keep immutable artifacts for
diagnosis; use a fresh attempt after review. No overwrite, deletion, blind retry
or stale-owner takeover exists. If writing FAILED also fails, preserve the
original exception and attach a note. An uncertain terminal write is not success.

## Work still required for the final single Glue job

1. Obtain fresh frozen HI/LI inventories and global ID validation; profile growth
   before setting reviewed full-data limits. Sample counts are not estimates for HI.
2. Review/package a new full release and configure the reusable job; no release
   assembler extension or cloud execution was performed in this increment.
3. Add an explicit publication adapter after generation returns: verify run and
   batch receipts, stage/copy a version, replace only the selected dataset,
   handle zero/stale owner dates, update/reconcile Catalog and preserve recovery
   references under reader/writer coordination. The approved production root is
   `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/`.
4. Complete independent Athena schema/count/value/lineage acceptance for HI and LI.
   Publication/validation teammate availability is unconfirmed; no completion is
   assumed. Role 6 owns lift/targeting/LI robustness, Sonia owns shared Step Functions.
5. Only then wire the integrated job's success to Role 6. Generation-only success
   is insufficient for the final pipeline contract.

Cost-conscious starting proposal, **not measured sizing**: use a coordinated
2-5 G.1X development profile (5 for the first bounded full-data assessment), zero
retries, no autoscaling and a reviewed service timeout. Source validation and
global uniqueness themselves scan/shuffle millions of rows. Confirm budget and
disk headroom; never claim a timeout is a guaranteed billing cap. A complete
HI/LI run's runtime and output growth remain unknown until measured.
