# Full HI/LI generation: local implementation and publication boundary

> **Not used in production (10 November 2026):** this Glue design was replaced by the Athena build in [`athena/`](athena/README.md), which builds `bq2_sequences` inside the Step Functions pipeline. Kept for reference.

The integrated entrypoint now generates, validates, copies an immutable production
version, updates the selected dataset's Catalog partitions and verifies publication.
All implementation testing is local: no production deployment, effective cloud
permissions, full-data runtime/growth or AWS publication success is claimed.
The accepted sample development workflow remains unchanged. See
[production publication and recovery](FULL_PUBLICATION.md) before deployment.

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
- `sequence_full_publication.py`: fixed production table/schema, conditional
  copies/receipts, table-wide publication ownership, selected-dataset Catalog
  actions, independent read-back and explicit recovery.
- `glue_full_generation.py`: reusable HI/LI CLI coordinating generation through
  independently verified publication, with runtime/SDK checks before writes.
- `package_full_generation.py`: separate deterministic flat 18-module ZIP.
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
--ALLOW_PUBLICATION                literal true; otherwise no runtime initialization
--READERS_PAUSED                   literal true; coordinated table-wide pause
--PUBLICATION_MAINTENANCE_EVIDENCE  owner/reader-pause coordination reference
--MAX_COPY_BYTES                   positive maximum per-file in-memory transfer size
--RESUME_PUBLICATION               optional false/true; explicit SAME-attempt recovery
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

## Deployment prerequisites still required

The single job now includes the publication/Catalog component. The generation
manifest deliberately remains staging-only and immutable; a separate verified
publication receipt gates the job's final PUBLISHED report and successful return.

1. Obtain fresh frozen HI/LI inventories and measured growth profiles before
   choosing limits. Never substitute sample thresholds or fabricated evidence.
2. Review a fresh committed release: separate `glue_full_generation.py`, the new
   18-module `role5_full_modules.zip`, and the approved SDK wheel bundle. The old
   sample release assembler is not a full-pipeline release verifier.
3. Verify effective S3/Glue permissions and configured SSE-S3, SDK bootstrap,
   timeout/concurrency/retry settings, and worker disk/shuffle capacity.
4. Coordinate Role 6's table-wide query pause and forbid out-of-band Catalog
   writers. The S3 lock coordinates this implementation's publishers only.
5. Execute independent Athena acceptance after the first authorized publication,
   retaining query IDs, runtime/cost evidence and prior versions for recovery.
   Teammate availability is not assumed. Sonia owns Step Functions integration;
   Role 6 owns analytics. The job must fail on any unverified publication.

Cost-conscious proposal, **not measured full-data sizing**: 5 G.1X workers for an
initial controlled assessment, concurrency one, zero retries, autoscaling off and
an explicitly reviewed 30-minute Glue service timeout. A 1,500-second cooperative
budget leaves some service-time headroom but cannot cancel an in-flight Spark
call or guarantee billing. Complete HI/LI runtime and output growth remain
unknown; review measurements/budget before extending any limit or timeout.
