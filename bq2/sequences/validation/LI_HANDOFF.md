# LI continuation handoff for the generation owner

**Prepared only. No LI AWS job, paid query, deployment or production update has
been performed.** The generation owner can pick up LI after HI using the same
tested bounded generator. This handoff does not create another algorithm.

Review branch: `role5/full-validation`. Shared branch inspected through
`6b50da4`. That commit has HI growth profilers and a full-source fingerprint
helper, but **not the new bounded chunked generator**. Read the blockers below
before attempting any run.

## 1. Fixed LI configuration

| Setting | Value / evidence requirement |
|---|---|
| Dataset / region | `LI-Medium` / `us-east-1` |
| Source table | `is459_g2t1_aml.transactions_clean` |
| Source root | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/dataset=LI-Medium/` |
| Owner dates | Every date September 1–27, 2022; 27 owners, both lengths |
| Reported source counts | 31,251,469 transactions; 16,041 positive labels |
| Output contract | Same v1 21 logical columns, including dataset/start_date partition keys |
| Link rule | Exact receiving/sending bank AND account; strictly later, at most 24h per adjacent link |
| Triple span | Up to 48h; no extra whole-chain 24h filter |
| Retention | All valid branches, same-bank, legitimate/mixed labels and repeated methods; no $20M cutoff or support filter |
| Identity | Signed bigint IDs, distinct members, unique ordered chains and canonical v1 SHA-256 |
| Staging | Isolated immutable LI run/attempt/chunk paths; never HI, sample or production locations |
| Existing staging-root contract | `s3://is459-g2t1-aml-processed-bucket/bq2_sequences_staging/full/` — reconfirm for the bounded release |
| Production publication | Not authorised by this handoff; do not run or modify it |

The source counts are transaction denominators, **not expected pair/triple
counts**. Fresh snapshot/count reconciliation is still NOT RUN. September 26
can only continue through September 27; September 27 cannot access September
28–29. Record unavailable future dates as extract boundaries, not empty data.

## 2. Fresh LI source fingerprints

| Required value | Actual value now |
|---|---|
| `source_fingerprint` | PENDING |
| `table_fingerprint` | PENDING |
| `partitions_fingerprint` | PENDING |
| Full LI inventory and UTC/source-freeze evidence | PENDING |

Do not use HI/sample hashes or invent values. The existing shared helper accepts
LI and performs two metadata inspections. It does not verify transaction counts
or lock the source. Use a reviewed shared-code checkout containing the helper;
it is not copied into this validation branch.

**Future metadata-read command, after coordination; not executed here:**

```bash
ROLE5_SHARED_CHECKOUT='<absolute path to your reviewed shared-code checkout>'
ROLE5_EXPECTED_ACCOUNT_ID='<verified 12-digit group account>'
ROLE5_LI_EVIDENCE="$HOME/role5-li-evidence-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$ROLE5_LI_EVIDENCE"
python3 "$ROLE5_SHARED_CHECKOUT/bq2/sequences/cloudshell_full_source_fingerprints.py" \
  --expected-account-id "$ROLE5_EXPECTED_ACCOUNT_ID" \
  --region us-east-1 --dataset LI-Medium \
  > "$ROLE5_LI_EVIDENCE/source-fingerprints.json"
```

Inspect the exit status and require `MATCHED_METADATA_ONLY`, LI-Medium and
27 available dates under the reviewed helper's current contract. Preserve the
complete inventories and source Catalog metadata, not just the three hashes.
After a refresh, obtain new evidence and fresh run IDs; do not reuse these values.

Render the actual source-row checks locally:

```text
python3 bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 02_source.sql
```

Execute its two SELECTs in Athena only with an agreed paid-query window/budget
and frozen source. Expect the reported row/label baselines, all 27 dates and
zero key/label/field/date/precision errors. Reconcile per-date counts to the
generator's final selected snapshot. Matching totals alone are not identity.

## 3. Glue arguments and release worksheet

Fill [LI_ARGUMENTS.template.json](LI_ARGUMENTS.template.json) with evidence.
It is intentionally **not an AWS CLI request**, and activation flags are false.
The existing integrated-interface fields are a reference only; map them to the
tested bounded staging-only interface before use.

| Arguments / configuration | LI setting or missing input |
|---|---|
| `--DATASET`, `--REGION` | `LI-Medium`, `us-east-1` |
| `--EXPECTED_ACCOUNT_ID` | Actual verified group account |
| `--RUN_ID`, `--ATTEMPT_ID`, `--OWNER_TOKEN` | Fresh safe LI identifiers; never reuse HI or failed attempts |
| `--BUILDER_VERSION`, `--BUILDER_FINGERPRINT` | Exact tested bounded release and uploaded application ZIP hash |
| Source/table/partition fingerprint arguments | Fresh three LI values from the bound source evidence |
| Timezone / no-refresh arguments | Actual UTC evidence and current source-freeze acknowledgment |
| Pair candidate/link, projected triple, per-middle and output bounds | LI measurements plus agreed chunk interpretation; no guessed HI limits |
| Records per file / cooperative runtime | Reviewed file sizing and elapsed budget |
| New chunk size / chunk ownership arguments | **PENDING shared bounded CLI; do not invent names or defaults** |
| Staging-only selection / no-publication behaviour | **PENDING tested shared interface and proof of no production writes** |
| Job name / entrypoint / library and SDK URIs | Actual deployed configuration and byte hashes, reviewed after HI |
| Worker type/count / timeout / concurrency / retries | Agreed LI capacity, combined resource budget and approved run window |

Do not copy the 496-million HI probe into an LI bound. LI has fewer positives
but generation uses all transactions, so it may also grow substantially.
Require measured LI candidates/valid links/triples and per-key/chunk skew under
the same rules. Do not trim dates, branches or time windows to fit limits.

**No staging-start command is supplied yet:** the shared bounded entrypoint and
its exact arguments are not available in the fetched release. The owner must
record the verified command and release in the worksheet once implemented.
The old `glue_full_generation.py` automatically publishes and rejects disabled
publication flags; it must not be used as an assumed staging-only job.

## 4. Staging execution instructions once the release is ready

1. Finish/review HI and use the same tested shared bounded algorithm for LI.
   Verify LI support and the unchanged 21-column/rule contract in its tests.
2. Review actual job script/library hashes and per-run arguments. Do not change
   a shared job's defaults underneath HI or use the old auto-publication entrypoint.
3. Confirm fresh LI fingerprints/counts and source freeze; review limits, workers,
   service/cooperative timeout and AWS budget. Obtain explicit LI run approval.
4. Verify staging-only mode cannot write HI/sample/production files or Catalog
   entries. Confirm a fresh LI prefix, ownership, chunk/receipt protocol and
   no automatic publication or old-attempt takeover.
5. Prefer HI then LI. Overlap only if isolated staging, concurrency/quota,
   temp/shuffle resources and combined cost are explicitly agreed. Different
   run IDs alone do not establish safe concurrency.
6. The generation owner runs the **verified staging-only command**. Record the
   exact command, actual job and application IDs, UTC start/end and resources.
7. Keep native Spark/Parquet read-back validators enabled for every chunk/date.
   Reconcile all selected chunks into each owner date with no lost/duplicate chains.
8. On failure, retain evidence and partial immutable outputs; stop for review.
   Do not delete files, blindly retry, relax rules or publish partial output.
9. Finish the staging evidence table below. Leave production publication and
   production reader-pause coordination to a later explicit decision.

Sonia's no-Step-Functions reply is a non-overlap confirmation, not LI budget/run
approval. Xiang Ying's production pause is conditional on a later agreed window.
Truly isolated staging does not itself require production readers to pause, but
this isolation must first be verified in the new shared implementation.

## 5. Staging validation commands and acceptance boundaries

Read-only evidence collection **after the approved LI run**, using its actual
job name and JobRunId; the following commands do not start a job:

```bash
ROLE5_LI_JOB_NAME='<actual approved LI staging job name>'
ROLE5_LI_GLUE_RUN_ID='<actual LI jr_... run ID>'
aws glue get-job-run --job-name "$ROLE5_LI_JOB_NAME" \
  --run-id "$ROLE5_LI_GLUE_RUN_ID" --region us-east-1 --output json \
  > "$ROLE5_LI_EVIDENCE/job-run-private.json"
```

Download the actual run/chunk manifests from the owner's confirmed isolated LI
staging prefix using read-only `aws s3 cp <actual-staging-json-uri> <local-file>`.
Use the bounded generator's exact manifest layout; do not guess that a new
chunked layout matches the older one. Keep original bytes, checksums and logs.

| Staging check | Pass requirement |
|---|---|
| Source identity/counts | All three hashes and baseline/date evidence match the actual LI job |
| Owner/chunk coverage | All 27 owners, explicit empty outputs and complete selected chunk ownership |
| Pair/triple counts | Every chunk/owner/run total reconciles; no branch truncation |
| Sequence identities | Correct v1 hashes, distinct members and no duplicate ordered chains across chunks |
| Schema, arrays and strings | Exact 21-column logical schema and aligned values; correct physical schema/compression |
| Original/derived labels | Exact member labels and correct any/all/final semantics |
| Lineage | Members exist in frozen LI source; exact bank/account/time links; gaps >0 and <=24h |
| Storage/read-back | Shared validator passes complete staged Parquet and exact inventoried files |
| Terminal evidence | All required staging chunks/dates validated; no failed/incomplete attempt accepted |
| Publication state | No production Catalog updates or publication; staging is not PUBLISHED |

The required chunk-aware staging validator/receipt command must come with the
shared bounded release. This workstream does not invent a different validator
or alter the owner's generation code. `review_run.py` requires a production
PUBLISHED receipt and is **not a staging validator**.

Production Athena queries are ready, but **must wait for a separately approved
future LI publication**. They cannot query unpublished staging automatically:

```text
python3 bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 01_dataset_partitions.sql
python3 bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 03_sequences.sql
python3 bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 05_path_inventory.sql
python3 bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 04_lineage.sql
```

Alternatively, render lineage for every owner with `--owner-date YYYY-MM-DD`.
After publication, all 27 dates must pass; member rows reconcile to 2*pairs +
3*triples. See [CloudShell commands](CLOUDSHELL_COMMANDS.md) for coordinated
submission/polling/results, and [results checklist](RESULTS_CHECKLIST.md) for
all dates. Do not create a temporary table or publish data just to run these checks.

## 6. Evidence to hand onward

| Field | Actual LI value to fill |
|---|---|
| Tested release / hashes / exact command | PENDING |
| Glue job / JobRunId / run / attempt / chunks | PENDING |
| Three source fingerprints and inventory references | PENDING |
| Reconciled source rows / positives / 27 dates | PENDING |
| Staging run/chunk manifest URIs and checksums | PENDING |
| Pairs / triples per chunk, owner and full run | PENDING |
| All 27 owner statuses, including zero outputs | PENDING |
| Native validation results and exceptions | PENDING |
| Workers / runtime / DPU / growth / cost evidence | PENDING |
| Production publication | NOT REQUESTED / NOT PERFORMED |

## Remaining blockers and pickup order

1. Tested shared bounded generator and actual staging-only CLI.
2. Confirmed chunk/manifest/read-back validator protocol and release artifacts.
3. Fresh LI inventories/fingerprints, UTC and source-count reconciliation.
4. Evidence-based LI limits, worker/time/budget agreement and explicit run approval.
5. Actual LI staging execution and complete staging validation evidence.
6. Later production publication and Athena acceptance require separate approval;
   neither is implied by completing staging.

Local preparation is ready; executable generation commands, actual fingerprints,
limits and successful staging results remain pending. No estimated sequence count,
runtime, hidden approval or test success is substituted for those missing inputs.
