# Full HI/LI production publication and recovery

> **Not used in production (10 November 2026):** this Glue design was replaced by the Athena build in [`athena/`](athena/README.md), which builds `bq2_sequences` inside the Step Functions pipeline. Kept for reference.

Local implementation only. No cloud deployment, permissions or full-data
performance certification is implied. Sample `bq2_sequences_dev`, sample staging
and existing publication-planning modules are not changed.

## Fixed production contract

- Database/table: `is459_g2t1_aml.bq2_sequences`.
- Exact 19 Parquet data columns from `SEQUENCE_SCHEMA`, plus ordered string
  partition keys **dataset, start_date**. These are 28 HI or 27 LI partitions,
  not one single-key dataset partition. Keeping only dataset would violate v1.
- Dataset values: exactly `HI-Medium` or `LI-Medium`.
- Table location: `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/`.
- Immutable version location:

```text
bq2_sequences/dataset=<dataset>/versions/<run_id>/<attempt_id>/
  data/dataset=<dataset>/start_date=<YYYY-MM-DD>/*.parquet
```

The repeated dataset segment is intentional: the outer prefix isolates writers;
the inner `data/` basePath preserves both partition fields in the existing
Spark read-back transport. Catalog partitions point explicitly to the innermost
date location. Never crawl or run MSCK REPAIR over these version trees.

Publication metadata is separate:

```text
bq2_sequences_metadata/production/table-lock.json
bq2_sequences_metadata/production/dataset=<dataset>/<run_id>/<attempt_id>/publication.json
```

## Normal execution

`glue_full_generation.py` uses one reusable job named
`is459-g2t1-bq2-sequence-builder`. Job naming here is a configuration contract,
not evidence that the current AWS job has this entrypoint. Use a separately
reviewed full release and the 18-module application ZIP; keep SDK wheels separate.

1. Strictly validate all arguments before clients or Spark start. Keep sample
   scope and arbitrary path overrides forbidden. Validate SDK versions/origins,
   conditional PutObject support, expected account and us-east-1.
2. Generate all selected dataset dates using the existing tested workflow.
3. Read the durable immutable generation manifest and every selected batch
   receipt. Check kind/state, owner/source/builder/schema, totals, complete date
   coverage, exact S3 file inventory, required fields and source identity evidence.
   Independently validate staged nonempty Parquet in Spark. For explicit empty
   batches, exact zero counts plus a file-free inventory prove there are no rows.
4. Obtain table-wide S3 CAS ownership. Existing active ownership is never
   expired, deleted or taken over. Inspect and bind existing table/partitions and
   previous selected-dataset object inventories into the publication journal.
5. Copy only inventoried Parquet using conditional source GetObject, destination
   PutObject `IfNoneMatch=*`, AES256 and SHA-256 upload checksum. Enforce the
   explicit per-file memory bound. Read every copy back and compare SHA-256.
   ETags are change tokens, not universal content hashes. No copy-object fallback,
   unconditional overwrite, old-data deletion or blind retry is implemented.
6. Independently validate the copied physical schema, Snappy codec, exact logical
   21-column schema, ownership, counts, IDs, arrays, labels and timing. Recheck
   staged receipts, source snapshots and destination inventory before switching.
7. Create the compatible external production table only if absent. Do not alter
   an incompatible existing table. Before each selected-date create/update/delete,
   compare current state with expected old/intended new and persist its intent.
   Register all agreed dates, including zero-output dates at fresh empty locations.
   Delete only selected-dataset stale Catalog dates outside the complete new
   coverage; never delete their files. Other dataset definitions/files stay intact.
8. Verify each operation by Catalog read-back. Reconcile exact selected coverage,
   unchanged other-dataset state, stable table and source/destination fingerprints.
   Persist and reread a separate PUBLISHED receipt, release ownership conditionally,
   and verify completion before returning success. Only then call Glue job.commit
   and emit the PUBLISHED report. Every error propagates; job exit is not evidence
   unless the durable verified publication receipt also exists.

The immutable generation manifest remains `STAGED_VALIDATED`, `published=false`.
The separate publication receipt records generation binding, previous definitions
and inventories, copied file SHA-256/size, action intent/observations, source and
Catalog reconciliation and verification time. Prior locations are recovery
references, not backups against external deletion.

## Required publication arguments

In addition to [full generation arguments](FULL_GENERATION.md):

| Argument | Requirement |
|---|---|
| `--ALLOW_PUBLICATION` | Literal `true`; disabled by default |
| `--READERS_PAUSED` | Literal `true`; disabled by default |
| `--PUBLICATION_MAINTENANCE_EVIDENCE` | Actual table-owner/Role 6 reader-pause agreement reference |
| `--MAX_COPY_BYTES` | Explicit positive per-file transfer memory bound, reviewed against worker memory/file sizes |
| `--RESUME_PUBLICATION` | Default `false`; set `true` only after reviewing a failed SAME-attempt publication |

Do not put permanent true activation/pause acknowledgments in job defaults.
Fresh normal reruns require new run/attempt/owner IDs and new frozen-source
evidence; they preserve old versions while replacing only the selected dataset's
Catalog partitions. Step Functions must not blindly retry failed jobs or invent
reader/no-refresh acknowledgments. Source fingerprints are dataset-specific.

The cooperative elapsed budget is shared across initialization/generation and
publication; actual Glue Timeout must also be configured. It cannot interrupt
an already running Spark action. Full-data growth limits remain explicit and
unmeasured until a separately approved profiling/assessment establishes them.

## Failures and explicit recovery

Catalog changes are **not atomic across dates**. Role 6 must stop all table reads
and out-of-band writers must stay paused for publication and recovery. The S3
lock does not prevent a human or another implementation from mutating Glue.

- Before any Catalog change, a failed copy/validation leaves old publication
  untouched. Retain partial new files and the journal; do not delete old outputs.
- During Catalog switching, failure can leave a mixture of old/new selected-date
  locations. The receipt becomes RECOVERY_REQUIRED where safely recordable, the
  lock remains owned, and the job fails. Keep readers paused.
- Resume via the SAME job and configuration/IDs with `--RESUME_PUBLICATION=true`.
  It loads the existing generation manifest instead of generating again. It
  requires original ownership, matching generation/source evidence and reviewed
  maintenance acknowledgment. It accepts existing copy bytes only when their
  SHA-256 matches; it never overwrites them.
- If generation completed but publication never started, explicit recovery can
  begin publication from that handoff only when there is no active table owner,
  no version objects, no publication receipt and no Catalog references to that
  version. This avoids repeating an expensive successful generation scan.
- Each Catalog operation is classified as expected old, intended new or conflict.
  Old is executed once with durable intent; new is verified without replaying the
  mutation; conflict stops. Lost responses can therefore be reconciled explicitly.
  No automatic retry, takeover or rollback execution exists.
- A PUBLISHED receipt is terminal and immutable. Explicit same-owner recovery can
  reverify it and release its lock if the prior reply/release failed. If another
  publication has superseded it, the old attempt cannot take ownership back.
- If the receipt is absent with active ownership, ownership changed, source
  refreshed, or Catalog state is unexpected, stop for manual reconciliation.
  Do not guess a fresh snapshot or force an unlock. Saved previous definitions
  and inventories support an independently reviewed manual rollback if required.
- If recording failure also fails, preserve the original exception/traceback and
  add a note. Uncertain outcomes never produce automatic success.

## Before the first paid run

Verify the committed release, all artifact hashes/imports and reviewed SDK bundle;
fresh source fingerprints and global source readiness; measured growth limits;
effective S3 List/Get/Put and Glue Get/Create/Update/DeletePartition permissions;
SSE-S3 and Catalog schema; absence of conflicting publishers; actual reader pause;
worker disk/shuffle headroom; service timeout, zero retries and concurrency one.
Do not register `bq2_sequences/` or its publication metadata/version trees with
the shared reset-outputs Lambda: deleting those prefixes destroys recovery data.
The 5 G.1X / 30-minute assessment proposal is not measured full-run sizing or a
billing cap. Do not claim final AWS readiness from fake-client tests alone.

After the first authorized run, independently validate Athena schema, counts,
labels/IDs and lineage; record query IDs and measured runtime/cost. Sonia can
then integrate the job with `glue:startJobRun.sync` after source quality readiness
and gate Role 6 on verified publication, not on generation/staging alone.
