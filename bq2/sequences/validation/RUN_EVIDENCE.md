# Final HI/LI run evidence checklist

Live status: **NOT RUN / NOT ACCEPTED**. Complete separately for HI and LI.
Preparation does not authorise deployment, Glue runs, CTAS, repair or data changes.

## Before the authorised run (generation owner / Sonia)

- [ ] Record source branch/commit, entry-script hash, 18-module ZIP hash and
  SDK bundle hash; verify deployed bytes, not just filenames.
- [ ] Confirm job name `is459-g2t1-bq2-sequence-builder`, entrypoint
  `glue_full_generation.py`, exact arguments and current AWS job definition.
- [ ] Record account, region, UTC evidence, no-refresh agreement, measured
  growth limits and reader-pause agreement. Never fabricate or reuse acknowledgments.
- [ ] Capture complete paginated source S3 inventory and GetTable/GetPartitions
  metadata for the selected dataset, matching all three job fingerprints.
  Include object keys, sizes, ETags/version IDs where available and schema.
- [ ] Record workers/type, Glue version, SDK/runtime evidence, service timeout,
  cooperative budget, zero retries, concurrency one, bookmarks disabled,
  temp location and current permissions. Proposed sizing is not measured sizing.

## Per-dataset run evidence

- [ ] JobRunId, application run/attempt IDs, owner reference, UTC start/end,
  Glue SUCCEEDED, actual workers, ExecutionTime/DPUSeconds where available,
  complete stdout/error and CloudWatch links.
- [ ] Immutable generation manifest: `STAGED_VALIDATED`, `published=false`,
  exact 21-column schema, rule v1, release/source bindings and 28/27 batches.
- [ ] Source validation: exact cleaned baselines, row/label counts per date,
  full selected-snapshot identity checks and explicit verification scope.
  Do not upgrade `provided_dataframe_only` into proof about unseen data.
- [ ] Every owner batch: continuation dates, boundary/empty evidence, candidate
  and expansion metrics, limits, exact pair/triple counts, read-back validation,
  file inventory, staging receipt URI/fingerprint; include zero-output dates.
- [ ] Separate publication receipt: `PUBLISHED`, `published=true`,
  `catalog_updated=true`, `verified=true`, same run/attempt/source; verified
  copies, Catalog reconciliation and ownership release. The staging manifest
  intentionally remains staging-only and must not be relabelled PUBLISHED.
- [ ] Table/schema export: 19 Parquet columns plus string partition keys
  dataset then start_date. Retain source and published schema/compression proof.
- [ ] All 28 HI or 27 LI Catalog dates point to one accepted immutable version,
  including empty dates. No `MSCK REPAIR`, crawler or table-root recursive scan.
- [ ] Other dataset's Catalog definitions and files remain unchanged.
  Compare before/after inventories; validate that dataset separately after its run.

## Independent read-only acceptance

- [ ] No source refresh, Catalog publication or outside writer overlaps checks;
  collect final fingerprints/metadata to bind all queries to the job snapshots.
- [ ] `00_schema.sql`: zero mismatch rows.
- [ ] `01_partitions.sql`: exactly 55 PASS dates after both publications;
  if only one is published, full HI/LI acceptance remains incomplete.
- [ ] Rendered `02_source.sql` for HI and LI: exact baseline matches,
  28/27 source dates, zero invalid keys/labels/fields/dates; compare per-date counts
  to manifest `source_validation.rows_by_date`, including attested empty dates.
- [ ] Rendered `03_sequences.sql`: expected 56 HI / 54 LI date-length rows;
  all counts reconcile with batch/run receipts; all bad counters zero;
  no duplicate sequence IDs or ordered chains; retain label distributions.
- [ ] Rendered `04_lineage.sql`: member count equals 2*pairs + 3*triples;
  zero missing/duplicate source matches, changed attributes or bank/account/time links.
- [ ] Rendered `05_path_inventory.sql`: no bad path shapes; every actual path
  belongs to the exact accepted version and date; compare object inventory and
  Glue locations independently for zero-row dates and stale-file exclusion.
- [ ] Record all query IDs, SQL versions/hashes, engine/workgroup, timestamps,
  result summaries, bytes scanned and cost/runtime evidence where available.
- [ ] Retain generator completeness evidence: all owners, branches and lengths,
  exact projections/output counts, no truncation/filtering and boundary limits.
  These checks do not independently regenerate every eligible full-data chain.
- [ ] Investigate any difference; keep status NOT ACCEPTED until resolved.

Local binding review (downloaded JSON only; no AWS client):

```powershell
python bq2/sequences/validation/review_run.py --manifest path/to/run.json --publication path/to/publication.json --partitions path/to/partitions.json
```

`partitions.json` is one array combining **every page** of the GetPartitions
export. The local review checks run/source/count/location bindings, including
empty dates. Its success is not Athena acceptance, physical file verification,
lock verification or proof of runtime success. Keep sensitive arguments and raw
evidence outside Git; share redacted summaries and evidence references.

## Role 6 release gate

- [ ] Supply accepted production table, source snapshot references, full-source
  denominators, per-date/length sequence counts, extract endpoints, validation
  summaries and publication IDs for both datasets.
- [ ] Explicitly state boundary truncation for final two owner dates and that
  chains are account-linked activity, not proof of identical funds.
- [ ] Separate any-member descriptive labels from original final labels; do not
  divide by sequence counts for transaction catch/review denominators.
- [ ] Role 6 confirms receipt; HI pattern selections are frozen before LI testing.
- [ ] Coordinate reader release only after final acceptance of the applicable
  publication and no unresolved recovery/partial Catalog switch.
