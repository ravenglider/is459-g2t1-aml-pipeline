# Ready-to-run HI/LI acceptance instructions

Prepared 8 November 2026 on `role5/full-validation`. Live acceptance is **NOT RUN**.
All commands below render local SQL; none submit queries, run Glue or change data.
Execute the read-only checks in Athena only after coordinating AWS access and a
stable source/output window with the owners. Use engine 3, database
`is459_g2t1_aml` and the agreed workgroup; record the actual engine/workgroup.

## What is ready now

| Availability | Checks that can start | Gate |
|---|---|---|
| Frozen cleaned source, even before output exists | `02_source.sql` for each dataset; source metadata reconciliation | Authorised read-only access and exact job snapshot evidence |
| Full HI publication available, LI pending | Schema, HI partitions, HI counts/values/IDs, HI paths and full HI lineage | Verified HI publication; no concurrent table switch |
| Full LI publication available | The same checks for LI | Verified LI publication; no concurrent table switch |
| Both full publications available | Shared `01_partitions.sql`, final reconciliation and Role 6 release | 55 dates, accepted run evidence for both; no unresolved recovery |
| Only a probe, staged batch or partly switched table | Record diagnostics only | Do not label it full production acceptance or expose it to Role 6 |

Do not wait for LI to prepare or validate a completed HI publication. However,
publication and recovery use a table-wide maintenance pause: do not query while
either dataset's Catalog partitions are being switched.

## 1. Obtain run identity and freeze the acceptance window

Ask the generation owner for the dataset, Glue JobRunId, run/attempt IDs,
release hashes, generation manifest, PUBLISHED receipt, full source inventory
and all three source fingerprints. Obtain complete paginated GetTable/
GetPartitions exports and copied-file inventory, including empty dates.

The owner's latest branch `06033b8` adds
`cloudshell_full_source_fingerprints.py`. Its `MATCHED_METADATA_ONLY` result
checks repeated inventories, not transaction counts or immutable snapshots.
Bind its evidence to the final job; do not run it or refresh fingerprints
silently on the owner's behalf. An identical count is not snapshot identity.

Stop if publication is incomplete, metadata changes during checks, receipts
refer to another run, or the source differs from the job. No repair/CTAS/retry
is part of this acceptance procedure.

## 2. Check schema, selected-date coverage and source population

Run the single statement in `00_schema.sql`: expect **zero mismatch rows**.

Render the selected dataset's statements locally; copy one SQL statement at a
time into Athena. These examples use HI; repeat with `LI-Medium` for LI.

```text
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 01_dataset_partitions.sql
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 02_source.sql
```

- Partitions: **28 HI or 27 LI PASS rows**, including zero-output dates; no
  missing/extra/duplicate selected dates. Verify actual Catalog locations against
  the receipt; SQL row scans cannot prove empty-date physical locations.
- Source totals: match the table below; all invalid-field/ID/label/date/precision
  counters zero. Reconcile per-date source counts with the generation manifest.
- Neither source query applies the BQ1 $20M cutoff or filters to laundering rows.

| Dataset | All cleaned transactions | Positive labels | Expected dates |
|---|---:|---:|---|
| HI-Medium | 31,898,218 | 35,230 | September 1–28, 2022 |
| LI-Medium | 31,251,469 | 16,041 | September 1–27, 2022 |

These are reported source baselines, **not sequence counts**. A changed final
snapshot requires reconciliation with the owners, not a silent baseline edit.

## 3. Check output counts, structure and actual version paths

```text
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 03_sequences.sql
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 05_path_inventory.sql
```

`03_sequences.sql` contains **four statements**; run separately in this order:

1. Daily pair/triple counts: **56 HI / 54 LI date-length rows**. Compare every
   count to its selected batch receipt and the summed run totals. Zero-output
   dates must have explicit receipts; no fixed full sequence total is assumed.
2. Structure, IDs, strings, labels and timing: every `bad_*` counter zero.
   Empty output is not accepted merely because its error counters are zero.
3. Duplicate sequence IDs and ordered chains: zero returned rows.
4. Pair/triple label distributions: retain as descriptive evidence, without
   inventing an expected prevalence or dropping legitimate/mixed chains.

Path inventory: all `bad_path_shape=false`; each actual path matches the exact
published run/attempt/date. Cross-check the complete object inventory and
Catalog locations, including empty dates. Keep old versions; do not crawl the root.

## 4. Check every member and adjacent link

Run lineage only after schema, source uniqueness, output uniqueness and value
checks pass. It can be expensive; record query statistics and use the agreed
scan/time budget. Either run once for the full dataset:

```text
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 04_lineage.sql
```

Or render a chunk for each owner date:

```text
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 04_lineage.sql --owner-date 2022-09-01
python bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 04_lineage.sql --owner-date 2022-09-27
```

Each chunk checks all sequences owned by that date and reads source dates
D through min(D+2, extract end). This changes scan scope, not sequence rules.
For chunked acceptance, retain **28 HI + 27 LI** successful query IDs/results;
do not accept a few selected dates as full lineage validation.

- Every `bad_*` count must be zero.
- Per-scope member rows must equal `2*pairs + 3*triples` from the count check.
- Chunk sums must equal the dataset totals, with exactly one result per owner date.
- Every member has exactly one matching source transaction and identical
  timestamp/method/currencies/original label. Adjacent transfers match both
  bank and account, with `0 < gap <= 24h`.
- Triples may span 48h. The last two owner dates have extract-boundary limits:
  HI September 27–28 and LI September 26–27. No absent future data is invented.

SQL errors, timeouts or missing chunk results are incomplete validation, not passes.

## 5. Close the evidence and handoff

After both datasets pass, run shared `01_partitions.sql`: exactly **55 PASS dates**.
Recheck source/Catalog inventories across the acceptance window and bind every
query result to the correct publication. Complete [RUN_EVIDENCE.md](RUN_EVIDENCE.md),
[SONIA_HANDOFF.md](SONIA_HANDOFF.md) and the Role 6 fields below.

| To Xiang Ying | Fill with actual accepted evidence |
|---|---|
| Input | `is459_g2t1_aml.bq2_sequences`, 21-column v1 |
| Publication identity | HI and LI run/attempt IDs, receipts and exact selected partition locations |
| Counts | Pair/triple totals and full daily count results |
| Denominators | Reconciled all-source/positive totals above, original labels and snapshot references |
| Completeness | All 28/27 owners, no branch/support/label/amount truncation, recorded boundary limits |
| Acceptance | Query IDs/results; zero value/identity/lineage defects; chunk reconciliation if used |
| Analytical reminder | Separate lengths; any-member labels for descriptive lift; original distinct-final labels for targeting; freeze HI patterns before LI |

Only then mark the applicable dataset ACCEPTED. Final full HI/LI readiness
requires both. A green Glue status or candidate-growth probe alone is insufficient.

## Reported performance probe

The owner reported approximately **496 million potential pair candidates for
the first HI owner date**. Retain the probe JobRunId, date/window, exact metric
name, value, release and source fingerprints. Its exact definition and whether
it is a conservative bound must be confirmed from probe evidence. It is not an
accepted pair count, triple count or prediction of final runtime/cost.

Generation optimisation and worker/limit decisions remain with the owner. Do
not raise bounds, sample branches, shorten the 24h window, drop dates/labels or
change schema to force acceptance. If an optimisation changes semantics or
coverage, stop and reconcile the contract before interpreting its outputs.
