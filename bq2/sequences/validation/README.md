# Full HI/LI acceptance preparation

Status: **local preparation only; live acceptance NOT RUN**. Starting point:
`role5/full-hi-li-generation` at `486f57a`. Working branch: `role5/full-validation`.
Generation, staging, publication, schema and sequence rules are unchanged.
Latest `main` (`cbd7863`) was subsequently merged locally at `f1f802d`.
The full Role 5 implementation from the starting branch is preserved. The user
authorised publishing this validation branch for owner review; integration into
main remains a separate coordinated step.

Start with [READY_TO_RUN.md](READY_TO_RUN.md) for the immediate run order,
per-dataset acceptance, date-by-date lineage option and Sonia/Role 6 handoff.
Copy-ready CLI examples are in [CLOUDSHELL_COMMANDS.md](CLOUDSHELL_COMMANDS.md).
Use [RESULTS_CHECKLIST.md](RESULTS_CHECKLIST.md) for all 55 owner dates and
[READER_PAUSE_AND_HANDOFF.md](READER_PAUSE_AND_HANDOFF.md) for the explicit
confirmation requests and final handoff message. The actual production pause
window remains unconfirmed and deferred until the generation owner is ready.

The generation owner can continue LI after HI using the consolidated
[LI_HANDOFF.md](LI_HANDOFF.md). Detailed preparation is in
[LI_WORKSTREAM.md](LI_WORKSTREAM.md), with a
[disabled argument worksheet](LI_ARGUMENTS.template.json). The newly requested
bounded generator/staging-only interface is not yet present in the reviewed
remote `6b50da4` release. No separate algorithm or AWS execution is introduced;
future LI staging requires the tested shared code and explicit run approval.
The reported 496-million-candidate probe is generation profiling, not an
expected accepted sequence count. No generation limit or rule is changed here.

## Scope agreed in the two prompts

The second prompt supersedes only the starting-branch instruction: start from
the full-generation branch, then work independently. Updating this validation
branch from main does not replace that implementation with main's older version.

| Work | Local preparation | Requires final-run evidence |
|---|---|---|
| 21-column contract and current code review | Complete | Actual deployed schema/read-back |
| HI/LI counts, IDs, labels, timing and lineage | SQL prepared and locally checked | Execute against authorised final outputs |
| 28 HI / 27 LI owner dates | Calendar, Catalog query and local binding tests prepared | Verify all registered locations and receipts |
| Role 6 denominators and boundaries | Reported baselines and limits documented | Reconcile job-specific source snapshots |
| Sonia handoff | Checklist and argument/output template prepared | Fill actual workers, runtime and successful acceptance |

Live acceptance is **not** complete. No AWS run, deployment, Catalog or dataset
modification is authorised by this preparation task. This validation branch may
be committed and pushed for review; do not merge it into main without coordination.

## Run order after an authorised production publication

1. Freeze source refreshes and sequence-table changes with the owners for the
   entire validation window. Retain the job's actual arguments, immutable
   generation manifest and separate publication receipt. Bind checks to those
   exact source fingerprints and published partition locations.
2. Use `00_schema.sql` and rendered `01_dataset_partitions.sql` in Athena engine 3
   for the selected publication. Use shared `01_partitions.sql` after both are
   ready. They check the schema and all 28 HI / 27 LI dates, including empty dates.
   Also retain Glue GetTable/GetPartitions exports for physical types, ordered
   partition keys and exact locations, including empty dates.
3. Render `02_source.sql`, `03_sequences.sql`, `04_lineage.sql` and
   `05_path_inventory.sql` separately for each dataset. Run one statement at a
   time in Athena, recording IDs, UTC execution times and scanned bytes.
4. Stop on any failed schema, identity or structure check before expensive
   lineage scans. Reconcile date/length counts with every selected batch receipt
   and run totals. Do not infer expected sequence counts from source counts.
5. Complete [the evidence checklist](RUN_EVIDENCE.md), then fill
   [the handoff template](SONIA_HANDOFF.md). Role 6 may consume only a fully
   accepted publication. An SQL success status alone does not mean its checks pass.

Local rendering example (no AWS calls):

```powershell
python bq2/sequences/validation/render_acceptance.py --dataset HI-Medium --query 03_sequences.sql
python bq2/sequences/validation/render_acceptance.py --dataset LI-Medium --query 04_lineage.sql
python -m unittest discover -s bq2/sequences/validation/tests -v
```

The renderer prints SQL to stdout; it does not execute it or change tables.
Both full populations are explicitly scoped; no BQ1 $20M cutoff, label-only,
cross-bank-only, support, sampling or whole-chain 24-hour filter is introduced.
Each adjacent gap is strictly positive and at most 24 hours; triples can span
48 hours. Preserve same-bank, mixed-label, repeated-method and branching paths.

## Acceptance interpretation

| Check | Required outcome |
|---|---|
| Schema | No mismatch rows; all 21 names, types and positions match |
| Catalog coverage | 55 expected dates; no missing, extra or duplicate registrations |
| Source | HI 31,898,218 / 35,230 positives; LI 31,251,469 / 16,041 positives, reconciled to job snapshots |
| Source keys | Zero null/duplicate IDs, bad dates, invalid required values or labels |
| Sequence counts | Exact agreement with manifest and per-date pair/triple receipts; zero dates explicit |
| Sequence integrity | Every `bad_*` counter zero; no duplicate IDs or ordered chains |
| Lineage | Exactly one source match per member; zero attribute or adjacent-link errors |
| Locations | Every registered partition and observed file belongs to the accepted run/attempt |
| Publication | Glue SUCCEEDED plus independently verified PUBLISHED receipt, stable metadata and completed checks |

Counts and consistency do not independently prove that every eligible chain was
generated. Retain exact candidate/output reconciliation and untruncated batch
evidence from the generator, then review bounded source-derived examples.
Do not claim an exhaustive independent regeneration from these SQL checks.

`$path` only exposes files containing rows. Catalog metadata, the publication
receipt and object inventories must verify empty locations and physical files.
Matching counts do not prove snapshot identity; the job's object, table and
partition fingerprints must reconcile before and after validation.

## Source coverage for Role 6

| Dataset | Owner dates (inclusive) | Source transactions | Source positives |
|---|---|---:|---:|
| HI-Medium | 2022-09-01 to 2022-09-28 | 31,898,218 | 35,230 |
| LI-Medium | 2022-09-01 to 2022-09-27 | 31,251,469 | 16,041 |

These are Amareet's reported cleaned-source baselines, also enforced by the
full-generation code; they are not observed sequence output counts. Owner date
D reads D/D+1/D+2 within the confirmed extract. The last two owner dates can
lack future continuation data: HI ends September 28; LI ends September 27.
Boundary dates remain in the denominator and output coverage, even if empty.
Timezone evidence must independently support upstream UTC semantics; setting a
Spark session timezone alone does not establish that evidence. Snapshot scope
does not certify unseen activity outside the extracts. Use original source
labels and all cleaned transactions as Role 6 denominators, not member counts.
Freeze HI patterns before LI evaluation; analyse lengths separately.

## References and limits

Contract: [schema](../schema_bq2_sequences.md), [rules](../sequence_rules.md),
[full generation](../FULL_GENERATION.md), [publication](../FULL_PUBLICATION.md).
SQL uses documented [Athena Catalog metadata](https://docs.aws.amazon.com/athena/latest/ug/querying-glue-catalog.html),
[partition metadata](https://docs.aws.amazon.com/athena/latest/ug/show-partitions.html)
and [Trino array functions](https://trino.io/docs/current/functions/array.html).
Local tests verify preparation and fixtures, not live Athena execution, Spark
publication, permissions, output completeness or production performance.
