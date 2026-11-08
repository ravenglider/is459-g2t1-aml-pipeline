# Local validation preparation results — 8 November 2026

Branch: `role5/full-validation`, created without tracking from
`origin/role5/full-hi-li-generation` at `486f57a`.
Latest `origin/main` at `cbd7863` was merged into this branch on the user's
request, creating local synchronization commit `f1f802d`. It preserved the
full-generation/publication implementation; compared with the owner's branch,
no tracked `bq2/sequences/` file changed. Main's incoming changes were Role 4
visualisations and Role 6 sample/dashboard evidence, not new full HI/LI acceptance.

All additions are isolated under `bq2/sequences/validation/`. No generation,
staging, publication, packaging, agreed schema/rules, Role 6 SQL or shared
Step Functions files were edited by this validation work. After the local
main-to-validation synchronization merge, the user authorised committing and
pushing the preparation pack to `role5/full-validation` for owner review only.
No commit/merge into main, AWS API call, Glue job, production table creation or
dataset modification was performed.

## Prepared files

| Files | Purpose |
|---|---|
| `00_schema.sql`, `01_partitions.sql` | Exact 21-column contract and 55 registered owner dates, including empty dates |
| `02_source.sql` | Rendered HI/LI source baselines, uniqueness, required fields, timestamps and daily coverage |
| `03_sequences.sql` | Daily pair/triple counts, array alignment, SHA-256 identity, labels, timing, strings and duplicate chains |
| `04_lineage.sql` | Every member's source existence/attributes and adjacent bank/account/time continuity |
| `05_path_inventory.sql` | Observed Parquet version/date paths for receipt reconciliation |
| `render_acceptance.py` | Local, allowlisted HI/LI SQL rendering; no execution |
| `review_run.py` | Local downloaded-manifest/publication/Catalog evidence binding checks |
| `tests/test_acceptance_preparation.py` | Focused schema/renderer and fail-closed evidence tests |
| `RUN_EVIDENCE.md`, `SONIA_HANDOFF.md`, `README.md` | Acceptance gates, source-population/boundary evidence and integration handoff template |

## Checks performed

- **20 focused local tests passed.** Includes both datasets, explicit zero-output
  dates, missing/duplicate/reversed Catalog dates, mixed run locations, changed
  source/manifest bindings, inconsistent counts, boolean-count rejection and
  unverified publication rejection. Schema names/types/order and source baselines
  are checked against the existing implementation without importing Spark.
- **18 SQL statements parsed** for Trino using SQLGlot 30.30.0: shared schema and
  partition queries plus every rendered HI/LI statement. Parser downloaded into
  a temporary directory with wheel SHA-256 verified against PyPI metadata;
  no project dependency or generator environment was changed.
- Read-only SQL review: SELECT/WITH only; no DDL, CTAS, repair, inserts or deletes.
- File/link/whitespace review and original-component preservation checked locally.

Reproduce focused tests:

```text
python -m unittest discover -s bq2/sequences/validation/tests -v
```

SQL parser acceptance checks grammar, not Athena engine availability, connector
behaviour, function type resolution or actual data. The new SQL has **not** been
executed in Athena. Local fixtures are synthetic, not production evidence.

## Open inputs and boundaries

1. Await authorised HI/LI runs and their exact source snapshots, deployed-release
   hashes, Glue metadata, generation manifests, verified publication receipts
   and complete paginated Catalog exports. All live acceptance remains NOT RUN.
2. Source baselines are cleaned transactions, not expected sequences. Reconcile
   exact pair/triple counts per date and run against selected receipts and
   generator completeness evidence. SQL consistency alone does not independently
   enumerate every eligible chain.
3. Full source and output freezes must cover the validation window. Counts do
   not establish snapshot identity; inventory and metadata fingerprints bind it.
4. Final two owner dates have bounded future coverage. Preserve all 28 HI and
   27 LI owner dates and full-source Role 6 denominators.
5. Actual workers, growth, runtime/cost and integration success remain unmeasured.
   Do not fill the handoff with proposed sizing or sample timings as full results.
6. The teammate's **387 local tests are reported prior results**, not rerun or
   recertified here. The available local environment did not provide a usable
   WSL/PySpark runtime; the added 20 tests use the existing local Python runtime.
7. Older README/schema status paragraphs predate the new production code.
   This pack follows `FULL_GENERATION.md`, `FULL_PUBLICATION.md` and the current
   implementation; resolving historical wording is left to the generation owner.

Review the published validation branch with the owner before integration into main.
Sonia retains ownership of deployment and shared orchestration.
