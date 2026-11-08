# LI-Medium preparation reference and staging acceptance

Status: **PREPARATION ONLY — NO AWS RUN APPROVED OR STARTED**.
Work stays on `role5/full-validation`; the HI owner owns the shared generator.
Use the same tested implementation for LI. No alternative algorithm, profiler,
deployment, job change, sample-table change or production publication is made here.
The owner will continue LI after HI. Start with the consolidated
[LI_HANDOFF.md](LI_HANDOFF.md); this document retains the detailed preparation
reference. No further execution by the validation workstream is implied.

## Current implementation boundary

The owner's remote branch was reviewed through `6b50da4`, which adds the HI
exact-growth profiler. It also contains the earlier source-fingerprint helper.
The new bounded chunked generator described in the request is not present in
that fetched commit. Both existing growth probes are HI-specific; do not change
their dataset constants or assume they can profile LI.

The existing `glue_full_generation.py` requires publication activation and
generates through production publication in one invocation. Setting its
publication switch false is not a staging-only run; it fails validation before
initialisation. **Do not use that integrated entrypoint to try isolated LI
staging.** Wait for the shared bounded generator's tested entrypoint, explicit
staging-only interface, output protocol and reviewed release.

## LI contract to retain

| Item | Agreed value / interpretation |
|---|---|
| Dataset | `LI-Medium`, kept separate from HI and sample |
| Source | `is459_g2t1_aml.transactions_clean` |
| Owner dates | Every date September 1–27, 2022: 27 dates |
| Reported source baseline | 31,251,469 cleaned transactions; 16,041 positive labels |
| Output | Same approved 21-column v1 schema; 19 Parquet columns plus dataset/start_date |
| Pairs/triples | Two/three transactions; all valid branches and both lengths |
| Adjacent links | Same dataset, exact receiving/sending bank AND account; strictly later, <=24h |
| Triple duration | Up to 48h; no extra whole-chain 24h filter |
| Preserved cases | Same-bank transfers, mixed/legitimate labels, repeated methods and allowed cycles |
| Identity | Ordered distinct bigint IDs; same deterministic v1 hash; no sample relabelling |
| Attempt metadata | Not needed; LI lacks HI's attempt annotations |
| Amounts | No BQ1 $20M cutoff, amount-equality condition or positive-only filter |

The date/baseline values are confirmed in the documented code contract, **not a
fresh live LI inspection**. Exact source coverage and counts must reconcile to
the LI job's frozen snapshot. Lower positive-label count does not imply small
candidate/output growth: generation uses all cleaned transactions.

For owner D, retain complete D/D+1/D+2 continuation within the confirmed extract.
September 26 has continuation through September 27; September 28 is unavailable.
September 27 only has extract data through that date; September 28–29 are
unavailable. Record those as extract boundaries, not verified-empty future data.
Do not drop the final owner dates or claim completeness beyond the extract.

## Arguments, source fingerprints and limits

[LI_ARGUMENTS.template.json](LI_ARGUMENTS.template.json) records known values
and unresolved inputs. It is **not** a valid AWS CLI request or deployable
argument file. Activation flags are false; missing fingerprints, bounds,
workers and approvals are null/pending. Do not replace them with guessed values.

1. Obtain the shared bounded release commit, entry script/module/SDK hashes,
   shared test results and exact LI argument contract from the HI owner.
2. Confirm whether the bounded generator still uses the same manifest/receipt
   schema and owner-date layout. If chunks are added, reconcile all selected
   chunks into each owner's pair/triple totals without duplicate or missing
   ordered chains. Adapt validation to the owner's contract; do not invent it.
3. Once metadata reads are explicitly coordinated, obtain a fresh **LI** full
   source object inventory plus table/partition metadata. The shared
   `cloudshell_full_source_fingerprints.py` supports LI, but is not run here.
   Its repeated-metadata result is not a transaction-count check or source lock.
4. Bind `source_fingerprint`, `table_fingerprint`, `partitions_fingerprint`,
   independent UTC evidence and no-refresh agreement to this exact LI run.
   Never use HI/sample fingerprints or recycle them after a source refresh.
5. Use the existing LI source SQL in `02_source.sql` only after paid-query
   coordination. Verify exact baselines, 27 dates, fields, labels, timestamps and
   full selected-source ID uniqueness before generating sequence identities.
6. Obtain LI growth measurements using the shared, owner-approved mechanism.
   Keep measurement scope clear: potential candidates, exact valid links,
   projected triples, output rows and per-key/chunk expansion differ.
7. Review the existing limits and any new chunk parameters against those LI
   measurements: pair candidates, continuation pair links, projected triples,
   triples per middle, output rows, file records, cooperative runtime and memory.
   Copy-size limits apply if the reviewed phase performs copying. Do not copy
   the reported 496-million HI figure into an LI limit or claim it is an output count.
8. Agree actual workers, local disk/shuffle headroom, Glue timeout, zero retries,
   concurrency and run budget before activation. The cooperative budget does
   not cancel in-flight Spark actions; service timeout/cost controls also matter.

## Deployment review before requesting explicit LI run approval

- [ ] Shared bounded implementation is committed/tested for both dataset values,
  preserves rules/schema, retains all chunk boundaries and has reviewed growth limits.
- [ ] Actual entrypoint and per-run arguments are reviewed. Do not edit the HI
  job definition or change a shared job's defaults underneath an active HI run.
- [ ] Shared release hashes match the uploaded artifacts; SDK and physical
  source/output schema checks still run. No local installation/workaround changes.
- [ ] Staging-only mode is real and tested: it cannot copy/publish production
  files or switch production Catalog partitions. Do not infer this from a flag name.
- [ ] Fresh LI run/attempt/owner IDs and isolated prefix are reserved by the
  shared protocol. Existing contract root is `bq2_sequences_staging/full/`;
  confirm the bounded release retains or explicitly changes it before use.
- [ ] No sample/HI prefix, sample table, production table or shared ownership
  record can be overwritten by LI staging. No stale-owner takeover or blind retry.
- [ ] Fresh LI source snapshots, baseline reconciliation, UTC and source freeze
  match the actual job; missing source partitions are investigated, not called empty.
- [ ] Owner agrees exact job, dataset, release, scope, worker configuration,
  limits, timeout, budget and execution window, with explicit LI run approval.
- [ ] Prefer HI then LI unless owners explicitly agree overlap. Overlap needs
  verified isolated staging, suitable job concurrency and sufficient AWS quota,
  disk/worker capacity and combined budget. Separate prefixes alone are insufficient.
- [ ] Production publication remains out of this LI task. Its table-wide reader
  pause and approval are separate; do not ask readers to pause for a read-only
  preparation or genuinely isolated staging run that never touches production.

## LI staging acceptance and handoff evidence

Do not run production Athena acceptance against staging paths or create a
temporary Catalog table as a shortcut. Use the shared generator's staged
read-back validation and local downloaded evidence, after explicit run approval.

| Evidence | Required outcome |
|---|---|
| Glue / application identity | Actual job name, JobRunId, LI run/attempt/chunk IDs, approved release |
| Source | LI baseline/dates, full inventory and all three matching fingerprints |
| Coverage | All 27 owners; explicit zero-output dates; complete selected chunk list if chunked |
| Counts | Per-owner/per-chunk pairs/triples, exact aggregate totals and growth reconciliation |
| IDs | No null/repeated member IDs, duplicate ordered chains or duplicate outputs across chunks |
| Values | Exact schema, aligned arrays/strings, original/derived labels and final fields |
| Links | All members tied to source; exact bank/account continuity and adjacent time rules |
| Storage | Isolated immutable files, Snappy/read-back/inventory evidence and matching receipts |
| Completion | Shared staging completion state only after every required chunk/date validates |
| Publication boundary | Staging evidence does not claim PUBLISHED or Catalog-updated success |
| Resources | Actual workers, service/cooperative duration, memory/growth metrics and cost evidence |
| Failure | Partial/failed chunks retained for diagnosis; no accepted incomplete dataset |

The existing `review_run.py` intentionally requires a production PUBLISHED
receipt. Do not pass staging-only evidence to it or weaken it to label staging
as production. Review the new bounded staging handoff once the owner supplies
its exact protocol; no compatible staging-only review helper is claimed yet.

## Message to the HI owner

> The LI continuation handoff is ready on `role5/full-validation` in
> `bq2/sequences/validation/LI_HANDOFF.md`. It includes configuration, fingerprint
> collection, argument worksheet, staging steps, validation commands and blockers
> so you can pick up LI after HI using your tested shared generator. Actual
> fingerprints, limits and the new staging-start/chunk-validation commands remain
> pending the verified release and run approval. No AWS jobs or production
> resources were touched.

**Next dependency:** the shared bounded generator, tested staging-only interface,
LI measurements/snapshots and explicit execution approval. No reliable LI runtime,
sequence count, complete source fingerprint or run window is invented here.
