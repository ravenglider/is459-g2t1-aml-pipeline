# Full HI/LI integration handoff template

Status: **PREPARED ONLY — LIVE RUNS AND ACCEPTANCE PENDING**.
Fill from measured run evidence; send to Sonia after the generation owner reviews it.
This is documentation, not a deployable Step Functions definition or run command.
Immediate read-only check order and Xiang Ying's data handoff fields are in
[READY_TO_RUN.md](READY_TO_RUN.md). The generation owner's reported 496-million
candidate probe does not establish final counts, publication or acceptance.
The copy-ready final message and explicit Sonia/Xiang Ying confirmation tracker
are in [READER_PAUSE_AND_HANDOFF.md](READER_PAUSE_AND_HANDOFF.md). Both pause
confirmations remain pending; the user will send the team-chat requests.

| Field | Contract / value to complete |
|---|---|
| Job type | AWS Glue Spark, generation through verified production publication |
| Exact job name | `is459-g2t1-bq2-sequence-builder` — verify actual configuration |
| Entry script | Versioned `glue_full_generation.py` URI + SHA-256: **pending** |
| Modules / SDK | Versioned 18-module `role5_full_modules.zip` and approved separate SDK bundle; URIs/hashes: **pending** |
| Output table | `is459_g2t1_aml.bq2_sequences` |
| Logical output root | `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/` |
| Actual partition roots | `dataset=<dataset>/versions/<run_id>/<attempt_id>/data/dataset=<dataset>/start_date=<date>/` under the output root |
| Dependency | Complete frozen `transactions_clean` HI or LI source with exact fingerprints, verified UTC and coverage |
| Runtime environment | Glue 5.0; deployed Spark/Python/Java/SDK evidence: **pending** |
| Worker type/count | **pending actual job settings**; 5 G.1X is a proposal, not established sizing |
| Runtime/cost | HI **pending**; LI **pending**; record execution/DPU time separately from validation time |
| Source baselines | HI 31,898,218 / 35,230 positives; LI 31,251,469 / 16,041 positives; reconcile exact snapshots |
| Coverage | HI Sep 1–28: 28 owner dates; LI Sep 1–27: 27; both lengths, including zero-output dates |
| Acceptance | Query IDs, receipt URIs, job run IDs and evidence-window fingerprints: **pending** |

## Dataset-specific arguments to record

One dataset per job invocation. These are required fields to fill, **not values
to deploy**. Use the exact CLI in `glue_full_generation.py` and the full-generation
contract; do not substitute the old sample script/package or sample thresholds.

| Arguments | Required evidence |
|---|---|
| `--DATASET`, `--REGION`, `--EXPECTED_ACCOUNT_ID` | Exact HI-Medium or LI-Medium, us-east-1, verified account |
| `--RUN_ID`, `--ATTEMPT_ID`, `--OWNER_TOKEN` | Fresh immutable normal-run identifiers; no blind reuse |
| `--BUILDER_VERSION`, `--BUILDER_FINGERPRINT` | Reviewed release and exact full-module ZIP hash |
| `--SOURCE_FINGERPRINT`, `--TABLE_FINGERPRINT`, `--PARTITIONS_FINGERPRINT` | Fresh full selected-dataset inventories matching the frozen job source |
| `--TIMEZONE_EVIDENCE` | Independently confirmed upstream UTC semantics |
| `--NO_REFRESH_ACKNOWLEDGED`, `--NO_REFRESH_EVIDENCE` | Current source-freeze agreement; true only when actually coordinated |
| `--MAX_PAIR_CANDIDATES`, `--MAX_PAIR_LINKS`, `--MAX_PROJECTED_TRIPLES`, `--MAX_TRIPLES_PER_MIDDLE`, `--MAX_OUTPUT_ROWS` | Explicit measured/reviewed positive bounds for this dataset |
| `--LIMITS_EVIDENCE`, `--MAX_RECORDS_PER_FILE`, `--MAX_RUN_SECONDS` | Growth review, file sizing and cooperative budget |
| `--ALLOW_AWS_WRITES`, `--ALLOW_PUBLICATION` | Explicit authorised true activation, not permanent job defaults |
| `--READERS_PAUSED`, `--PUBLICATION_MAINTENANCE_EVIDENCE` | Real table-wide reader/writer maintenance agreement |
| `--MAX_COPY_BYTES` | Reviewed per-file in-memory copy bound |
| `--RESUME_PUBLICATION` | false for normal run; true only after owner-reviewed SAME-attempt recovery |

Retain actual Glue bootstrap settings, including script location, extra-py-files,
SDK bundle, no-index installation option, temp location, bookmarks disabled,
retries zero, concurrency one and service timeout. The cooperative budget cannot
cancel an in-flight Spark action. Do not put secrets into public evidence.

## Completion and failure contract for Sonia

1. Depend on the correct full source snapshot, not merely an ETL job exit.
   HI and LI publication share a table-wide lock: serialize publications and
   coordinate table readers; do not run them as independent concurrent writers.
2. Use completion-aware Glue integration (for example `.sync`) after owner review.
   Glue success alone is insufficient for downstream consumption.
3. Require the matching `FULL_GENERATION` PUBLISHED report and separately read-back
   publication receipt: same dataset/run/attempt, source and builder bindings,
   `published=true`, `catalog_updated=true`, `verified=true` in the receipt.
   Confirm ownership release, exact 28/27 partition reconciliation and acceptance.
4. Do not treat the generation `STAGED_VALIDATED` manifest as publication success;
   it intentionally remains `published=false` and `publication_required=true`.
5. Failed copies, validation, Catalog switching or timeouts stop downstream work.
   Catalog switching is not atomic across dates. Keep readers paused on uncertain
   state/RECOVERY_REQUIRED and retain evidence; do not repair, delete or retry blindly.
6. Recovery belongs to the publication owner using the documented same-attempt
   path. A new automatic retry must not invent acknowledgments or seize a lock.
7. Only release Role 6 after independent acceptance. Record the integrated
   execution ARN/history, both job run IDs and validated output references.

## Fill after the authorised runs

| Evidence | HI | LI |
|---|---|---|
| Glue JobRunId / SUCCEEDED | pending | pending |
| Application run / attempt | pending | pending |
| Three source fingerprints | pending | pending |
| Generation manifest URI / digest | pending | pending |
| Publication receipt URI / verified state | pending | pending |
| Pair / triple counts | pending | pending |
| Registered dates / accepted version | pending | pending |
| Source row / positive totals reconciled | pending | pending |
| Athena acceptance IDs and zero-error results | pending | pending |
| All-date lineage results / chunk reconciliation if used | pending | pending |
| Workers / runtime / DPU / cost evidence | pending | pending |
| Boundary notes / Role 6 acknowledgment | pending | pending |

Until filled and reviewed, this handoff does not claim full HI/LI publication,
live permission verification, Step Functions deployment or analytical acceptance.
