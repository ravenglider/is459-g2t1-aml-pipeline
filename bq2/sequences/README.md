# Role 5: BQ2 Payment Sequence Builder

**Owner:** Ji Ning. **Status updated:** 5 November 2026.

Role 5 builds linked payment-method and currency sequences from Role 2's cleaned transactions, preserving transaction identities and laundering labels for Xiang Ying's Role 6 Athena analysis. The builder and supporting workflow have been tested locally. Release `_07` completed the sample-only AWS Glue preflight successfully in run09 (`SUCCEEDED`, `VALIDATED_READ_ONLY`). Real sequence generation and publication remain unfinished; this validation covers only the provided 500,000-row sample.

## Sample Development Publication — 2026-11-07

**Published, queryable and accepted for Role 6 sample integration testing:** `is459_g2t1_aml.bq2_sequences_dev` in `us-east-1`, partition `dataset='sample', start_date='2022-09-01'`. This dated update supersedes the earlier pending-publication status statements preserved below.

- **218,380 sequences:** 184,449 pairs and 33,931 triples across 8 verified Parquet files.
- **Exact approved 21-column v1 schema confirmed:** 19 data columns plus the string partition columns `dataset` and `start_date`.
- Run: `sample_gen_20261107_c36493028788`; attempt: `attempt01_47ce5555c887`.
- S3 development copy verified; Glue table and partition registered; original staging files and manifest unchanged.

All four Athena acceptance queries returned `SUCCEEDED`:

| Check | Query ID | Accepted result |
|---|---|---|
| Counts | `4971124e-fee8-4fc4-8551-07af320896d4` | 184,449 pairs; 33,931 triples |
| Structure and labels | `ada6c118-152f-4619-9dc8-a533295ec722` | 218,380 rows; 0 bad rows |
| Transaction-ID existence | `13bf9fdf-bd08-43a7-90e5-8332dae6ee39` | 0 missing members |
| Full lineage | `d4e7f074-9d57-4c12-81a8-13ad4860abd0` | 0 link errors; 0 member errors; 0 duplicate/null source IDs in the selected source partitions |

Evidence: [publication summary](evidence/role5-publication-summary.txt). The file-level archive `evidence/role5-publication-evidence.zip` is local-only, excluded from Git and unavailable through GitHub. The original staging manifest remains **`STAGED_VALIDATED`, `published=false`**; the separate manual development publication is supported by independent copy, Catalog and Athena acceptance evidence, not a `PUBLISHED` staging-manifest state.

This is a **sample-only integration dataset, not the final HI/LI analytical dataset**. Output ownership is September 1, 2022, with continuation inputs through September 3. The sample may be truncated; publication does not establish completeness of the underlying HI-Medium or LI-Medium datasets.

## Sequence-building rules

Here, **2-hop and 3-hop mean two and three transactions**, respectively. For consecutive transactions `a` and `b`, require:

```text
a.dataset = b.dataset
a.to_bank = b.from_bank
a.to_account = b.from_account
a.txn_ts < b.txn_ts <= a.txn_ts + 24 hours
```

Match both bank and account, preserving account strings and leading zeros. The 24-hour limit is inclusive for each adjacent link; equal timestamps are excluded. There is no additional whole-chain 24-hour limit, so a three-transaction chain can span up to 48 hours.

Retain all valid branches, same-bank transfers, repeated payment methods, legitimate transactions and mixed-label chains. Amounts and currencies need not match. Exclude repeated transaction IDs within a chain and deduplicate identical ordered chains. Validate source ID uniqueness within each dataset rather than assuming the source hash cannot collide. These paths indicate linked account activity, not proof that the same funds were forwarded.

For example, transfers at 09:00, 12:00 and 12:00 the following day form a valid triple if the receiving/sending identities match: its gaps are 3 and 24 hours, despite a total duration of 27 hours. See the fictional transaction example in [sequence_rules.md](sequence_rules.md).

## Approved output and Role 6 interface

Xiang Ying approved the **21-column v1 interface** in [schema_bq2_sequences.md](schema_bq2_sequences.md). One row represents one distinct ordered chain, with `sequence_length` distinguishing pairs and triples.

- Ordered arrays retain transaction IDs, timestamps, payment methods, payment/receiving currencies and original member labels. IDs remain signed 64-bit `bigint` values, including negative values.
- Readable method and currency strings use the exact separator ` -> `. Sender-side and receiver-side currencies remain separate.
- Labels include `laundering_count`, `any_laundering`, `all_laundering` and the original `final_is_laundering`. For member labels `[1,0]`, `any_laundering=1` while `final_is_laundering=0`.
- `final_txn_id`, `gap_seconds`, `duration_seconds`, `dataset` and `start_date` support targeting, validation and partition ownership.
- Deterministic `sequence_id` is SHA-256 over UTF-8 `v1 + LF + dataset + LF + comma-separated ordered decimal IDs`, without trailing newline or floating-point conversion.

The intended storage format is Snappy Parquet partitioned by **`dataset`, then `start_date`**. This gives 19 physical file columns and two logical partition columns. No sequence table has been published yet.

Role 6 uses `any_laundering` for descriptive lift, separately by dataset and sequence length. Low-support patterns stay in the baseline; support sensitivity uses 50, 100 and 500. Targeted checks count distinct final transactions using original labels and all cleaned transactions as denominators. HI-selected patterns must be frozen before LI evaluation; sample results are interface checks, not final findings.

## Implementation

| File | Responsibility and current boundary |
|---|---|
| [bq2_sequence_builder.py](bq2_sequence_builder.py) | Importable PySpark source validation, two-/three-transaction links and 21-column output; not a standalone runnable Glue job. |
| [sequence_processing.py](sequence_processing.py) | Pure date planning. Owner date D reads D/D+1/D+2, distinguishing missing partitions, verified-empty dates and confirmed extract boundaries. |
| [sequence_source.py](sequence_source.py) | Local Parquet reader, exact 20-column schema validation, identity checks and deterministic source-inventory fingerprints. Global completeness needs separate evidence. |
| [sequence_batch.py](sequence_batch.py) | Anchored batch processing, conservative pair-candidate bounds, exact triple-degree projections, explicit growth limits and owned-cache cleanup. Current runner requires verified UTC semantics. |
| [sequence_staging.py](sequence_staging.py) | Local immutable attempts, Snappy Parquet read-back and distributed schema/value/identity validation, including empty and failed attempts. |
| [sequence_manifest.py](sequence_manifest.py) | Run evidence, selected receipts, local replacement plans, revision checks and local ownership locks. Local planning states never imply publication. |
| [sequence_aws.py](sequence_aws.py) | Injected sample-only Catalog reader with partition pushdown, complete sample metadata inventories and physical-schema checks. Tested with fake clients and local Spark. |
| [sequence_s3.py](sequence_s3.py) | Injected S3 staging/read-back and conditional receipt/manifest adapters. Locally tested with fake S3 and real local Parquet; no real AWS staging performed. S3 run manifests stop at `READY_TO_PLAN`. |
| [sequence_publication.py](sequence_publication.py) | Increment 3A only: pure development publication plans, recovery evidence and simulated ordered journals. No copying, Catalog mutations, rollback execution or `PUBLISHED` success. |
| [glue_sequence_job.py](glue_sequence_job.py) | Offline configuration CLI and gated runtime/batch helpers. Cloud sequence-generation orchestration remains unfinished. |
| [glue_sample_preflight.py](glue_sample_preflight.py) | Dedicated sample validation/candidate-profile entry point; release `_07` succeeded in real AWS run09. No pairs/triples or sequence outputs were generated. |
| [package_glue_modules.py](package_glue_modules.py) | Deterministic allowlisted ZIP creation; no upload or dependency installation. |
| [preflight_job_settings.md](preflight_job_settings.md) | Proposed runtime settings, argument behavior, evidence requirements and execution checklist. Current upload/deployment status is recorded below. |
| [tests/](tests/) | Synthetic fixtures, Spark regression checks, fake AWS clients, local Parquet tests and packaging/import checks. |

Anchored processing publishes only sequences starting on D, although continuation transactions come from later dates. Starting links are restricted before triple expansion. Five explicit limits guard pair candidates, pair links, projected triples, expansion per middle transaction and output rows; exceeding a limit fails rather than dropping valid branches. No production thresholds have been established.

Staging validates written data before marking an attempt `VALIDATED`; `_SUCCESS` alone is insufficient. Unique attempts, explicit empty receipts and preserved failures support diagnosis. Inventory equality detects changes but is not a transactional snapshot. Local locks and planned reader-maintenance acknowledgments are cooperative controls, not atomic multi-partition publication.

## Existing AWS infrastructure and proposed outputs

| Resource | Exact value | Status |
|---|---|---|
| Region | `us-east-1` | Existing group region |
| Glue database | `is459_g2t1_aml` | Existing |
| Source table | `is459_g2t1_aml.transactions_clean` | Existing Role 2 output |
| Processed/source bucket | `is459-g2t1-aml-processed-bucket` | Existing |
| Shared IAM role | `AWSGlueServiceRole-is459g2t1` | Existing; run09 exercised sample-read permissions, not sequence staging/publication permissions |
| Script bucket/prefix | `s3://is459-g2t1-aml-athena-bucket/scripts/` | Existing team location |
| Glue temporary prefix | `s3://is459-g2t1-aml-athena-bucket/glue-temp/` | Existing team location |
| Dedicated preflight job | `is459-g2t1-bq2-sample-preflight` | Existing; release `_07` run09 **SUCCEEDED** |
| Future sequence-builder job | `is459-g2t1-bq2-sequence-builder` | Intended name in integration helpers; not deployed |

Read the actual cleaned schema in [etl/schema_transactions_clean.md](../../etl/schema_transactions_clean.md), Role 2's [handoff](../../etl/README.md), and the [group infrastructure handoff](../../docs/ingestion-handoff.md). Input has 20 logical columns, partitioned by string `dataset` and `txn_date`. Always select a dataset; do not mix the sample with HI or LI. The builder now targets cleaned data, not the old raw-sample fallback.

All paths below are under `s3://is459-g2t1-aml-processed-bucket/`:

| Purpose | Table / relative prefix | Status |
|---|---|---|
| Clean input | `is459_g2t1_aml.transactions_clean`; `transactions_clean/dataset=sample/txn_date=<D>/` | Existing sample input |
| Development output | `is459_g2t1_aml.bq2_sequences_dev`; `bq2_sequences_dev/` | Approved intended interface; not published |
| Production output | `is459_g2t1_aml.bq2_sequences`; `bq2_sequences/` | Future HI/LI destination; not published |
| Staging attempts | `bq2_sequences_staging/<run_id>/<attempt_id>/` | Adapter implemented; no real staging performed |
| Development metadata | `bq2_sequences_metadata/development/` | Adapter/report design; no real evidence writes claimed |
| Development versions | `bq2_sequences_dev_versions/<publication_id>/dataset=sample/start_date=<D>/` | Publication-planning layout only; future explicit Catalog partition locations |

The existing local replacement planner uses fixed canonical locations. The separate development publication planner supports versioned locations; executing that design and reconciling Catalog state remain future work. Staging/metadata stay outside queryable table roots. No development or production output table is claimed to exist from Role 5 work.

## AWS S3 upload: completed

**Current release:** `sample_preflight_20261105_07`, packaged from committed `main` at `f9b182e748faf3f8d2f545b7603cf80441aef8a5`. Ji Ning confirms both artifacts were uploaded and the existing Glue job now references this script and ZIP. Release `_07` successfully executed in AWS as run09 after both release artifacts were verified and the job configuration was updated via AWS CLI without overwriting the corrected script. Earlier runs are recorded below.

| Artifact | Uploaded S3 URI |
|---|---|
| Main script | `s3://is459-g2t1-aml-athena-bucket/scripts/role5/sample_preflight_20261105_07/glue_sample_preflight.py` |
| Modules ZIP | `s3://is459-g2t1-aml-athena-bucket/scripts/role5/sample_preflight_20261105_07/role5_modules.zip` |

Recorded SHA-256 values supplied with the release:

```text
Main script:
c66724a64153756eeba21388bd93ea4001b4ae97454923bc679d9cdf35b7ef43

Modules ZIP:
fa2f2090bd184e72718058557ce4c40f22bbbca0006e12253297456ed6489432
```

The ZIP contains these nine Python modules at its root, matching the packaging allowlist:

```text
bq2_sequence_builder.py
glue_sample_preflight.py
sequence_aws.py
sequence_batch.py
sequence_manifest.py
sequence_processing.py
sequence_s3.py
sequence_source.py
sequence_staging.py
```

The entry script is selected separately in Glue; the ZIP is supplied through Python library path / `--extra-py-files`. Publication planning and the generic offline CLI are not included in this preflight package. Rebuild and record new hashes whenever release code changes; never assume the recorded hashes describe later source edits.

## Testing and local commands

Reported Ubuntu WSL2 results using Python 3.11, Java 17 and PySpark 3.5.4:

| Check | Result |
|---|---|
| Complete Role 5 regression suite | **182 passed for release `_07`** |
| Sample-preflight tests | **41 passed** |
| Glue entry-point/packaging suite | **9 passed** |

These are local results, including fake AWS clients and real local Spark/Parquet fixtures. They do not verify live AWS permissions, Glue argument injection, the real S3 connector or current sample data quality. Earlier Windows worker/filesystem problems motivated Ubuntu testing; no random Windows Hadoop binaries are required by this workflow.

From the repository root in the activated Ubuntu test environment:

```bash
python -m unittest discover -s bq2/sequences/tests -p test_glue_sample_preflight.py -v
python -m unittest discover -s bq2/sequences/tests -p test_glue_sequence_job.py -v
python -m unittest discover -s bq2/sequences/tests -v
```

For a future local package, choose a fresh output filename; creation is exclusive:

```bash
python bq2/sequences/package_glue_modules.py --output /tmp/role5_preflight_new_release.zip
python -m zipfile -l /tmp/role5_preflight_new_release.zip
```

Packaging performs no upload. Keep virtual environments, credentials, transaction data and generated artifacts out of Git.

## AWS sample-preflight scope and run09 evidence

Release `sample_preflight_20261105_07` completed the real sample preflight successfully. Verified run09 evidence:

| Validation evidence | Run09 result |
|---|---|
| Job / region | `is459-g2t1-bq2-sample-preflight` / `us-east-1` |
| Application Run ID | `sample_preflight_20261105_run09` |
| AWS JobRunId | `jr_6cc9cc1dc06ef48480158bddfdee3348b0450ef9e4f35ff4337ea88c92a34dc3` |
| AWS Glue job status | **`SUCCEEDED`** |
| CloudWatch `PREFLIGHT` status / phase | **`VALIDATED_READ_ONLY` / `complete`** |
| Sample transactions / laundering-labelled transactions | **500,000 / 35** |
| Date coverage | All six dates: `2022-09-01` through `2022-09-06` |
| Schema and timestamps | Physical schema and UTC timestamp consistency passed |
| Runtime | Glue 5.0; Spark `3.5.4-amzn-0`; Python `3.11.15`; Java `17.0.20` |
| Source consistency | Before/after source, table and partition fingerprints matched |
| Conservative pair-candidate upper bound | **751,390**; not an actual pair count |
| Validation elapsed time | **96.553 seconds**; not total job or billed runtime |
| Materialized sequences | **No pairs or triples materialized** |
| Report storage | JSON evidence in CloudWatch only; **no S3 report written** |
| Identity verification scope | Provided sample DataFrame only; dataset-wide identity was **not verified beyond that frame** |

Matching inventories are change-detection evidence, not a transactional snapshot or proof of full-dataset coverage. The successful sample preflight does not complete sequence generation, staging or publication.

### Deployment history

| Run | Outcome |
|---|---|
| Run06, release `_06` | Real sample validation passed (`VALIDATED_READ_ONLY`, `phase=complete`), but Glue marked the job **Failed** because the entry-point guard raised `SystemExit(0)`. |
| Runs07 and 08, release `_07` deployment attempts | **Failed** because the deployed S3 entry-point script contained outdated exit-handling code. |
| Run09, release `_07` | **SUCCEEDED** after verifying both release artifacts and updating the job configuration via AWS CLI without overwriting the corrected script. |

Release `_07` returns normally on success while preserving nonzero failures and exceptions. It was packaged from commit `f9b182e748faf3f8d2f545b7603cf80441aef8a5` and passed 182 Ubuntu tests. Run09 used the artifact paths and ZIP fingerprint recorded above with `--RUN_ID=sample_preflight_20261105_run09`.

The dedicated job validates and profiles the real sample only. It does not generate payment sequences, write sequence Parquet, publish tables, query Athena or modify the Catalog.

1. Check the confirmed group account and `us-east-1`, runtime versions and explicit UTC session setting.
2. Inventory all six cleaned `sample` partitions for **1–6 September 2022**, checking exact Catalog/S3 locations and physical Parquet types.
3. Read only those partitions using Catalog pushdown. Validate the 20-column schema, required fields, original binary labels, minute timestamps, `txn_date` consistency and ID uniqueness within the provided sample DataFrame. This is not dataset-wide identity verification beyond that frame.
4. Require the documented baseline of **500,000 transactions and 35 laundering-labelled rows**; discrepancies stop the run.
5. Estimate a conservative pair-candidate bound for owner **1 September**, using continuation input through **3 September** and preceding dates 1–2 September. This joins grouped degrees, not transaction-level pairs; exact triple projection is deferred.
6. Recheck source/Catalog inventories, release owned persistence and log the JSON evidence with prefix `PREFLIGHT |`.

The sample is a 500,000-row extract, so continuation coverage may be incomplete, especially near its final boundary. Treat results as validation of observed sample data, not full-HI coverage or final analytical results. Amareet's confirmed UTC-compatible interpretation is required evidence; explicitly setting UTC alone does not verify upstream semantics. Other timezones/DST completeness are not supported by the current batch runner.

Proposed settings: **Glue 5.0 / Spark 3.5.4 / Python 3.11 / Java 17; 2 G.1X workers; Flex; zero retries; concurrency one; 15-minute timeout; bookmarks disabled**. Full details are in [preflight_job_settings.md](preflight_job_settings.md).

Required execution arguments:

| Argument | Required value / evidence |
|---|---|
| `--EXPECTED_ACCOUNT_ID` | Confirmed 12-digit group account ID |
| `--RUN_ID` | Fresh safe run identifier |
| `--TIMEZONE_EVIDENCE` | Reference to Amareet's UTC parsing confirmation |
| `--NO_REFRESH_ACKNOWLEDGED` | `true` |
| `--NO_REFRESH_EVIDENCE` | Renewed no-refresh coordination reference |
| `--BUILDER_FINGERPRINT` | ZIP SHA-256 for the exact uploaded release |
| `--DATASET` | `sample`; other datasets rejected |
| `--REGION` | `us-east-1`; other regions rejected |
| `--ALLOW_AWS_READS` | Literal lowercase `true` for real AWS reads |

Missing/false activation produces offline `NOT_RUN` output with `preflight_completed=false`. Glue-shaped invocations with a supplied job name/ID return exit code 2 when activation is absent; a missing flag must never be reported as a successful real preflight. Malformed/duplicate activation and unknown Role 5 arguments are rejected.

A real successful validation must include a `VALIDATED_READ_ONLY` report with matching before/after inventory fingerprints, counts, coverage, runtime versions and candidate estimate, as well as successful job completion. Early argument/import/account failures may instead produce a traceback. `_SUCCESS` or a green job exit alone is insufficient evidence.

**JSON evidence is currently logged, not stored in S3.** The code includes a design-only report URI under `bq2_sequences_metadata/development/preflight/<run_id>/report.json`, but `report_storage_enabled=false` and no report writer exists. Glue itself may write logs/temp artifacts during a future run.

## Deployment status

| Milestone | Status |
|---|---|
| Local builder, validation, batch and staging tests | COMPLETED locally |
| Preflight Python script and ZIP uploaded to S3 | **COMPLETED**, release recorded above |
| Dedicated preflight Glue job created | **COMPLETED**; existing job configured for `_07` |
| Real sample validation evidence | Run09: `VALIDATED_READ_ONLY`, `phase=complete`; 500,000 transactions and 35 laundering labels |
| Release `_07` AWS execution | **COMPLETED**: run09 Glue status `SUCCEEDED` |
| Real BQ2 sequence generation | **NOT YET** |
| HI-Medium/LI-Medium processing by Role 5 | **NOT YET** |
| Catalog table publication | **NOT YET** |
| Publication preparation/copying, Catalog switching and rollback execution | **NOT YET**; only pure planning/journal logic exists |

Any further paid execution requires separate approval and coordination with Sonia, a renewed no-refresh agreement and review of saved job settings. Do not run the shared Step Functions pipeline for the first sample test: its current ETL argument selects HI-Medium. No full-data runtime or final growth thresholds are known.

## Intended Step Functions handoff for Sonia

The repository's [state-machine definition](../../ingestion/stepfunctions/is459-g2t1-pipeline.asl.json) still has a Role 5 placeholder. This handoff describes future integration, not a deployed state.

| Field | Dedicated preflight | Future BQ2 sequence workflow |
|---|---|---|
| Job type | Existing dedicated Glue Spark job | Planned Glue Spark workflow |
| Exact name | `is459-g2t1-bq2-sample-preflight` (release `_07` run09 succeeded) | Intended `is459-g2t1-bq2-sequence-builder`; final runnable orchestration awaiting completion |
| Integration | Potential `arn:aws:states:::glue:startJobRun.sync`; standalone sample preflight succeeded, shared integration remains planned | Intended `.sync` task after source readiness and publication completion logic are verified |
| Inputs/dependencies | Cleaned sample, timezone evidence, current no-refresh agreement, tested release | Cleaned dataset, dataset-wide validation, consistent source inventory, confirmed coverage and measured limits |
| Output table | None; validation/profile logs only | Intended `bq2_sequences_dev` first, then `bq2_sequences` after separate validation/approval |
| Output path | Design-only metadata URI; no report S3 writes | Proposed processed-bucket paths above; versioned development publication still requires implementation |
| Dataset arguments | `sample` only, explicit read activation | Future HI/LI orchestration requires Sonia's coordination; initial integration helpers remain sample/development-only |
| Runtime and cost | Validation elapsed: 96.553 seconds; total billed runtime/cost not recorded | Full-data runtime/cost unknown |
| Downstream gate | Validated evidence; does not supply Role 6's sequence table | Role 6 queries only after independently verified publication and reader coordination |

Do not replace the sequence-builder placeholder with a preflight that merely exits successfully. The real sequence workflow and table publication must work before the BQ2 analytics dependency can be satisfied.

## Remaining documentation and delivery items

- Retain run06 through run09 CloudWatch evidence and the verified run09 JobRunId above. Record total job runtime, measured cost and actual worker/settings evidence separately; 96.553 seconds is validation elapsed time, not billed runtime.
- Agree sample growth limits and a bounded development sequence batch with Xiang Ying before generation.
- Complete publication version preparation, durable journals, Catalog reconciliation, reader coordination and recovery execution before exposing development data.
- Add a real chain walkthrough after obtaining and validating actual sequence output. The current chain example in `sequence_rules.md` is fictional; `example_chain.md` has not been created.
- Validate HI and LI independently later, retaining all source transactions for Role 6's denominators and freezing HI pattern selections before LI evaluation.

## Contribution summary for Week 13

Implemented and locally tested the Role 5 PySpark builder for linked two- and three-transaction payment-method/currency sequences, including the approved labelled output, source validation, bounded batching, growth safeguards and staging/publication preparation. Completed the read-only AWS Glue sample preflight with release `_07` in run09, validating 500,000 sample transactions and profiling conservative candidate growth. Real sequence generation and sequence-table publication remain pending.
