# Role 6: run the accepted sample development batch

> **Historical (10 November 2026):** sample-stage runbook, completed 7 to 8 November 2026. Final results are in [results.md](../results.md).

Status: Ji Ning reports completed publication and Athena acceptance. Role 6 has not rerun these queries in this session. This runbook tests the Role 6 integration using the published sample partition; outputs are not final HI/LI findings.

## Confirmed handoff supplied by Ji Ning

- Table: `is459_g2t1_aml.bq2_sequences_dev`.
- Partition: `dataset='sample'`, `start_date='2022-09-01'`.
- Counts: 184,449 pairs + 33,931 triples = 218,380 sequences.
- Eight verified Parquet files at an immutable approved partition location; staging manifest unchanged (teammate report).

| Acceptance evidence supplied | Query ID | Reported result |
|---|---|---|
| Sequence counts | `4971124e-fee8-4fc4-8551-07af320896d4` | SUCCEEDED, counts matched |
| Focused values/labels | `ada6c118-152f-4619-9dc8-a533295ec722` | SUCCEEDED, 0 bad rows |
| Transaction-ID existence | `13bf9fdf-bd08-43a7-90e5-8332dae6ee39` | SUCCEEDED, 0 missing |
| Full lineage | `d4e7f074-9d57-4c12-81a8-13ad4860abd0` | SUCCEEDED, 0 member/link errors and 0 duplicate/null source IDs |

Ji Ning also reports the exact 21-column Catalog schema matched. Retain her evidence and source version; these IDs are not new Role 6 runs. If publication/source changes, acceptance must be re-evaluated. Do not run Glue or refresh the source as part of these steps.

## Step 1. Open Athena in the team account

Use the same team-account browser session that produced your earlier baseline screenshots. Region `us-east-1`, workgroup `primary`, data source `AwsDataCatalog`, database `is459_g2t1_aml`. Existing result location: `s3://is459-g2t1-aml-athena-bucket/results/`. Athena engine 3 is expected.

Run one SQL statement at a time. You may clear the editor between statements; that only replaces editor text. Record your own query execution ID, result, runtime and data scanned after each step.

## Step 2. Confirm your access and counts

Open [00_readiness.sql](00_readiness.sql), copy ONLY its first statement into Athena and Run. Expect exactly:

| sequence_length | sequences |
|---|---:|
| 2 | 184449 |
| 3 | 33931 |

Save `../screenshots/06-dev-sequence-counts.png`. Stop for missing table/permission errors, missing length or different counts. Ask Ji Ning about a publication change rather than editing expected numbers.

## Step 3. Inspect actual schema/location

Run the second statement in `00_readiness.sql`: 21 logical columns must match [Role 5 v1](../../sequences/schema_bq2_sequences.md), including arrays, signed bigint IDs and partition columns. Save `../screenshots/07-dev-sequence-schema.png` (multiple images if necessary).

Run the third statement to inspect the table definition. In Glue → Data Catalog → Tables → bq2_sequences_dev → Partitions, select sample/2022-09-01 and compare its Location with Ji Ning's approved immutable location. A partition Location can differ from the table root. Do not edit the Catalog.

## Step 4. Validate the selected inputs

[01_validation.sql](01_validation.sql) contains the production validations adapted to the exact sample partition. Run each statement separately in order if independently reproducing acceptance or if the version changed. Ji Ning has already supplied successful acceptance evidence; do not repeatedly rerun unchanged full-lineage scans merely to obtain more screenshots.

The first query checks ALL cleaned sample rows: 500,000 transactions, 35 positive labels, unique_txns = transactions, no null IDs/bad labels. The sequence summary must show v1 and the counts above. Every subsequent defect query must return zero rows. They check array shapes/nulls, canonical strings, derived labels, endpoints, duplicate chains, original member attributes, bank/account links and gaps.

Any failed check blocks downstream calculations until resolved with Ji Ning/Amareet. A zero-row defect result means pass, not missing data; the count/summary queries establish that the expected populations exist. Preserve the upstream acceptance evidence and record which checks you personally ran. Suggested screenshot: `08-dev-validation.png`; do not present query IDs from Ji Ning as your own executions.

## Step 5. Inspect pattern frequencies

Run [02_frequency.sql](02_frequency.sql). This reads only the accepted sample/start-date partition and counts occurrences separately by length, without labels. Save `09-dev-pattern-frequency.png`.

This is an integration test of support counts. It does not select the final HI operating cutoff. Keep all 50/100/500 sensitivity policies in the test; do not reduce support until an attractive pattern appears.

## Step 6. Check development destinations BEFORE CTAS

The following analytical steps CREATE three Role 6 development tables and write Parquet under the processed bucket:

| File | Created table | S3 prefix below processed bucket |
|---|---|---|
| 03_lift.sql | bq2_lift_dev | bq2_lift_dev/sample_20220901/ |
| 04_tradeoff.sql | bq2_tradeoff_dev | bq2_tradeoff_dev/sample_20220901/ |
| 05_sankey.sql | bq2_sankey_dev | bq2_sankey_dev/sample_20220901/ |

All names use database `is459_g2t1_aml` and bucket `s3://is459-g2t1-aml-processed-bucket/`. The input table/partition is read-only for Role 6. Use these development files, not the root production CTAS files.

In Athena table list and S3 Console, confirm these destination tables do not exist and the three destination prefixes have no files. If a table/path already exists, DO NOT drop/delete shared data or click Run again. Choose a fresh, consistent run suffix for ALL three output table names, references and paths, and record the mapping. CTAS is not overwrite. A failed attempt may leave files; a new run prefix is the simplest retry. No concurrent writers should use the same destinations.

## Step 7. Create development lift

Copy all of [03_lift.sql](03_lift.sql) into Athena and Run (one CTAS statement). After success, run:

```sql
SELECT sequence_length, min_support, method_sequence,
       support_count, laundering_count, baseline_rate, lift, risk_rank
FROM is459_g2t1_aml.bq2_lift_dev
WHERE min_support = 100 AND risk_rank <= 10
ORDER BY sequence_length, risk_rank;
```

Save `10-dev-lift-ranking.png`. There is no expected winning pattern. If a length has no positive baseline, lift/rank is NULL. If no patterns qualify, retain that result and inspect support/positive counts; do not invent zero-valued findings.

For ONE support policy, total pattern support must reconcile to 184449/33931 by length:

```sql
SELECT sequence_length, SUM(support_count) AS sequences,
       SUM(laundering_count) AS positive_sequences
FROM is459_g2t1_aml.bq2_lift_dev
WHERE min_support = 100
GROUP BY sequence_length ORDER BY sequence_length;
```

Do not sum support across the three policy copies.

## Step 8. Create development targeted-check comparison

Run [04_tradeoff.sql](04_tradeoff.sql), then:

```sql
SELECT sequence_length, min_support, policy, actual_patterns,
       reviewed, laundering_flagged, legitimate_reviewed,
       laundering_catch_rate, legitimate_review_rate, review_precision
FROM is459_g2t1_aml.bq2_tradeoff_dev
WHERE min_support = 100
ORDER BY sequence_length, top_n;
```

Save `11-dev-targeted-tradeoff.png`. Exactly one length and cutoff must be selected when comparing policies. ALL cleaned sample transactions remain denominators (500000 total, 35 positive), although only sequences starting on the approved day can flag final transactions. Counts are distinct finals, not every member. `[1,0]` chains contribute to positive-chain lift but flag a legitimate final.

This sample comparison selects and evaluates patterns on the same sample; it is in-sample arithmetic verification, not HI-to-LI robustness or a representative performance estimate. The review-all endpoint is 100%/100% where populations exist. Final review counts must not decrease from top-3 to top-5 to top-10. Actual patterns may be fewer than N.

## Step 9. Reconcile targeting

Run [06_reconciliation.sql](06_reconciliation.sql). Expect ZERO rows. Save `12-dev-tradeoff-reconciliation.png`. It checks reviewed = laundering flagged + legitimate reviewed, flags never exceed source populations and review-all equals the source transaction count. Stop for any failure.

## Step 10. Create development Sankey inputs

Run [05_sankey.sql](05_sankey.sql), then:

```sql
SELECT sequence_length, SUM(sequence_count) AS sequences,
       SUM(laundering_sequence_count) AS positive_sequences
FROM is459_g2t1_aml.bq2_sankey_dev
GROUP BY sequence_length ORDER BY sequence_length;
```

Expect sequence counts 184449 and 33931. Positive counts must agree with the lift reconciliation for each length. Save `13-dev-sankey-counts.png`. These are chain weights; they do not represent USD or unique transactions.

## Step 11. Test Tableau and document the run

Connect Tableau to the three `_dev` tables using [viz instructions](../../../viz/bq2_tableau.md), or export aggregate SELECT results as local CSV and use Tableau's Text file connector if the Athena connector is unavailable. Do not commit CSV/extracts. There is no need for Excel conversion or another raw-data upload.

Build development ranking, Sankey and trade-off views with visible `SAMPLE DEVELOPMENT — NOT FINAL FINDINGS` labels. A temporary checklist can use sample-ranked methods but has no LI evidence; do not call it a validated operational recommendation. Do not run `robustness.sql` on this sample-only batch.

Save the workbook per repository instructions; record query IDs, locations, source partition, counts, validation outcome and screenshots in `../results.md`. Update the README status only for successful steps actually completed, then review and push the evidence. Full HI/LI findings, frozen HI-to-LI evaluation and final recommendations remain separate later work.

## Regenerating the development SQL

`python3 bq2/lift/prepare_development.py` reuses the maintained production statements to generate 01–06 with this fixed sample mapping. The checked-in copies can be pasted directly into Athena; you do not need to run Python for the walkthrough. Tests check the generated copies and sample-specific arithmetic. Athena array functions, IAM, actual inputs, writes and Tableau rendering still require the real checks above.
