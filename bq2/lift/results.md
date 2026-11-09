# BQ2 execution record

Status: pre-Role-5 checks completed by Xiang Ying in the AWS console and reviewed from supplied results/screenshots. The earlier CLI attempt used a different personal account and failed before execution; it does not describe the successful team-console session. No real-data BQ2 ranking, percentages or recommendation is claimed. Proposal pages 8–9 contain illustrations, not results.

Before marking Role 6 complete, record:
- Confirmed Role 5 contract, construction version, chain label, window/overlap rules and dataset/date coverage.
- Validation results, HI/LI chain counts, transaction reconciliation and support threshold/sensitivity.
- AWS query IDs, actual table S3 locations, execution time and bytes scanned.
- Top patterns: rank, method sequence, length, support, laundering count/rate, dataset baseline and lift.
- Review-all and top-3/5/10: actual patterns, distinct reviewed/labelled/legitimate counts, catch/review rates and precision on HI and frozen-rule LI.
- LI pattern support/status/rank changes and top-set overlap; report missing or unsupported patterns explicitly.
- Chosen review budget or catch target, recommendation and limits (in-sample HI, synthetic data, chain linking and retrospective flagging).
- Tableau workbook/screenshots and end-to-end demo evidence.

Local synthetic test results are verification evidence only and must not be pasted as dataset findings.

## Pre-Role-5 access check — 2 Nov 2026

- Existing AWS CLI login authenticated successfully. No credentials were added or changed.
- `primary` workgroup is readable, effective engine `Athena engine version 3`. Workgroup result configuration was empty and enforcement was false at inspection. The submission explicitly supplied the documented team staging location.
- First query in `start_now.sql` failed at `StartQueryExecution` with `InvalidRequestException`: `Unable to verify/create output bucket is459-g2t1-aml-athena-bucket`; `AthenaErrorCode: INSUFFICIENT_PERMISSIONS`.
- No query execution ID or data-scan metrics were returned. Queries 2–5 were not submitted. No project table, job or IAM permission was changed.
- This establishes an output-bucket access failure for this login, not a missing cleaned table or missing Role 5 output. Verify correct team account/role and S3/bucket policy access with Sonia before retrying.
- Eight synthetic SQLite checks pass, including executing all five preparation statements with the sequence tables removed. These are not AWS results.

## Console verification — 2 Nov 2026

Evidence: [screenshot index](screenshots/README.md). Schema verified against both the 20-column result pasted in chat and the corrected `01-cleaned-schema.png`.

| Check | Observed result | Displayed runtime | Displayed data scanned |
|---|---|---|---|
| Cleaned schema | 20 columns, required fields/types present (pasted result and corrected screenshot) | Not supplied | Not supplied |
| HI, 2022-09-01 | 4,465,985 transactions, 1,056 laundering, 0 missing USD | 754 ms | 7.32 KB |
| Full HI | 31,898,218 total = 35,230 laundering + 31,862,988 legitimate; 2022-09-01–2022-09-28 | 1.384 sec | 91.46 KB |
| Full LI | 31,251,469 total = 16,041 laundering + 31,235,428 legitimate; 2022-09-01–2022-09-27 | 1.075 sec | 69.79 KB |
| Proposed BQ2 input names | Only transactions_clean visible among the three queried names | 460 ms | 0.04 KB |

Full HI/LI counts reconcile to Role 2's documented counts. Query execution IDs were not supplied. These are input-readiness checks, not BQ2 sequence findings. No full-dataset duplicate-ID or full-dataset missing-USD check is claimed from these screenshots.

Next action: confirm Ji Ning's actual table names/schema, HI/LI sequence availability, chain labels/windows and transaction membership. The absence of the proposed names is not proof the sequence job is unfinished. Do not execute dependent CTAS until the agreed interface and pre-CTAS validations pass.

## Role 5 v1 implementation alignment

Role 6 now consumes `any_laundering`, `final_txn_id` and ordered member arrays from the documented 21-column interface. Lift/rank and frozen HI rules are scoped by dataset, length and min_support (50/100/500). Targeting counts distinct final transfers against original clean labels/full denominators. Sankey uses one copy of pattern counts. Eleven synthetic SQL checks pass, including mixed positive chains ending in legitimate transfers, overlapping finals and separate lengths/cutoffs. No Athena sequence results, development-batch acceptance or native Tableau workbook is claimed. Array-specific Athena validation still requires actual-engine execution.

## Published development handoff — received 7 Nov 2026

Ji Ning reports the sample partition `bq2_sequences_dev / dataset=sample / start_date=2022-09-01` is published and has passed acceptance: 184,449 pairs, 33,931 triples, total 218,380. Counts query `4971124e-fee8-4fc4-8551-07af320896d4`, values query `ada6c118-152f-4619-9dc8-a533295ec722`, existence query `13bf9fdf-bd08-43a7-90e5-8332dae6ee39`, and lineage query `d4e7f074-9d57-4c12-81a8-13ad4860abd0` are reported SUCCEEDED with no value/member/link/source-ID errors. Exact 21-column schema and eight immutable-location Parquet files are reported verified.

These are teammate-provided acceptance results, not independently retrieved AWS results in this session. Role 6's generated sample-specific queries and [runbook](development/README.md) are ready. Own counts/schema checks were subsequently recorded below. CTAS execution, output reconciliation and Tableau evidence remain pending. No sample ranking or trade-off findings have been computed here.

## Role 6 development checks — 7 Nov 2026

Own console results supplied by Xiang Ying and inspected from the original screenshots:

- [Sequence count check](screenshots/06-dev-sequence-counts.png): 184,449 pairs and 33,931 triples, sum 218,380, matching the handoff. Displayed runtime 837 ms, queue 100 ms, scanned 32.72 KB; result reuse toggle off. Query ID not supplied.
- [Schema check](screenshots/07-dev-sequence-schema.png): all 21 column names, data types and ordinal positions match the v1 interface. Signed transaction IDs are bigint/array(bigint), timestamps array(timestamp(3)), labels integer/array(integer), string arrays array(varchar), and dataset/start_date varchar. Metrics/query ID not visible.
- [Source validation](screenshots/08-dev-source-validation.png): all 500,000 cleaned sample rows have distinct, non-null transaction IDs; 35 laundering labels, 0 invalid labels. Displayed runtime 1.191 sec, queue 99 ms, scanned 3.82 MB. Query ID not supplied. This check covers the full cleaned sample, without a start-date filter.
- Evidence copied byte-for-byte from Desktop to the role screenshot folder; colon-separated Desktop names were normalized to folder/filename paths.

These verify access, counts and catalog schema; they do not independently reproduce value or lineage acceptance. Source-ID uniqueness and label checks now pass for the cleaned sample. Next: inspect method-pattern frequencies using development/02_frequency.sql, retaining the sequence partition filters. No development CTAS, lift rankings or trade-off findings have been produced by these checks.


## Development pattern frequencies — 7 Nov 2026

[Own frequency screenshot](screenshots/09-dev-pattern-frequency.png) shows a completed query with 236 pattern rows, runtime 691 ms, queue 108 ms and 225.55 KB scanned. The first six visible pair patterns have counts: Reinvestment -> Reinvestment 44,822; Cash -> Cheque 29,245; Cash -> Credit Card 17,994; Cash -> Cash 16,196; Credit Card -> Reinvestment 12,288; Cheque -> Reinvestment 11,637. Full CSV and query ID were not supplied, so complete support totals and triple-pattern frequencies have not been reconciled from this image. These are occurrence counts, not laundering-risk rankings or final HI/LI findings.

Next: check that the three development destination tables are absent and their S3 prefixes empty before running development/03_lift.sql. No development CTAS execution is claimed.


## Development output-table check — 7 Nov 2026

[Own console evidence](screenshots/09a-dev-output-table-check.png) shows the destination-table lookup completed with zero rows for bq2_lift_dev, bq2_tradeoff_dev and bq2_sankey_dev in is459_g2t1_aml. Runtime 412 ms, queue 104 ms, data scanned displayed as a dash; query ID not supplied. SQL attribution is from the accompanying conversation. This establishes no matching tables were returned in the current session; S3 prefix emptiness remains unverified. Next: inspect all three proposed S3 destinations before CTAS.


## Development lift creation — 7 Nov 2026

Xiang Ying confirmed in chat that all three proposed development S3 output prefixes were empty before creation; this is a user-reported check, not a retrieved S3 listing. The preceding destination-table lookup returned no matching names.

[Own completion screenshot](screenshots/10a-dev-lift-created.png) shows the development lift CTAS completed successfully: runtime 1.454 sec, queue 108 ms, scanned 454.24 KB; result reuse toggle off. The walkthrough identifies the statement as development/03_lift.sql creating is459_g2t1_aml.bq2_lift_dev at s3://is459-g2t1-aml-processed-bucket/bq2_lift_dev/sample_20220901/. The crop does not display SQL or query ID; table attribution comes from the accompanying conversation.

Next: SELECT sample rankings and reconcile support totals by length for one cutoff. CTAS completion does not yet establish ranking values or output reconciliation. Trade-off/Sankey creation, Tableau views and final HI/LI findings remain pending.


## Sample lift inspection and trade-off creation — 7 Nov 2026

Inline screenshots supplied in chat show the min_support=100 ranking query returned 20 rows (runtime 472 ms, queue 99 ms, scanned 3.44 KB). Visible leading patterns: pair Cheque -> Wire, support 300, 1 positive sequence, lift 4.5883; triple Cash -> Cheque -> Wire, support 106, 9 positive sequences, lift 75.8141. Small positive counts and overlapping chains limit interpretation; these are in-sample associations, not final recommendations.

The subsequent inline reconciliation screenshot shows length 2: 184,449 sequences, 134 positive sequences; length 3: 33,931 sequences, 38 positive sequences. Runtime 733 ms, queue 104 ms, scanned 1.05 KB. Support totals match the published batch; positive sequences can share the same source transaction. These two inline images have not been copied to the repository because their local file paths were not supplied. No CSV or query IDs were supplied.

[Trade-off creation evidence](screenshots/11a-dev-tradeoff-created.png) shows successful completion of the walkthrough development/04_tradeoff.sql statement creating is459_g2t1_aml.bq2_tradeoff_dev. Runtime 2.369 sec, queue 99 ms, scanned 5.71 MB; query ID not supplied. Statement attribution comes from the accompanying conversation, since the screenshot omits the SQL. Next: inspect the eight min_support=100 policy rows and run development/06_reconciliation.sql. Trade-off values and reconciliation, Sankey outputs, Tableau and final HI/LI findings remain pending.


## Sample trade-off inspection and Sankey creation — 7 Nov 2026

Inline chat evidence shows eight min_support=100 trade-off rows. For pairs, top-3/5/10 reviewed 5,720/10,677/72,308 distinct final transactions and flagged 12/17/18 original positive transactions. For triples, corresponding review counts were 538/1,795/5,008 and each policy flagged 2 positives. Both broad rows reviewed all 500,000 source transactions, including 35 positives and 499,965 legitimate transactions. Runtime 460 ms, queue 94 ms, scanned 1.47 KB. These are in-sample integration results; pair and triple counts must not be added because their finals can overlap.

The subsequent inline reconciliation result returned zero defect rows (486 ms, queue 105 ms, 0.73 KB scanned), passing the checked accounting equalities and upper bounds in development/06_reconciliation.sql. This does not independently revalidate every join or all rate formulas. Neither inline image has a supplied local path, so neither is claimed copied to the repository; query IDs and CSVs were not supplied.

[Saved Sankey creation screenshot](screenshots/13a-dev-sankey-created.png) shows successful completion of the walkthrough development/05_sankey.sql creating is459_g2t1_aml.bq2_sankey_dev. Runtime 961 ms, queue 80 ms. The scan figure begins 2.55 K but its unit is cropped, so a complete scan measurement is not recorded. Query ID not supplied; statement attribution comes from conversation. Next: reconcile Sankey counts to 184,449/33,931 total and 134/38 positive sequences before Tableau import. Final HI/LI findings remain pending.


## Local Tableau sample exports validated — 7 Nov 2026

Read the three user-supplied CSV exports under the local BQ2_Tableau_Sample folder; CSVs remain outside Git. Lift export: 236 patterns (36 pairs, 200 triples), dataset sample and min_support 100 only. Sankey export: 236 unique complete paths, ordered methods match method_sequence, all per-pattern weights match lift, and positive plus legitimate weights equal total weights. Both exports reconcile to 184,449 pairs / 134 positive chains and 33,931 triples / 38 positive chains. The inline Athena Sankey reconciliation screenshot agrees (408 ms runtime, 95 ms queue, 0.82 KB scanned); no local screenshot path or query ID supplied.

Trade-off CSV: eight unique length/policy combinations, sample and min_support 100 only. Original population is 500,000 / 35 positive / 499,965 legitimate in every row. Reviewed counts reconcile to positive plus legitimate counts, and all four exported rate formulas match recomputation. These checks verify exported arithmetic and alignment, not independent source-lineage reconstruction. Next: import the lift CSV as a Tableau Text file; add trade-off and Sankey as separate data sources, without joins. Sample workbook and final HI/LI analysis remain pending.


## Sample targeted checklist — 8 November 2026

[Saved screenshot](../../viz/screenshots/20-sample-targeted-checklist.png) shows length 2 and Patterns to review 3, support 100, and a sample-only / LI-validation-pending status. Rows match the checked lift export: Cheque -> Wire (rank 1, support 300, 1 labelled sequence, 0.33%, lift 4.59); ACH -> ACH (rank 2, support 1,702, 5 labelled, 0.29%, lift 4.04); Cash -> Cheque (rank 3, support 29,245, 84 labelled, 0.29%, lift 3.95). The screenshot establishes rendered values for this selection, not every control combination or the exact saved calculation. No final recommendation or LI robustness is claimed. Next: confirm selector behaviour and assemble the sample dashboard.


## Combined sample pairs dashboard — 8 November 2026

[Saved dashboard evidence](../../viz/screenshots/21-sample-dashboard-pairs.png) combines pair trade-offs, pair Sankey, top-ten pair rankings and top-five checklist. Visible values agree with previously checked exports. The dashboard states development-only status and distinguishes minimum-support rankings/checklist, labelled sequence weights and distinct final-transaction trade-offs. Sequence-length control is hidden on this pairs-specific dashboard; Patterns to review remains visible at 5. Xiang Ying reports the preceding checklist top-3/5/10 and length tests passed. Filter mappings, saved workbook settings and tooltip behaviour were not independently replayed from this image.

Screenshot copied byte-for-byte from Desktop, original retained. Some headings/legend labels remain truncated and the checklist has a scrollbar. Current native workbook delivery/inspection and triples dashboard remain pending. No final HI/LI findings or LI robustness is claimed.


## Revised sample triples dashboard — 8 November 2026

[Revised Desktop screenshot](../../viz/screenshots/22-sample-dashboard-triples.png) copied byte-for-byte after Xiang Ying selected this replacement. Original retained. Visible triple results match checked exports: all targeted catch rates 5.71%, legitimate review rates 0.11%/0.36%/1.00%, top-ranked Cash -> Cheque -> Wire lift 75.81, and expected top-3 checklist. Three Sankey stages are present without a Null node. The dashboard states development-only status and distinct counting units. Some headings and long paths remain truncated. The image does not independently establish saved filter independence or all interactive behaviour.

At the preceding file check, Downloads/Big Data bq2 analysis.twbx contained only BQ2 Sample - Pairs. The active workbook may be saved elsewhere or contain unsaved changes: save and confirm its location before native-workbook delivery. Final HI/LI findings and LI robustness remain pending.


## Final input metadata checks — 9 November 2026

[Final schema screenshot](screenshots/23-final-sequence-schema.png) shows 21 expected names in the agreed order, with compatible types including varchar(2) for rule_version and varchar(9) for dataset. Runtime 634 ms, queue 99 ms, scanned 1.66 KB; schema-query ID not supplied. Screenshot copied byte-for-byte from Desktop.

User-reported SHOW PARTITIONS is459_g2t1_aml.bq2_sequences failed with “not a partitioned table”, query ff58d7e3-29b8-4504-9d00-ed0a3cdffaa2. This indicates the queried Catalog table is registered as unpartitioned, contrasting with the checked-in Athena generator's partitioned_by configuration; it does not establish a Step Functions failure or data loss. dataset/start_date column presence alone is not partition metadata. Next: SHOW CREATE TABLE to inspect actual LOCATION, partition declaration and properties; obtain Sonia's current table definition and completed-run acceptance evidence. No repair/rebuild, final CTAS or broad data scan was executed here. Do not assume date filters provide partition pruning until this discrepancy is resolved.


## Final publication view and underlying partitions — 9 November 2026

Subsequent user-supplied DDL resolves the earlier metadata error: bq2_sequences is a view selecting all columns from is459_g2t1_aml.bq2_sequences_a. The physical external table has 19 data columns and dataset/start_date partition keys, uses Snappy Parquet, and points to s3://is459-g2t1-aml-processed-bucket/bq2_sequences/. varchar(9) dataset and varchar(2) rule_version are compatible with the interface. The earlier SHOW PARTITIONS failure on the view does not establish unpartitioned underlying storage or a Step Functions failure. Keep Role 6 analytics on the published view and use the physical name for partition metadata only.

[Partition listing screenshot](screenshots/24-final-sequence-partitions.png) shows successful SHOW PARTITIONS on the underlying table, with HI-Medium and LI-Medium date entries. Runtime 958 ms, queue 174 ms, scanned displayed as a dash; query ID not supplied. Copied byte-for-byte from Desktop, original retained. A metadata summary should confirm exact distinct-date counts and boundaries instead of relying on visual counting. Partition presence does not certify pair/triple completion or acceptance. No final analytical CTAS or broad data scan was executed here.


## Final partition coverage discrepancy — 9 November 2026

[Own metadata result](screenshots/25-final-partition-coverage.png) from bq2_sequences_a$partitions: HI-Medium has 28 partitions and distinct dates from 2022-09-01 to 2022-09-28, matching expected date registration. LI-Medium has 25 partitions and distinct dates from 2022-09-01 to 2022-09-25. These 25 distinct dates exhaust that interval, so September 26 and 27 are not registered. Runtime 557 ms, queue 91 ms, displayed scan 0.98 KB; query ID not supplied. Original Desktop screenshot retained and repository copy verified byte-for-byte.

Do not infer missing transaction data or a failed pipeline solely from missing output partitions: dates with zero valid sequences may produce no files/partition. Ask Sonia for per-date build completion or zero-output evidence for LI September 26/27, plus final acceptance results. Do not silently shorten final LI evaluation to September 25 or remove September 26/27 transactions from the cleaned-population denominator. HI date registration alone does not certify both lengths or row-level acceptance. Final full-scope LI completeness remains unresolved; no repair or paid sequence scan was run here.
