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
