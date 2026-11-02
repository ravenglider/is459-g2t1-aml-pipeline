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

Evidence: [screenshot index](screenshots/README.md). Schema verified against both the 20-column result pasted in chat and the corrected `01-cleaned-schema.png`. The earlier incorrectly named console-home image is retained separately as `00-personal-console-home.png`.

| Check | Observed result | Displayed runtime | Displayed data scanned |
|---|---|---|---|
| Cleaned schema | 20 columns, required fields/types present (pasted result and corrected screenshot) | Not supplied | Not supplied |
| HI, 2022-09-01 | 4,465,985 transactions, 1,056 laundering, 0 missing USD | 754 ms | 7.32 KB |
| Full HI | 31,898,218 total = 35,230 laundering + 31,862,988 legitimate; 2022-09-01–2022-09-28 | 1.384 sec | 91.46 KB |
| Full LI | 31,251,469 total = 16,041 laundering + 31,235,428 legitimate; 2022-09-01–2022-09-27 | 1.075 sec | 69.79 KB |
| Proposed BQ2 input names | Only transactions_clean visible among the three queried names | 460 ms | 0.04 KB |

Full HI/LI counts reconcile to Role 2's documented counts. Query execution IDs were not supplied. These are input-readiness checks, not BQ2 sequence findings. No full-dataset duplicate-ID or full-dataset missing-USD check is claimed from these screenshots.

Next action: confirm Ji Ning's actual table names/schema, HI/LI sequence availability, chain labels/windows and transaction membership. The absence of the proposed names is not proof the sequence job is unfinished. Do not execute dependent CTAS until the agreed interface and pre-CTAS validations pass.
