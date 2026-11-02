# BQ2 execution record

Status: preparation checked locally; first AWS query submission attempted on 2 Nov 2026 but rejected before execution. No real-data BQ2 ranking, percentages or recommendation is claimed. Proposal pages 8–9 contain illustrations, not results.

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
