# Start Role 6 now: numbered walkthrough

> **Historical (10 November 2026):** completed on 2 November 2026. Kept as a record of the pre-Role-5 checks.

This walkthrough needs only Role 2's cleaned data. Do these steps in order. The SQL is in [start_now.sql](start_now.sql). It does not create tables, run Glue, reset S3 folders or depend on Role 5. Xiang Ying subsequently completed the console checks on 2 Nov 2026. See [verified results](results.md#console-verification--2-nov-2026) and [saved screenshots](screenshots/README.md). The earlier CLI error used a different personal account.

## 1. Open the right GitHub branch

Open the repository and select `main`, then `bq2/lift/`. The reviewed Role 6 preparation is published on main. Read this walkthrough first; `role6_guide.md` explains the metric choices in more detail.

## 2. Open Athena

Sign in to the team's AWS account using your existing login. Search for **Athena**, open its query editor and select:

| Setting | Value |
|---|---|
| Region | US East (N. Virginia), `us-east-1` |
| Workgroup | `primary` |
| Data source/catalog | `AwsDataCatalog` |
| Database | `is459_g2t1_aml` |
| Existing results location | `s3://is459-g2t1-aml-athena-bucket/results/` |

Use the existing workgroup configuration. If the database is missing or access is denied, check the account/region and ask Sonia about your access. Do not create a new database or recreate Role 2's table. These steps follow [AWS's Athena getting-started guidance](https://docs.aws.amazon.com/athena/latest/ug/getting-started.html), using your team's existing resources rather than the tutorial's sample resources.

## Historical CLI access issue (team-console checks subsequently succeeded)

The earlier CLI login used the personal account, whereas Xiang Ying used the team console for successful queries. The CLI attempt was rejected before execution:

```text
Unable to verify/create output bucket is459-g2t1-aml-athena-bucket
AthenaErrorCode: INSUFFICIENT_PERMISSIONS
```

Only if the error also occurs in the correct team account, use this message (not sent):

> My Role 6 first Athena query could not start: `Unable to verify/create output bucket is459-g2t1-aml-athena-bucket` / `INSUFFICIENT_PERMISSIONS`. I used us-east-1, primary, database is459_g2t1_aml and the documented staging prefix results/. The CLI login authenticates and primary is readable, but no query ID was issued. Could you confirm I am using the correct team account/role and check that role's result-bucket access and bucket policy? I have not changed IAM, buckets or the working pipeline.

Use the authorized team account/role in the console. If the same error occurs there, resolve it with Sonia before retrying. Do not create a replacement bucket or change shared policies just to bypass this error. [AWS's S3 access troubleshooting](https://docs.aws.amazon.com/athena/latest/ug/troubleshooting-athena.html) covers Athena output-bucket permissions. Once access works, continue with the five queries below.

## 3. Run query 1: check the cleaned columns

Open `start_now.sql` on GitHub. Copy only numbered query 1 into Athena and click **Run**. Do not paste and execute all five statements together.

Check that `txn_id`, `txn_date`, `dataset`, `is_laundering`, `payment_format`, `amount_paid_usd`, `amount_received_usd`, and `usd_missing` exist. Compare types with `etl/schema_transactions_clean.md`. `txn_id` should be bigint; `is_laundering` an integer; `dataset`/`txn_date` strings. Stop if important columns are missing and resolve the schema with Amareet. No data has been scanned for your analytical results yet: this is metadata inspection. [AWS column-metadata reference](https://docs.aws.amazon.com/athena/latest/ug/querying-glue-catalog-listing-columns.html).

## 4. Run query 2: check what inputs are visible

Run numbered query 2. `transactions_clean` should appear. The proposed `bq2_sequences` and `bq2_sequences_dev` may not appear yet. That does not stop the remaining queries in this file. A missing metadata row can also reflect permissions or a different table name; confirm with Ji Ning rather than assuming their work is unfinished.

If Role 5 tables appear, get their schema and rules before executing the prepared sequence SQL. Existing names alone do not prove schema compatibility.

## 5. Run query 3: try one HI day

Run numbered query 3. It filters `dataset = 'HI-Medium'` and `txn_date = '2022-09-01'` to limit the scan.

Expected checks: transactions > 0, invalid_labels = 0, null_txn_ids = 0, usd_flag_issues = 0. Laundering count may be small or zero on a selected day; that alone is not a failure. A zero transaction count needs a partition/date/access check. Do not compare this one-day count with 31.9 million.

After success, record the query execution ID, execution time and data scanned in the evidence table below. Keep an aggregate-results screenshot locally or in `bq2/lift/screenshots/`; do not include account IDs, credentials or transaction/account-level rows.

## 6. Run query 4: reconcile all HI data

Run numbered query 4. It intentionally has no date filter so its scope matches the complete HI dataset. Compare with the team/repository reference:

| HI check | Reference from Role 2, not a new execution |
|---|---:|
| Transactions | 31,898,218 |
| Laundering transactions | 35,230 |
| Legitimate transactions | 31,862,988 |
| Date bounds | 2022-09-01 through 2022-09-28 |

`transaction_laundering_rate` is a fraction; multiply by 100 for a percentage. It is **transaction context**, not the sequence-lift baseline. Counts should reconcile: transactions = laundering + legitimate. If counts differ, record the actual result and ask Amareet whether the table was refreshed; do not change SQL to force the reference numbers.

## 7. Run query 5: reconcile all LI data

Run numbered query 5 separately. Reference from `etl/data_quality.md`: 31,251,469 transactions, 16,041 laundering transactions, 31,235,428 legitimate transactions, dates 2022-09-01 through 2022-09-27. Treat those as documented references until verified in your account. Record actual outputs and query details. Never pool HI and LI into one laundering-rate denominator.

## 8. Get Role 5's development batch

Ji Ning has documented the 21-column v1 schema, and Role 6 SQL is aligned to it. Send the ready-to-share message in [input_contract.md](input_contract.md). Ask for the actual development table/schema, original dataset value, source coverage, builder/rule version and successful publication evidence. Ordered transaction membership is `txn_ids`, and targeting uses distinct `final_txn_id` values with original clean-source labels.

Follow the isolated development mapping in that contract before running validations. No physical membership table is required. A sample-only batch tests the interface, not full HI/LI robustness or final rankings.

## 9. Prepare the Tableau layout

Use [viz/bq2_tableau.md](../../viz/bq2_tableau.md) to plan four sheets: ranked sequence table, Sankey, policy trade-off and targeted checklist. You can decide titles, layout and colours now. Real fields/charts wait for actual sequence aggregates; do not enter the proposal's illustrative patterns or percentages as findings.

## 10. Record what happened and update GitHub

Fill this table only after actual runs. The README currently records preparation, not completed AWS checks.

| Query | Actual outcome / counts | Query execution ID | Time | Bytes scanned |
|---|---|---|---|---|
| 1: cleaned schema | 20 columns confirmed from pasted result and corrected screenshot | Not supplied | Not supplied | Not supplied |
| 2: visible inputs | Only transactions_clean among queried names | Not supplied | 460 ms | 0.04 KB |
| 3: one-day HI check | 4,465,985 transactions, 1,056 laundering, 0 missing USD (simplified console query) | Not supplied | 754 ms | 7.32 KB |
| 4: full HI baseline | Counts/date bounds match Role 2 | Not supplied | 1.384 sec | 91.46 KB |
| 5: full LI baseline | Counts/date bounds match Role 2 | Not supplied | 1.075 sec | 69.79 KB |

Add these verified results to `results.md` and change the README's progress status only for steps actually completed. Commit those documentation changes and push the Role 6 working branch, then review it against current main before merging. If you share the aggregate outputs and query details in this chat, they can be interpreted and recorded here without exposing AWS credentials.

**Stop point:** once these checks and the handoff request are complete, you have finished the useful pre-Role-5 execution work in this walkthrough. Do not run `bq2_lift.sql`, `tradeoff.sql`, `robustness.sql` or `sankey.sql` until Role 5's inputs and label/window rules are confirmed and the pre-CTAS checks in `validation.sql` pass. The final validation statement runs after the analytics tables exist.
