# BQ2 analytics and targeting (Role 6)

Owner: Xiang Ying. Status: SQL and synthetic checks prepared; not deployed or run on AWS.

## What it does

Ranks payment-method sequences by laundering lift, compares top-3/5/10 rules with reviewing all transactions, and evaluates frozen HI rules on LI-Medium. Outputs support the ranked table, Sankey, trade-off chart and targeted checklist promised on proposal pages 8–9.

**Start with [Role 6 guide](role6_guide.md).** Role 5 has no committed schema yet. [Input contract](input_contract.md) is a proposal for Ji Ning to confirm, not documentation of an existing table.

## AWS resources

Existing resources: region `us-east-1`, database `is459_g2t1_aml`, Athena workgroup `primary`, staging `s3://is459-g2t1-aml-athena-bucket/results/`. No new IAM role, EC2 instance or Glue job is needed for Role 6.

## Inputs and outputs

| File | Reads | Proposed output table / processed-bucket prefix |
|---|---|---|
| `bq2_lift.sql` | `bq2_sequences` | `bq2_lift` / `bq2_lift/` |
| `tradeoff.sql` | lift, sequences, membership, `transactions_clean` | `bq2_tradeoff` / `bq2_tradeoff/` |
| `robustness.sql` | lift | `bq2_robustness` / `bq2_robustness/` |
| `sankey.sql` | lift | `bq2_sankey` / `bq2_sankey/` |

All tables use database `is459_g2t1_aml`. Output prefixes are under `s3://is459-g2t1-aml-processed-bucket/`. Input locations: [input_contract.md](input_contract.md).

## How to run

1. Confirm Ji Ning's contract and run the pre-CTAS statements of `validation.sql` separately (the last check runs after CTAS). Stop for failed checks.
2. Use Athena engine 3 and the database/workgroup above. Confirm the workgroup permits CTAS with explicit `external_location`.
3. Run `bq2_lift.sql`, then `tradeoff.sql`, `robustness.sql`, `sankey.sql`, one statement per execution. Each file contains one CTAS. `100` is a provisional minimum count, not a class requirement. Agree it before viewing rankings; sensitivity-check 50/100/500 in separate runs.
4. Inspect with explicit `ORDER BY`; Parquet has no guaranteed row order. Connect aggregate outputs to Tableau using [viz guide](../../viz/bq2_tableau.md).
5. Record measured results and query IDs in `results.md`. Do not substitute proposal illustration numbers.

First run: destination tables must not exist and prefixes must be empty. For reruns, prefer changing **all output table names/references and prefixes** to a new consistent run suffix, preserving old results. CTAS does not overwrite existing destinations; dropping a table does not remove S3 data. Do not automate shared-prefix deletion. Never run concurrent refreshes against the same destinations. Failed CTAS can leave files; retry with a fresh prefix.

Local verification: `python3 -m unittest discover -s bq2/lift/tests -v`. Tests execute portable lift, trade-off, robustness and Sankey SQL against synthetic SQLite fixtures. They do not certify Athena syntax, IAM, S3 or real-data quality.

## Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | Athena `arn:aws:states:::athena:startQueryExecution.sync` |
| Exact SQL | One CTAS per file above, in that order |
| Context | Database `is459_g2t1_aml`, workgroup `primary` |
| Output | Four tables/prefixes above |
| Depends on | Validated HI and LI sequences and membership; both cleaned partitions |
| Runtime | Not measured; capture time and bytes scanned on real runs |

After fetching current main, the checked-in state machine has live ETL/BQ1 tasks; BQ2 remains Pass placeholders. These files do not update the deployed state machine. Gate the branch on quality-check success; do not blindly retry CTAS against fixed prefixes. The existing `is459-g2t1-reset-outputs` Lambda is reusable for a coordinated refresh; ask Sonia to register only the four Role 6 destinations, preserving existing outputs. She must prevent concurrent writers and ensure both HI/LI inputs are ready. Alternatively use consistent per-run names/locations. Claim automation only after a real successful execution.

## Contribution summary

Prepared Athena sequence lift, distinct-transaction targeting evaluation, frozen-rule LI robustness and Tableau aggregate inputs, with a Role 5 contract and synthetic SQL checks. Real-data execution, findings and Tableau workbook remain pending.

## Platform references

- [AWS CTAS destination rules](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html)
- [Athena integration in Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/connect-athena.html)
