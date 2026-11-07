# BQ2 analytics and targeting (Role 6)

Owner: Xiang Ying. Status: cleaned schema and HI/LI baselines verified from Xiang Ying's console results; screenshots saved. Ji Ning reports the sample development partition published and accepted; Role 6 integration checks can begin. Full HI/LI findings remain pending.

## What it does

Ranks payment-method sequences by laundering lift, compares top-3/5/10 rules with reviewing all transactions, and evaluates frozen HI rules on LI-Medium. Outputs support the ranked table, Sankey, trade-off chart and targeted checklist promised on proposal pages 8–9.

**Start now with the [accepted-sample development runbook](development/README.md).** Earlier source checks are in the [baseline walkthrough](start_now.md). The [Role 6 guide](role6_guide.md) explains the metric choices. [Input contract](input_contract.md) maps Ji Ning's documented 21-column v1 schema to this implementation; the actual development output is still pending verification.

## AWS resources

Existing resources: region `us-east-1`, database `is459_g2t1_aml`, Athena workgroup `primary`, staging `s3://is459-g2t1-aml-athena-bucket/results/`. No new IAM role, EC2 instance or Glue job is needed for Role 6.

## Inputs and outputs

| File | Reads | Proposed output table / processed-bucket prefix |
|---|---|---|
| `bq2_lift.sql` | `bq2_sequences` | `bq2_lift` / `bq2_lift/` |
| `tradeoff.sql` | lift, sequences (`final_txn_id`), `transactions_clean` | `bq2_tradeoff` / `bq2_tradeoff/` |
| `robustness.sql` | lift | `bq2_robustness` / `bq2_robustness/` |
| `sankey.sql` | lift | `bq2_sankey` / `bq2_sankey/` |

All tables use database `is459_g2t1_aml`. Output prefixes are under `s3://is459-g2t1-aml-processed-bucket/`. Input locations: [input_contract.md](input_contract.md).

## Current progress

- Implemented: v1 interface alignment, separate dataset/length lift, 50/100/500 support policies, distinct-final targeting, frozen-rule LI checks and nonduplicated Sankey counts.
- Twelve local analytical SQL regression tests pass; Athena array/lineage validation and actual integration are not yet executed.
- Completed console evidence: schema, one-day HI missing-USD check and full HI/LI baselines, with [saved screenshots](screenshots/README.md). Query IDs remain unrecorded. Earlier personal-account CLI failure does not describe team-console access.
- [Handoff and development mapping](input_contract.md) ready to share; no direct message or deployed builder is claimed.
- [Four-view Tableau plan](../../viz/bq2_tableau.md) prepared; native workbook and real charts remain pending.
- Ji Ning reports the development partition accepted: 218,380 sequences (184,449 pairs; 33,931 triples). Her four acceptance query IDs are recorded in the development runbook; these are teammate evidence, not Role 6 reruns.
- Ready for Role 6: pinned sample queries and isolated `_dev` destinations, generated from the production SQL with regression checks.
- Own development checks passed: counts reconcile (184,449 pairs; 33,931 triples) and all 21 names/types match v1. Original screenshots 06/07 are saved; count query ran in 837 ms and scanned 32.72 KB.
- Own cleaned-sample source check passed: 500,000 rows and unique IDs, 35 laundering labels, no invalid labels or null IDs. Screenshot 08 saved; runtime 1.191 sec, scanned 3.82 MB.
- Own sample pattern-frequency query completed: 236 result rows, runtime 691 ms, scanned 225.55 KB; screenshot 09 saved. Full frequency totals remain unreconciled because only the first page was supplied.
- Pending: development destination checks, remaining integration validation as needed, development analytical outputs, Tableau charts and full HI/LI publication.

## How to run

1. Inspect actual v1 catalog schema and publication evidence, then run the pre-CTAS statements of `validation.sql` separately (the last check runs after CTAS). Stop for failed checks.
2. Use Athena engine 3 and the database/workgroup above. Confirm the workgroup permits CTAS with explicit `external_location`.
3. First run `frequency.sql` and agree the primary cutoff from outcome-blind frequency evidence. Then run `bq2_lift.sql`, then `tradeoff.sql`, `robustness.sql`, `sankey.sql`, one statement per execution. Each file contains one CTAS. All three 50/100/500 support policies are produced together; choose the main cutoff from frequency evidence, not the best lift. Filter `min_support` and `sequence_length` when reading policy outputs.
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
| Depends on | Validated v1 HI/LI sequence occurrences and both cleaned partitions (no membership table) |
| Runtime | Not measured; capture time and bytes scanned on real runs |

After fetching current main, the checked-in state machine has live ETL/BQ1 tasks; BQ2 remains Pass placeholders. These files do not update the deployed state machine. Gate the branch on quality-check success; do not blindly retry CTAS against fixed prefixes. The existing `is459-g2t1-reset-outputs` Lambda is reusable for a coordinated refresh; ask Sonia to register only the four Role 6 destinations, preserving existing outputs. She must prevent concurrent writers and ensure both HI/LI inputs are ready. Alternatively use consistent per-run names/locations. Claim automation only after a real successful execution.

## Contribution summary

Implemented v1-aligned sequence lift by length/support policy, distinct-final targeting, frozen-rule LI robustness and Tableau aggregate inputs, with lineage validation and twelve synthetic regression checks. Real-data execution, findings and Tableau workbook remain pending.

## Platform references

- [AWS CTAS destination rules](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html)
- [Athena integration in Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/connect-athena.html)

Array validation references: [AWS UNNEST](https://docs.aws.amazon.com/athena/latest/ug/flattening-arrays.html), [array reductions](https://docs.aws.amazon.com/athena/latest/ug/arrays-and-aggregation.html), and [Athena SELECT / ordinality](https://docs.aws.amazon.com/athena/latest/ug/select.html). These references do not replace execution against the development table.
