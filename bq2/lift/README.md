# BQ2 analytics and targeting (Role 6)

Owner: Xiang Ying. Status (9 November 2026): final Athena analytics and three Tableau dashboards completed and checked. Xiang Ying confirms all three dashboards load correctly after saving, closing and reopening. A credential-free [native workbook](../../viz/BQ2_Final_HI_LI_Analysis.twb) is included; reopening this sanitized copy and automation handoff remain pending.

## What it does

Ranks payment-method sequences by laundering lift, compares top-3/5/10 rules with reviewing all transactions, and evaluates frozen HI rules on LI-Medium. Outputs support the ranked table, Sankey, trade-off chart and targeted checklist promised on proposal pages 8–9.

Final execution and findings are recorded in [results](results.md). The [sample development runbook](development/README.md) and [baseline walkthrough](start_now.md) retain earlier checks. The [Role 6 guide](role6_guide.md) explains the metrics, and the [input contract](input_contract.md) documents the 21-column sequence interface.

## AWS resources

Existing resources: region `us-east-1`, database `is459_g2t1_aml`, Athena workgroup `primary`, staging `s3://is459-g2t1-aml-athena-bucket/results/`. No new IAM role, EC2 instance or Glue job is needed for Role 6.

## Inputs and outputs

| File             | Reads                                                  | Proposed output table / processed-bucket prefix |
| ---------------- | ------------------------------------------------------ | ----------------------------------------------- |
| `bq2_lift.sql`   | `bq2_sequences`                                        | `bq2_lift` / `bq2_lift/`                        |
| `tradeoff.sql`   | lift, sequences (`final_txn_id`), `transactions_clean` | `bq2_tradeoff` / `bq2_tradeoff/`                |
| `robustness.sql` | lift                                                   | `bq2_robustness` / `bq2_robustness/`            |
| `sankey.sql`     | lift                                                   | `bq2_sankey` / `bq2_sankey/`                    |

All tables use database `is459_g2t1_aml`. Output prefixes are under `s3://is459-g2t1-aml-processed-bucket/`. Input locations: [input_contract.md](input_contract.md).

## Completed dashboard checks

- Final HI pairs and triples dashboards use separate Athena aggregate sources: bq2_lift, bq2_tradeoff and bq2_sankey. The robustness dashboard uses bq2_robustness.
- HI worksheets retain dataset=HI-Medium and their fixed sequence lengths (2 or 3). Lift, trade-off and robustness sources use min_support=100; Sankey reads one complete set of sequence counts.
- Ranked leaders, trade-off rates and robustness values match the validated exports. Sankey positive occurrence totals reconcile to 1,274,451 HI pairs and 2,344,593 HI triples.
- Final [pairs dashboard](../../viz/screenshots/37-final-hi-dashboard-pairs.png) and corrected [triples dashboard](../../viz/screenshots/38-final-hi-dashboard-triples.png) captured; triples support counts and all five selected checklist rows are visible.
- Robustness filter states [2](../../viz/screenshots/35-final-robustness-pairs.png) and [3](../../viz/screenshots/36-final-robustness-triples.png) captured. LI lift remains above 1 for 10/10 HI-selected pairs and 8/10 triples.
- Count/rate formatting, final-data labels and tooltip population notes reviewed. Some dashboard headings are abbreviated and small Sankey nodes have no visible labels.
- Xiang Ying confirms save/close/reopen succeeded for all three dashboards. This is user-reported native-app acceptance; the final workbook file has not yet been inspected by the assistant.

Remaining: credential-free native workbook packaging/delivery and the validated two-stage SQL handoff for Sonia's automation. No deployment is claimed.

## Earlier progress log

The dated history below records what was pending at each earlier stage; the completed checks above are the current status.

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
- Development destination-table lookup returned zero rows for all three proposed names; screenshot 09a saved. S3 output-prefix emptiness remains unverified.
- User confirmed all three S3 development destinations empty before CTAS. Development lift creation succeeded (screenshot 10a): runtime 1.454 sec, scanned 454.24 KB.
- Sample lift support totals reconcile: 184,449 pairs (134 positive sequences), 33,931 triples (38 positive sequences), as shown in inline chat evidence. Development trade-off CTAS succeeded; screenshot 11a saved (2.369 sec, 5.71 MB scanned).
- Sample trade-off inspection returned eight rows; accounting reconciliation returned zero defects (inline chat evidence). Development Sankey creation succeeded; screenshot 13a saved (961 ms).
- All three local Tableau sample CSV exports checked: lift and Sankey path counts/weights agree, and eight trade-off policies reconcile with source denominators and all exported rate formulas. CSVs remain outside Git.
- Pending: remaining integration validation as needed, sample Tableau workbook/charts and full HI/LI publication.

## How to run

1. Inspect actual v1 catalog schema and publication evidence, then run the pre-CTAS statements of `validation.sql` separately (the last check runs after CTAS). Stop for failed checks.
2. Use Athena engine 3 and the database/workgroup above. Confirm the workgroup permits CTAS with explicit `external_location`.
3. First run `frequency.sql` and agree the primary cutoff from outcome-blind frequency evidence. Then run `bq2_lift.sql`, then `tradeoff.sql`, `robustness.sql`, `sankey.sql`, one statement per execution. Each file contains one CTAS. All three 50/100/500 support policies are produced together; choose the main cutoff from frequency evidence, not the best lift. Filter `min_support` and `sequence_length` when reading policy outputs.
4. Inspect with explicit `ORDER BY`; Parquet has no guaranteed row order. Connect aggregate outputs to Tableau using [viz guide](../../viz/bq2_tableau.md).
5. Record measured results and query IDs in `results.md`. Do not substitute proposal illustration numbers.

First run: destination tables must not exist and prefixes must be empty. For reruns, prefer changing **all output table names/references and prefixes** to a new consistent run suffix, preserving old results. CTAS does not overwrite existing destinations; dropping a table does not remove S3 data. Do not automate shared-prefix deletion. Never run concurrent refreshes against the same destinations. Failed CTAS can leave files; retry with a fresh prefix.

Local verification: `python3 -m unittest discover -s bq2/lift/tests -v`. Tests execute portable lift, trade-off, robustness and Sankey SQL against synthetic SQLite fixtures. They do not certify Athena syntax, IAM, S3 or real-data quality.

## Step Functions info for Sonia

| Field      | Value                                                                                     |
| ---------- | ----------------------------------------------------------------------------------------- |
| Job type   | Athena `arn:aws:states:::athena:startQueryExecution.sync`                                 |
| Exact SQL  | One CTAS per file above, in that order                                                    |
| Context    | Database `is459_g2t1_aml`, workgroup `primary`                                            |
| Output     | Four tables/prefixes above                                                                |
| Depends on | Validated v1 HI/LI sequence occurrences and both cleaned partitions (no membership table) |
| Runtime    | Not measured; capture time and bytes scanned on real runs                                 |

After fetching current main, the checked-in state machine has live ETL/BQ1 tasks; BQ2 remains Pass placeholders. These files do not update the deployed state machine. Gate the branch on quality-check success; do not blindly retry CTAS against fixed prefixes. The existing `is459-g2t1-reset-outputs` Lambda is reusable for a coordinated refresh; ask Sonia to register only the four Role 6 destinations, preserving existing outputs. She must prevent concurrent writers and ensure both HI/LI inputs are ready. Alternatively use consistent per-run names/locations. Claim automation only after a real successful execution.

## Pipeline automation (Role 1)

The Role 6 SQL now runs inside `is459-g2t1-pipeline`, in the BQ2 branch straight after `BQ2_Publish`, so it always reads the sequence copy that has just passed every gate. `bq2/sequences/athena/build_asl.py` copies the statements from the files above into the state machine at build time; edit the SQL here and rebuild, never the JSON.

| State                          | What it runs                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BQ2_Lift_Reset`               | `is459-g2t1-reset-outputs` drops `bq2_lift`, `bq2_tradeoff_flagged`, `bq2_tradeoff`, `bq2_robustness`, `bq2_sankey` and empties their folders                                                                                                                                                                                                                        |
| `BQ2_Lift`                     | `bq2_lift.sql`                                                                                                                                                                                                                                                                                                                                                       |
| `BQ2_Flagged_HI2/HI3/LI2/LI3`  | trade-off Stage 1 from `recovery/bq2_tradeoff_two_stage.sql`: one CTAS, then three INSERTs, one at a time                                                                                                                                                                                                                                                            |
| `BQ2_Tradeoff`                 | `recovery/tradeoff_stage2.sql`                                                                                                                                                                                                                                                                                                                                       |
| `BQ2_Robustness`, `BQ2_Sankey` | `robustness.sql`, `sankey.sql`                                                                                                                                                                                                                                                                                                                                       |
| `BQ2_Lift_Check`               | `pipeline_gate.sql`: lift and Sankey totals equal `bq2_sequences` per dataset and length, 12 lift groups, no duplicate ranks, every expected Stage 1 group present with no duplicate final IDs and ranks 1 to 10, 48 trade-off rows with zero accounting errors, one robustness row per HI top-10 pattern. Any failure stops the run with the check row as the cause |

Reruns are safe because the reset runs first, so the INSERTs never append to an earlier run's rows. Athena steps are retried only when Athena refuses to start the query. `lift_only.asl.json` is the same chain as a small state machine for a quick test. `tests/pipeline_chain_check.py` runs the exact embedded statements on synthetic data in DuckDB, checks the gate passes, and checks that nine corruptions (duplicate INSERT, missing Stage 1 group, lost rows, accounting error, changed sequences, bad rank) are all blocked.

Trade-off: during the few minutes the chain runs, the four Tableau sources are being rebuilt, so refresh the dashboards after the run succeeds.

## Contribution summary

Implemented v1-aligned sequence lift by length/support policy, distinct-final targeting, frozen-rule LI robustness and Tableau aggregate inputs, with lineage validation and twelve synthetic regression checks. Real-data execution, findings and Tableau workbook remain pending.

## Platform references

- [AWS CTAS destination rules](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html)
- [Athena integration in Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/connect-athena.html)

Array validation references: [AWS UNNEST](https://docs.aws.amazon.com/athena/latest/ug/flattening-arrays.html), [array reductions](https://docs.aws.amazon.com/athena/latest/ug/arrays-and-aggregation.html), and [Athena SELECT / ordinality](https://docs.aws.amazon.com/athena/latest/ug/select.html). These references do not replace execution against the development table.

Sample Tableau progress (8 November 2026): base worksheets and combined pairs/triples dashboards rendered, with nine screenshots and business descriptions in [viz/screenshots](../../viz/screenshots/README.md). The checklist top-3 pairs match the lift export. Xiang Ying reports checklist control tests passed; the dashboard shows the expected top-5 pairs. Current native workbook save/inspection/delivery and final HI/LI analysis remain pending.

Final input check (9 November 2026): 21-column schema is compatible. The published bq2_sequences name is a view over partitioned bq2_sequences_a; the initial SHOW PARTITIONS error was against the view, not a storage failure. Physical partition listing succeeded (screenshot 24). Exact date-coverage summary and final acceptance evidence remain to be confirmed; see [results](results.md#final-publication-view-and-underlying-partitions--9-november-2026).

Final date coverage (9 November 2026): metadata confirms HI 28/28 expected dates. LI has 25 dates ending September 25; September 26/27 need owner confirmation as validated empty outputs or pending publication. Full-scope LI completeness is not yet established; retain the full agreed cleaned-source denominator. Screenshot 25 and details are in [results](results.md#final-partition-coverage-discrepancy--9-november-2026).

Final-run readiness update (9 November 2026): Sonia reports gated publication passed and LI September 26/27 are validated empty sequence dates; formal acceptance receipts remain to be linked. Own final output-name lookup returned zero rows (screenshot 26). Next: confirm final S3 output destinations are empty, then execute production Role 6 SQL against bq2_sequences.

Final lift created (9 November 2026): production bq2_lift CTAS succeeded, 6.882 sec and 1.74 GB scanned (screenshot 27), after user-confirmed empty final output destinations. Final lift reconciliation/rankings, trade-off/robustness/Sankey tables and final Tableau updates remain pending.

Final lift totals checked (9 November 2026): all four HI/LI length groups present and baseline arithmetic matches the displayed counts. Counts are recorded in [results](results.md#final-lift-totals--9-november-2026); formal upstream acceptance comparison remains pending receipt availability. Next: production tradeoff.sql; final robustness/Sankey outputs and Tableau updates still pending.

## Full-scale trade-off recovery

The single tradeoff.sql CTAS exhausted resources at actual scale. Use the [reviewed two-stage files and runbook](recovery/README.md) for this final run; retain tradeoff.sql as the reference calculation and sample-generator input. Normal Stage 1 completed and helper check passed (screenshot 29). Next: recovery/tradeoff_stage2.sql after final-destination checks. Thirteen local regression tests pass, including normal/weekly equivalence. Actual Stage 2 execution and final reconciliation remain pending. Automation must account for helper lifecycle, failure recovery and source stability rather than invoking the original heavy CTAS.

Final trade-off Stage 2 succeeded (9 November 2026): 14.614 sec, 905.07 MB scanned; screenshot 30 saved. Normal two-stage recovery succeeded; weekly fallback is unnecessary. Final trade-off reconciliation and interpretation, robustness/Sankey CTAS and final Tableau updates remain pending.

Final trade-off accounting check passed: 12 groups × 4 policy rows, zero accounting errors ([evidence](screenshots/31-final-tradeoff-reconciliation.png)). Final value inspection, robustness and final Tableau updates remain pending.

Final min_support=100 trade-off CSV validated: 16 policy rows, 64 rate formulas, expected populations and monotonic review/flag counts. See [results](results.md). Final pattern-level robustness, Sankey and Tableau updates remain pending.

Final robustness export checked at support 100: 20 HI-selected patterns; LI lift remains above 1 for 10/10 pairs and 8/10 triples. All LI rates/lifts recomputed; see [results](results.md). Final lift cross-comparison, Sankey and Tableau updates remain pending.

Final Sankey CTAS completed ([screenshot 33](screenshots/33-final-sankey-created.png)); grouped reconciliation and final CSV export remain pending.

Final Sankey grouped reconciliation passed for all four HI/LI and pair/triple scopes ([screenshot 34](screenshots/34-final-sankey-reconciliation.png)). Final Sankey/lift CSV exports and Tableau updates remain pending.

Final Sankey CSV validated: 522 unique rows, correct ordered method fields and reconciled counts across all four scopes. Final lift CSV and Tableau updates remain pending.

All four final support-100 CSV exports have now been checked: lift, trade-off, robustness and Sankey. Lift rankings/formulas and the cross-export comparisons passed. Final Tableau updates, packaging and automation handoff remain pending.

Final Tableau HI–LI robustness dashboard captured for [pairs](../../viz/screenshots/35-final-robustness-pairs.png) and [triples](../../viz/screenshots/36-final-robustness-triples.png). Visible values match the checked export. Final HI dashboard screenshots, native workbook save/reopen verification and automation handoff remain pending.

Final HI [pairs](../../viz/screenshots/37-final-hi-dashboard-pairs.png) and [triples](../../viz/screenshots/38-final-hi-dashboard-triples.png) dashboards captured. The corrected triples capture resolves Support Count widths and checklist visibility. Xiang Ying confirms save/reopen passed; native-workbook inspection/delivery and automation handoff remain pending.

## Native Tableau workbook

[Open BQ2_Final_HI_LI_Analysis.twb](../../viz/BQ2_Final_HI_LI_Analysis.twb). It contains nine worksheets and three dashboards, connected to four final Athena aggregate sources. The GitHub copy clears saved AWS access-key IDs and removes three unused sample CSV sources with personal Downloads paths. XML checks confirm all worksheet/dashboard definitions and retained final-source metadata are unchanged except cleared connection usernames; no nonempty password/secret/session-token attributes or AWS access IDs remain. The Downloads original is unchanged. Recipients must enter their own authorised AWS credentials. Xiang Ying verified the original workbook reopens; this sanitized copy still needs a Tableau reopen check. Keep the packaged .twbx separately for submission.
