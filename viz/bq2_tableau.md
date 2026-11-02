# BQ2 Tableau build guide — Xiang Ying

Status: four-sheet dashboard layout and aggregate SQL prepared; real findings and a native Tableau workbook remain pending. Native Tableau UI access was unavailable in this session; a workbook has not been created.

## Connection

Use the team's Athena connector/driver with region `us-east-1`, database `is459_g2t1_aml`, workgroup `primary`, and staging `s3://is459-g2t1-aml-athena-bucket/results/`. Use your authorized AWS login and the Tableau connection dialog; do not put keys in the workbook or Git. Read each aggregate table as a separate data source, avoiding joins between pattern and transaction-policy grains (they multiply counts).

## Ranked patterns and targeted checklist

Source `bq2_lift`. Filter one dataset, one `sequence_length`, one `min_support`, `eligible = 1`, non-null rank. Rows: method sequence. Columns: lift, support count, laundering count, laundering rate, baseline rate; sort risk rank ascending. Show length and cutoff. Lift is a multiplier; rates use percentage format. For an actionable checklist, filter HI rank <= selected 3/5/10 and show exact ordered methods, support, HI lift and LI status from `bq2_robustness`. Do not hard-code the proposal's example sequences. Rules use method/length only, never the true label.

## Trade-off chart

Source `bq2_tradeoff`. Filter exactly one dataset, one `sequence_length` and one `min_support`. Use policy on columns and Measure Values on rows with `laundering_catch_rate` and `legitimate_review_rate`, grouped bars, axis fixed 0–100%, visible labels. Order top-3, top-5, top-10, review-all. Include reviewed count, precision and actual_patterns in tooltips/table. Add a second view with legitimate-review rate on X and catch rate on Y if it helps compare budgets. Do not label legitimate-review rate as false-discovery rate: the denominator differs.

Review-all is 100%/100% where populations exist. NULL rates mean the denominator is absent; do not display them as zero. HI results are in-sample, LI applies frozen HI rules. Label these views accordingly. Overlapping chains flag only distinct final transactions, using original final labels, in SQL.

## Sankey

Source `bq2_sankey`, one aggregate row per dataset/length/complete ordered pattern. SQL selects one identical count copy from the three lift support policies to prevent tripling weights. Filter one dataset and one sequence length at a time. For an initial dashboard, keep every pattern; to restrict to top-N, apply the identical full-pattern selection from lift rather than an isolated method filter. Thickness = `laundering_sequence_count` (or explicitly labelled all-chain count), **not USD**. These chain counts differ from the trade-off's unique transaction unit.

Tableau provides a free [Sankey Viz Extension](https://exchange.tableau.com/en-gb/products/932) for Tableau 2024.2 and later. If available in your setup:
1. Connect the aggregate table in a new worksheet. Open Marks → Add Extension → Tableau's Sankey.
2. With `sequence_length = 2`, drag `method_1` then `method_2` to Levels; put `SUM(laundering_sequence_count)` on Links.
3. Duplicate the sheet, filter length 3, and add `method_3` as the third Level. Keep length-2 and length-3 sheets separate so a missing third hop does not imply a real method.
4. Add full `method_sequence` and counts to tooltip/detail where supported. Check repeated methods remain separate stage nodes, and check complete pathways against the ranked table. A merged Sankey shows aggregate transitions, not proof of every possible path through those transitions.
5. Use labels to state `Laundering-labelled chain occurrences`. Show zero-laundering patterns in the ranked table rather than forcing them to have nonzero bands.

[Tableau's extension instructions](https://help.tableau.com/current/pro/desktop/en-us/viz_extensions.htm) describe Levels and Links. Availability depends on your Tableau version/site settings; no extension has been installed here. If unavailable, native path/densification is a fallback requiring workbook implementation and visual verification; a heatmap alone does not complete the promised Sankey.

Because this SQL keeps full patterns in one row, summing its weights once per dataset gives total chain occurrences. Do not call those weights unique transfers or unique funds; overlapping chains share transactions. Keep the same pattern/length filters when comparing totals.

## Dashboard and evidence

Suggested layout: ranked patterns and Sankey above, policy comparison and targeted checklist below; purple for laundering and pink for legitimate reviews, matching the proposal. Include dataset, chain length, support threshold and policy controls only where meaningful. Do not let a filter silently change the full cleaned-dataset denominators. Never sum policy rows across lengths or support cutoffs.

Save `viz/bq2_dashboard.twb` without embedded credentials, then screenshot the actual dashboard into `viz/screenshots/` for slides. Before committing, inspect XML for credentials/private paths and use `git check-ignore` on extracts. Commit `.twb` only; `.twbx` and `.hyper` embed data. Add exact connection steps, real screenshots and measured completion status to your README. Never present a synthetic test chart as a dataset result.

## Four-sheet layout prepared before Role 5

Dashboard name: `BQ2 — Payment-method sequences`. Fixed desktop canvas: 1440 × 1000. Suggested layout positions in pixels:

| Area / sheet | X, Y | Width × height | Source and encoding |
|---|---|---|---|
| Header + status | 0, 0 | 1440 × 100 | Title and `Preparation only — awaiting Role 5 sequence outputs` |
| Ranked sequences | 0, 100 | 720 × 400 | `bq2_lift`: method/length, rank, support, laundering rate and lift |
| Payment-method Sankey | 720, 100 | 720 × 400 | `bq2_sankey`: ordered method levels, laundering-chain counts |
| Targeted vs broad checks | 0, 500 | 720 × 400 | `bq2_tradeoff`: policy vs catch/legitimate-review percentages |
| Targeted checklist | 720, 500 | 720 × 400 | HI top-N from lift, with LI status/rank/support from robustness |
| Definitions / limitations | 0, 900 | 1440 × 100 | Units, support cutoff, HI in-sample / LI frozen-rule status |

Use purple `#5B2CCB` for laundering/catch and pink `#D74C78` for legitimate reviews, with direct labels so colour is not the only distinction. Keep result areas empty or show a plain pending-data note. Do not add dummy winners, zeros or proposal percentages. This is an authoring specification, not a completed Tableau dashboard or a screenshot of one.

Prepare worksheet names exactly: `01 Ranked sequences`, `02 Payment-method Sankey`, `03 Targeted vs broad checks`, `04 Targeted checklist`. A 2-hop/3-hop Sankey may use separate underlying worksheets later; the four areas are the reader-facing dashboard views.

Control behaviour once connected:
- Evaluation dataset: changes the ranking/Sankey/trade-off dataset, but the operational checklist always selects the frozen HI-ranked rule set and labels it as such. LI fields describe evaluation of those rules.
- Policy parameter N: choices 3, 5, 10. It selects the checklist and highlights the matching trade-off policy; retain all policy rows in the comparison.
- Sequence length: select 2 or 3 consistently across ranking, Sankey, trade-off and checklist. Each has its own HI rule selection and sequence baseline; do not combine rates.
- Support threshold: select min_support 50, 100 or 500 on lift/trade-off/robustness. SQL already ranks these separately. Sankey is unfiltered chain counts with no repeated cutoff rows; a support-policy filter must not duplicate or redefine its weights.

When the aggregates exist, follow the earlier connection/field instructions, populate the sheets, place them in these areas, and save `viz/bq2_dashboard.twb`. Verify repeated-method nodes, rates/denominators, policy selection and NULL handling in Tableau before marking it complete. No AWS login or embedded credentials belong in the repository.

## v1 field mapping

`any_laundering` defines positive-chain lift/Sankey counts; it is not the target label. Trade-off counts distinct `final_txn_id` values and joins the original label from clean. Select one length and min_support before top-N or rank filters. Show `actual_patterns` where fewer than N patterns qualify. Checklist rows use HI rank plus LI evidence joined on method_sequence + sequence_length + min_support; avoid a many-to-many join across policy scopes. No physical sequence-membership table or generic chain is_laundering field is expected.
