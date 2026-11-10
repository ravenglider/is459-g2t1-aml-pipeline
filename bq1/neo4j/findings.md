# Role 4 graph findings

> **Pipeline integration (10 November 2026):** the graph branch runs inside `is459-g2t1-pipeline` through SSM document `is459-g2t1-role4-graph`. Full pipeline executions `9thrun_fixedbq2sequences` and `demo_run` (both 9 November 2026) succeeded end to end, which includes this branch validating the run's `success.json` and updating `bq1_graph_bank_features_latest` before the priority list was rebuilt.

Owner: Role 4. Manual run validated on 3 November 2026. BQ1: Which bank pairs are most involved in laundering, and which should prioritise information sharing?

HI graph/import/GDS and the Athena join preview are validated. Final Browser scenes, HI USD-weight sensitivity and primary LI analysis are recorded below. The planned HI cutoff comparison is complete. Task 11 direct sample/HI/repeat trials passed. Task 12 SSM sample success/output acceptance,duplicate-run rejection,isolated shell-timeout and read-only feature-validator probes passed. Ryan now reports live priority-list adoption; Sonia reports a completed whole-pipeline run and automatic rebuild with the updated SQL. Independent execution references and rebuilt-output acceptance remain to be recorded. Live full-run publication suppression under injected faults was not established by the read-only probe. Optional investigation upgrades prepared on8 November remain locally checked,not live-executed. This is a findings record, not a claim that all Role 4 deliverables are complete.

## Population and reconciliation

Source: `is459_g2t1_aml.transactions_clean`, dataset HI-Medium, labelled laundering rows with `amount_paid_usd < 20000000`. The team cutoff removes counts as well as USD; it is not a regulatory threshold. Of 35,230 labelled rows, 339 were excluded. Missing attempt annotations do not exclude rows from the graph.

| Measure | Accepted result |
| --- | --- |
| Bank graph nodes / directed corridors | 5,159 / 29,374 |
| Cross-bank transactions / USD | 34,552 / $2,308,523,888.96 |
| Account evidence transactions / USD | 34,891 / $2,311,266,994.08 |
| Combined unordered bank pairs / transaction weight | 28,347 / 34,552 |
| Repeated imports | Counts and USD unchanged |

Evidence: [Athena source totals](screenshots/08b-hi-source-reconciliation.png), [repeat-import validation](screenshots/08c-hi-repeat-import-validation.png).

## Intermediaries

Neo4j 5.26.32 Community with GDS. Betweenness uses directed, unweighted LAUNDERING_TO relationships, concurrency 2; reported estimate 1,048 KiB. Scores describe shortest-path structure, not probabilities or dollars.

| Bank | Betweenness |
| --- | --- |
| 11 | 921,448.5217782246 |
| 20 | 863,387.9575309356 |
| 0 | 704,118.5412964496 |

Bank 11 has 125 incoming corridors / 256 transactions / $16,821,685.22, and 144 outgoing corridors / 245 transactions / $7,238,465.11. Structural example: 20 → 11 → 12. These separate corridor totals do not prove the same money flowed through the bank.

Evidence: [ranking](screenshots/08i-hi-betweenness-ranking.png), [activity](screenshots/08j-hi-top-intermediary-activity.png), [route](screenshots/08k-hi-top-intermediary-route.png).

## Communities

Louvain uses undirected BANK_LINK with combined transaction-count weights, concurrency 1. The projection has 56,694 relationships. Result: 113 communities, modularity 0.3596071963885728; all 5,159 banks assigned. IDs are labels meaningful only within this accepted run, not risk scores or stable labels across reruns.

| Community | Banks |
| --- | --- |
| 3870 | 1,835 |
| 3260 | 1,089 |
| 3881 (includes bank 11) | 553 |

Example cross-community corridor: 114976 → 297435, communities 3881 → 3166, one retained transaction worth $19,729,837.11. USD sorting highlights large individual transfers and is not itself the final sharing priority.

Evidence: [Louvain summary](screenshots/08l-hi-louvain-summary.png), [example membership](screenshots/08m-hi-community-membership.png), [cross-community corridors](screenshots/08n-hi-cross-community-corridors.png).

## Transaction-supported candidates

Rules: same intermediary bank AND account, strictly increasing timestamps, maximum 24 hours per adjacent transfer, distinct transaction IDs. Amounts need not match; timestamps have no supplied timezone. These rules establish candidate continuity, not proof of the same funds or criminal intent.

| Candidate | Evidence and limitation |
| --- | --- |
| 7818 → 121523 → 134266 | 6 September: 05:10 → 13:24 (8h14m). Accounts 8037732C0 → 80D1BD2F0 → 814167590. Transactions 9173283034951331860 and -1430906872139732606; $10,559.66 and $9,714.20. Both labelled; attempt 2 in source. |
| 7818 → 25828 → 212353 → 11796 | 13 September: 06:46 → 09:10 → 15:21. Gaps 2h24m and 6h11m. Accounts 805753DD0 → 804B45920 → 805754320 → 80139E060. Transactions 7394026252962401598, -2980862507599786311, 606750438349263592; $3,459.32, $8,438.51, $9,143.41. All labelled, but attempt annotations 670, 2117, 2117: not a single annotated attempt. |

Evidence: [two-hop source](screenshots/08e-hi-two-hop-source-verification.png), [two-hop path](screenshots/08f-hi-verified-two-hop-path.png), [three-hop source](screenshots/08g-hi-three-hop-source-verification.png), [three-hop path](screenshots/08h-hi-verified-three-hop-path.png). The structural route 1488 → 0 → 215275 had no qualifying account/time candidate; it must remain a structural illustration only.

## Ryan handoff and lineage

Feature table: `is459_g2t1_aml.bq1_graph_bank_features_hi01`. CSV schema: `bank_id,betweenness,community_id` (three columns). S3: `s3://is459-g2t1-aml-processed-bucket/bq1_graph_results/hi-01/bank_features/bank_features.csv`.

Initial Browser export contained only 5,000 banks. Increased the result limit and re-exported: corrected CSV has 5,160 populated lines including the header. Athena confirms 5,159 unique banks, 113 communities, zero incomplete rows. CSV data remains in S3 and is ignored by Git.

The LEFT JOIN preview preserves all 1,697,382 corridor rows. Example 260802 → 264284: direct $0; shared-attempt context and extra value $97,006,377.05; gap 1.0; endpoint betweenness 9,114.007834089867 and 6,326.959687051349; communities 727 and 3260. This is a sharing-context pair, not evidence of a direct transfer. Gap 1.0 is not a measured 100% detection failure. Missing features must remain missing, not zero risk.

Ryan's shared-attempt values require annotations; direct values do not. Full attempt amounts repeat across pairs. Do not sum them as unique funds or describe them as causal sharing benefits. Retain his ranking and agree any adjustment with him. Verify the graph and corridor source snapshots still match after refreshes; matching counts alone do not establish identical contents.

| Evidence | Athena query ID |
| --- | --- |
| 08g three-hop source | `1a3a3486-6c80-42a4-b9a2-16e247fe20eb` |
| 09a feature validation | `17d50c17-5c66-4ce9-b996-55a98196f19a` |
| 09b join preview | `47187dbd-4b2e-4948-9836-75462cd0f393` |
| 09c unchanged join count | `7a8f1d2a-8858-4741-8412-fdd92a26c5b7` |

Other execution IDs were not supplied here; do not infer them from transaction IDs. Source SQL: [exports](export_edges.sql) and [feature registration, checks and join](athena_handoff.sql). The preceding results refer to the accepted manual run. Later sections record direct EC2 automation trials separately; no successful SSM/Step Functions integration is claimed.

Automation implementation/status: [pipeline interface](README.md#step-functions-info-for-sonia). Offline tests and direct EC2 trials pass; SSM and integrated workflow acceptance remain pending.

## HI/LI cutoff measurement - first robustness query validated

Athena query ID: `679b8d6e-083d-4d27-90ab-d72cc0d72d1c`. Results supplied from the successful manual query. Evidence: [counts and dataset labels](screenshots/10a-hi-li-cutoff-counts.png) and [USD values and dates](screenshots/10b-hi-li-cutoff-values.png). The values panel omits dataset labels; read it with the counts panel and the query's dataset ordering (HI first, LI second). Population is all labelled laundering rows per dataset before applying the cutoff. USD values below are rounded to cents; Athena DOUBLE output contains minor floating-point representation differences.

| Measure | HI-Medium | LI-Medium |
|---|---:|---:|
| Labelled rows | 35,230 | 16,041 |
| Missing USD | 0 | 0 |
| Rows at/above $20M | 339 | 149 |
| Excluded row percentage | 0.9622% | 0.9289% |
| Retained rows below $20M | 34,891 | 15,892 |
| Retained cross-bank rows | 34,552 | 15,782 |
| Unfiltered labelled USD | $132,159,715,152.67 | $80,531,500,875.21 |
| Excluded USD | $129,848,448,158.59 | $79,622,696,502.86 |
| Retained USD | $2,311,266,994.08 | $908,804,372.35 |
| Excluded share of unfiltered USD | 98.2512% | 98.8715% |
| First labelled transaction | 2022-09-01 00:01:00 | 2022-09-01 00:00:00 |
| Last labelled transaction | 2022-09-28 15:58:00 | 2022-09-27 14:58:00 |

The excluded-value shares were calculated as excluded_usd / unfiltered_usd using the supplied results. HI retained counts and USD match the accepted manual graph input baseline. This reconciliation does not independently establish identical source contents after a refresh.

Interpretation: fewer than 1% of labelled rows account for more than 98% of labelled USD in both datasets. The threshold therefore has a major effect on value-based conclusions. This measures the influence of the team's synthetic-outlier convention; it does not establish that every excluded transaction is erroneous, select an optimal cutoff, or complete a graph-ranking robustness comparison. Report the population/filter prominently and complete the remaining comparisons before claiming robust priorities.

## HI/LI filtered and unfiltered key checks - second robustness query passed

Athena query ID: `cf3afa7b-c85c-4f3b-883a-f34dc08d1543`. Evidence: [HI/LI population key checks](screenshots/10c-hi-li-population-key-checks.png). User-supplied Athena results verified against the saved screenshot:

| Dataset | Population | Selected rows | Bad banks | Bad accounts | Bad times | Missing USD | Duplicate/null transaction IDs |
|---|---|---:|---:|---:|---:|---:|---:|
| HI-Medium | below_20m | 34,891 | 0 | 0 | 0 | 0 | 0 |
| HI-Medium | unfiltered | 35,230 | 0 | 0 | 0 | 0 | 0 |
| LI-Medium | below_20m | 15,892 | 0 | 0 | 0 | 0 | 0 |
| LI-Medium | unfiltered | 16,041 | 0 | 0 | 0 | 0 | 0 |

All counts match the preceding cutoff summary. The duplicate/null-ID check is COUNT(*) minus COUNT(DISTINCT txn_id), so zero also establishes no null transaction IDs within each selected population. These checks establish key/timestamp/USD completeness and transaction-ID uniqueness for the queried snapshot; they do not verify every timestamp's correctness, make extreme amounts plausible, or complete graph robustness.

## Corridor cutoff comparison - third robustness query executed

Athena query ID: `3bbfb6b1-d294-478a-8e94-c76256630cbb`. The query includes labelled cross-bank rows from both datasets and sorts by dataset, then excluded USD descending. Evidence: [HI corridor cutoff impact](screenshots/10d-hi-corridor-cutoff-impact.png), showing 43,518 total result rows and leading HI corridors. The six examples recorded below are HI-Medium only; LI examples and aggregate reconciliation are recorded in the following sections. Local CSV download is optional; preserve the query's Athena output location for full-result access.

| HI corridor | Unfiltered transactions | Retained transactions | Unfiltered / excluded USD | Retained USD |
|---|---:|---:|---:|---:|
| 171692 -> 240298 | 1 | 0 | $15,938,012,093.16 | $0.00 |
| 27512 -> 22235 | 1 | 0 | $6,958,804,909.33 | $0.00 |
| 22235 -> 224668 | 1 | 0 | $6,953,680,383.30 | $0.00 |
| 159430 -> 52481 | 1 | 0 | $6,329,143,641.81 | $0.00 |
| 1158540 -> 1158270 | 1 | 0 | $4,728,323,260.40 | $0.00 |
| 114976 -> 230522 | 1 | 0 | $3,566,430,066.84 | $0.00 |

Each listed corridor's only labelled transaction is above the primary threshold, so its retained count and value are zero. This removes these labelled corridor edges from the filtered graph; it does not establish that the bank endpoints disappear entirely, that the banks never transact, or that the synthetic record is a confirmed data error. This is direct-corridor sensitivity, not a rerun of Role 3's shared-attempt ranking or a graph-centrality/community robustness result.

## LI corridor cutoff examples - preview executed

Athena query ID: `ff1ff99e-ada4-454c-b54e-2f4597c55d3e`. The separate LI-only query returns the five corridors with the greatest excluded USD. Results supplied by the user and verified against [saved LI corridor evidence](screenshots/10e-li-corridor-cutoff-impact.png).

| LI corridor | Unfiltered transactions | Retained transactions | Unfiltered / excluded USD | Retained USD |
|---|---:|---:|---:|---:|
| 112901 -> 18379 | 1 | 0 | $11,639,247,997.83 | $0.00 |
| 2204310 -> 11 | 1 | 0 | $6,514,489,569.29 | $0.00 |
| 14745 -> 110320 | 1 | 0 | $5,969,941,668.18 | $0.00 |
| 30671 -> 181000 | 1 | 0 | $5,715,749,939.62 | $0.00 |
| 58172 -> 2114931 | 1 | 0 | $5,544,103,899.56 | $0.00 |

Each listed LI corridor's only labelled transfer is excluded by the primary cutoff, as in the leading HI examples. These observations support reporting the strong influence of extreme values in both synthetic populations. They do not establish stability of centrality, communities or the final priority list. Numeric bank IDs across HI and LI are not assumed to identify the same real entity.

## Whole-corridor totals - aggregate check passed

The fifth robustness SELECT executed successfully. Athena query ID: `77ffcf9b-e438-41f6-b759-74aadb63c17d`. Evidence: [HI/LI corridor totals](screenshots/10f-hi-li-corridor-totals.png). The screenshot clearly records corridor/transaction counts and unfiltered USD; the rightmost USD columns are cropped, so their complete values below come from the user-supplied result table. Results cover labelled cross-bank transactions only. USD below is rounded to cents.

| Measure | HI-Medium | LI-Medium |
|---|---:|---:|
| Unfiltered directed corridors | 29,660 | 13,858 |
| Filtered directed corridors | 29,374 | 13,724 |
| Corridors absent after cutoff | 286 | 134 |
| Unfiltered cross-bank transactions | 34,887 | 15,930 |
| Filtered cross-bank transactions | 34,552 | 15,782 |
| Excluded cross-bank transactions | 335 | 148 |
| Unfiltered cross-bank USD | $131,229,225,009.70 | $80,507,204,879.45 |
| Filtered cross-bank USD | $2,308,523,888.96 | $908,116,109.62 |
| Excluded cross-bank USD | $128,920,701,120.74 | $79,599,088,769.83 |

The two unfiltered corridor counts sum to 43,518, matching the earlier full comparison's reported row count. Filtered HI corridor count, transaction count and USD match the accepted HI graph. Filtered transaction counts match the independent HI/LI population summary (34,552 and 15,782). Counts alone do not establish identical source snapshots.

Excluded cross-bank transactions differ from all excluded labelled rows: HI has 335 cross-bank plus 4 same-bank exclusions (339 total), and LI has 148 cross-bank plus 1 same-bank exclusion (149 total). Likewise, account-evidence totals include retained same-bank transfers; do not compare them directly to this cross-bank USD summary. Removed corridors are those with no retained labelled transfer; their endpoint banks may still have other graph connections.

This completes the observed source/corridor arithmetic checks. It does not establish that centrality, communities or the final sharing priorities are stable under a different cutoff, weighting or dataset. LI graph algorithms were subsequently run; see the LI measurements below. Use the LI filtered corridor/transaction/USD totals above as the later import baseline, with the same source snapshot and filter.

## Final visualisation tool - team decision

On 3 November 2026 the user reported the team's approval: **use Neo4j Browser instead of Bloom**. The compatibility/fallback decision is resolved. Use Browser for the intermediary, community and verified account-path scenes; the three final scenes and captions are recorded below. Record the substitution from the proposal in the report/presentation. This is team agreement, not an assertion of instructor approval or Bloom deployment. No Bloom licence or perspective file is required for this agreed route.

## Final Browser scenes - captured

- [Intermediary scene](screenshots/08p-hi-browser-intermediary-scene.png): bank 11's five highest-count incoming and outgoing corridors; nine distinct banks and ten directed corridors. Structural context does not establish continuous funds.
- [Community scene](screenshots/08q-hi-browser-community-scene.png): 114976 (community 3881) -> 297435 (community 3166), with selected same-community neighbours; nine banks and nine corridors. Colours do not encode community membership. The selected corridor is an illustration, not a final priority ranking.
- [Account scene](screenshots/08r-hi-browser-account-path-scene.png): accounts 8037732C0 -> 80D1BD2F0 -> 814167590, across banks 7818 -> 121523 -> 134266. Caption-data check returned 2022-09-06 05:10/$10,559.66 and 13:24/$9,714.20, both attempt 2. Same intermediary account and 8h14m ordering support a candidate path, not proof of identical funds.

## LI graph analysis and source reconciliation

LI later imported successfully as `li-01`: 4,924 bank nodes, 13,724 directed corridors, 15,782 represented cross-bank transactions / $908,116,109.62; 23,284 account nodes and 15,892 transfers / $908,804,372.35. Header/readability checks passed as container UID 7474; CSV line counts were 13,725 and 15,893 including headers. Evidence: [S3 inputs](screenshots/10j-li-graph-inputs-s3.png), [import reconciliation](screenshots/10k-li-import-reconciliation.png). Repeated-import totals were unchanged. On 5 November 2026, the user reported repeat-import summaries: bank import set 27,448 properties; account import set 63,568 properties; neither reported creating nodes or relationships. The deferred execution-summary check is complete.

Directed unweighted LI betweenness wrote 4,924 properties after a 788,744-byte memory estimate. Top three: bank 0 (1,073,253.0024833838), 14 (732,794.5456180857), 20 (491,126.4010718889). Evidence: [ranking](screenshots/10l-li-betweenness-ranking.png). Raw scores are not directly comparable risk measures across different graph populations.

LI count-weighted Louvain used 13,453 unordered links with total weight 15,782, projected as 26,906 undirected relationships over 4,924 banks. Maximum estimated memory: 1,484,648 bytes. Result: 142 communities, modularity 0.45376073179686327, 4,924 properties written. Saved coverage confirmed all 4,924 banks scored and assigned. Largest observed communities: 3465 (1,190 banks), 3749 (699), 2373 (298). Evidence: [Louvain summary](screenshots/10m-li-louvain-summary.png), [membership preview](screenshots/10n-li-community-membership.png).

The [HI/LI saved-graph comparison](screenshots/10o-hi-li-graph-comparison.png) confirms full score/assignment coverage in both scopes, original HI 113 communities and LI 142. HI remains 5,159 banks / 29,374 corridors / 34,552 transactions; LI remains 4,924 / 13,724 / 15,782. These are different synthetic populations; matching IDs do not establish entity identity. This does not prove ranking stability or detection improvement. The subsequent HI cutoff comparison is recorded separately; SSM and integrated automation remain pending.

Athena query ID: `3ae95519-35fc-42ef-9bfe-1afcb4205a34`. Query: the source-reconciliation SELECT in `export_edges.sql`, with dataset changed to `LI-Medium`; laundering label 1 and individual USD below $20M. Reported and screenshot-verified totals: 15,892 evidence transactions / $908,804,372.35; 15,782 cross-bank transactions / $908,116,109.62. These match the earlier LI baseline. Evidence: [LI source reconciliation](screenshots/10i-li-source-reconciliation.png). The screenshot shows result totals; SQL/filter context is recorded here. This check does not establish CSV completeness or successful LI import. Subsequent LI import and analysis results are recorded in this section.

## HI community weighting sensitivity - measurements

The accepted hi-01 population and original assignments were preserved. Existing 28,347 unordered bank-pair links received summed USD weights; invalid weights: zero. The undirected comparison projection contained 5,159 nodes and 56,694 relationships. Maximum estimated Louvain memory was 2,039,120 bytes. Results were written separately to `community_id_usd_sensitivity`.

| Measure | Original transaction-count weights | USD-value weights |
|---|---:|---:|
| Assigned banks | 5,159 | 5,159 |
| Communities | 113 | 207 |
| Modularity | 0.3596071963885728 | 0.8350260762765536 |

Evidence: [USD-weighted summary](screenshots/10g-hi-usd-weighted-louvain-summary.png), [overlap spread](screenshots/10h-hi-community-weighting-overlap-summary.png). Screenshots show result tables; population, weight and query context are recorded here and in robustness.cypher. Additional user-reported checks: 525 non-empty overlap combinations account for all 5,159 banks; 22 of 113 original communities (19.5% of communities, not banks) span multiple USD groups; the maximum spread is 99 groups. The remaining 91 each map into one USD group, which may also include other original communities.

These runs produce different memberships. Higher modularity under different weights does not establish a better partition. Numeric community IDs are arbitrary. Repeated-run variability has not been measured, so the difference cannot be attributed exclusively to weights. Retain the original count-weighted assignments for the accepted Role 3 handoff; use this result as sensitivity context.

## HI cutoff graph comparison

Subsequent manual cutoff graph comparison completed on 5 November 2026:

- Uncapped scope `hi-uncapped-01`: 5,166 banks, 29,660 corridors, 34,887 represented transactions / $131,229,225,009.70. [Input upload](screenshots/10p-hi-uncapped-input-s3.png), [import reconciliation](screenshots/10q-hi-uncapped-import-reconciliation.png).
- Directed unweighted betweenness wrote 5,166 scores. Top-10 overlap with accepted HI: 9 banks; top four remain 11,20,0,14. Bank 1 appears only in the filtered top ten; 2310 only in uncapped. Banks 12/15 and 25/3 swap order. [Ranking](screenshots/10r-hi-uncapped-betweenness-ranking.png), [top-ten comparison](screenshots/10t-hi-cutoff-top10-overlap.png).
- Count-weighted Louvain used 28,622 unordered links with total weight 34,887, projected as 57,244 relationships. Maximum estimated memory: 2,040,016 bytes. Result: 111 communities, modularity 0.3596721194379274; all 5,166 banks scored and assigned. [Summary](screenshots/10s-hi-uncapped-louvain-summary.png).
- All 5,159 filtered bank IDs match uncapped banks; seven banks are uncapped-only. Membership comparison restricted to the shared banks has 245 non-empty overlaps, accounting for 5,159 banks and covering 113 filtered / 111 uncapped communities. Of the 113 filtered groups, 15 (13.3% of communities, not banks) span multiple uncapped groups; maximum spread is 17. The remaining 98 may still merge into groups containing other original communities. [Spread summary](screenshots/10u-hi-cutoff-community-overlap-summary.png); values are readable, long headers are truncated, and query definitions are preserved in robustness.cypher.

Conclusion: leading intermediary selection is largely consistent in this top-ten comparison, while some community memberships differ. This is not a claim of full ranking or final priority-list stability. Modularity values alone do not measure partition similarity. Repeated-run variation was not measured, so differences cannot be attributed exclusively to the cutoff. Original filtered HI remains the accepted Role 3 baseline; neither LI nor uncapped results replace its feature table. Accepted hi-01 was restored after comparison; reported active coverage was hi-01 / 5159 banks / 5159 scored / 5159 assigned / 113 communities. No reimport or algorithm rerun was needed.

Athena query ID: `7c2e2f42-8bd3-4f32-b924-ade41416f735`. User reported 29,660 result rows, matching the earlier uncapped directed-corridor baseline. The query selects HI-Medium, laundering label 1 and cross_bank 1, aggregates by directed bank pair and omits the per-transaction $20M cutoff. Intended comparison scope/input folder: `hi-uncapped-01`. CSV staging, import reconciliation and uncapped GDS results were subsequently confirmed as recorded above. Accepted `hi-01` and `li-01` results remain separate.

## Recommendations for Role 3 - adoption reported; refreshed-output checks pending

Preserve Ryan's Extra Laundering Value / Blindspot Gap order and enrich it with
the accepted HI endpoint features. Do not invent a combined priority score or
interpret gap as measured detection performance.

| Finding | Recommended use | Limit |
|---|---|---|
| Bank 11 leads HI betweenness; banks 20 and 0 follow | Review high-ranked corridor pairs involving structural intermediaries and relevant counterpart banks | Betweenness is not laundering value or proof of bank intent |
| Structural 20 -> 11 -> 12 and retained activity around 11 | Explain intermediary context when considering sharing partners | Aggregated legs do not establish account/fund continuity |
| Cross-community 114976 -> 297435 | Illustrate a connection between groups, alongside corridor metrics | One $19.73M transaction; USD order alone is not priority |
| Verified two-hop and three-hop account/time candidates | Provide concrete investigation examples and information-sharing rationale | Three-hop example crosses attempt annotations; neither proves identical funds |
| 9/10 top intermediaries shared across HI cutoff runs | Report bounded consistency of leading intermediary selection | Not full ranking or priority-list stability |
| Community membership differs under cutoff and USD weights | Treat communities as explanatory context rather than a rigid risk classification | Arbitrary IDs and unmeasured repeated-run variability |

Update reported by the user on 8 November 2026: Ryan confirmed that his earlier
Tableau-only update did not refresh Athena tables since the original join check,
that he can read `bq1_graph_bank_features_hi01`,and that he built a live priority
table joining both endpoints' betweenness/community fields. He checked populated
bank11 values. Sonia subsequently reports that a completed whole-pipeline run
rebuilt `bq1_priority_sharing_list` using the corrected SQL. The repository now
uses `bq1_graph_bank_features_latest` and preserves NULL when community features
are missing. These are teammate-reported live results and repository evidence;
the execution ARN,graph run identity and independent rebuilt-output validation
have not yet been supplied here. The old source-alignment confirmation predates
that pipeline rerun; compare current context with the saved hi-01 case before
presenting them as identical. No manual CTAS/table rebuild should be performed.

Historical repository cross-check on 5 November 2026 (superseded by the update above): Role 3 commit `37e8554` (4 November,
18:21 Singapore time) added `viz/bq1_corridors_viz.twb`; `4dfd1ad` removed the old
`Book1.twbx`. The XML contains BQ1 Heatmap and BQ1 Ranked Table sheets and an
Athena relation to `is459_g2t1_aml.bq1_corridors_ranked`. It does not reference the
graph feature table, betweenness or community fields. Corridor SQL is unchanged
from `2272e52` (2 November processed-bucket location fix); no `priority_list.sql`
is present in the corridor folder. This establishes workbook/source-code status,
not whether live Athena tables were rerun. Current source-snapshot alignment
and final graph-feature adoption still require confirmation; no graph reimport
is warranted solely because the workbook was committed.

## Direct automation trial - sample validated

On 6 November 2026 the user executed release `role4-20261106-01` on the existing
EC2 host with run ID `sample-auto-20261106-01` and dataset `sample`. The runner
printed SUCCEEDED, total elapsed37.155682 seconds, runner exit0 and log exit0.
Reported output table:
`is459_g2t1_aml.bq1_graph_bank_features_sample_auto_20261106_01`.
This establishes a successful runner-reported sample execution, not full HI,
SSM/Step Functions integration or an independent preservation verification.

| Stage | Wall seconds |
|---|---:|
| Readiness | 1.296263 |
| Source/input exports, including polling and downloads | 16.210757 |
| Input validation and staging | 1.945599 |
| Neo4j import and validation | 5.215950 |
| Betweenness with projection/estimate | 2.852008 |
| Louvain with link/projection/estimate preparation | 0.769485 |
| Feature validation and publication | 1.679032 |
| Athena registration and validation | 6.538750 |
| Total through success-receipt publication | 37.155682 |

The algorithm stages include preparation, not isolated algorithm compute time.
Elapsed time is not EC2 billed uptime or peak memory.

The published S3 receipt was subsequently read successfully. It reports dataset
sample,42 banks,35 corridors/transactions,USD434634.33,8 communities,
modularity0.7102040816326531 and42 assignments, matching manual sample totals.
The accepted HI post-trial preservation check passed:scope/node count/relationship
count/node-properties SHA256/relationship-properties SHA256 all match. HI retains
46,615 scoped nodes and92,612 relationships; Active labels were intentionally
excluded from the fingerprints. Verification record is stored on EC2 at
`/home/ssm-user/role4/trial-evidence/hi-preservation-after-sample-auto-20261106-01.json`.
Independent Athena inspection of
`is459_g2t1_aml.bq1_graph_bank_features_sample_auto_20261106_01` subsequently
returned42 rows,42 unique banks,8 communities and0 invalid rows (bank ID blank/null,
betweenness null/negative,or community null). Query ID has not yet been supplied.
The first direct sample trial passed its reported execution, published receipt,
HI preservation and independent feature-table checks. Full HI and fresh-run repeatability subsequently passed as recorded below. SSM/Step Functions trials remain pending.

## Direct automation trial - first HI run validated

User-reported run on6 November2026: release role4-20261106-01,
run ID `hi-auto-20261106-01`,dataset HI-Medium;SUCCEEDED with runner exit0/log
exit0. Output table:
`is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_01`.

| Stage | Wall seconds |
|---|---:|
| Readiness | 1.200815 |
| Source/input exports, including polling and downloads | 16.356384 |
| Input validation and staging | 2.712180 |
| Neo4j import and validation | 23.176010 |
| Betweenness with projection/estimate | 5.677433 |
| Louvain with link/projection/estimate preparation | 3.878652 |
| Feature validation and publication | 2.797833 |
| Athena registration and validation | 7.067132 |
| Total through success-receipt publication | 63.522618 |

The published HI S3 receipt was subsequently inspected:5159 banks,29374 corridors,
34552 cross-bank transactions/USD2308523888.96,34891 account transfers/
USD2311266994.08;113 communities,modularity0.3596071963885728,5159 assignments.
These source totals match accepted manual HI. Post-HI preservation verification
returned all five checks true and unchanged=true against the original fingerprint
(46615 scoped nodes,92612 relationships). Independent Athena feature validation
returned5159 rows,5159 unique banks,113 communities,0 invalid rows. Query ID:
`5fb8a8a9-d2e4-4acf-9ac9-7a75413a5676`.

The first full-HI direct trial passed terminal execution, published-receipt,
accepted-HI preservation and independent feature-table checks. Fresh-run repeatability subsequently passed as recorded below; manual/automated community membership also matched.
This is not a successful SSM or Step Functions execution. Runtime is not total
EC2 billed uptime or peak RAM. Evidence saved: [terminal](screenshots/11e-hi-runner-terminal-success.png)
and [HI preserved](screenshots/11f-hi-preserved-after-hi-trial.png).

Independent manual/automated bank-feature comparison returned5159 matching bank
IDs,0 manual-only banks,0 auto-only banks,0 betweenness mismatches beyond
max(1e-6 absolute,1e-9 relative) tolerance. Maximum score delta:
1.1641532182693481E-10,well below the specified tolerance. Query ID:
`a465ff5f-5561-4df1-9394-9f1b06202cbf`.
This validates bank coverage and numerical score agreement; community
membership equivalence was separately checked because labels are arbitrary.

Independent community overlap comparison returned5159 matching banks,113 manual
communities,113 automated communities,0 manual groups split across automated
groups and0 automated groups merging manual groups. Together with complete
unique bank-ID matching, this establishes the same partition of these banks
in the two compared outputs, regardless of numeric community labels. Query ID:
`ee12264a-1031-460c-8245-8e0aaafa6b40`.
This does not establish stability across all future runs;
the subsequent fresh-ID repeatability trial is recorded below.

## Direct automation repeat trial - second HI run validated

Run `hi-auto-20261106-02`,dataset HI-Medium,release role4-20261106-01 returned
SUCCEEDED on6 November2026;runner exit0/log exit0. Output table:
`is459_g2t1_aml.bq1_graph_bank_features_hi_auto_20261106_02`.

| Stage | Wall seconds |
|---|---:|
| Readiness | 1.240984 |
| Source/input exports, including polling and downloads | 16.203580 |
| Input validation and staging | 2.675848 |
| Neo4j import and validation | 14.152538 |
| Betweenness with projection/estimate | 5.021460 |
| Louvain with link/projection/estimate preparation | 3.440045 |
| Feature validation and publication | 2.372268 |
| Athena registration and validation | 6.693308 |
| Total through success-receipt publication | 52.480852 |

The published run02 S3 receipt was inspected:5159 banks,29374 corridors,34552
cross-bank transactions/USD2308523888.96,34891 account transfers/USD2311266994.08;
113 communities,modularity0.3596071963885728,5159 assignments. These match the
first HI run and accepted manual totals. The feature table/prefix identify run02,
separately from run01. The completed repeatability checks are recorded below. The preservation check returned all_preserved=true, with all
five checks true for accepted manual hi-01 and first automated hi-auto-20261106-01.
Independent Athena run02 validation returned5159 rows,5159 unique banks,113
communities,0 invalid rows;query ID`d55dc6f8-0a52-467e-9bc0-dc286429be15`.
Repeat lineage comparison returned all8 checks true:correct IDs/success,
same dataset/filter/code/totals,separate tables/expected output locations,and
first published receipt unchanged. Recorded transaction CSV hashes match, but
bank-edge CSV hashes differ. This proves differing exported bytes,not by itself
differing numerical corridor values. Semantic comparison subsequently confirmed
both staged files match their receipt hashes;29374 unique pairs each,0 pairs
exclusive to either run,same pair order,0 transaction-count differences. There
are140 USD text/numeric differences with maximum absolute delta1E-9 dollars;
none exceeds the stated one-cent per-pair tolerance. With identical recorded
transaction exports,these tiny aggregate differences are consistent with
floating-point summation variation;they are documented rather than claiming
byte-identical bank exports. Output-feature and membership comparisons subsequently passed, as recorded below. The run01
fingerprint baseline was saved before the repeat (46615 nodes,92612 relationships)
at `/home/ssm-user/role4/trial-evidence/hi-auto-01-before-repeat.json`.
Do not interpret a lower second runtime as an established performance trend:
cache/warmup and other execution differences have not been controlled.

Independent run01/run02 feature comparison matched5159 banks,with0 banks
exclusive to either run and0 betweenness mismatches beyond the existing
max(1e-6 absolute,1e-9 relative) tolerance. Maximum absolute score delta:
2.3283064365386963E-10. Athena query ID:
`1fa74ea8-81ad-453d-8f6f-2feff5199312`.
Repeat community-membership comparison returned5159 matching banks,113 groups
per run,0 run01 splits and0 run02 merges. With unique complete bank-ID matching,
this establishes the same partition in the two compared outputs,ignoring
arbitrary numeric labels. Query ID:`189b5da1-06ce-409b-b50b-d463661a759c`.
Evidence:[repeat community comparison](screenshots/11o-hi-repeat-community-comparison.png).

Task11 direct-run functional/repeatability acceptance checks passed for the
reviewed release and observed source populations:sample37.155682s,HI run01
63.522618s,HI run02 52.480852s;complete outputs and recorded within-tolerance
differences. Earlier manual HI and first automated HI fingerprints are unchanged;
new scopes/tables/prefixes are separate. This does not certify future runs,
failures/timeouts or SSM/Step Functions integration. Those belong to Task12.

## Task 11 completion and evidence index

Completed on 6 November 2026 for release `role4-20261106-01`, whose three
deployed hashes match the audited snapshot. No additional workflow run is
required to close Task 11. The first automated graph and accepted manual graph
were preserved during the fresh-ID repeat. Inputs/counts/coverage agree; tiny
recorded monetary and score differences are within stated tolerances. This
completion is scoped to the observed direct trials, not future runs or failure
handling. Original manual HI remains the agreed Role 3 baseline until the team
explicitly adopts an automated run.

| Evidence | Record |
|---|---|
| [11a release uploaded](screenshots/11a-runner-release-s3.png) | Three deployment sources, no credentials/data |
| [11b sample terminal](screenshots/11b-sample-runner-terminal-success.png) | 37.155682s, runner/log exits 0 |
| [11c HI preserved after sample](screenshots/11c-hi-preserved-after-sample-trial.png) | Accepted HI fingerprints unchanged |
| [11d sample features](screenshots/11d-sample-auto-features-athena-validation.png) | 42/42/8/0; independent query ID not supplied |
| [11e HI terminal](screenshots/11e-hi-runner-terminal-success.png) | Run01 63.522618s, exits 0 |
| [11f HI preserved](screenshots/11f-hi-preserved-after-hi-trial.png) | Accepted manual HI unchanged |
| [11g HI features](screenshots/11g-hi-auto-features-athena-validation.png) | 5159/5159/113/0; query `5fb8a8a9-d2e4-4acf-9ac9-7a75413a5676` |
| [11h manual/auto scores](screenshots/11h-hi-manual-auto-feature-comparison.png) | Query `a465ff5f-5561-4df1-9394-9f1b06202cbf` |
| [11i manual/auto communities](screenshots/11i-hi-manual-auto-community-comparison.png) | Query `ee12264a-1031-460c-8245-8e0aaafa6b40` |
| [11j repeat terminal](screenshots/11j-hi-repeat-runner-terminal-success.png) | Run02 52.480852s, exits 0 |
| [11k repeat preservation](screenshots/11k-hi-repeat-preservation.png) | Manual HI and run01 fingerprints unchanged |
| [11l repeat features](screenshots/11l-hi-repeat-features-athena-validation.png) | 5159/5159/113/0; query `d55dc6f8-0a52-467e-9bc0-dc286429be15` |
| [11m input differences](screenshots/11m-hi-repeat-input-semantic-comparison.png) | Identical pair/count/order; 140 USD deltas, maximum $1E-9 |
| [11n repeat scores](screenshots/11n-hi-repeat-feature-comparison.png) | Query `1fa74ea8-81ad-453d-8f6f-2feff5199312` |
| [11o repeat communities](screenshots/11o-hi-repeat-community-comparison.png) | Query `189b5da1-06ce-409b-b50b-d463661a759c` |

Screenshots generally show selected results, not the whole SQL/source context;
that context is preserved in shared SQL/setup records and the supplied query
IDs. The independent sample query ID is an evidence-reference follow-up, not
a failed functional check; recover it from query history if possible without
rerunning a completed workflow. Per-run source/register/validation query IDs,
code/input hashes and timing metrics also reside in each S3 success receipt.

## Task 12 SSM sample invocation (7 November 2026)

Document `is459-g2t1-role4-graph` version 1 was created; evidence:
[12a document creation](screenshots/12a-ssm-command-document.png).
The user reported Command ID `fe553b23-2f76-48a4-bf58-dd59f06b1997`,
invocation status/detailed status `Success`, response code `0` and runner
`SUCCEEDED` for `sample-ssm-20261107-01` in 42.217389 seconds.
Published feature table reported by the runner:
`is459_g2t1_aml.bq1_graph_bank_features_sample_ssm_20261107_01`.
Independent Athena feature validation returned 42 rows,42 unique banks,
8 communities and0 invalid rows; query ID
`d40eb381-bdfd-40c7-a12a-b5a58ccc991e`.
Invocation evidence: [12b SSM success](screenshots/12b-sample-ssm-command-success.png).
The user inspected the run-specific S3 success receipt: status SUCCEEDED,
dataset sample,35 evidence/cross-bank transactions,$434634.33 for both totals,
35 corridors and42 banks. Louvain returned8 communities,modularity
0.7102040816326531 and42 properties written. The feature table and S3 location
match this run. Independent coverage evidence:
[12c Athena validation](screenshots/12c-sample-ssm-features-athena-validation.png).
The SSM sample success/output acceptance check passed. Duplicate-run rejection
and an isolated shell-timeout probe, plus read-only validator rejection, are
recorded below. Sonia-owned Step Functions acceptance remains pending.
No complete Task 12 claim is made.

### SSM duplicate-run rejection

On 7 November 2026, deliberately reusing `sample-ssm-20261107-01` through
document version 1 returned invocation status/detailed status `Failed`, response
code `1`, and `FAILED: Run prefix exists; choose a NEW run ID`.
Command ID: `b4f77ec0-dec7-4392-9537-dcb144a7bf93`.
Evidence: [12d duplicate rejection](screenshots/12d-ssm-duplicate-run-rejected.png).
The pre-test accepted receipt SHA256 was
`065cf54575d145c033a4399ac966e7c84d6009f9e2b583b2410af71de5cd8f56`;
the post-test hash matched exactly, confirming the accepted receipt was unchanged.
This hash check covers the receipt bytes, not every output object or graph record.
The old success receipt belongs
to the earlier successful invocation, not this rejected attempt. This checks
preflight rejection/nonzero SSM reporting, not output-validation failure,
timeout handling or Step Functions failure propagation.

### Isolated SSM execution-timeout probe

On 7 November 2026, the AWS-RunShellScript probe used executionTimeout=30
seconds with a 90-second sleep. Command ID:
`0c6bc427-bcd0-4cbf-b618-69a75ca1d8bf`.
The instance invocation reported `Timed Out`, detailed status `Execution Timed
Out`, response code `137`. Output contained only `ROLE4_TIMEOUT_PROBE_STARTED`;
the completion marker was absent. The displayed start/finish interval was
06:09:52-06:10:22 GMT (30 seconds).
Evidence: [12e SSM timeout](screenshots/12e-ssm-execution-timeout.png).
This confirms timeout reporting for the isolated shell probe. It did not run
the graph runner and does not establish database-query cancellation or
Step Functions timeout/failure handling. The read-only output-validator probe
is recorded next. Sonia subsequently completed integration and a full pipeline
run, as confirmed by the team and user; this isolated probe is not an integrated
failure test.

### Read-only feature-validator probe (7 November 2026)

Command `438554c5-5991-4b71-8ea3-1cc1edeb35cb` reported status/detailed status
`Failed`, response code `1`. The pasted traceback reaches the deployed `validate_features`
row-count check and ends with `RuntimeError: Incomplete feature export`.
Evidence: [12f validator rejection](screenshots/12f-ssm-feature-validation-rejected.png).
The user also confirmed both existing stdout markers:
`ORIGINAL_FEATURES_VALIDATED: 42 rows` and
`INJECTED_IN_MEMORY_TRUNCATION: 41 rows; expected 42`.
Together with the exact error and response code 1, these establish that original features
passed and the intentionally incomplete in-memory copy was rejected. The
component probe passed. The screenshot has Output collapsed and shows only
part of the error traceback; the full markers/error are recorded in this text.
This calls the real validator on a read-only/in-memory copy, not the complete
runner; it does not establish live publication suppression or integrated
Step Functions failure propagation. No repeated graph trial is needed.

## Verified investigation views

The read-only saved-scope queries use `hi-01`; they do not change graph
properties, scores, Active labels or shared pipeline outputs.

| View | Verified result | Evidence |
|---|---|---|
| Bank 260802 local view | 3 accounts, 2 selected transfers | [Local view](screenshots/sharing-01-bank-260802-local-view.png) |
| Bank 151989 local view | 3 accounts, 2 selected transfers | [Local view](screenshots/sharing-02-bank-151989-local-view.png) |
| Combined account path | 4 accounts, 3 selected transfers | [Combined path](screenshots/sharing-03-combined-account-path.png) |
| Visibility comparison | Individual counts 2/2; combined union 3 | [Comparison](screenshots/sharing-04-visibility-comparison.png) |
| Transfer timeline | Increasing timestamps; gaps 3h45m and 15h40m | [Timeline](screenshots/sharing-05-transfer-timeline.png) |
| Bank 11 neighbourhood | 9 banks, 10 directed corridors; up to 5 incoming/outgoing | [Neighbourhood](screenshots/sharing-06-bank11-intermediary-counts.png) |
| Bank 11 activity | All incoming/outgoing activity, beyond the displayed edges | [Activity](screenshots/sharing-07-bank11-activity-summary.png) |
| Community neighbourhoods | 9 banks, 9 directed corridors; memberships verified | [Community view](screenshots/sharing-08-cross-community-counts.png) |

Bank 11's activity matches the accepted analysis: betweenness
921,448.5217782246; incoming 125 corridors / 256 transactions /
USD16,821,685.22; outgoing 144 corridors / 245 transactions / USD7,238,465.11.
These directional sums do not establish identical funds or retained balance.

The community example connects bank 114976 (community 3881) to bank 297435
(community 3166): one labelled transaction worth USD19,729,837.11.
Displayed members are 110912, 114976, 12, 15011 and 18077 in community 3881;
297435, 6, 95274 and 97075 in community 3166. These are selected neighbours,
not complete communities or evidence of criminal organisations. The saved
layout puts 3881 on the left and 3166 on the right; colours identify bank
nodes, not community membership. Arrow captions show transaction counts.

The current priority row for 260802 -> 151989 returned direct value
USD7,758,401.24, shared-attempt context USD96,840,838.84, extra value
USD89,082,437.60 and gap 0.9198850264729905. Endpoint betweenness values were
9114.007834089867 and 13611.63291627669; both communities were 727 and the
cross-community flag was 0. This scoped match does not prove complete source
snapshot identity. The priority-row query ID has not been supplied.

The three selected transfers matched Athena and each had one saved Neo4j
relationship with matching bank/account keys, times, amounts and attempt 1885.
Source query: `d084c4cc-c9af-44dc-84f4-10ec1e74294c`.
Evidence: [Athena source records](screenshots/demo-02-sharing-case-source.png).
Account circles are captioned by bank ID; their identity remains account-level.
Transfer arrows show USD amounts. Source timestamps have no stated timezone.

Selection was retrospective: inspect Ryan's documented
worked-example pair `260802 -> 151989`, then check its retained annotated
transactions in the saved `hi-01/transactions.csv`. It is not a blind discovery,
an independent detection evaluation or a claim about today's priority rank.

The local CSV contains this three-leg account-connected subpath:

| From bank/account | To bank/account | Time (source has no timezone) | Transaction ID | USD |
|---|---|---|---|---:|
| 199098 / 825E09140 | 260802 / 816607960 | 2022-09-16 18:36 | 1923680745310817531 | 9,709,463.06 |
| 260802 / 816607960 | 151989 / 8154771C0 | 2022-09-16 22:21 | -5560620630767630164 | 7,758,401.24 |
| 151989 / 8154771C0 | 18366 / 822B0D450 | 2022-09-17 14:01 | -2118989140943353923 | 10,240,223.73 |

Local checks establish matching intermediate bank/account keys, distinct IDs,
increasing times, gaps of 3h45m and 15h40m (both below24h), and annotation1885 /
RANDOM on all three. The saved feature CSV places both selected banks in
community727. This is a same-community example; do not label it a community
bridge. Annotation agreement does not prove identical funds.

Simulation assumption: within these selected benchmark transfers, a bank sees
records where it is the sender or receiver. The complete labelled benchmark
is available to the researcher; these view filters are not access controls.
There is no claim about real banks' complete records or detection uplift.

| Simulated view | Selected transfers visible | Sum of transaction values (USD) |
|---|---:|---:|
| Bank260802 alone | 2 | 17,467,864.30 |
| Bank151989 alone | 2 | 17,998,624.97 |
| Combined pair | 3 | 27,708,088.03 |

The common direct transfer appears once in the combined view. Sharing exposes
one additional selected leg compared with either individual view; it does not
recover the full annotated attempt. The local retained annotation1885 contains
10 transactions totalling96,840,838.84 in transaction value. That benchmark is
different from the bounded three-leg view. Neither total is unique proceeds;
do not describe Ryan's gap as the percentage this pair would newly detect.

Recommended follow-up: jointly examine the connected
accounts and preceding/following transfer records to establish wider context.
Centrality/community fields support discussion, not a new risk score.
Sources: visualisations.cypher CASE0-6; athena_handoff.sql optional CASEA-C.
Browser styling source: browser-style.grass,with separate UI caption settings
recorded in README.md. Both individual views, the combined view, timeline and
visibility summary have been checked against user-supplied live results.
Existing evidence is preserved. Sonia owns the shared pipeline; it is unchanged.

## Remaining validation and submission work

The manual graph analysis and the planned component acceptance checks are
complete within their documented scopes. There is no identified missing
implementation file from the original Role 4 graph breakdown. This is not a
claim that every team/submission deliverable is finished. The remaining work
includes independent acceptance of Sonia's reported integration and Ryan's
reported adoption, as well as final cost/report/
slides/video contributions. The independent Task 11 sample feature-query ID is
still a minor evidence-reference follow-up; it must not be invented.

- Ownership confirmed 7 November 2026: Role 4 sends Sonia the tested command,
  output table/S3 path, dependencies, runtime and component-test evidence.
  Sonia owns shared Step Functions/role changes and integrated execution trials;
  Role 4 records the evidence she supplies. No shared pipeline deployment was
  performed by Role 4 during the component tests documented here.
- Direct EC2 runner trials passed;retain measured run/release/output references.
- SSM sample success,duplicate-run rejection,isolated shell-timeout and read-only
  feature-validator rejection checks passed. Full-run publication suppression
  under injected faults and integrated failure/timeout checks are not established
  by the read-only probe; Sonia owns shared-workflow acceptance.
- User confirmed the tested graph interface was personally sent to Sonia on
  7 November 2026; provide her the subsequent validator-probe result if needed.
- Record Sonia's reported integrated run references,check accepted-run consumption
  and confirm EC2 lifecycle behaviour/ownership; do not rerun CTAS manually.
- Ryan reports adoption; independently validate the refreshed priority-table
  join/coverage. Final team report/slides/video artefacts remain separate work.

Offline tests and static file checks are not live AWS/Cypher validation. Evidence
screenshots are selected result views, not complete source snapshots. Query IDs
are recorded where supplied; missing execution IDs must not be invented.
