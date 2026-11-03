# Role 4 — HI run hi-01 findings

## Role 4: HI run hi-01

Owner: Role 4. Manual run validated on 3 November 2026. BQ1: Which bank pairs are most involved in laundering, and which should prioritise information sharing?

HI graph/import/GDS and the Athena join preview are validated. Final Neo4j Browser scenes, remaining LI/cutoff/weight graph sensitivity, the final recommendation package, live runner trials and end-to-end Step Functions integration remain pending. This is a findings record, not a claim that all Role 4 deliverables are complete.

### Population and reconciliation

Source: `is459_g2t1_aml.transactions_clean`, dataset HI-Medium, labelled laundering rows with `amount_paid_usd < 20000000`. The team cutoff removes counts as well as USD; it is not a regulatory threshold. Of 35,230 labelled rows, 339 were excluded. Missing attempt annotations do not exclude rows from the graph.

| Measure | Accepted result |
| --- | --- |
| Bank graph nodes / directed corridors | 5,159 / 29,374 |
| Cross-bank transactions / USD | 34,552 / $2,308,523,888.96 |
| Account evidence transactions / USD | 34,891 / $2,311,266,994.08 |
| Combined unordered bank pairs / transaction weight | 28,347 / 34,552 |
| Repeated imports | Counts and USD unchanged |

Evidence: [Athena source totals](screenshots/08b-hi-source-reconciliation.png), [repeat-import validation](screenshots/08c-hi-repeat-import-validation.png).

### Intermediaries

Neo4j 5.26.32 Community with GDS. Betweenness uses directed, unweighted LAUNDERING_TO relationships, concurrency 2; reported estimate 1,048 KiB. Scores describe shortest-path structure, not probabilities or dollars.

| Bank | Betweenness |
| --- | --- |
| 11 | 921,448.5217782246 |
| 20 | 863,387.9575309356 |
| 0 | 704,118.5412964496 |

Bank 11 has 125 incoming corridors / 256 transactions / $16,821,685.22, and 144 outgoing corridors / 245 transactions / $7,238,465.11. Structural example: 20 → 11 → 12. These separate corridor totals do not prove the same money flowed through the bank.

Evidence: [ranking](screenshots/08i-hi-betweenness-ranking.png), [activity](screenshots/08j-hi-top-intermediary-activity.png), [route](screenshots/08k-hi-top-intermediary-route.png).

### Communities

Louvain uses undirected BANK_LINK with combined transaction-count weights, concurrency 1. The projection has 56,694 relationships. Result: 113 communities, modularity 0.3596071963885728; all 5,159 banks assigned. IDs are labels meaningful only within this accepted run, not risk scores or stable labels across reruns.

| Community | Banks |
| --- | --- |
| 3870 | 1,835 |
| 3260 | 1,089 |
| 3881 (includes bank 11) | 553 |

Example cross-community corridor: 114976 → 297435, communities 3881 → 3166, one retained transaction worth $19,729,837.11. USD sorting highlights large individual transfers and is not itself the final sharing priority.

Evidence: [Louvain summary](screenshots/08l-hi-louvain-summary.png), [example membership](screenshots/08m-hi-community-membership.png), [cross-community corridors](screenshots/08n-hi-cross-community-corridors.png).

### Transaction-supported candidates

Rules: same intermediary bank AND account, strictly increasing timestamps, maximum 24 hours per adjacent transfer, distinct transaction IDs. Amounts need not match; timestamps have no supplied timezone. These rules establish candidate continuity, not proof of the same funds or criminal intent.

| Candidate | Evidence and limitation |
| --- | --- |
| 7818 → 121523 → 134266 | 6 September: 05:10 → 13:24 (8h14m). Accounts 8037732C0 → 80D1BD2F0 → 814167590. Transactions 9173283034951331860 and -1430906872139732606; $10,559.66 and $9,714.20. Both labelled; attempt 2 in source. |
| 7818 → 25828 → 212353 → 11796 | 13 September: 06:46 → 09:10 → 15:21. Gaps 2h24m and 6h11m. Accounts 805753DD0 → 804B45920 → 805754320 → 80139E060. Transactions 7394026252962401598, -2980862507599786311, 606750438349263592; $3,459.32, $8,438.51, $9,143.41. All labelled, but attempt annotations 670, 2117, 2117: not a single annotated attempt. |

Evidence: [two-hop source](screenshots/08e-hi-two-hop-source-verification.png), [two-hop path](screenshots/08f-hi-verified-two-hop-path.png), [three-hop source](screenshots/08g-hi-three-hop-source-verification.png), [three-hop path](screenshots/08h-hi-verified-three-hop-path.png). The structural route 1488 → 0 → 215275 had no qualifying account/time candidate; it must remain a structural illustration only.

### Ryan handoff and lineage

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

Other execution IDs were not supplied here; do not infer them from transaction IDs. Source SQL: [exports](export_edges.sql) and [feature registration, checks and join](athena_handoff.sql). All observations are from the manual run and local screenshots; the SQL files consolidate those statements and have not been executed as an automated pipeline.

Automation implementation/status: [pipeline interface](README.md#step-functions-info-for-sonia). Offline tests pass; no deployed automation success is claimed.

### HI/LI cutoff measurement - first robustness query validated

Athena query ID: `679b8d6e-083d-4d27-90ab-d72cc0d72d1c`. Results supplied from the successful manual query. Evidence: [counts and dataset labels](screenshots/10a-hi-li-cutoff-counts.png) and [USD values and dates](screenshots/10b-hi-li-cutoff-values.png). The values panel omits dataset labels; read it with the counts panel and the query's dataset ordering (HI first, LI second). Full result CSV archival is not yet confirmed. Population is all labelled laundering rows per dataset before applying the cutoff. USD values below are rounded to cents; Athena DOUBLE output contains minor floating-point representation differences.

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

### HI/LI filtered and unfiltered key checks - second robustness query passed

Athena query ID: `cf3afa7b-c85c-4f3b-883a-f34dc08d1543`. Evidence: [HI/LI population key checks](screenshots/10c-hi-li-population-key-checks.png). User-supplied Athena results verified against the saved screenshot:

| Dataset | Population | Selected rows | Bad banks | Bad accounts | Bad times | Missing USD | Duplicate/null transaction IDs |
|---|---|---:|---:|---:|---:|---:|---:|
| HI-Medium | below_20m | 34,891 | 0 | 0 | 0 | 0 | 0 |
| HI-Medium | unfiltered | 35,230 | 0 | 0 | 0 | 0 | 0 |
| LI-Medium | below_20m | 15,892 | 0 | 0 | 0 | 0 | 0 |
| LI-Medium | unfiltered | 16,041 | 0 | 0 | 0 | 0 | 0 |

All counts match the preceding cutoff summary. The duplicate/null-ID check is COUNT(*) minus COUNT(DISTINCT txn_id), so zero also establishes no null transaction IDs within each selected population. These checks establish key/timestamp/USD completeness and transaction-ID uniqueness for the queried snapshot; they do not verify every timestamp's correctness, make extreme amounts plausible, or complete graph robustness.

### Corridor cutoff comparison - third robustness query executed

Athena query ID: `3bbfb6b1-d294-478a-8e94-c76256630cbb`. The query includes labelled cross-bank rows from both datasets and sorts by dataset, then excluded USD descending. Evidence: [HI corridor cutoff impact](screenshots/10d-hi-corridor-cutoff-impact.png), showing 43,518 total result rows and leading HI corridors. The six examples recorded below are HI-Medium only; LI rows and full-result reconciliation remain to inspect. Local CSV download is optional; preserve the query's Athena output location for full-result access.

| HI corridor | Unfiltered transactions | Retained transactions | Unfiltered / excluded USD | Retained USD |
|---|---:|---:|---:|---:|
| 171692 -> 240298 | 1 | 0 | $15,938,012,093.16 | $0.00 |
| 27512 -> 22235 | 1 | 0 | $6,958,804,909.33 | $0.00 |
| 22235 -> 224668 | 1 | 0 | $6,953,680,383.30 | $0.00 |
| 159430 -> 52481 | 1 | 0 | $6,329,143,641.81 | $0.00 |
| 1158540 -> 1158270 | 1 | 0 | $4,728,323,260.40 | $0.00 |
| 114976 -> 230522 | 1 | 0 | $3,566,430,066.84 | $0.00 |

Each listed corridor's only labelled transaction is above the primary threshold, so its retained count and value are zero. This removes these labelled corridor edges from the filtered graph; it does not establish that the bank endpoints disappear entirely, that the banks never transact, or that the synthetic record is a confirmed data error. This is direct-corridor sensitivity, not a rerun of Role 3's shared-attempt ranking or a graph-centrality/community robustness result.

### LI corridor cutoff examples - preview executed

Athena query ID: `ff1ff99e-ada4-454c-b54e-2f4597c55d3e`. The separate LI-only query returns the five corridors with the greatest excluded USD. Results supplied by the user and verified against [saved LI corridor evidence](screenshots/10e-li-corridor-cutoff-impact.png).

| LI corridor | Unfiltered transactions | Retained transactions | Unfiltered / excluded USD | Retained USD |
|---|---:|---:|---:|---:|
| 112901 -> 18379 | 1 | 0 | $11,639,247,997.83 | $0.00 |
| 2204310 -> 11 | 1 | 0 | $6,514,489,569.29 | $0.00 |
| 14745 -> 110320 | 1 | 0 | $5,969,941,668.18 | $0.00 |
| 30671 -> 181000 | 1 | 0 | $5,715,749,939.62 | $0.00 |
| 58172 -> 2114931 | 1 | 0 | $5,544,103,899.56 | $0.00 |

Each listed LI corridor's only labelled transfer is excluded by the primary cutoff, as in the leading HI examples. These observations support reporting the strong influence of extreme values in both synthetic populations. They do not establish stability of centrality, communities or the final priority list. Numeric bank IDs across HI and LI are not assumed to identify the same real entity.

### Whole-corridor totals - aggregate check passed

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

This completes the observed source/corridor arithmetic checks. It does not establish that centrality, communities or the final sharing priorities are stable under a different cutoff, weighting or dataset. LI graph algorithms have not yet been run. Use the LI filtered corridor/transaction/USD totals above as the later import baseline, with the same source snapshot and filter.

### Final visualisation tool - team decision

On 3 November 2026 the user reported the team's approval: **use Neo4j Browser instead of Bloom**. The compatibility/fallback decision is resolved. Use Browser for the intermediary, community and verified account-path scenes; the final scenes/captions are still pending. Record the substitution from the proposal in the report/presentation. This is team agreement, not an assertion of instructor approval or Bloom deployment. No Bloom licence or perspective file is required for this agreed route.

### Measurements still to add

The robustness source, key, corridor-preview and whole-corridor checks have executed with results, query IDs and screenshot evidence recorded above. The visualisation sources on the Role 4 branch and graph-robustness Cypher on the Role 4 branch remain unexecuted. Add the three agreed Browser scene filenames/captions, LI graph results, cutoff/weighting comparison conclusions, final sharing recommendations and live automation runtime/execution references as those tasks finish. These source/corridor checks alone do not complete graph robustness.
