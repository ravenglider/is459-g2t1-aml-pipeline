# Role 4 — HI run hi-01 findings

## Role 4: HI run hi-01

Owner: Role 4. Manual run validated on 3 November 2026. BQ1: Which bank pairs are most involved in laundering, and which should prioritise information sharing?

HI graph/import/GDS and the Athena join preview are validated. Bloom, LI and cutoff sensitivity, the final recommendation package, live trials of the prepared SSM runner and end-to-end Step Functions integration remain pending. This is a findings record, not a claim that all Role 4 deliverables are complete.

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

Automation implementation/status: [pipeline interface](README.md#pipeline-interface). Offline tests pass; no deployed automation success is claimed.

### Measurements still to add

Final Bloom/fallback access and scenes, LI/cutoff/weighting comparisons, final sharing recommendations and live automation validation remain pending. Their draft sources and runner tests are held locally and are not part of this validated manual-work submission. Add measured results and genuine generated artefacts when those tasks finish.
