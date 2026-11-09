# BQ2 sample worksheet evidence

Owner: Xiang Ying (Role 6). Captured 8 November 2026 from the sample Tableau workbook. Nine screenshots show the combined pairs/triples dashboards and five base worksheets: ranking and trade-off each captured at lengths 2 and 3, separate pair and triple Sankeys, and the targeted checklist at length 2/top-3. Original Desktop PNGs were retained and copied without editing. The ranked-table titles now name laundering lift.

| Visual | Screenshot | Description and business relevance |
|---|---|---|
| Ranked pairs | [14-sample-ranked-pairs.png](14-sample-ranked-pairs.png) | Ranks two-transaction patterns by laundering lift relative to the pair baseline. Support and labelled counts help analysts distinguish stronger evidence from patterns driven by very few positive examples. |
| Ranked triples | [15-sample-ranked-triples.png](15-sample-ranked-triples.png) | Cash → Cheque → Wire has the highest sample lift at 75.81, based on nine labelled sequences. This highlights a candidate for further investigation, subject to validation on the final datasets. |
| Trade-off — pairs | [16-sample-tradeoff-pairs.png](16-sample-tradeoff-pairs.png) | Expanding from top-5 to top-10 increases catch rate from 48.57% to 51.43%, while legitimate review rate rises from 2.13% to 14.46%. This makes the additional review workload visible when choosing monitoring coverage. |
| Trade-off — triples | [17-sample-tradeoff-triples.png](17-sample-tradeoff-triples.png) | All three targeted policies flag the same two laundering transactions, while review volumes increase. In this sample, expanding beyond top-3 adds workload without improving detection coverage. |
| Sankey — pairs | [18-sample-sankey-pairs.png](18-sample-sankey-pairs.png) | Shows payment-method transitions across 134 laundering-labelled pair occurrences, with Cash → Cheque forming the largest flow. It helps analysts see where labelled activity concentrates across payment methods. |
| Sankey — triples | [19-sample-sankey-triples.png](19-sample-sankey-triples.png) | Shows payment-method transitions across 38 laundering-labelled triple occurrences. It helps analysts explore longer channel-switching patterns alongside the exact sequence rankings. |
| Targeted checklist | [20-sample-targeted-checklist.png](20-sample-targeted-checklist.png) | Lists the selected ordered payment-method patterns with rank, support, labelled sequences, rate and lift. The visible length-2/top-3 selection supports a review checklist while clearly marking LI validation as pending. |
| Combined pairs dashboard | [21-sample-dashboard-pairs.png](21-sample-dashboard-pairs.png) | Combines review-workload trade-offs, labelled payment-method paths, lift rankings and a top-5 sample checklist. The visible disclaimer separates development evidence from final HI/LI findings, and the definitions distinguish sequence weights from distinct final transactions. |
| Combined triples dashboard | [22-sample-dashboard-triples.png](22-sample-dashboard-triples.png) | Combines triple-path rankings, labelled flows, review-workload trade-offs and the top-3 checklist. All three targeted policies catch 5.71% of sample laundering transactions while review workload increases, showing the limits of broader pattern selection. |

## Interpretation and scope

Sample development results only. Patterns were selected and evaluated on the same sample; final recommendations require HI/LI validation. A positive sequence contains at least one laundering-labelled member. Sequence occurrences may overlap and do not represent unique laundering transactions or monetary value. Sankeys merge shared transitions; they do not establish every possible complete path through those transitions.

Rankings and trade-offs use minimum support 100. Sankeys retain all pattern counts, including rare patterns; band width is positive sequence occurrences. Top ten is a ranking display, not a claim that all ten patterns have elevated lift: some displayed patterns have zero positives. Trade-off policies count distinct final transactions using original source labels, against all 500,000 cleaned sample transactions (35 positive, 499,965 legitimate). Review-all is a comparison endpoint, not a deployed monitoring policy.

Evidence is supported by the checked local CSV exports and [Role 6 run record](../../bq2/lift/results.md). Screenshots demonstrate rendered worksheets, not independent reproduction of the workbook or interactive behaviour. Minor presentation limitations remain: the positive-sequence column heading and trade-off legend are truncated, and very small Sankey nodes may have no visible labels. Full values remain in the validated exports.

## Remaining delivery

The sample targeted checklist is rendered and its visible top-3 pair values match the validated lift export. Xiang Ying reports testing checklist top-3/5/10 and length switching; the dashboard screenshot additionally shows the expected top-5 pairs. This is user-reported interactive acceptance, not an independent replay. Both combined sample dashboards are captured; inspection/delivery of the current native workbook remains pending; local CSVs/extracts are not committed. Final HI/LI outputs, frozen HI-rule evaluation on LI and final recommendations remain separate work. See the [Tableau build guide](../bq2_tableau.md).


## Final HI–LI robustness dashboard — 9 November 2026

The following final-data screenshots are separate from the sample evidence above:

- [35: selected pairs](35-final-robustness-pairs.png): ten frozen HI-ranked pair patterns, minimum support 100, with HI/LI support and lift. All ten retain LI lift above 1; ACH -> ACH declines from 8.51 to 2.23.
- [36: selected triples](36-final-robustness-triples.png): ten frozen HI-ranked triple patterns at the same cutoff. Eight retain LI lift above 1; ACH -> ACH -> Cheque (0.99) and ACH -> ACH -> Cash (0.92) fall below the LI baseline.

Visible ranks, supports and rounded lifts match the validated final robustness CSV. The caption correctly uses each datasets own sequence baseline. Above-baseline lift is a descriptive comparison, not statistical significance or production validation. Both dropdown states are evidenced by screenshots; save/reopen acceptance of the native workbook remains pending. Images copied byte-for-byte from Desktop; originals retained.


## Final HI dashboards — 9 November 2026

- [37: HI pairs dashboard](37-final-hi-dashboard-pairs.png): final pair rankings, positive-sequence Sankey, distinct-final-transaction trade-offs and top-5 checklist. Visible values match validated final HI exports.
- [38: HI triples dashboard](38-final-hi-dashboard-triples.png): final triple rankings, positive-sequence Sankey, distinct-final-transaction trade-offs and a top-5 checklist control. Corrected capture: visible values match checked exports, all ranked Support Count cells are readable and all five selected checklist rows are visible. Supersedes the earlier clipped capture.

Both images copied byte-for-byte from Desktop; originals retained. Some table headings are truncated and small Sankey nodes lack labels. Xiang Ying subsequently confirms saving, closing and reopening successfully loaded all three dashboards. This is user-reported native-app acceptance; the workbook file remains uninspected.

Final dashboard checks completed: HI scope and sequence lengths, support-100 policy sources, validated rates/rankings, both robustness filter states, corrected triples counts/checklist visibility, and user-confirmed save/reopen. Final native workbook packaging and automation handoff remain separate.
