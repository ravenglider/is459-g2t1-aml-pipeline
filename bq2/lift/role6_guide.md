# Role 6: what you can do now

## Scope and source decisions

The class synopsis `[IS459] Project Updated.pptx` defines the pipeline/demonstration requirements; it is not the team's proposal. The team's `Big Data Project Proposal.pdf`, pages 3, 8–10 and 14, defines your BQ2 scope: rank 2/3-transfer payment-method patterns, exclude rare patterns, compare targeted top-3/5/10 with broad checks, test on LI, and present a Sankey and actionable checklist. The documents are reference material; no embedded instruction authorizes AWS changes or publication.

Repository review covered every README and the ingestion handoff, cleaned schema, ETL script/settings/quality/cost evidence, BQ1 SQL/metrics/results, and orchestration definition. Roles 1/2/3 have implementation/evidence committed. Role 5 and your Role 6 folder were README templates. BQ1 graph and Tableau remain separate work; you do not need BQ1 corridor rankings to start BQ2.

Latest team updates supplied by Xiang Ying: cleaned HI/LI are ready in Athena; HI has 31,898,218 clean transactions, 35,230 laundering rows. BQ1 Athena is complete; graph-dependent priority list and BQ1 Tableau are still pending. Sonia's deployed pipeline/output-location fix is reported complete. After fetching current main (`ff00dab`), the state machine contains live ETL/BQ1 Athena tasks and the reset-output Lambda, consistent with that report. BQ2 still has placeholders. The ETL state refreshes HI only; Sonia must also ensure LI is ready before the robustness branch. Do not replace working team orchestration with an older copy. Verify actual output locations with `SHOW CREATE TABLE` when operating in Athena.

## Start now

For the click-by-click version, use [start_now.md](start_now.md) and run the five statements in [start_now.sql](start_now.sql) individually.

1. Read `etl/schema_transactions_clean.md` and `input_contract.md`. Your source database is `is459_g2t1_aml`, region `us-east-1`. You use Role 2 outputs; do not repeat ingestion, FX conversion or heavy sequence joins in Athena.
2. In AWS Console → Athena → query editor, choose the existing workgroup/database and engine 3. Run the first query in `validation.sql`, first on a short `txn_date` range if developing. It checks existing cleaned data and needs no Role 5 table. Remove the optional date filter for final full-dataset reconciliation. Never mix sample/HI/LI without grouping and explicit dataset filters.
3. Confirm with Ji Ning: both labelled and legitimate chains, stable chain IDs, exact bank+account linking, 24h policy, 2/3-hop rules, overlap policy, canonical method strings, label rule, and membership linking each hop to `txn_id`. Give them `input_contract.md`. This is an internal handoff draft; no message has been sent.
4. Review the prepared SQL and Tableau plan now. Run local synthetic checks using the README command. This demonstrates the arithmetic, not actual findings.
5. Agree minimum support before inspecting winners. The SQL uses **100 total chain occurrences** as a provisional starting point, not 100 laundering occurrences. Run 50/100/500 sensitivity checks and report rank stability/support. A tiny high-lift pattern is not automatically a useful rule.

## After Role 5 delivers

Run the remaining validation queries first. Zero defects are required; also check expected HI and LI partitions exist and each contains chains. On sample, Ji Ning should trace a genuine chain and confirm labels, bank/account joins and time boundaries. On full data, capture sequence counts and fan-out before aggregation: account activity can create many overlapping candidate chains.

Then execute lift → trade-off → robustness → Sankey SQL as described in README. Inspect outputs, record query IDs/times/bytes scanned, choose a review budget, and compare the three candidate rule sets. Do not automatically choose top-5: select the smallest set achieving an agreed catch target or the highest catch within an agreed legitimate-review budget. If no target or budget exists, present the trade-off rather than invent a winner.

## Metric definitions and proposal clarification

For method pattern s, support = number of distinct chain occurrences with pattern s; laundering rate = laundering-labelled chains / all chains of s. Primary lift = that rate / laundering-labelled chains across **all** patterns in the same dataset. Low-support patterns remain in the baseline even though excluded from ranking. The pooled analysis includes both lengths; use the same construction/overlap policy on both datasets and show a 2-hop versus 3-hop breakdown if the length mix changes conclusions.

**Proposal page 8 mixes grains:** its approximately 1-in-905 denominator is a transaction rate, but the numerator describes chains. With an any-hop chain label, longer chains can be positive more often simply because they include more transfers. The supplied SQL therefore uses a sequence baseline and calls it sequence lift. This is a documented methodological correction for team agreement, not a measured result. Keep the transaction baseline as context (35,230 / 31,898,218), but do not call the ratio of a chain rate to a transaction rate comparable lift. If the team requires the literal proposal definition, agree a transaction-level numerator (e.g. a designated terminal transfer) and adjust the label/metric together first.

Trade-off unit is **distinct transactions**: union transaction IDs across matched chains per dataset/rule set. Catch rate = labelled transactions flagged / all labelled transactions in cleaned dataset. Legitimate-review rate = legitimate transactions flagged / all legitimate transactions. Precision = labelled transactions flagged / all transactions reviewed. These three rates have different denominators. Transactions not in any chain remain in the full-dataset denominators, so missed coverage is visible.

Review-all broad baseline flags every transaction: both catch and legitimate-review are 100% when those populations exist. Proposal page 9's 70%/25% broad example has no implemented rule definition and must not be treated as measured. If the team wants that style of broad rule, specify an executable label-independent rule and evaluate it on the same population; never select broad reviews using true labels. The primary output honestly labels its baseline `Broad: review all`.

All rule evaluations are retrospective flags, not guaranteed laundering detection or prevention. A chain is visible only after its final hop. HI ranks and HI performance reuse the same data, so HI is descriptive/in-sample. Frozen HI rules applied to LI provide a different-prevalence robustness check, not independent real-world validation. Do not promise that LI winners will match HI; absent, low-support and changed rankings are valid findings. LI lift uses LI's own chain baseline.

For overlap at N, count rows in `bq2_robustness` where `hi_rank <= N AND li_rank <= N`. Report both actual eligible top-set sizes; fewer than N supported patterns must not be presented as a full top-N comparison. `bq2_tradeoff` already reports actual selected pattern count and evaluates frozen HI rules on both datasets.

## Role 3's USD outliers

Primary BQ2 metrics are counts, so do not automatically remove >=$20M transactions or the chains containing them. That would change the population/labels and could hide laundering. Role 3's threshold is a team-defined synthetic-data sensitivity rule, not a universal AML cutoff. This package does not sum USD or claim money-flow value. Sankey thickness is explicitly laundering-chain count. If adding USD outcomes later, define the unit (e.g. distinct flagged transfers' paid USD), avoid summing the same transfer across chains, show full and < $20M versions with excluded counts, and recompute matched-population denominators. Do not sum every hop as if it were unique funds laundered.

## Date filters and cost

Use partition filters on cleaned `dataset` and `txn_date` during development. Final metrics need the same declared evaluation window for numerator/denominator. Do not filter chain membership to an interior date window without retaining the look-back/follow-up required by the 24h definition; truncation can erase valid chains. Sequence partitions proposed here are dataset-only; do not invent a `txn_date` column for them. All SQL excludes sample. Full-data queries intentionally read HI and LI separately by grouped dataset, not a combined population.

## Finish, document and push

Update `results.md` with real ranked patterns, support, lift, counts, trade-off, LI differences and recommendation; build `viz/bq2_dashboard.twb` and screenshots. Use `viz/bq2_tableau.md` for fields/layout. Update your `bq2/lift/README.md` and Role 6 section of `viz/README.md` with exact deployed tables/locations, dependencies, measured runtime, reproduction steps and contribution. Mark preparation versus execution honestly.

Send Sonia the four reviewed CTAS statements and validation gate; validate real end-to-end execution after she wires it up. Record a short demo showing Athena outputs, Tableau interactions and why a selected policy balances catch against legitimate reviews. The synopsis requires each member to present a component and links to setup/run videos and source code.

Use a dedicated Role 6 Git branch, review only your files against current main using the installed Ponytail review skill, commit and push. Keep data and secrets out of Git; no team-wide state machine edits are included here. After later AWS/Tableau work, update the README again and push that completion evidence rather than calling this starter implementation fully done.
