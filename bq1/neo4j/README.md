# bq1/neo4j/ (Role 4: BQ1 graph)

Owner: Le Xin

## What it does

Builds bank and account graphs from cleaned laundering-labelled transactions for BQ1: which bank pairs are most involved, and which should prioritise information sharing? Cypher paths, GDS betweenness and Louvain provide network context for Role 3's corridor ranking. The team approved Neo4j Browser in place of the proposed Bloom visuals.

| File | Purpose |
|---|---|
| [ec2_setup.md](ec2_setup.md) | Existing shared EC2/container configuration, staging, connection and stopping |
| [validation.sql](validation.sql) | Source/schema/key checks and exact transaction verification |
| [export_edges.sql](export_edges.sql) | Three primary export/reconciliation SELECTs; same template supports sample/HI/LI |
| [import.cypher](import.cypher) | Scope-qualified keys, imports and reconciliation; manual population mapping |
| [queries.cypher](queries.cypher) | Paths, directed betweenness, count-weighted Louvain and feature export |
| [visualisations.cypher](visualisations.cypher) | Verified HI sharing case, intermediary activity and community neighbourhood views |
| [browser-style.grass](browser-style.grass) | Browser colour/size/width rules; bank/account and relationship captions selected separately in Browser |
| [robustness.sql](robustness.sql) | Executed HI/LI source/cutoff checks and uncapped HI export |
| [robustness.cypher](robustness.cypher) | Weighting/cutoff overlaps, saved-scope comparisons and HI restoration |
| [athena_handoff.sql](athena_handoff.sql) | HI feature registration, completeness and Role 3 join checks |
| [findings.md](findings.md) | Measured results, limitations, evidence and query references |
| [run_graph.py](run_graph.py), [test_run_graph.py](test_run_graph.py) | Tested EC2/SSM runner and offline tests; integrated into Sonia's completed shared pipeline |
| [screenshots/](screenshots/) | Named evidence images only |

Manual sample/HI/LI analyses, Browser scenes and the planned HI weighting/cutoff
comparisons are complete based on reported executions and saved evidence.
Task 11 direct EC2 validation is complete: one sample and two fresh-ID HI runs
passed output, baseline and repeatability checks, preserving the accepted HI
graph's stored properties. Task 12 SSM sample success/output acceptance,
duplicate-ID rejection, an isolated shell-timeout probe and read-only
feature-validator rejection passed on 7 November 2026. Current Active labels may
point to the latest pipeline graph run. The initial Sonia handoff was sent.
Sonia completed the whole-pipeline run and automatic priority-table rebuild,
as confirmed by the team and user. Ryan confirms graph-feature adoption.
Execution references and independent read-only checks remain evidence follow-ups,
not unfinished pipeline implementation.

## AWS resources

| Resource | Exact name / configuration |
|---|---|
| Region | `us-east-1` |
| Shared EC2 | `is459-g2t1-ingest` / `i-0125056b848e5634a` |
| Instance type | Temporarily `t3.medium` with team agreement |
| IAM role | `is459-g2t1-ec2-s3-role` |
| Verified access | Attached `AmazonS3FullAccess` and `AmazonSSMManagedInstanceCore`; direct-run Athena/Glue operations and sample SSM execution verified. Shared orchestration permissions remain Sonia's integration responsibility |
| Container / database | `role4-neo4j`, Neo4j 5.26.32 Community with GDS; persistent volumes |
| Access | Session Manager; Browser 7474 and Bolt 7687 restricted to current public IP |
| Athena workgroup / database | `primary` / `is459_g2t1_aml` |

Reuse the shared instance, coordinate use and stop idle compute. Protected instance-local credentials and data CSVs remain outside GitHub.

## Inputs and outputs

| Direction | Table / file | Location |
|---|---|---|
| Reads | `transactions_clean` | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/` |
| Reads for join | `bq1_corridors_ranked` | `s3://is459-g2t1-aml-processed-bucket/bq1_corridors_ranked/` |
| Stages inputs | `bank_edges.csv`, `transactions.csv` | Processed bucket `bq1_graph_edges/RUN_ID/` |
| Accepted HI output | `bq1_graph_bank_features_hi01` | Processed bucket `bq1_graph_results/hi-01/bank_features/bank_features.csv` |
| Current pipeline interface | `bq1_graph_bank_features_latest` | Sonia's view points to the validated run-specific feature table |
| Consuming priority table | `bq1_priority_sharing_list` | Processed bucket `bq1_priority_sharing_list/`; rebuilt by the shared pipeline |

Tables are in `is459_g2t1_aml`. Feature schema is exactly `bank_id,betweenness,community_id`; run identity is in the table/prefix. Accepted HI uses `is_laundering=1 AND amount_paid_usd<20000000`; bank edges also require `cross_bank=1`. Account evidence retains same-bank transfers and missing attempt annotations. The cutoff is a team convention, not a regulatory threshold.

Saved populations are `sample-01`, `hi-01`, `li-01` and bank-only `hi-uncapped-01`. Comparisons do not replace the accepted HI feature table. CSV completeness/import totals must be checked for each source snapshot.

## How to run

The saved-scope queries in `visualisations.cypher` read `hi-01` without changing
Active labels. The sharing-case source records, individual/combined account
views, visibility summary and timeline were checked against live results.
Bank 11's neighbourhood and activity totals, plus a cross-community corridor
and its displayed memberships, were also verified. See `findings.md` for
results, evidence and interpretation limits. Earlier evidence remains valid.

These are sectioned source records. Run the intended statements individually; do not execute every Cypher file from top to bottom or rerun accepted analysis merely to inspect results.

1. Check the shared instance and Neo4j/GDS using [ec2_setup.md](ec2_setup.md). Preserve credentials and volumes.
2. Run relevant `validation.sql` statements in Athena. Investigate bad keys/duplicates before importing.
3. Use `export_edges.sql` for the chosen primary population, or the uncapped bank-only SELECT in `robustness.sql`. Save complete CSVs under a new input prefix; reconcile source totals.
4. Stage files using the setup record. In Browser, set the matching `$scope` and use matching file URLs in `import.cypher`. Verify active scope, counts and USD; repeat imports must create no new nodes/relationships.
5. Run the selected path and TASK 6/7 sections of `queries.cypher`. Create projections from the verified active scope, check memory estimates before algorithm writes and verify saved assignment coverage. Validate account-path examples against Athena.
6. Create bounded scenes using `visualisations.cypher`. Read or reproduce sensitivity results using the explicitly labelled sections of `robustness.cypher`. Restore accepted HI after switching populations.
7. For the accepted feature handoff, export every HI bank using the final query in `queries.cypher`. Browser's result limit must exceed graph size: the corrected HI CSV has **5,159 data rows plus header**. The earlier 5,000-row export was incomplete.
8. Upload features and run `athena_handoff.sql`. Check uniqueness/completeness and unchanged join counts before consumption by Role 3. Keep missing features NULL, not zero risk.

Bank keys are `(scope,bank_id)`; account keys are `(scope,bank_id,account_id)`. New source contents require a fresh scope. GDS projections disappear on restart and must be rebuilt after active-scope changes. Saved properties persist. Betweenness is directed/unweighted; primary Louvain is undirected/count-weighted. Community IDs are arbitrary labels. Account/time continuity supports a candidate, not proof of identical funds or bank intent.

### Browser visualization configuration

Use `:style` in Browser to export a backup and upload browser-style.grass.
The observed Browser export uses node `size` and relationship `width`; caption
rules were not retained from the initial stylesheet. Select captions separately
in Results overview: Account/ActiveAccount -> `bank_id`,TRANSFER ->
`amount_paid_usd`,Bank/ActiveBank -> `bank_id`,and LAUNDERING_TO ->
`laundering_txns` when inspecting aggregated corridors. Both account-path
and bank-corridor captions have been visually checked.

In the account-path view,each circle remains an Account even when its caption
shows a bank ID. Inspect account_id and transaction timestamps in properties or
the CASE6 timeline; do not call it an aggregated bank graph. Transfer captions
are USD amounts,not unique proceeds. Colours distinguish entity/relationship
types,not risk scores or communities. Arrange the selected path in transfer
order and keep arrows/amounts separated. These Browser-only settings do not
change graph records or the shared pipeline.

For COMMUNITY2, place community 3881's displayed banks on the left and community
3166's on the right, with `114976 -> 297435` between them. COMMUNITY3 lists their
verified memberships. Layout separates these selected neighbourhoods; the
stylesheet does not colour nodes by community. Positions may need arranging
again after rerunning the query.

## Step Functions info for Sonia

**Pipeline integration (10 November 2026):** the graph branch runs inside `is459-g2t1-pipeline` through SSM document `is459-g2t1-role4-graph`. Full pipeline executions `9thrun_fixedbq2sequences` and `demo_run` (both 9 November 2026) succeeded end to end, which includes this branch validating the run's `success.json` and updating `bq1_graph_bank_features_latest` before the priority list was rebuilt.

| Field | Value / status |
|---|---|
| Job type | Neo4j export/import/analysis on existing EC2; sample SSM invocation validated |
| Exact runner | `run_graph.py`, deployed release `/home/ssm-user/role4/code/role4-20261106-01/run_graph.py` |
| Required release files | Runner, `export_edges.sql`, `import.cypher` together |
| Tested direct EC2 command | `sudo python3 -u /home/ssm-user/role4/code/role4-20261106-01/run_graph.py --run-id hi-auto-20261106-01 --dataset HI-Medium` (historical run; use a fresh ID for future work) |
| Target | `i-0125056b848e5634a`, container `role4-neo4j` |
| Tested SSM interface | `is459-g2t1-role4-graph`, version `1`; parameters `runId` (fresh immutable ID) and `dataset` (`sample`, `HI-Medium`, `LI-Medium`) |
| Observed SSM sample trial | `sample-ssm-20261107-01`,42.217389s,Success/response0; output coverage42 banks/8 communities. Rejection and isolated shell-timeout evidence are in findings.md |
| Accepted manual output table | `is459_g2t1_aml.bq1_graph_bank_features_hi01` |
| Accepted manual output prefix | `s3://is459-g2t1-aml-processed-bucket/bq1_graph_results/hi-01/bank_features/` |
| Automated output contract | New run-specific prefix/table; validated `success.json` published last; never overwrite accepted runs |
| Dependencies | Stable accepted `transactions_clean`, ready EC2/Neo4j/GDS, protected credentials, verified Athena/Glue/S3 permissions |
| Later join dependency | Role 3's corridor table; not required to calculate graph features |
| Observed direct runtime | HI run01:63.522618s; fresh HI run02:52.480852s; sample:37.155682s. Not an SSM/end-to-end SLA or billed EC2 uptime |
| Completion/failure handling | Poll terminal SSM outcome with bounded timeout; require zero response code and validated outputs; coordinate EC2 lifecycle and exclusive graph use |

Sonia owns changes to the shared graph branch (formerly the `BQ1_Neo4jLoad` placeholder), state machine and
orchestration permissions. Role 4 sent her the tested command/interface, output
table/S3 path, dependencies, runtime and evidence, following
[the ingestion handoff](../../docs/ingestion-handoff.md#how-your-job-gets-added).
Role 4 does not edit or deploy the shared Step Functions workflow. Sonia completed
the whole pipeline and rebuilt `bq1_priority_sharing_list` with
the updated SQL. The checked-in graph branch validates the receipt,updates
`bq1_graph_bank_features_latest`,then the downstream priority CTAS joins this view.
Collect the actual execution/run references and read-only output checks before
claiming independently verified end-to-end acceptance. Do not manually run CTAS,
drop/reset shared tables or refresh Tableau during a pipeline run.

Direct trials, SSM sample output acceptance, duplicate-ID rejection, the isolated
shell-timeout probe and read-only feature-validator rejection passed. The last
probe checked the real validator on a deliberately truncated in-memory copy;
it did not execute a full failed graph run or prove live publication suppression.
Integrated failure/timeout handling remains Sonia-owned acceptance. The runner
enforces the primary cutoff; it does not implement uncapped comparisons or
start/stop EC2. Personal messages and draft orchestration configuration stay
local; Sonia retains adopted configuration in the team's pipeline source.

Validated direct-run performance instrumentation records stage wall times, Athena execution statistics and result-download times in the success receipt; algorithm timings include their projection/estimate preparation. The final log elapsed time includes success-receipt publication, while the receipt explicitly records elapsed time before its own publication. Direct wall times are measured and recorded in findings.md; EC2 billed uptime, cost and observed peak RAM have not been established.

## Contribution summary

Built and manually validated the bank/account graph workflow, source-supported path examples, intermediary/community analyses, three Browser scenes and the HI feature join. Compared HI/LI populations and HI cutoff/weighting sensitivity; documented evidence and limitations.

Accepted HI: **5,159 banks, 29,374 corridors, 34,552 cross-bank transactions, 113 communities**; feature join preserves **1,697,382 rows**. LI: **4,924 banks, 142 communities**. Uncapped HI: **5,166 banks, 111 communities**, with **9/10** top intermediaries shared with filtered HI. Detailed results and weighting caveats are in [findings.md](findings.md).

Current improvement work: finish the saved-scope investigation views,verify Browser styling and capture readable visualization evidence. Ryan reports graph-feature adoption and Sonia reports a completed pipeline rebuild; execution references and independent refreshed-output checks remain to be recorded. Performance stages and GDS estimates are documented; no full-run publication-suppression claim is made from the read-only validator probe. The independent Task11 sample feature query ID remains an evidence-reference follow-up. Personal guide/notes,helpers,CSVs and coordination drafts remain gitignored.
