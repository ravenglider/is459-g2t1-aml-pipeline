# bq1/neo4j/ (Role 4: BQ1 graph)

Owner: Role 4

## What it does

Builds a Neo4j graph from retained labelled laundering transactions in `transactions_clean`. Cypher identifies bank routes and account/time-supported path candidates; GDS betweenness identifies intermediaries and Louvain groups connected banks. The resulting features and evidence support Role 3's priority information-sharing list for BQ1.

Files:

- [ec2_setup.md](ec2_setup.md): shared EC2, Docker, Neo4j/GDS configuration and connection details
- [validation.sql](validation.sql): source/schema, cutoff and graph-key checks
- [export_edges.sql](export_edges.sql): bank-edge/account-transaction exports and source reconciliation
- [import.cypher](import.cypher): scoped imports, constraints and repeat-import checks
- [queries.cypher](queries.cypher): multi-hop candidates, verified examples, betweenness, Louvain and feature export
- [athena_handoff.sql](athena_handoff.sql): feature-table registration, validation and Role 3 join
- [findings.md](findings.md): measured HI results, evidence links and Athena query IDs

## AWS resources

| Resource | Exact name / configuration |
|---|---|
| Region | `us-east-1` |
| Existing EC2 | `is459-g2t1-ingest` / `i-0125056b848e5634a` |
| Instance type | Temporarily resized to `t3.medium` for Neo4j/GDS |
| IAM role | `is459-g2t1-ec2-s3-role` |
| Verified role policies | `AmazonS3FullAccess`, `AmazonSSMManagedInstanceCore` |
| Access | Systems Manager Session Manager; Neo4j ports restricted to the current public IP |
| Docker container | `role4-neo4j` |
| Database | Neo4j 5.26.32 Community with GDS; persistent Docker volumes |
| Athena workgroup / database | `primary` / `is459_g2t1_aml` |
| Accepted HI output table | `is459_g2t1_aml.bq1_graph_bank_features_hi01` |

Reuse the group's instance. Stop it when idle after coordinating shared use. Passwords stay in the protected instance-local `.env`, never in Git. The existing S3/SSM policies do not establish the Athena/Glue permissions needed for future automation.

## Inputs and outputs

| | Table / file | S3 path |
|---|---|---|
| Reads | `is459_g2t1_aml.transactions_clean` | `s3://is459-g2t1-aml-processed-bucket/transactions_clean/` |
| Reads for final join | `is459_g2t1_aml.bq1_corridors_ranked` | `s3://is459-g2t1-aml-processed-bucket/bq1_corridors_ranked/` |
| Writes graph inputs | `bank_edges.csv`, `transactions.csv` | `s3://is459-g2t1-aml-processed-bucket/bq1_graph_edges/hi-01/` |
| Writes bank features | `bank_features.csv`, registered as `bq1_graph_bank_features_hi01` | `s3://is459-g2t1-aml-processed-bucket/bq1_graph_results/hi-01/bank_features/` |

Feature schema: `bank_id,betweenness,community_id`. Run identity is recorded in the prefix/table. Primary population: HI-Medium labelled laundering rows with `amount_paid_usd < 20000000`; bank edges additionally require `cross_bank=1`. Account evidence includes retained same-bank transfers and rows without attempt annotations. The cutoff affects counts and sums and is a team analysis convention, not a regulatory threshold.

## How to run

1. In **us-east-1**, connect to the existing EC2 through Session Manager and verify Neo4j/GDS as described in [ec2_setup.md](ec2_setup.md). Preserve existing credentials and volumes.
2. In Athena, database `is459_g2t1_aml`, run `validation.sql` statements separately. Investigate missing keys or duplicate transaction IDs before loading.
3. Run the SELECTs in `export_edges.sql` separately. Download complete results, reconcile counts/USD, and upload the CSVs into the correct S3 run prefix.
4. Copy the files to EC2's dedicated import directory. For HI, the container URLs are `file:///hi-01/bank_edges.csv` and `file:///hi-01/transactions.csv`.
5. In Neo4j Browser, set `:param scope => 'hi-01'`, then run `import.cypher` statements separately. Repeat the two imports and confirm counts/USD remain unchanged.
6. Run the relevant sections of `queries.cypher` separately. Check active scope and memory estimates before GDS writes; verify account/time candidates against Athena source records.
7. Export every bank's features. Increase Browser's record limit above the graph size, rerun the query and download. Accepted HI export: **5,159 data rows plus header**, not the initial truncated 5,000 rows.
8. Upload `bank_features.csv`, then run `athena_handoff.sql` statements separately. Verify unique/complete features and unchanged Role 3 join counts. Keep missing graph features as NULL, not zero risk.

Use a new prefix/scope when inputs change; do not overwrite accepted runs or mix datasets. Bank identity is `(scope,bank_id)`; account identity is `(scope,bank_id,account_id)`. GDS projections must be rebuilt after active-scope changes. Algorithm reruns may change community labels, so export accepted results first.

Betweenness is directed and unweighted. Primary Louvain uses undirected pairs weighted by combined transaction counts. A bank route alone does not establish account continuity; account/time-supported candidates still do not prove the same funds moved. Community IDs are categories, not risk scores or stable labels across reruns.

## Step Functions info for Sonia

| Field | Value / status |
|---|---|
| Job type | Neo4j import/analysis on existing EC2; future invocation through SSM |
| Manual sources | `export_edges.sql`, `import.cypher`, `queries.cypher`, `athena_handoff.sql` |
| Tested automated command | Pending EC2/SSM trial; runner remains a local draft |
| Target | `i-0125056b848e5634a`, container `role4-neo4j` |
| Accepted output table | `is459_g2t1_aml.bq1_graph_bank_features_hi01` |
| Accepted output S3 path | `s3://is459-g2t1-aml-processed-bucket/bq1_graph_results/hi-01/bank_features/` |
| Depends on | Accepted `transactions_clean` snapshot and ready Neo4j/GDS; Role 3 corridor table is needed for the later join |
| Typical full automated runtime | Not measured; manual query times are not end-to-end runtime |
| Required success checks | Reconciled input/import totals, complete unique features, accepted run identity, terminal SSM success and zero response code |
| Failure handling | Bound polling/timeouts, propagate failures, prevent concurrent scope changes and stale-output consumption |

The shared `BQ1_Neo4jLoad` remains a placeholder until the orchestration owner integrates and tests it. Confirm required IAM permissions and EC2 start/readiness/stop ownership before automation. Personal coordination drafts and proposed orchestration documents stay local.

## Contribution summary (for the Week 13 slide)

Validated sample and HI graph imports, account/time-supported multi-hop candidates, intermediary-bank scores, Louvain communities and the Role 3 Athena feature join. Saved reproducible SQL/Cypher, server configuration, findings and screenshot evidence.

Results: HI contains **5,159 bank nodes**, **29,374 directed corridors**, **34,552 represented cross-bank transactions** ($2,308,523,888.96) and **34,891 account transfers** ($2,311,266,994.08). All banks have community assignments across **113 communities**; the feature join preserves **1,697,382 corridor rows**. Full results and limitations are in [findings.md](findings.md).

Sample development checks also passed: 42 bank nodes, 35 corridors/transfers and $434,634.33 reconciled; repeated imports did not change totals. Sample Louvain produced eight communities; sample findings are not substituted for HI results.

Remaining: Bloom access/agreed fallback and final scenes; LI/cutoff/weighting robustness; final recommendations for Role 3; live runner/SSM/integration checks and measured runtime; final team presentation/video artefacts. Prepared drafts are not executed results and remain outside the validated GitHub submission.

Evidence screenshots: [screenshots/](screenshots/) (linked to findings in [findings.md](findings.md)). Personal walkthroughs, tools, coordination notes, data exports and credentials remain gitignored.
