# BQ1 bank graph - Role 4

Loads retained labelled laundering transactions into Neo4j on the existing group EC2 instance. Bank routes, account/time-supported candidates, betweenness and Louvain add network context to Role 3's corridor ranking. This submission records the validated manual sample/HI workflow and HI feature join. Role 4 is not yet fully complete.

## Validated source and records

| File | Purpose |
|---|---|
| [ec2_setup.md](ec2_setup.md) | Shared EC2/Docker/Neo4j/GDS configuration, connection and recovery |
| [validation.sql](validation.sql) | Input/schema, cutoff and graph-key checks; two-hop source verification |
| [export_edges.sql](export_edges.sql) | HI bank/account CSV exports and source reconciliation |
| [import.cypher](import.cypher) | Scoped constraints/imports and repeat-import reconciliation |
| [queries.cypher](queries.cypher) | Paths, verified examples, betweenness, activity, Louvain and feature export |
| [athena_handoff.sql](athena_handoff.sql) | Accepted feature-table registration, quality checks, Role 3 join and execution IDs |
| [findings.md](findings.md) | Measured results, evidence links, interpretation and limitations |
| [screenshots/](screenshots/) | Named evidence from manual setup and sample/HI runs |

Personal walkthroughs, helper tools, CSV/Parquet data, coordination messages and proposed orchestration documents are excluded by `.gitignore`. Untested visualisation/robustness sources and the offline-tested runner are deliberately held for a later submission after live validation. No automated graph deployment is claimed.

## AWS resources

Region `us-east-1`, database `is459_g2t1_aml`, existing workgroup `primary` (verify live). Reuse `is459-g2t1-ingest`, instance `i-0125056b848e5634a`, temporarily resized to `t3.medium`, with role `is459-g2t1-ec2-s3-role`. Container `role4-neo4j` runs Neo4j 5.26.32 Community and GDS with persistent named volumes. Coordinate shared use and stop EC2 when idle. Credentials stay in the protected instance-local `.env`, never in Git.

## Inputs and outputs

| Item | Location |
|---|---|
| Source | `is459_g2t1_aml.transactions_clean` |
| Accepted HI inputs | `s3://is459-g2t1-aml-processed-bucket/bq1_graph_edges/hi-01/` |
| Accepted feature CSV | `s3://is459-g2t1-aml-processed-bucket/bq1_graph_results/hi-01/bank_features/bank_features.csv` |
| Accepted feature table | `is459_g2t1_aml.bq1_graph_bank_features_hi01` |
| Role 3 join input | `is459_g2t1_aml.bq1_corridors_ranked` |

Feature CSV: `bank_id,betweenness,community_id` (three columns). Run identity is in the prefix/table. Primary input population: HI-Medium labelled laundering rows below $20M; bank graph additionally requires `cross_bank=1`. Account evidence includes retained same-bank rows and missing attempt annotations. The cutoff affects counts and sums and is a team analysis convention, not a regulatory threshold.

## How to run the manual workflow

1. Read `ec2_setup.md` for existing server access/configuration. Do not recreate credentials or rerun ingestion merely to inspect the graph.
2. Run the input checks in `validation.sql` separately in Athena. Investigate missing keys/duplicate transaction IDs before import.
3. Run each SELECT in `export_edges.sql` separately, download full CSV results and upload into a distinct run prefix. Preserve the source snapshot, query IDs and counts.
4. Copy CSVs to the dedicated EC2 import directory. For accepted HI, use `file:///hi-01/bank_edges.csv` and `file:///hi-01/transactions.csv`. In Browser set `:param scope => 'hi-01'`, then run `import.cypher` statements individually. Changed inputs require a new scope and matching file paths; never mix populations.
5. Reconcile source/import counts and USD, repeat the two imports and confirm totals stay unchanged. Run `queries.cypher` sections individually, checking the active scope and memory estimates before GDS writes. Re-running algorithms may change community labels; preserve accepted results first.
6. Export all bank features. Raise Browser's result limit above the graph size before rerunning/exporting; the accepted HI file contains 5,159 data rows plus header. CSVs remain in S3, not GitHub.
7. Run `athena_handoff.sql` statements individually. Confirm unique/complete bank features and unchanged corridor join count. Do not modify Role 3 ranking weights or invent transfer edges from shared-attempt pairs.

## Graph model and assumptions

- Bank identity: `(scope,bank_id)`; account identity: `(scope,bank_id,account_id)`.
- `LAUNDERING_TO`: directed bank-pair count and USD; `TRANSFER`: source transaction with signed string ID, local timestamp, USD and optional attempt annotation.
- `BANK_LINK`: one unordered pair weighted by combined retained transaction counts. Primary Louvain uses this undirected weight; betweenness is directed and unweighted.
- Active labels select one run; GDS projections must be rebuilt after scope changes.
- Account candidates use matching intermediary accounts and strictly increasing time, at most 24 hours per link. They do not prove identical funds or one common annotated attempt.
- Community IDs are categories, not risk scores or stable labels across reruns. Missing graph features are not zero-risk evidence.

## Pipeline interface

| Field | Current value/status |
|---|---|
| Job type | Manual Athena exports plus Neo4j import/analysis; future EC2/SSM runner pending live validation |
| Exact sources | `export_edges.sql`, `import.cypher`, `queries.cypher`, `athena_handoff.sql` |
| Existing target | `i-0125056b848e5634a`, container `role4-neo4j` |
| Output | Accepted feature table and S3 prefix above |
| Depends on | Accepted `transactions_clean` snapshot and ready Neo4j/GDS; Role 3 corridor table for the join |
| Full automated runtime | Not measured; no tested production import command published yet |
| Future completion checks | Source/import reconciliation, unique complete features, accepted run identity, terminal SSM success and bounded failure propagation |

The ingestion handoff requests the tested import/job command, output table/path and dependencies when the job is ready. Personal coordination drafts remain local. The group's `BQ1_Neo4jLoad` placeholder is not replaced by this commit; live runner/SSM trials and lifecycle handling remain to complete.

## Accepted results and remaining work

Sample smoke test: 42 bank nodes, 35 directed corridors and 35 account transfers; source/import totals $434,634.33 and unchanged repeated imports. Sample Louvain: eight communities, modularity 0.7102040816326531. Sample top betweenness ties of 55.0 are consistent with its observed 12-bank structural cycle; these are development findings.

HI: 29,374 corridors; 34,552 represented cross-bank transactions / $2,308,523,888.96; 34,891 account transfers / $2,311,266,994.08. Features cover 5,159 unique banks and 113 communities. The Role 3 LEFT JOIN preserves all 1,697,382 rows. Detailed evidence and query IDs are in `findings.md`.

Remaining: Bloom entitlement or agreed fallback and three final scenes; LI/cutoff/weighting sensitivity; final intermediary/cluster recommendations for Role 3; live runner/SSM/integrated execution and measured runtime; final team architecture, narrated setup/run videos and presentation artefacts. Add actual generated outputs when completed, not empty placeholders.

## Contribution summary

Role 4 validated sample/HI graph imports, multi-hop candidates, intermediary scores, communities and the Role 3 feature join. The source and findings preserve the manual analysis and its limits; final visualisation, robustness and deployed automation remain in progress.
