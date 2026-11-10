# Final trade-off recovery

The original tradeoff.sql exhausted resources at full scale (user report). The teammate-supplied two-stage and weekly SQL files are retained unchanged. tradeoff_stage2.sql is the complete Stage 2 CREATE statement extracted for easy copying, with no DROP.

## Current run

Xiang Ying reports normal Stage 1 CTAS and all three INSERTs completed. Screenshot 29 verifies twelve helper groups, unique final IDs within each group and ranks 1–10. Do not repeat successful INSERTs or drop this completed helper. Weekly fallback was not needed. Stage 2 subsequently completed successfully (screenshot 30: 14.614 sec, 905.07 MB scanned). Final reconciliation then passed (48 rows, zero accounting errors; see [results](../results.md)). The pipeline now runs this two-stage trade-off automatically.

## Run and recovery rules

1. Before building, inspect existing helper/final tables, actual S3 locations and ownership. CTAS destinations must be empty. DROP removes metadata, not S3 files. Use fresh consistent run names/paths if preserving existing outputs; prevent concurrent writers and source refresh during the run.
2. Normal Stage 1: deliberate helper DROP only for an agreed restart, then CTAS HI pairs, INSERT HI triples, INSERT LI pairs, INSERT LI triples. Execute complete statements separately; record each query ID and successful batch.
3. If Stage 1 fails, stop and inspect its manifest/output before retrying. Weekly fallback REPLACES all normal Stage 1, using a clean helper destination or fresh run prefix. Never append it to partial normal output. It has one CTAS and fifteen INSERTs across four owner-date windows for each dataset/length. Stage 2 globally deduplicates weekly finals using MIN(best_rank).
4. Check helper counts, distinct IDs and rank range. Completion receipts are still needed: incomplete batches can otherwise look valid. This run has all twelve groups; truly empty populations may legitimately have none.
5. Confirm bq2_tradeoff is absent or only the known failed Role 6 output with replacement agreed, and its destination is empty. Only then run the Stage 2 DROP from the supplied file if needed. Copy ALL of tradeoff_stage2.sql and run once. Do not rerun Stage 1.
6. Expect 48 final rows (2 datasets × 2 lengths × 3 cutoffs × 4 policies). Reconcile accounting, full cleaned-source denominators, monotonic policy counts and all four rates. Export one cutoff for Tableau. Keep LI dates with no sequences in source denominators.

All ranks come from HI, including LI evaluation. Stage 1 stores at most top-10 candidates per cutoff; any new Stage 2 policy above 10 requires increasing every Stage 1 rank limit and rebuilding. Normal/weekly strategies match original analytics in synthetic SQLite tests, including overlap, mixed chain/final labels, cross-week repeats and duplicate helper rows. Thirteen tests pass; this does not certify Athena memory use.

Failed original query: 2fe94c3f-5da1-4205-9d18-9e1cb871152d. Reported leftovers: s3://is459-g2t1-aml-athena-bucket/results/Unsaved/2026/11/09/tables/2fe94c3f-5da1-4205-9d18-9e1cb871152d/. No cloud cleanup was performed by the assistant. Verify that exact attempt's manifest/location before narrowly scoped owner cleanup. Never empty parent tables/results folders. Actual CTAS LOCATION may differ from query-result location; inspect the resulting table definition before automation.
