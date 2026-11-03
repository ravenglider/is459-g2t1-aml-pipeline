// Role 4 graph sensitivity sources. PREPARED, not executed.
// Run separately; inspect each population and memory estimate before proceeding.
// This file does not switch active scopes or run primary HI/LI imports for you.

// 1. Inspect saved scope coverage without changing Active labels.
// Replace li-01 if the accepted LI run has a different run ID.
UNWIND ['hi-01','li-01'] AS run_id
OPTIONAL MATCH (b:Bank {scope:run_id})
WITH run_id,count(b) AS banks,count(b.betweenness) AS scored_banks,
     count(b.community_id) AS assigned_banks,
     count(DISTINCT b.community_id) AS communities
OPTIONAL MATCH (:Bank {scope:run_id})-[r:LAUNDERING_TO]->(:Bank {scope:run_id})
RETURN run_id,
       CASE WHEN banks=0 THEN 'NOT_IMPORTED'
            WHEN scored_banks<>banks OR assigned_banks<>banks THEN 'ANALYSIS_INCOMPLETE'
            ELSE 'ANALYSIS_PRESENT' END AS coverage_status,
       banks,scored_banks,assigned_banks,communities,
       count(r) AS corridors,sum(r.laundering_txns) AS represented_transactions,
       sum(r.laundering_usd) AS represented_usd,
       CASE WHEN banks>1 THEN toFloat(count(r))/(banks*(banks-1)) ELSE null END AS directed_density;

// 2. Compare top intermediaries within each scope. Raw scores depend on size;
// do not claim the same IDs represent the same synthetic entity across datasets.
UNWIND ['hi-01','li-01'] AS run_id
CALL {
  WITH run_id
  MATCH (b:Bank {scope:run_id})
  RETURN b.bank_id AS bank_id,b.betweenness AS betweenness
  ORDER BY betweenness DESC,bank_id LIMIT 10
}
RETURN run_id,bank_id,betweenness;

// 3. Inspect current scope before the optional USD-weight Louvain experiment.
// Must be only the accepted HI scope; export current features/results first.
MATCH (b:ActiveBank)
RETURN collect(DISTINCT b.scope) AS active_scopes,count(b) AS banks,
       count(b.community_id) AS primary_assignments;

// 4. Add USD weight on existing unordered-pair links, leaving primary weight
// and community_id unchanged. Same retained <20M HI population as primary run.
MATCH (a:ActiveBank)-[r:LAUNDERING_TO]->(b:ActiveBank)
WITH CASE WHEN a.bank_id<b.bank_id THEN a ELSE b END AS lo,
     CASE WHEN a.bank_id<b.bank_id THEN b ELSE a END AS hi,
     sum(r.laundering_usd) AS usd_weight
MATCH (lo)-[u:BANK_LINK]->(hi)
SET u.usd_weight=usd_weight;

// Check missing/negative weights before projecting; investigate any nonzero.
MATCH (:ActiveBank)-[u:BANK_LINK]->(:ActiveBank)
RETURN count(u) AS pairs,
       count(CASE WHEN u.usd_weight IS NULL OR u.usd_weight<0 THEN 1 END) AS invalid_weights;

CALL gds.graph.drop('role4CommunityUsdSensitivity',false);

CALL gds.graph.project('role4CommunityUsdSensitivity','ActiveBank',
  {BANK_LINK:{orientation:'UNDIRECTED',properties:'usd_weight'}});

CALL gds.louvain.write.estimate('role4CommunityUsdSensitivity',
  {writeProperty:'community_id_usd_sensitivity',
   relationshipWeightProperty:'usd_weight',concurrency:1})
YIELD requiredMemory,bytesMax RETURN requiredMemory,bytesMax;
// STOP: inspect estimate vs configured heap before the next statement.

CALL gds.louvain.write('role4CommunityUsdSensitivity',
  {writeProperty:'community_id_usd_sensitivity',
   relationshipWeightProperty:'usd_weight',concurrency:1})
YIELD communityCount,modularity,nodePropertiesWritten
RETURN communityCount,modularity,nodePropertiesWritten;

// Compare overlap counts, NOT equality of numeric community IDs across runs.
// Export the complete contingency table if used in the final sensitivity report.
MATCH (b:ActiveBank)
RETURN b.community_id AS count_weighted_community,
       b.community_id_usd_sensitivity AS usd_weighted_community,
       count(*) AS shared_banks
ORDER BY count_weighted_community,shared_banks DESC,usd_weighted_community;

CALL gds.graph.drop('role4CommunityUsdSensitivity',false);

// Record measured HI/LI sizes, distributions and weighting/cutoff conclusions
// in findings.md. Primary community_id is intentionally preserved. Further
// stochastic reruns need their own named properties/exports before comparison.
