// Role 4 graph sensitivity sources. HI USD-weight comparison executed manually.
// Primary LI analysis and saved HI/LI coverage comparison completed manually.
// Uncapped HI import/betweenness/Louvain and bounded comparisons completed.
// SECTIONED SOURCE RECORD: do not run the whole file as a script.
// USD experiment statements require hi-01 active; compare-only statements read
// explicit saved scopes. Current accepted HI was restored after the comparisons.
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
// Summary queries below avoid requiring an extra local CSV download.
MATCH (b:ActiveBank)
RETURN b.community_id AS count_weighted_community,
       b.community_id_usd_sensitivity AS usd_weighted_community,
       count(*) AS shared_banks
ORDER BY count_weighted_community,shared_banks DESC,usd_weighted_community;

// Coverage check: observed 5159,5159,5159,113,207.
MATCH (b:ActiveBank)
RETURN count(b) AS banks,
       count(b.community_id) AS original_assignments,
       count(b.community_id_usd_sensitivity) AS usd_assignments,
       count(DISTINCT b.community_id) AS original_communities,
       count(DISTINCT b.community_id_usd_sensitivity) AS usd_communities;

// Overlap reconciliation: observed 525,5159.
MATCH (b:ActiveBank)
WITH b.community_id AS original_community,
     b.community_id_usd_sensitivity AS usd_community,
     count(*) AS shared_banks
RETURN count(*) AS overlap_groups,sum(shared_banks) AS banks_accounted_for;

// Original-community spread: observed 113,22,99. Evidence 10h.
MATCH (b:ActiveBank)
WITH b.community_id AS original_community,
     count(DISTINCT b.community_id_usd_sensitivity) AS usd_groups
RETURN count(*) AS original_communities,
       sum(CASE WHEN usd_groups>1 THEN 1 ELSE 0 END)
         AS communities_spanning_multiple_usd_groups,
       max(usd_groups) AS largest_number_of_usd_groups;

// Cleanup only when comparison queries are complete; stored properties persist.
CALL gds.graph.drop('role4CommunityUsdSensitivity',false);

// Record measured HI/LI sizes, distributions and weighting/cutoff conclusions
// in findings.md. Primary community_id is intentionally preserved. Further
// stochastic reruns need their own named properties/exports before comparison.

// 5. CUTOFF COMPARISON: read saved results without switching active scopes.
// Uncapped run: hi-uncapped-01, 5166 banks,29660 corridors,34887 transactions,
// USD 131229225009.70. Directed unweighted betweenness wrote 5166 properties.
// Count-weighted undirected Louvain: 28622 stored links / weight 34887,
// 57244 projected relationships; 111 communities, modularity
// 0.3596721194379274, 5166 assignments. Coverage confirmed.
// Executed:9 common banks,top four11,20,0,14; evidence10t. Bank-ID tie break.
CALL {
  MATCH (b:Bank {scope:'hi-01'})
  WITH b ORDER BY b.betweenness DESC,b.bank_id LIMIT 10
  RETURN collect(b.bank_id) AS filtered_top10
}
CALL {
  MATCH (b:Bank {scope:'hi-uncapped-01'})
  WITH b ORDER BY b.betweenness DESC,b.bank_id LIMIT 10
  RETURN collect(b.bank_id) AS uncapped_top10
}
RETURN filtered_top10,uncapped_top10,
       [bank IN filtered_top10 WHERE bank IN uncapped_top10] AS common_banks,
       size([bank IN filtered_top10 WHERE bank IN uncapped_top10]) AS common_count;

// Community membership comparison must match Bank nodes by bank_id within
// these two HI scopes and report banks present only in one scope separately.
// Do not compare community IDs numerically or interpret similar modularity
// as unchanged membership. Repeated-run stochastic variability is unmeasured.

// Shared-bank check executed: 5159 filtered,5159 matched,0 filtered-only.
// Cutoff community overlap summary executed: 245,5159,113,111.
// Excludes the seven banks present only in the uncapped scope.
MATCH (f:Bank {scope:'hi-01'})
MATCH (u:Bank {scope:'hi-uncapped-01',bank_id:f.bank_id})
WITH f.community_id AS filtered_community,
     u.community_id AS uncapped_community,
     count(*) AS shared_banks
RETURN count(*) AS overlap_groups,
       sum(shared_banks) AS matched_banks,
       count(DISTINCT filtered_community) AS filtered_communities,
       count(DISTINCT uncapped_community) AS uncapped_communities_among_shared_banks;

// Cutoff community spread executed: 113,15,17. Evidence 10u.
MATCH (f:Bank {scope:'hi-01'})
MATCH (u:Bank {scope:'hi-uncapped-01',bank_id:f.bank_id})
WITH f.community_id AS filtered_community,
     count(DISTINCT u.community_id) AS uncapped_groups
RETURN count(*) AS filtered_communities,
       sum(CASE WHEN uncapped_groups>1 THEN 1 ELSE 0 END)
         AS communities_spanning_multiple_uncapped_groups,
       max(uncapped_groups) AS largest_number_of_uncapped_groups;

// Saved primary-population comparison executed; evidence10o.
UNWIND ['hi-01','li-01'] AS run_id
CALL {
  WITH run_id
  MATCH (b:Bank {scope:run_id})
  RETURN count(b) AS banks,count(b.betweenness) AS scored_banks,
         count(b.community_id) AS assigned_banks,
         count(DISTINCT b.community_id) AS communities
}
CALL {
  WITH run_id
  MATCH (:Bank {scope:run_id})-[r:LAUNDERING_TO]->(:Bank {scope:run_id})
  RETURN count(r) AS corridors,sum(r.laundering_txns) AS cross_bank_transactions
}
RETURN run_id,banks,scored_banks,assigned_banks,communities,corridors,
       cross_bank_transactions ORDER BY run_id;

// Scope-aware read-only ranking/membership checks. Browser: set $scope FIRST.
// :param scope => 'li-01' (or hi-uncapped-01); choose the intended saved scope.
MATCH (b:Bank {scope:$scope})
RETURN b.bank_id AS bank_id,b.betweenness AS betweenness,b.scope AS run_id
ORDER BY betweenness DESC,bank_id LIMIT 10;

MATCH (b:Bank {scope:$scope})
WITH b ORDER BY b.community_id,b.bank_id
WITH b.community_id AS community_id,count(*) AS bank_count,
     collect(b.bank_id)[0..10] AS example_members
RETURN community_id,bank_count,example_members,$scope AS run_id
ORDER BY bank_count DESC,community_id LIMIT 10;

// Operational restoration, executed after comparisons. Manual only; does not
// rerun algorithms or delete scope-qualified nodes. Coordinate exclusive use.
CALL gds.graph.drop('role4Directed',false);
CALL gds.graph.drop('role4Community',false);
CALL gds.graph.drop('role4CommunityUsdSensitivity',false);
MATCH (b:ActiveBank) REMOVE b:ActiveBank;
MATCH (a:ActiveAccount) REMOVE a:ActiveAccount;
MATCH (b:Bank {scope:'hi-01'}) SET b:ActiveBank;
MATCH (a:Account {scope:'hi-01'}) SET a:ActiveAccount;
// Browser parameter must also be restored separately: :param scope => 'hi-01'
MATCH (b:ActiveBank)
RETURN collect(DISTINCT b.scope) AS active_scopes,count(b) AS banks,
       count(b.betweenness) AS scored_banks,count(b.community_id) AS assigned_banks,
       count(DISTINCT b.community_id) AS communities;
// Reported restored coverage:hi-01,5159,5159,5159,113.
