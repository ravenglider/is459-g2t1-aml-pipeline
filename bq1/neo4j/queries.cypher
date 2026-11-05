// Role 4 — manual HI analysis source, run hi-01.
// Run ONE statement at a time in Browser; this is not an automated runner.
// Captures the statements used in the validated manual workflow.
// Do not rerun GDS merely to save source: write calls can change communities.
// Check both Active labels contain ONLY hi-01 before any projection.
// Old in-memory projections do not follow active-label or source changes.

// TASK 5: structural routes (not evidence of account/time continuity).
MATCH (a:ActiveBank)-[:LAUNDERING_TO]->(b:ActiveBank)
      -[:LAUNDERING_TO]->(c:ActiveBank)
WHERE a<>b AND b<>c AND a<>c
RETURN a.bank_id AS start_bank, b.bank_id AS middle_bank,
       c.bank_id AS end_bank LIMIT 20;

// Bounded account/time candidates: strictly increasing time, <=24h per link.
// Do not require attempt_id equality or equal amounts. Not proof of same funds.
MATCH (a:ActiveAccount)-[r1:TRANSFER]->(b:ActiveAccount)
      -[r2:TRANSFER]->(c:ActiveAccount)
WHERE r1.txn_ts<r2.txn_ts
  AND r2.txn_ts<=r1.txn_ts+duration('PT24H')
  AND r1.txn_id<>r2.txn_id
  AND (a.bank_id<>b.bank_id OR b.bank_id<>c.bank_id)
RETURN a.bank_id AS start_bank, a.account_id AS start_account,
       b.bank_id AS middle_bank, b.account_id AS connecting_account,
       c.bank_id AS end_bank, c.account_id AS end_account,
       r1.txn_id AS first_txn_id, toString(r1.txn_ts) AS first_time,
       r1.amount_paid_usd AS first_usd,
       r2.txn_id AS second_txn_id, toString(r2.txn_ts) AS second_time,
       r2.amount_paid_usd AS second_usd LIMIT 5;

// 08f: exact two-hop candidate verified against Athena, both attempt 2.
MATCH p=(:ActiveAccount {scope:'hi-01',bank_id:'7818',account_id:'8037732C0'})
  -[r1:TRANSFER {txn_id:'9173283034951331860'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'121523',account_id:'80D1BD2F0'})
  -[r2:TRANSFER {txn_id:'-1430906872139732606'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'134266',account_id:'814167590'})
WHERE r1.txn_ts<r2.txn_ts
  AND r2.txn_ts<=r1.txn_ts+duration('PT24H')
RETURN p;

// Three-hop discovery bounded by start bank, length and result count.
MATCH p=(a:ActiveAccount {scope:'hi-01',bank_id:'7818'})
        -[rs:TRANSFER*3]->(:ActiveAccount)
WHERE all(n IN nodes(p) WHERE n:ActiveAccount AND n.scope='hi-01')
  AND all(i IN range(0,1) WHERE rs[i].txn_ts<rs[i+1].txn_ts
      AND rs[i+1].txn_ts<=rs[i].txn_ts+duration('PT24H'))
  AND all(r IN rs WHERE single(s IN rs WHERE s.txn_id=r.txn_id))
  AND any(r IN rs WHERE startNode(r).bank_id<>endNode(r).bank_id)
RETURN [n IN nodes(p)|n.bank_id] AS bank_route,
       [n IN nodes(p)|n.account_id] AS account_route,
       [r IN rs|r.txn_id] AS transaction_ids,
       [r IN rs|toString(r.txn_ts)] AS transfer_times,
       [r IN rs|r.amount_paid_usd] AS amounts_usd LIMIT 5;

// 08h: exact source-verified three-hop candidate.
// Attempt annotations 670,2117,2117: account/time connection across annotations,
// NOT one continuous annotated laundering attempt or proof of identical funds.
MATCH p=(:ActiveAccount {scope:'hi-01',bank_id:'7818',account_id:'805753DD0'})
  -[:TRANSFER {txn_id:'7394026252962401598'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'25828',account_id:'804B45920'})
  -[:TRANSFER {txn_id:'-2980862507599786311'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'212353',account_id:'805754320'})
  -[:TRANSFER {txn_id:'606750438349263592'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'11796',account_id:'80139E060'})
RETURN p;

// TASK 6: directed UNWEIGHTED betweenness. Do not use raw USD as path distance.
CALL gds.graph.drop('role4Directed',false);

CALL gds.graph.project('role4Directed','ActiveBank',
  {LAUNDERING_TO:{orientation:'NATURAL'}})
YIELD graphName,nodeCount,relationshipCount
RETURN graphName,nodeCount,relationshipCount;
// Accepted: 5159 nodes,29374 relationships.

CALL gds.betweenness.write.estimate('role4Directed',
  {writeProperty:'betweenness',concurrency:2})
YIELD requiredMemory RETURN requiredMemory;
// STOP and inspect estimate vs available heap before the write call.

CALL gds.betweenness.write('role4Directed',
  {writeProperty:'betweenness',concurrency:2})
YIELD nodePropertiesWritten,computeMillis
RETURN nodePropertiesWritten,computeMillis;

MATCH (b:ActiveBank {scope:'hi-01'})
RETURN b.bank_id AS bank_id,b.betweenness AS betweenness,b.scope AS run_id
ORDER BY betweenness DESC,bank_id LIMIT 20;

// Aggregate directions separately to avoid multiplying incoming/outgoing totals.
MATCH (b:ActiveBank {scope:'hi-01',bank_id:'11'})
OPTIONAL MATCH (b)-[out:LAUNDERING_TO]->(:ActiveBank)
WITH b,count(out) AS out_corridors,
     sum(out.laundering_txns) AS out_transactions,
     sum(out.laundering_usd) AS out_usd
OPTIONAL MATCH (:ActiveBank)-[inc:LAUNDERING_TO]->(b)
RETURN b.bank_id AS bank_id,b.betweenness AS betweenness,
       count(inc) AS in_corridors,sum(inc.laundering_txns) AS in_transactions,
       round(sum(inc.laundering_usd),2) AS in_usd,
       out_corridors,out_transactions,round(out_usd,2) AS out_usd,
       b.scope AS run_id;

// Structural intermediary illustration, not a transaction-continuous chain.
MATCH p=(:ActiveBank {scope:'hi-01',bank_id:'20'})-[:LAUNDERING_TO]->
        (:ActiveBank {scope:'hi-01',bank_id:'11'})-[:LAUNDERING_TO]->
        (:ActiveBank {scope:'hi-01',bank_id:'12'})
RETURN p;

// TASK 7: one link per unordered bank pair, combined transaction-count weight.
// The TASK 6/7 projection, estimate, write and pair-link statements operate on
// ActiveBank and were also used for LI and uncapped HI after scope verification.
// Scope-specific ranking/membership/export queries below are accepted HI records;
// use $scope or the intended literal when examining another saved population.
// Observed LI: 4924 banks,13724 directed corridors;13453 pairs/weight15782;
// Louvain142,modularity0.45376073179686327,4924 assignments.
// Observed uncapped HI:5166 banks,29660 corridors;28622 pairs/weight34887;
// Louvain111,modularity0.3596721194379274,5166 assignments.
MATCH (a:ActiveBank)-[r:LAUNDERING_TO]->(b:ActiveBank)
WITH CASE WHEN a.bank_id<b.bank_id THEN a ELSE b END AS lo,
     CASE WHEN a.bank_id<b.bank_id THEN b ELSE a END AS hi,
     sum(r.laundering_txns) AS weight
MERGE (lo)-[u:BANK_LINK]->(hi)
SET u.weight=toFloat(weight);

MATCH (:ActiveBank)-[r:BANK_LINK]->(:ActiveBank)
RETURN count(r) AS bank_pairs,sum(r.weight) AS represented_transactions;
// Accepted: 28347,34552.0.

CALL gds.graph.drop('role4Community',false);

CALL gds.graph.project('role4Community','ActiveBank',
  {BANK_LINK:{orientation:'UNDIRECTED',properties:'weight'}})
YIELD graphName,nodeCount,relationshipCount
RETURN graphName,nodeCount,relationshipCount;
// Accepted: 5159 nodes,56694 projected relationships (both directions).

CALL gds.louvain.write.estimate('role4Community',
  {writeProperty:'community_id',relationshipWeightProperty:'weight',concurrency:1})
YIELD requiredMemory RETURN requiredMemory;
// STOP and inspect memory estimate before running the algorithm.

CALL gds.louvain.write('role4Community',
  {writeProperty:'community_id',relationshipWeightProperty:'weight',concurrency:1})
YIELD communityCount,modularity,nodePropertiesWritten
RETURN communityCount,modularity,nodePropertiesWritten;
// Accepted run: 113,0.3596071963885728,5159. Labels can change on later runs.

MATCH (b:ActiveBank {scope:'hi-01'})
RETURN count(b) AS active_banks,count(b.community_id) AS assigned_banks,
       count(DISTINCT b.community_id) AS communities;

MATCH (b:ActiveBank {scope:'hi-01'})
WITH b ORDER BY b.community_id,b.bank_id
WITH b.community_id AS community_id,count(*) AS bank_count,
     collect(b.bank_id)[0..10] AS example_members
RETURN community_id,bank_count,example_members,'hi-01' AS run_id
ORDER BY bank_count DESC,community_id LIMIT 10;

// Full community-size export: 113 data rows, no LIMIT.
MATCH (b:ActiveBank {scope:'hi-01'})
RETURN b.community_id AS community_id,count(*) AS bank_count,'hi-01' AS run_id
ORDER BY bank_count DESC,community_id;

MATCH (a:ActiveBank {scope:'hi-01'})-[r:LAUNDERING_TO]->
      (b:ActiveBank {scope:'hi-01'})
WHERE a.community_id<>b.community_id
RETURN a.bank_id AS from_bank,b.bank_id AS to_bank,
       a.community_id AS from_community,b.community_id AS to_community,
       r.laundering_txns AS laundering_transactions,
       round(r.laundering_usd,2) AS laundering_usd
ORDER BY laundering_usd DESC,from_bank,to_bank LIMIT 10;

// TASK 9: accepted bank_features.csv contract, exactly THREE columns.
// Browser record/result limit must exceed 5159 (use 10000), then rerun/export.
// File must contain 5159 data rows + header, not a 5000-row truncated export.
// No LIMIT. Validate parsed counts in Athena before joining to Role 3.
MATCH (b:ActiveBank {scope:'hi-01'})
RETURN b.bank_id AS bank_id,b.betweenness AS betweenness,
       b.community_id AS community_id
ORDER BY bank_id;
