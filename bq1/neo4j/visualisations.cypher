// Role 4 final-scene source. Executed manually on hi-01; scenes saved as 08p-08r.
// Run one statement at a time after confirming the accepted HI scope is active.
// Team decision reported 3 November 2026: use Neo4j Browser instead of Bloom.
// Scene C caption data verified: 05:10/13:24 on 2022-09-06, attempt 2 on both.
// Record the proposal-tool substitution in the report/slides. No Bloom licence
// or perspective export is required for this agreed route.

// Check first. Expected manual run: only hi-01, 5159 banks.
MATCH (b:ActiveBank)
RETURN collect(DISTINCT b.scope) AS active_scopes, count(b) AS banks;

// SCENE A: top intermediary 11, at most five incoming/five outgoing corridors.
// Structural context; different corridor totals are not continuous funds.
MATCH (center:ActiveBank {scope:'hi-01',bank_id:'11'})
CALL {
  WITH center
  MATCH (n:ActiveBank {scope:'hi-01'})-[r:LAUNDERING_TO]->(center)
  RETURN n,r ORDER BY r.laundering_txns DESC,n.bank_id LIMIT 5
  UNION ALL
  WITH center
  MATCH (center)-[r:LAUNDERING_TO]->(n:ActiveBank {scope:'hi-01'})
  RETURN n,r ORDER BY r.laundering_txns DESC,n.bank_id LIMIT 5
}
RETURN center,r,n;

// SCENE B: one accepted cross-community bridge and up to four same-community
// neighbors on each side. Colour by saved community_id; IDs have no risk order.
// Keep a legend and identify this as a bounded illustration of larger clusters.
MATCH (a:ActiveBank {scope:'hi-01',bank_id:'114976'})
      -[bridge:LAUNDERING_TO]->
      (b:ActiveBank {scope:'hi-01',bank_id:'297435'})
WHERE a.community_id<>b.community_id
CALL {
  WITH a,b,bridge
  RETURN a AS x,bridge AS r,b AS y
  UNION ALL
  WITH a,b,bridge
  MATCH (a)-[edge:LAUNDERING_TO]-(n:ActiveBank {scope:'hi-01'})
  WHERE n.community_id=a.community_id
  RETURN a AS x,edge AS r,n AS y
  ORDER BY r.laundering_txns DESC,y.bank_id LIMIT 4
  UNION ALL
  WITH a,b,bridge
  MATCH (b)-[edge:LAUNDERING_TO]-(n:ActiveBank {scope:'hi-01'})
  WHERE n.community_id=b.community_id
  RETURN b AS x,edge AS r,n AS y
  ORDER BY r.laundering_txns DESC,y.bank_id LIMIT 4
}
RETURN x,r,y;

// SCENE C: source-verified two-hop account candidate; keep this account model
// separate from the bank/community perspective. Display times and txn IDs.
MATCH p=(:ActiveAccount {scope:'hi-01',bank_id:'7818',account_id:'8037732C0'})
  -[r1:TRANSFER {txn_id:'9173283034951331860'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'121523',account_id:'80D1BD2F0'})
  -[r2:TRANSFER {txn_id:'-1430906872139732606'}]->
  (:ActiveAccount {scope:'hi-01',bank_id:'134266',account_id:'814167590'})
WHERE r1.txn_ts<r2.txn_ts AND r2.txn_ts<=r1.txn_ts+duration('PT24H')
RETURN p;

// Matching caption data: 8h14m; same intermediary account; labelled retained
// transfers, not proof of the same funds. Do not add amounts as unique proceeds.
MATCH (:ActiveAccount {scope:'hi-01'})
      -[r:TRANSFER]->(:ActiveAccount {scope:'hi-01'})
WHERE r.txn_id IN ['9173283034951331860','-1430906872139732606']
RETURN r.txn_id AS txn_id,toString(r.txn_ts) AS transfer_time,
       r.amount_paid_usd AS amount_paid_usd,r.attempt_id AS attempt_id
ORDER BY r.txn_ts;
