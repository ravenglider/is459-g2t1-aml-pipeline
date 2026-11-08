// Neo4j Browser views for the accepted HI graph. Run one statement at a time.
// All views read saved hi-01 data, independent of current Active labels.
// Styling and caption settings are documented in README.md.

// SCENE C: account-connected two-hop path with transfers within 24 hours.
MATCH p=(:Account {scope:'hi-01',bank_id:'7818',account_id:'8037732C0'})
  -[r1:TRANSFER {txn_id:'9173283034951331860'}]->
  (:Account {scope:'hi-01',bank_id:'121523',account_id:'80D1BD2F0'})
  -[r2:TRANSFER {txn_id:'-1430906872139732606'}]->
  (:Account {scope:'hi-01',bank_id:'134266',account_id:'814167590'})
WHERE r1.txn_ts<r2.txn_ts AND r2.txn_ts<=r1.txn_ts+duration('PT24H')
RETURN p;

// Transfer details: same intermediary account, 8h14m apart.
// Transaction amounts are not unique proceeds.
MATCH (:Account {scope:'hi-01'})
      -[r:TRANSFER]->(:Account {scope:'hi-01'})
WHERE r.txn_id IN ['9173283034951331860','-1430906872139732606']
RETURN r.txn_id AS txn_id,toString(r.txn_ts) AS transfer_time,
       r.amount_paid_usd AS amount_paid_usd,r.attempt_id AS attempt_id
ORDER BY r.txn_ts;

// INTERMEDIARY1: bank 11 neighbourhood in saved hi-01.
// Captions: bank_id on nodes; laundering_txns on arrows.
MATCH (center:Bank {scope:'hi-01',bank_id:'11'})
CALL {
  WITH center
  MATCH (n:Bank {scope:'hi-01'})-[r:LAUNDERING_TO]->(center)
  RETURN n,r ORDER BY r.laundering_txns DESC,n.bank_id LIMIT 5
  UNION ALL
  WITH center
  MATCH (center)-[r:LAUNDERING_TO]->(n:Bank {scope:'hi-01'})
  RETURN n,r ORDER BY r.laundering_txns DESC,n.bank_id LIMIT 5
}
RETURN center,r,n;

// INTERMEDIARY2: all incoming/outgoing activity for bank 11.
// Aggregate directions separately to avoid multiplying totals.
// Directional amounts do not measure retained balance or unique proceeds.
MATCH (b:Bank {scope:'hi-01',bank_id:'11'})
OPTIONAL MATCH (b)-[out:LAUNDERING_TO]->(:Bank {scope:'hi-01'})
WITH b,count(out) AS out_corridors,
     sum(out.laundering_txns) AS out_transactions,
     sum(out.laundering_usd) AS out_usd
OPTIONAL MATCH (:Bank {scope:'hi-01'})-[inc:LAUNDERING_TO]->(b)
RETURN b.bank_id AS bank_id,b.betweenness AS betweenness,
       count(inc) AS in_corridors,sum(inc.laundering_txns) AS in_transactions,
       round(sum(inc.laundering_usd),2) AS in_usd,
       out_corridors,out_transactions,round(out_usd,2) AS out_usd,
       b.scope AS run_id;

// Sharing example: pair 260802 -> 151989, both in community 727.
// Read-only views of three labelled transfers; visibility is simulated.

// CASE0: saved HI coverage; expect 5,159 banks and 113 communities.
MATCH (b:Bank {scope:'hi-01'})
RETURN 'hi-01' AS saved_scope,count(b) AS banks,
       count(b.betweenness) AS scored_banks,
       count(b.community_id) AS assigned_banks,
       count(DISTINCT b.community_id) AS communities;

// CASE1: verify one stored relationship per transaction against Athena.
UNWIND ['1923680745310817531','-5560620630767630164',
        '-2118989140943353923'] AS requested_txn_id
OPTIONAL MATCH (a:Account {scope:'hi-01'})
  -[r:TRANSFER {txn_id:requested_txn_id}]->(b:Account {scope:'hi-01'})
RETURN requested_txn_id,count(r) AS matches,
       collect({from_bank:a.bank_id,from_account:a.account_id,
                to_bank:b.bank_id,to_account:b.account_id,
                transfer_time:toString(r.txn_ts),usd:r.amount_paid_usd,
                attempt_id:r.attempt_id}) AS records;

// CASE2: transfers involving bank 260802 within the selected three records.
// This simulates local visibility, not real access controls.
MATCH (a:Account {scope:'hi-01'})-[r:TRANSFER]->(b:Account {scope:'hi-01'})
WHERE r.txn_id IN ['1923680745310817531','-5560620630767630164',
                  '-2118989140943353923']
  AND (a.bank_id='260802' OR b.bank_id='260802')
RETURN a,r,b;

// CASE3: transfers involving bank 151989 within the same three records.
MATCH (a:Account {scope:'hi-01'})-[r:TRANSFER]->(b:Account {scope:'hi-01'})
WHERE r.txn_id IN ['1923680745310817531','-5560620630767630164',
                  '-2118989140943353923']
  AND (a.bank_id='151989' OR b.bank_id='151989')
RETURN a,r,b;

// CASE4: combined account path; four accounts, three transfers, attempt 1885.
// Gaps: 3h45m and 15h40m. Shared annotation does not prove identical funds.
MATCH p=(:Account {scope:'hi-01',bank_id:'199098',account_id:'825E09140'})
  -[r1:TRANSFER {txn_id:'1923680745310817531'}]->
  (:Account {scope:'hi-01',bank_id:'260802',account_id:'816607960'})
  -[r2:TRANSFER {txn_id:'-5560620630767630164'}]->
  (:Account {scope:'hi-01',bank_id:'151989',account_id:'8154771C0'})
  -[r3:TRANSFER {txn_id:'-2118989140943353923'}]->
  (:Account {scope:'hi-01',bank_id:'18366',account_id:'822B0D450'})
WHERE r1.txn_ts<r2.txn_ts AND r2.txn_ts<r3.txn_ts
  AND r2.txn_ts<=r1.txn_ts+duration('PT24H')
  AND r3.txn_ts<=r2.txn_ts+duration('PT24H')
  AND r1.attempt_id='1885' AND r2.attempt_id='1885' AND r3.attempt_id='1885'
RETURN p;

// CASE5: compare local and combined visibility; count the shared transfer once.
// Expected counts: 2, 2, 3. Amounts are transaction sums, not detection uplift.
MATCH (a:Account {scope:'hi-01'})-[r:TRANSFER]->(b:Account {scope:'hi-01'})
WHERE r.txn_id IN ['1923680745310817531','-5560620630767630164',
                  '-2118989140943353923']
WITH collect({txn_id:r.txn_id,from_bank:a.bank_id,to_bank:b.bank_id,
              usd:r.amount_paid_usd}) AS transactions
UNWIND [{view:'Bank260802 alone',banks:['260802']},
        {view:'Bank151989 alone',banks:['151989']},
        {view:'Combined pair',banks:['260802','151989']}] AS perspective
WITH perspective,[t IN transactions WHERE t.from_bank IN perspective.banks
                                     OR t.to_bank IN perspective.banks] AS visible
RETURN perspective.view AS simulated_view,size(visible) AS visible_transfers,
       round(reduce(total=0.0,t IN visible|total+t.usd),2) AS transaction_value_usd;

// CASE6: transaction timeline; source timezone is unspecified.
MATCH (a:Account {scope:'hi-01'})-[r:TRANSFER]->(b:Account {scope:'hi-01'})
WHERE r.txn_id IN ['1923680745310817531','-5560620630767630164',
                  '-2118989140943353923']
RETURN a.bank_id AS from_bank,b.bank_id AS to_bank,
       toString(r.txn_ts) AS transfer_time,r.amount_paid_usd AS amount_paid_usd,
       r.txn_id AS txn_id,r.attempt_id AS attempt_id
ORDER BY r.txn_ts;

// COMMUNITY1: verify the selected corridor and its saved community assignments.
MATCH (a:Bank {scope:'hi-01',bank_id:'114976'})
      -[r:LAUNDERING_TO]->(b:Bank {scope:'hi-01',bank_id:'297435'})
RETURN a.bank_id AS from_bank,a.community_id AS from_community,
       b.bank_id AS to_bank,b.community_id AS to_community,
       a.community_id<>b.community_id AS cross_community,
       r.laundering_txns AS laundering_transactions,r.laundering_usd AS laundering_usd;

// COMMUNITY2: cross-community corridor with up to four local edges per side.
// Small neighbourhoods illustrate the boundary; they are not full communities.
MATCH (a:Bank {scope:'hi-01',bank_id:'114976'})
      -[bridge:LAUNDERING_TO]->(b:Bank {scope:'hi-01',bank_id:'297435'})
WHERE a.community_id<>b.community_id
CALL {
  WITH a,b,bridge
  RETURN a AS x,bridge AS r,b AS y
  UNION ALL
  WITH a,b,bridge
  MATCH (a)-[edge:LAUNDERING_TO]-(n:Bank {scope:'hi-01'})
  WHERE n.community_id=a.community_id
  RETURN a AS x,edge AS r,n AS y
  ORDER BY r.laundering_txns DESC,y.bank_id LIMIT 4
  UNION ALL
  WITH a,b,bridge
  MATCH (b)-[edge:LAUNDERING_TO]-(n:Bank {scope:'hi-01'})
  WHERE n.community_id=b.community_id
  RETURN b AS x,edge AS r,n AS y
  ORDER BY r.laundering_txns DESC,y.bank_id LIMIT 4
}
RETURN x,r,y;

// COMMUNITY3: verify membership of the nine banks displayed in this saved view.
MATCH (b:Bank {scope:'hi-01'})
WHERE b.bank_id IN ['114976','297435','110912','15011','12','18077',
                   '6','95274','97075']
RETURN b.community_id AS community_id,b.bank_id AS bank_id
ORDER BY community_id,bank_id;
