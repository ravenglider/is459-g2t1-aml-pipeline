// Role 4 — accepted manual HI import, scope hi-01.
// SOURCE RECORD: do not rerun merely to save these commands.
// In Browser run each statement separately. First set the Browser parameter:
// :param scope => 'hi-01'
// A future runner must supply scope as a parameter and validate input files first.
// Paths below are specifically the accepted HI files, not sample or LI.
// A changed source requires a NEW scope AND matching file paths.
// No DELETE statements: historical scoped nodes are preserved.

// Before switching Active labels, drop snapshots; serialize with other runs.
CALL gds.graph.drop('role4Directed', false);
CALL gds.graph.drop('role4Community', false);

CREATE CONSTRAINT role4_bank IF NOT EXISTS
FOR (b:Bank) REQUIRE (b.scope, b.bank_id) IS UNIQUE;

CREATE CONSTRAINT role4_account IF NOT EXISTS
FOR (a:Account) REQUIRE (a.scope, a.bank_id, a.account_id) IS UNIQUE;

// These labels belong to Role 4. Do not run alongside another graph analysis.
MATCH (n:ActiveBank) REMOVE n:ActiveBank;
MATCH (n:ActiveAccount) REMOVE n:ActiveAccount;

LOAD CSV WITH HEADERS FROM 'file:///hi-01/bank_edges.csv' AS row
MERGE (a:Bank {scope:$scope, bank_id:row.from_bank})
SET a:ActiveBank
MERGE (b:Bank {scope:$scope, bank_id:row.to_bank})
SET b:ActiveBank
MERGE (a)-[r:LAUNDERING_TO]->(b)
SET r.laundering_txns=toInteger(row.laundering_txns),
    r.laundering_usd=toFloat(row.laundering_usd);

LOAD CSV WITH HEADERS FROM 'file:///hi-01/transactions.csv' AS row
MERGE (a:Account {scope:$scope, bank_id:row.from_bank,
                  account_id:row.from_account})
SET a:ActiveAccount
MERGE (b:Account {scope:$scope, bank_id:row.to_bank,
                  account_id:row.to_account})
SET b:ActiveAccount
MERGE (a)-[r:TRANSFER {txn_id:row.txn_id}]->(b)
SET r.txn_ts=localdatetime(row.txn_ts),
    r.amount_paid_usd=toFloat(row.amount_paid_usd),
    r.attempt_id=CASE WHEN row.attempt_id='' THEN null ELSE row.attempt_id END,
    r.pattern_type=row.pattern_type;

// Verify exactly one intended scope is active before accepting the import.
MATCH (b:ActiveBank)
RETURN collect(DISTINCT b.scope) AS active_scopes, count(b) AS banks;

MATCH (a:ActiveAccount)
RETURN collect(DISTINCT a.scope) AS active_scopes, count(a) AS accounts;

// Compare to Athena's same source population; then rerun only the two LOAD CSV
// statements and this check. Totals must remain unchanged, with no new entities.
// 08c accepted: 29374,34552,2308523888.96,34891,2311266994.08.
CALL {
  MATCH (:ActiveBank)-[r:LAUNDERING_TO]->(:ActiveBank)
  RETURN count(r) AS corridors,
         sum(r.laundering_txns) AS cross_bank_transactions,
         round(sum(r.laundering_usd),2) AS cross_bank_usd
}
CALL {
  MATCH (:ActiveAccount)-[r:TRANSFER]->(:ActiveAccount)
  RETURN count(r) AS evidence_transactions,
         round(sum(r.amount_paid_usd),2) AS evidence_usd
}
RETURN $scope AS run_id, corridors, cross_bank_transactions, cross_bank_usd,
       evidence_transactions, evidence_usd;
