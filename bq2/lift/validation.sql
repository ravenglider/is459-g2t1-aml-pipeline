-- Run each statement separately. First statement is available before Role 5.
-- Final reconciliation uses full partitions; use a short txn_date range only for development.
SELECT dataset, COUNT(*) AS transactions, COUNT(DISTINCT txn_id) AS unique_txns,
       SUM(is_laundering) AS laundering,
       SUM(CASE WHEN is_laundering IS NULL OR is_laundering NOT IN (0, 1) THEN 1 ELSE 0 END) AS bad_labels,
       SUM(CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END) AS null_txn_ids,
       SUM(usd_missing) AS usd_missing, MIN(txn_date) AS first_date, MAX(txn_date) AS last_date
FROM is459_g2t1_aml.transactions_clean
WHERE dataset IN ('HI-Medium', 'LI-Medium')
-- AND txn_date BETWEEN '2022-09-01' AND '2022-09-02'
GROUP BY dataset;

-- Expect both datasets, >0 rows, unique chains = chain count, zero bad rows.
SELECT dataset, COUNT(*) AS chains, COUNT(DISTINCT sequence_id) AS unique_chains,
       SUM(CASE WHEN sequence_id IS NULL OR trim(sequence_id) = ''
         OR method_sequence IS NULL OR trim(method_sequence) = ''
         OR sequence_length IS NULL OR sequence_length NOT IN (2, 3)
         OR cardinality(split(method_sequence, ' -> ')) <> sequence_length
         OR is_laundering IS NULL OR is_laundering NOT IN (0, 1)
         THEN 1 ELSE 0 END) AS bad_rows,
       SUM(is_laundering) AS laundering_chains
FROM is459_g2t1_aml.bq2_sequences
WHERE dataset IN ('HI-Medium', 'LI-Medium') GROUP BY dataset;

-- Expect zero rows. Match complete hop set, no repeated transaction, correct MAX label.
SELECT s.dataset, s.sequence_id
FROM is459_g2t1_aml.bq2_sequences s
LEFT JOIN is459_g2t1_aml.bq2_sequence_transactions m
  ON m.dataset = s.dataset AND m.sequence_id = s.sequence_id
LEFT JOIN is459_g2t1_aml.transactions_clean t
  ON t.dataset = m.dataset AND t.txn_id = m.txn_id
WHERE s.dataset IN ('HI-Medium', 'LI-Medium')
GROUP BY s.dataset, s.sequence_id, s.sequence_length, s.is_laundering
HAVING COUNT(m.txn_id) <> s.sequence_length
    OR COUNT(DISTINCT m.txn_id) <> s.sequence_length
    OR COUNT(DISTINCT m.hop_index) <> s.sequence_length
    OR MIN(m.hop_index) <> 1 OR MAX(m.hop_index) <> s.sequence_length
    OR COUNT(t.txn_id) <> s.sequence_length
    OR MAX(t.is_laundering) <> s.is_laundering;

-- Expect zero: orphan memberships or duplicate hop positions.
SELECT m.dataset, m.sequence_id, m.hop_index, COUNT(*) AS rows_at_hop
FROM is459_g2t1_aml.bq2_sequence_transactions m
LEFT JOIN is459_g2t1_aml.bq2_sequences s
  ON s.dataset = m.dataset AND s.sequence_id = m.sequence_id
WHERE m.dataset IN ('HI-Medium', 'LI-Medium')
GROUP BY m.dataset, m.sequence_id, m.hop_index
HAVING COUNT(*) <> 1 OR COUNT(s.sequence_id) <> 1;

-- Expect zero: the same ordered chain must not appear under different IDs.
WITH ordered_chains AS (
  SELECT dataset, sequence_id, array_agg(txn_id ORDER BY hop_index) AS ordered_txn_ids
  FROM is459_g2t1_aml.bq2_sequence_transactions
  WHERE dataset IN ('HI-Medium', 'LI-Medium')
  GROUP BY dataset, sequence_id
)
SELECT dataset, ordered_txn_ids, COUNT(*) AS duplicate_chains
FROM ordered_chains
GROUP BY dataset, ordered_txn_ids HAVING COUNT(*) > 1;

-- Expect zero: canonical method tokens must match actual transactions.
SELECT s.dataset, s.sequence_id, m.hop_index
FROM is459_g2t1_aml.bq2_sequences s
JOIN is459_g2t1_aml.bq2_sequence_transactions m
  ON m.dataset = s.dataset AND m.sequence_id = s.sequence_id
JOIN is459_g2t1_aml.transactions_clean t
  ON t.dataset = m.dataset AND t.txn_id = m.txn_id
WHERE s.dataset IN ('HI-Medium', 'LI-Medium')
  AND (t.payment_format IS NULL OR
       element_at(split(s.method_sequence, ' -> '), m.hop_index) <> t.payment_format);

-- Expect zero under the proposed strictly-later, per-hop <=24h rule.
SELECT a.dataset, a.sequence_id, a.hop_index
FROM is459_g2t1_aml.bq2_sequence_transactions a
JOIN is459_g2t1_aml.bq2_sequence_transactions b
  ON b.dataset = a.dataset AND b.sequence_id = a.sequence_id AND b.hop_index = a.hop_index + 1
JOIN is459_g2t1_aml.transactions_clean x ON x.dataset = a.dataset AND x.txn_id = a.txn_id
JOIN is459_g2t1_aml.transactions_clean y ON y.dataset = b.dataset AND y.txn_id = b.txn_id
WHERE a.dataset IN ('HI-Medium', 'LI-Medium')
  AND (x.to_bank IS NULL OR x.to_account IS NULL OR y.from_bank IS NULL OR y.from_account IS NULL
       OR x.to_bank <> y.from_bank OR x.to_account <> y.from_account
       OR x.txn_ts IS NULL OR y.txn_ts IS NULL OR y.txn_ts <= x.txn_ts
       OR y.txn_ts > x.txn_ts + INTERVAL '24' HOUR);

-- After CTAS: expect zero rows. Reconcile reviewed = flagged + legitimate.
SELECT * FROM is459_g2t1_aml.bq2_tradeoff
WHERE reviewed <> laundering_flagged + legitimate_reviewed
   OR laundering_flagged > total_laundering OR legitimate_reviewed > total_legitimate
   OR reviewed > total_transactions
   OR (top_n = 0 AND reviewed <> total_transactions);
