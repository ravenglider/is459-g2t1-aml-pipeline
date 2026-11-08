-- Run only after schema, source-ID, output-ID and array checks pass.
-- This is a complete selected-dataset scan, not a sample or top-N check.
-- PASS: zero bad_*; compare member_rows to 2*pairs+3*triples from accepted counts.
WITH members AS (
    SELECT q.sequence_id,q.sequence_length,q.start_date,u.txn_id,u.member_position,
           element_at(q.txn_timestamps,u.member_position) AS member_time,
           element_at(q.payment_methods,u.member_position) AS member_method,
           element_at(q.payment_currencies,u.member_position) AS member_payment_currency,
           element_at(q.receiving_currencies,u.member_position) AS member_receiving_currency,
           element_at(q.txn_laundering_labels,u.member_position) AS member_label,
           element_at(q.txn_ids,u.member_position+1) AS next_txn_id
    FROM is459_g2t1_aml.bq2_sequences q
    CROSS JOIN UNNEST(q.txn_ids) WITH ORDINALITY AS u(txn_id,member_position)
    WHERE q.dataset='__DATASET__'
), source AS (
    SELECT txn_id,txn_ts,from_bank,from_account,to_bank,to_account,
           payment_format,payment_currency,receiving_currency,is_laundering
    FROM is459_g2t1_aml.transactions_clean
    WHERE dataset='__DATASET__' AND txn_date BETWEEN '2022-09-01' AND '__LAST_DATE__'
), joined AS (
    SELECT m.sequence_id,m.member_position,m.sequence_length,
           count(s.txn_id) AS source_matches,
           count_if(s.txn_id IS NOT NULL AND NOT coalesce(
               m.member_time=s.txn_ts AND m.member_method=s.payment_format
               AND m.member_payment_currency=s.payment_currency
               AND m.member_receiving_currency=s.receiving_currency
               AND m.member_label=s.is_laundering,false)) AS attribute_errors,
           count_if(m.member_position<m.sequence_length AND NOT coalesce(
               s.to_bank=n.from_bank AND s.to_account=n.from_account
               AND s.txn_ts<n.txn_ts AND n.txn_ts<=s.txn_ts+INTERVAL '24' HOUR,
               false)) AS link_errors
    FROM members m LEFT JOIN source s ON m.txn_id=s.txn_id
    LEFT JOIN source n ON m.next_txn_id=n.txn_id
    GROUP BY m.sequence_id,m.member_position,m.sequence_length
)
SELECT '__DATASET__' AS dataset,count(*) AS member_rows,
       count_if(source_matches<>1) AS bad_member_source_matches,
       coalesce(sum(attribute_errors),0) AS bad_member_attributes,
       coalesce(sum(link_errors),0) AS bad_adjacent_links
FROM joined;
