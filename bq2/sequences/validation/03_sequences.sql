-- Render per dataset. Reconcile every count to selected batch receipts, including zeros.
WITH expected AS (
    SELECT cast(day AS varchar) AS start_date,sequence_length
    FROM UNNEST(sequence(DATE '2022-09-01',DATE '__LAST_DATE__',INTERVAL '1' DAY)) AS u(day)
    CROSS JOIN (VALUES 2,3) AS lengths(sequence_length)
), actual AS (
    SELECT start_date,sequence_length,count(*) AS sequences,
           count_if(any_laundering=1) AS any_positive_sequences,
           count_if(all_laundering=1) AS all_positive_sequences,
           count_if(final_is_laundering=1) AS positive_final_sequences,
           count(DISTINCT final_txn_id) AS distinct_final_transactions,
           count_if(duration_seconds>86400) AS chains_over_24h
    FROM is459_g2t1_aml.bq2_sequences WHERE dataset='__DATASET__'
    GROUP BY start_date,sequence_length
)
SELECT '__DATASET__' AS dataset,e.start_date,e.sequence_length,
       coalesce(a.sequences,0) AS sequences,
       coalesce(a.any_positive_sequences,0) AS any_positive_sequences,
       coalesce(a.all_positive_sequences,0) AS all_positive_sequences,
       coalesce(a.positive_final_sequences,0) AS positive_final_sequences,
       coalesce(a.distinct_final_transactions,0) AS distinct_final_transactions,
       coalesce(a.chains_over_24h,0) AS chains_over_24h
FROM expected e LEFT JOIN actual a
  ON e.start_date=a.start_date AND e.sequence_length=a.sequence_length
ORDER BY e.start_date,e.sequence_length;

-- One aggregate row even for empty output. PASS: all bad_* counters are zero.
-- Empty output still requires manifest/count reconciliation; zero errors alone is insufficient.
WITH checks AS (
    SELECT *,
      rule_version='v1' AND sequence_length IN (2,3)
        AND start_date=cast(cast(element_at(txn_timestamps,1) AS date) AS varchar)
        AND start_date BETWEEN '2022-09-01' AND '__LAST_DATE__' AS ownership_ok,
      cardinality(txn_ids)=sequence_length
        AND cardinality(txn_timestamps)=sequence_length
        AND cardinality(payment_methods)=sequence_length
        AND cardinality(payment_currencies)=sequence_length
        AND cardinality(receiving_currencies)=sequence_length
        AND cardinality(txn_laundering_labels)=sequence_length
        AND cardinality(gap_seconds)=sequence_length-1
        AND all_match(txn_ids,x->x IS NOT NULL)
        AND all_match(txn_timestamps,x->x IS NOT NULL)
        AND all_match(txn_laundering_labels,x->x IS NOT NULL AND x IN (0,1))
        AND all_match(gap_seconds,x->x IS NOT NULL AND x>0 AND x<=86400)
        AND all_match(payment_methods,x->x IS NOT NULL AND length(trim(x))>0 AND strpos(x,' -> ')=0)
        AND all_match(payment_currencies,x->x IS NOT NULL AND length(trim(x))>0 AND strpos(x,' -> ')=0)
        AND all_match(receiving_currencies,x->x IS NOT NULL AND length(trim(x))>0 AND strpos(x,' -> ')=0)
        AS arrays_ok,
      cardinality(array_distinct(txn_ids))=sequence_length
        AND regexp_like(sequence_id,'^[0-9a-f]{64}$')
        AND sequence_id=lower(to_hex(sha256(to_utf8(concat(
            rule_version,chr(10),dataset,chr(10),
            array_join(transform(txn_ids,x->cast(x AS varchar)),',')))))) AS identity_ok,
      method_sequence=array_join(payment_methods,' -> ')
        AND payment_currency_sequence=array_join(payment_currencies,' -> ')
        AND receiving_currency_sequence=array_join(receiving_currencies,' -> ') AS strings_ok,
      laundering_count=reduce(txn_laundering_labels,0,(s,x)->s+x,s->s)
        AND any_laundering=IF(laundering_count>0,1,0)
        AND all_laundering=IF(laundering_count=sequence_length,1,0)
        AND final_txn_id=element_at(txn_ids,-1)
        AND final_is_laundering=element_at(txn_laundering_labels,-1) AS labels_ok,
      all_match(sequence(1,2),i -> i>=sequence_length OR (
          element_at(txn_timestamps,i)<element_at(txn_timestamps,i+1)
          AND element_at(txn_timestamps,i+1)<=element_at(txn_timestamps,i)+INTERVAL '24' HOUR
          AND element_at(gap_seconds,i)=date_diff('second',element_at(txn_timestamps,i),
                                                                       element_at(txn_timestamps,i+1))))
        AND duration_seconds=date_diff('second',element_at(txn_timestamps,1),
                                                        element_at(txn_timestamps,-1))
        AND duration_seconds=reduce(gap_seconds,BIGINT '0',(s,x)->s+x,s->s)
        AND duration_seconds>0 AND duration_seconds<=86400*(sequence_length-1) AS timing_ok
    FROM is459_g2t1_aml.bq2_sequences WHERE dataset='__DATASET__'
)
SELECT '__DATASET__' AS dataset,count(*) AS sequences,
       count_if(NOT coalesce(ownership_ok,false)) AS bad_ownership,
       count_if(NOT coalesce(arrays_ok,false)) AS bad_arrays,
       count_if(NOT coalesce(identity_ok,false)) AS bad_identity,
       count_if(NOT coalesce(strings_ok,false)) AS bad_strings,
       count_if(NOT coalesce(labels_ok,false)) AS bad_labels,
       count_if(NOT coalesce(timing_ok,false)) AS bad_timing
FROM checks;

-- PASS: zero rows for duplicate IDs or duplicate ordered chains (check both).
SELECT 'sequence_id' AS key_type,sequence_id AS key_value,count(*) AS copies
FROM is459_g2t1_aml.bq2_sequences WHERE dataset='__DATASET__'
GROUP BY sequence_id HAVING count(*)>1
UNION ALL
SELECT 'ordered_txn_ids',array_join(transform(txn_ids,x->cast(x AS varchar)),','),count(*)
FROM is459_g2t1_aml.bq2_sequences WHERE dataset='__DATASET__'
GROUP BY txn_ids HAVING count(*)>1;

-- Separate pair/triple and mixed-label distributions; retain every eligible pattern.
SELECT dataset,sequence_length,any_laundering,all_laundering,final_is_laundering,
       count(*) AS sequences
FROM is459_g2t1_aml.bq2_sequences WHERE dataset='__DATASET__'
GROUP BY dataset,sequence_length,any_laundering,all_laundering,final_is_laundering
ORDER BY sequence_length,any_laundering,all_laundering,final_is_laundering;
