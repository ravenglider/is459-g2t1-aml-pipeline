-- Engine 3. PASS: zero mismatch rows. Read-only; no table creation/repair.
WITH expected(position, column_name, data_type) AS (
    VALUES (1,'sequence_id','varchar'), (2,'rule_version','varchar'),
    (3,'sequence_length','integer'), (4,'txn_ids','array(bigint)'),
    (5,'txn_timestamps','array(timestamp)'), (6,'payment_methods','array(varchar)'),
    (7,'method_sequence','varchar'), (8,'payment_currencies','array(varchar)'),
    (9,'receiving_currencies','array(varchar)'), (10,'payment_currency_sequence','varchar'),
    (11,'receiving_currency_sequence','varchar'), (12,'txn_laundering_labels','array(integer)'),
    (13,'laundering_count','integer'), (14,'any_laundering','integer'),
    (15,'all_laundering','integer'), (16,'final_txn_id','bigint'),
    (17,'final_is_laundering','integer'), (18,'gap_seconds','array(bigint)'),
    (19,'duration_seconds','bigint'), (20,'dataset','varchar'), (21,'start_date','varchar')
), actual AS (
    SELECT ordinal_position AS position, column_name,
           replace(regexp_replace(lower(data_type),'\s+',''),'timestamp(3)','timestamp') AS data_type
    FROM information_schema.columns
    WHERE table_schema='is459_g2t1_aml' AND table_name='bq2_sequences'
)
SELECT e.position AS expected_position, e.column_name AS expected_column,
       e.data_type AS expected_type, a.position AS actual_position,
       a.column_name AS actual_column, a.data_type AS actual_type
FROM expected e FULL OUTER JOIN actual a ON e.position=a.position
WHERE e.column_name IS DISTINCT FROM a.column_name
   OR e.data_type IS DISTINCT FROM a.data_type
ORDER BY coalesce(e.position,a.position);
