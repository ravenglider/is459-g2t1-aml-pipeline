-- bq2 gate lift outputs
-- Run by Step Functions after the Role 6 tables are rebuilt. One row; ok must be 'true'.
-- Every count below must be 0 (or match), otherwise the run fails with this row as the cause.
-- flagged_groups = expected_flagged_groups: every (dataset, length, support) group whose HI top-10
-- patterns occur in that dataset has trade-off stage 1 rows (a missing group = a statement wrote nothing).
WITH seq AS (
  SELECT dataset, sequence_length, COUNT(*) AS n, SUM(any_laundering) AS l
  FROM is459_g2t1_aml.bq2_sequences
  WHERE dataset IN ('HI-Medium', 'LI-Medium')
  GROUP BY dataset, sequence_length
), lift_totals AS (
  SELECT dataset, sequence_length, min_support,
         SUM(support_count) AS n, SUM(laundering_count) AS l,
         COUNT(risk_rank) - COUNT(DISTINCT risk_rank) AS dup_ranks
  FROM is459_g2t1_aml.bq2_lift
  GROUP BY dataset, sequence_length, min_support
), lift_check AS (
  SELECT
    (SELECT COUNT(*) FROM seq) AS sequence_groups,
    COUNT(*) AS lift_groups,
    COUNT_IF(s.n IS NULL OR s.n <> t.n OR s.l <> t.l) AS lift_support_mismatches,
    COALESCE(SUM(t.dup_ranks), 0) AS lift_duplicate_ranks
  FROM lift_totals t LEFT JOIN seq s
    ON s.dataset = t.dataset AND s.sequence_length = t.sequence_length
), flagged_check AS (
  SELECT COUNT(*) AS flagged_rows,
         (SELECT COUNT(*) FROM (
            SELECT DISTINCT dataset, sequence_length, min_support
            FROM is459_g2t1_aml.bq2_tradeoff_flagged) g) AS flagged_groups,
         (SELECT COUNT(*) FROM (
            SELECT DISTINCT d.dataset, h.sequence_length, h.min_support
            FROM is459_g2t1_aml.bq2_lift h
            JOIN is459_g2t1_aml.bq2_lift d
              ON d.method_sequence = h.method_sequence AND d.sequence_length = h.sequence_length
             AND d.min_support = h.min_support
            WHERE h.dataset = 'HI-Medium' AND h.risk_rank <= 10 AND d.support_count > 0) e) AS expected_flagged_groups,
         (SELECT COUNT(*) FROM (
            SELECT 1 FROM is459_g2t1_aml.bq2_tradeoff_flagged
            GROUP BY dataset, sequence_length, min_support, final_txn_id HAVING COUNT(*) > 1) d) AS flagged_duplicates,
         COUNT_IF(best_rank IS NULL OR best_rank NOT BETWEEN 1 AND 10) AS flagged_bad_ranks
  FROM is459_g2t1_aml.bq2_tradeoff_flagged
), tradeoff_check AS (
  SELECT COUNT(*) AS tradeoff_rows,
         COUNT_IF(reviewed <> laundering_flagged + legitimate_reviewed
                  OR reviewed > total_transactions
                  OR laundering_flagged > total_laundering) AS tradeoff_accounting_errors
  FROM is459_g2t1_aml.bq2_tradeoff
), robustness_check AS (
  SELECT COUNT(*) AS robustness_rows,
         (SELECT COUNT(*) FROM is459_g2t1_aml.bq2_lift
          WHERE dataset = 'HI-Medium' AND risk_rank <= 10) AS hi_top10_rows
  FROM is459_g2t1_aml.bq2_robustness
), sankey_check AS (
  SELECT COUNT_IF(s.n IS NULL OR s.n <> k.n OR s.l <> k.l) AS sankey_mismatches,
         COUNT(*) AS sankey_groups
  FROM (SELECT dataset, sequence_length, SUM(sequence_count) AS n, SUM(laundering_sequence_count) AS l
        FROM is459_g2t1_aml.bq2_sankey GROUP BY dataset, sequence_length) k
  LEFT JOIN seq s ON s.dataset = k.dataset AND s.sequence_length = k.sequence_length
)
SELECT CASE WHEN c.sequence_groups = 4
             AND c.lift_groups = 12
             AND c.lift_support_mismatches = 0
             AND c.lift_duplicate_ranks = 0
             AND f.expected_flagged_groups > 0 AND f.flagged_groups = f.expected_flagged_groups AND f.flagged_duplicates = 0 AND f.flagged_bad_ranks = 0
             AND t.tradeoff_rows = 48 AND t.tradeoff_accounting_errors = 0
             AND r.robustness_rows = r.hi_top10_rows AND r.robustness_rows > 0
             AND k.sankey_groups = 4 AND k.sankey_mismatches = 0
            THEN 'true' ELSE 'false' END AS ok,
       c.*, f.*, t.*, r.*, k.*
FROM lift_check c CROSS JOIN flagged_check f CROSS JOIN tradeoff_check t
     CROSS JOIN robustness_check r CROSS JOIN sankey_check k
