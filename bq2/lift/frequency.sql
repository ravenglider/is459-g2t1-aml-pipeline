-- Inspect frequencies without outcomes before choosing the primary support cutoff.
-- Do not use lift/labels to select whichever policy produces the best result.
SELECT sequence_length, method_sequence, COUNT(*) AS support_count
FROM is459_g2t1_aml.bq2_sequences
WHERE dataset = 'HI-Medium'
GROUP BY sequence_length, method_sequence
ORDER BY sequence_length, support_count DESC, method_sequence;
