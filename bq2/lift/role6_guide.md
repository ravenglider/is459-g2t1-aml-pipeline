# Role 6 execution guide — Role 5 v1 interface

## What is complete

Cleaned schema, one-day HI missing-USD check and full HI/LI baseline reconciliation are evidenced in [results.md](results.md) and [screenshots](screenshots/README.md). Do not rerun them merely because the sequence schema changed. The role implements proposal pages 8–9: ranking, top-3/5/10 checks, LI robustness, Sankey and targeted checklist.

## Work ready before Role 5 data

The SQL, validation, local regression fixtures, [input mapping](input_contract.md) and [Tableau plan](../../viz/bq2_tableau.md) match the 21-column v1 schema. No sequence table, real ranking, winner or populated Tableau workbook has been verified. A ready-to-send message for Ji Ning is in the input contract; publishing it does not imply a direct message or agreement on their behalf.

## Run order once development output exists

1. Obtain actual table/schema, source coverage, rule/builder version and publication manifest. Use the isolated development mapping in the input contract; preserve `dataset = 'sample'` and avoid production destinations. A sample-only batch cannot validate HI-to-LI transfer.
2. Execute each pre-CTAS statement of `validation.sql` in order. Compare source rows with distinct txn IDs per dataset; null/bad IDs/labels or duplicate source IDs block joins. Require v1 only, correct lengths, no duplicate chains and zero defect rows. Stop if checks fail. Sparse/empty expected length or dataset populations require explicit reporting, not invented zero results.
3. After development acceptance and FULL HI/LI publication, run `frequency.sql` before inspecting labels/lift. Agree the main support cutoff from frequency evidence; retain the 50/100/500 sensitivity outputs.
4. Run lift → trade-off → robustness → Sankey CTAS, one statement at a time, using empty destinations or consistent new run names. Then run the final post-CTAS reconciliation in validation.sql.
5. Inspect ordered results, actual eligible pattern counts and LI absence/support. Record query IDs, elapsed time, scanned bytes, source snapshot, rule version and output locations. Use `SHOW CREATE TABLE` to inspect real locations.
6. Populate Tableau and the final results, choose a policy under an agreed review budget/catch target, and update README. No automatic top-5 recommendation.

## Exact metric scopes

| Metric | Definition / scope |
|---|---|
| Support | Count of distinct ordered chain occurrences per dataset, length and method pattern |
| Sequence positive rate | SUM(any_laundering) / COUNT(chains) |
| Sequence baseline | Same positive rate across ALL patterns within dataset × length, including rare patterns |
| Lift | Pattern rate / dataset-length baseline; NULL if the baseline is zero |
| Eligibility | support_count >= min_support, independently at 50, 100, 500 |
| Rank | Separate dataset × length × min_support; lift desc, positive-chain count desc, support desc, method string asc |
| Targeted checks | Union of distinct final_txn_id across matched chains within dataset × length × min_support × top-N |
| Catch | Flagged original-positive finals / ALL original-positive cleaned transactions in that dataset |
| Legitimate review | Flagged original-negative finals / ALL original-negative cleaned transactions in that dataset |
| Precision | Flagged original-positive finals / distinct reviewed finals |
| Broad baseline | Review all cleaned transactions; 100% catch/review when those populations exist |

No rates/counts should be summed across lengths or support policies. A combined pair/triple policy would require a separate final-ID union, not adding rates. Transactions outside chains remain in denominators. Early positive hops do not make a legitimate final transaction a positive review.

HI selects patterns independently for each length/cutoff. The same frozen HI patterns apply unchanged to LI, even when absent or low-support on LI. Independent LI ranks are descriptive comparisons. For top-N overlap, count robustness rows with hi_rank <= N and li_rank <= N WITH ONE length/cutoff selected; report actual eligible set sizes. Fewer than N patterns is not a full top-N list. Zero denominators give NULL rather than zero; an empty selection can have zero reviews and undefined precision.

Sankey weights are chain counts, not USD or unique finals. `bq2_lift` repeats each pattern for three support policies; `sankey.sql` deliberately takes the min_support=50 copy without filtering eligibility, so each pattern appears once and rare patterns remain visible. That fixed copy is not a chosen operating cutoff.

## Proposal and interpretation limits

The proposal's approximate 1-in-905 rate describes transactions. It is context, not a comparable denominator for an any-hop chain rate. Use dataset-length sequence baselines for lift. Proposal chart numbers are illustrations, not results. Its broad 70%/25% example has no executable rule; the implementation explicitly uses review-all. Any other broad rule needs a label-independent definition and matched population.

HI performance is in-sample. LI is a synthetic different-prevalence robustness check, not real-world validation. A candidate account path does not prove the same funds moved. Checks at chain completion do not prove an earlier transfer was blocked. Overlapping chains are dependent observations.

## USD outliers and costs

Primary metrics use counts; do not apply Role 3's >=$20M value exclusion to these populations. An optional USD view would need distinct-final value sums, full/filtered sensitivity, excluded counts and matched denominators; it cannot sum hops as unique funds.

Develop with dataset/date partition filters, retaining all look-back/follow-up required by the chain rule. Sequence dates are `start_date`, clean dates are `txn_date`. A triple can extend 48 hours after its start. Final full-population rates must not silently narrow source denominators or chain coverage using mismatched date filters. Do not mix sample/HI/LI or source refreshes. Sonia coordinates orchestration and both-dataset readiness; no shared pipeline changes are made here.
