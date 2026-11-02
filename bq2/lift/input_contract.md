# Proposed Role 5 → Role 6 input contract

Status: **not confirmed or deployed**. If Ji Ning already has a different schema, adapt these inputs with an agreed mapping rather than rebuilding the sequence job.

## Sample-first handoff for Ji Ning

Xiang Ying has verified the cleaned schema and full HI/LI counts. Ji Ning has not started the builder yet (team update, 2 Nov 2026). This document is the proposed interface, not a signed-off design. Publish the actual names/mapping here before connecting Role 6.

1. Confirm the decisions below before the first builder run.
2. Deliver a small development batch first, for example 100–200 complete chains plus every constituent transaction ID. Include legitimate and positive chains where present, both 2- and 3-hop cases, a repeated payment method, and overlapping chains. Do not manufacture positive labels if the selected window has none: report that limitation.
3. Share the development database/table names, schema, dataset values, S3 locations and one traced real chain with timestamps/bank-account links. Keep transaction/account-level data in team-controlled S3, not GitHub.
4. Xiang Ying maps the interface and checks completeness, labels, hop order and duplicate handling. The existing SQLite fixtures can be used while this batch is being built.
5. Only after the sample checks pass, run the same rules on full HI and LI. Report counts, date coverage and builder version for each. The 100–200 figure is a suggested test-batch size, not the minimum-support threshold.

A handpicked batch is for technical verification only. Its laundering prevalence and ranks must not be presented as representative findings. Keep development outputs separate from production tables. The prepared final analytics excludes `dataset = 'sample'`; do not relabel sample data as full HI/LI just to make it run. Agree a development mapping before testing the sample with Athena.

## Decisions awaiting Ji Ning/team confirmation

| Decision | Role 6 proposed default | Approval status |
|---|---|---|
| Chain label | MAX of constituent `is_laundering` labels (any positive hop) | Pending |
| 24h interpretation | Each consecutive hop strictly later and <=24h later | Pending |
| Total chain duration | No additional <=24h total-duration limit under the proposed per-hop rule | Pending |
| Equal timestamps | Exclude tied-time links because minute timestamps do not establish order | Pending |
| Length/overlap | Build 2- and 3-transfer chains; allow overlaps, prohibit repeated txn within one chain; deduplicate exact ordered chains | Pending |
| Bank/account link | Match both bank and account, identically on HI/LI | Pending |
| Self-transfers/cycles/branching | Ji Ning documents explicit handling before the sample run | Pending |
| Actual input names/schema | Tables below or a documented adapter over the builder's existing fields | Pending |

Example of the window distinction: transfers at Monday 00:00, Monday 23:00 and Tuesday 22:00 meet a 24h-per-hop rule (23h gaps) but fail a 24h-total-chain rule (46h total). Pick one interpretation and apply it identically to HI/LI. `validation.sql` currently checks strictly later, per-hop <=24h and the MAX label. If the team selects different rules, update that validation and the explanatory copy together before execution.

## Ready-to-share message

> Ji Ning, my Role 6 schema and HI/LI baseline checks are complete. Before you build the sequences, please review this proposed contract on main: bq2/lift/input_contract.md. Can we confirm the MAX/any-positive-hop label and strictly-later, <=24h-per-consecutive-hop rule (no extra whole-chain limit), plus overlap/tied-time handling? Please deliver a small development batch of complete chains with legitimate and positive examples where available, ordered methods, chain length/label and constituent transaction IDs before the full runs. An ID array is fine if we map it to membership. Please reply with your actual database/table names, schema and the agreed decisions; both full HI and LI must eventually use the same rules. The batch is for testing, not final findings.

This message is prepared for Xiang Ying to share; no direct message has been sent and no agreement has been claimed. Record Ji Ning's reply below.

- Confirmed by / date: pending.
- Agreed rules or differences: pending.
- Development table names and schema: pending.
- Full HI/LI readiness: pending.

## `is459_g2t1_aml.bq2_sequences`

Parquet at `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/`, partitioned by `dataset`.

| Column | Athena type | Required meaning |
|---|---|---|
| dataset | varchar | `HI-Medium` or `LI-Medium`; `sample` is excluded from final SQL |
| sequence_id | varchar | Stable unique ordered chain ID within dataset, based on transaction IDs, not labels |
| method_sequence | varchar | Canonical methods separated by literal ` -> `, e.g. `Cash -> Wire -> Bitcoin` |
| currency_sequence | varchar | Canonical ordered currency values under a documented rule; secondary analysis only |
| sequence_length | integer | Number of transfers, 2 or 3, matching method tokens and membership rows |
| is_laundering | integer | Proposed `MAX` of constituent transaction labels; confirm with team |

One row per distinct ordered chain, including legitimate chains. Do not build only laundering chains or collapse all occurrences of a method pattern into one row.

## `is459_g2t1_aml.bq2_sequence_transactions`

Proposed Parquet prefix `s3://is459-g2t1-aml-processed-bucket/bq2_sequence_transactions/`, partitioned by dataset. Membership enables distinct transaction trade-offs. An adapter over an existing transaction-ID array is sufficient.

| Column | Athena type | Meaning |
|---|---|---|
| dataset | varchar | Same partition as chain and cleaned transaction |
| sequence_id | varchar | Chain key |
| hop_index | integer | 1-based transfer position |
| txn_id | bigint | Exact `transactions_clean.txn_id` |

One row per hop; unique `(dataset, sequence_id, hop_index)`. A transaction may occur in several chains. Join transaction IDs with dataset, never across HI/LI/sample.

## Rules Ji Ning must document

- Link incoming `(to_bank, to_account)` to outgoing `(from_bank, from_account)` using both fields.
- Proposed window: each next transfer strictly later and at most 24 hours later. Clarify whether the whole chain must also fit 24 hours. Minute-resolution ties need a consistent policy; hashes do not establish time order.
- No repeated transaction within a chain. Define self-transfers, cycles, branching and overlapping 2/3-hop subchains identically on HI/LI.
- Define currency sequence (paid/received currency and FX within a hop). Do not fragment the primary payment-method ranking by currency.
- Publish counts, date bounds, rejected rows, branch fan-out and follow-up-window policy. Dataset boundaries can truncate chains.
- A linked chain is a candidate pathway, not proof the same funds moved. Under a MAX label, some constituent transactions may be legitimate.
- Never require HI-only `attempt_id` for construction or labels; LI lacks it.

Without membership, lift can proceed after chain checks pass, but transaction trade-offs must wait. Chain-level rates must be labelled as such and cannot substitute for the proposal's transaction chart.
