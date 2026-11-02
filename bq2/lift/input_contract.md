# Proposed Role 5 → Role 6 input contract

Status: **not confirmed or deployed**. If Ji Ning already has a different schema, adapt these inputs with an agreed mapping rather than rebuilding the sequence job.

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
