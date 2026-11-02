# Proposed bq2_sequences output schema

## Status

Contract version: `v1`. Ji Ning approved documenting this 21-column proposal after agreeing analytical requirements with Xiang Ying (Role 6). Exact interface approval from Xiang Ying is still pending. No builder, table, development batch or AWS deployment is claimed by this document.

The confirmed analytical requirements and pending operational decisions are in [sequence_rules.md](sequence_rules.md). This file specifies the proposed physical interface. It follows the repository's lowercase table/column names, processed-bucket ownership and `us-east-1` region conventions.

## Row meaning and locations

One row represents one distinct ordered chain within a dataset. It does not represent an aggregated method pattern. Pairs and triples share the same schema and table, distinguished by `sequence_length`; their analytical populations remain separate.

| Purpose | Proposed Glue/Athena table | Proposed S3 root |
|---|---|---|
| Production | `is459_g2t1_aml.bq2_sequences` | `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/` |
| Development | `is459_g2t1_aml.bq2_sequences_dev` | `s3://is459-g2t1-aml-processed-bucket/bq2_sequences_dev/` |

Read from `is459_g2t1_aml.transactions_clean` at `s3://is459-g2t1-aml-processed-bucket/transactions_clean/`. Join transaction lineage back to the source using both dataset and transaction ID.

Development uses the identical schema, with the original dataset identity (initially `sample`). Do not mix development data into production or relabel sample data as HI/LI. Xiang Ying must explicitly select the development table and sample filter when testing. A small selected batch is an interface fixture, not a complete analytical population.

## Exact 21-column proposal

Types below use Spark SQL / Glue DDL notation. Spark `bigint` is a signed 64-bit `LongType`; `int` is `IntegerType`; `string` is `StringType`; arrays map to `ArrayType` with the indicated element type. Athena SELECT queries expose strings as `varchar`, integers as `integer`, and arrays as `array(element_type)`. Catalog DDL uses forms such as `array<bigint>`.

Examples are fictional. Array examples are illustrative values, not serialized JSON stored inside a string column.

| # | Column | Spark/Glue type | Meaning | Example |
|---|---|---|---|---|
| 1 | `sequence_id` | `string` | Stable SHA-256 reference derived from rule version, dataset and ordered IDs | 64-character lowercase hexadecimal string |
| 2 | `rule_version` | `string` | Semantic contract version | `v1` |
| 3 | `sequence_length` | `int` | Transaction count, 2 or 3 | `3` |
| 4 | `txn_ids` | `array<bigint>` | Original IDs in chain order | `[101,102,103]` |
| 5 | `txn_timestamps` | `array<timestamp>` | Original timestamps in the same order | `[2022-09-01 09:00:00,2022-09-01 11:00:00,2022-09-02 08:00:00]` |
| 6 | `payment_methods` | `array<string>` | Ordered original `payment_format` values | `["Wire","ACH","Cheque"]` |
| 7 | `method_sequence` | `string` | Readable canonical method pattern | `Wire -> ACH -> Cheque` |
| 8 | `payment_currencies` | `array<string>` | Ordered sender-side currencies | `["US Dollar","Euro","Euro"]` |
| 9 | `receiving_currencies` | `array<string>` | Ordered receiver-side currencies | `["Euro","Euro","UK Pound"]` |
| 10 | `payment_currency_sequence` | `string` | Readable sender-side currency pattern | `US Dollar -> Euro -> Euro` |
| 11 | `receiving_currency_sequence` | `string` | Readable receiver-side currency pattern | `Euro -> Euro -> UK Pound` |
| 12 | `txn_laundering_labels` | `array<int>` | Original member labels, aligned with IDs | `[0,1,1]` |
| 13 | `laundering_count` | `int` | Number of positive member labels | `2` |
| 14 | `any_laundering` | `int` | 1 if any member is positive, otherwise 0 | `1` |
| 15 | `all_laundering` | `int` | 1 if every member is positive, otherwise 0 | `0` |
| 16 | `final_txn_id` | `bigint` | Last element of `txn_ids` | `103` |
| 17 | `final_is_laundering` | `int` | Last element of `txn_laundering_labels` | `1` |
| 18 | `gap_seconds` | `array<bigint>` | Adjacent timestamp differences in seconds | `[7200,75600]` |
| 19 | `duration_seconds` | `bigint` | Last timestamp minus first, in seconds | `82800` |
| 20 | `dataset` | `string`, partition | Original source dataset | `HI-Medium` |
| 21 | `start_date` | `string`, partition | Date of the first transaction, `YYYY-MM-DD` | `2022-09-01` |

The example is a 23-hour triple with 2-hour and 21-hour links. Bank/account linkage must also be checked against source transactions; timestamp order alone is insufficient.

## Representation guarantees

### Arrays and ordering

- `txn_ids`, `txn_timestamps`, `payment_methods`, `payment_currencies`, `receiving_currencies` and `txn_laundering_labels` have exactly `sequence_length` elements.
- Position i in every member array describes the same transaction.
- `gap_seconds` has exactly `sequence_length - 1` elements. Each value is greater than zero and at most 86,400.
- Pairs have two member elements, without a padded null third element.
- Build arrays from explicitly ordered transaction aliases. Do not rely on unordered collection/aggregation to determine order.
- Preserve IDs as bigint, including negative hashes. Never convert them to floating-point numbers or sort the ID array numerically.
- No repeated ID is allowed within a chain; no required published field or array element may be null. Validate these conditions explicitly rather than relying only on schema nullability.

Athena uses 1-based array subscripts: `txn_ids[1]` is the first transaction. `element_at(txn_ids, -1)` selects the last. Spark APIs have different indexing conventions; use explicit expressions and tests in implementation. If Xiang Ying needs member rows for auditing, arrays can be expanded with ordinality. A separate physical `bq2_sequence_transactions` table is not proposed.

### Canonical strings

Create each readable sequence string from its corresponding ordered array, using exactly the literal ASCII delimiter ` -> ` (one space, hyphen, greater-than sign, one space):

```text
method_sequence: Wire -> ACH -> Cheque
payment_currency_sequence: US Dollar -> Euro -> Euro
receiving_currency_sequence: Euro -> Euro -> UK Pound
```

Preserve the source token spelling and case. Validate that tokens are non-null, non-empty and do not contain the delimiter; fail visibly on a violation rather than producing ambiguous strings. Do not silently normalize values differently on HI and LI. The same mapping must be used for both lengths and datasets.

The two currency arrays retain direction: at position i, `payment_currencies[i]` is sent and `receiving_currencies[i]` is received. Do not require these to match within or across transactions. There is no ambiguous generic `currency_sequence` column in this proposal.

### Identity and labels

The logical unique key is `(dataset, ordered txn_ids)`. Deduplicate on that key, not solely on `sequence_id` or method pattern. The builder must first validate input `(dataset, txn_id)` uniqueness; a hash is not a guarantee. Conflicting IDs block publication, and even identical repeated source rows require coordinated resolution to keep Role 6's full-source denominators consistent. See the source-ID policy in the rules document.

`sequence_id` is SHA-256 over an unambiguous serialization of rule version, dataset and ordered IDs. Exclude labels, method/currency attributes and runtime values. The precise byte serialization must be fixed and covered by a repeatability fixture before implementation is accepted; it is not yet specified here. Identical input identities under the same rule version must retain their sequence IDs across reruns.

Every original label is 0 or 1. `laundering_count` is their sum; `any_laundering` indicates a sum above zero; `all_laundering` indicates a sum equal to length. `final_is_laundering` is the last original label, not a derived chain label. For `[1,0]`, `any_laundering=1` and `final_is_laundering=0`.

`final_txn_id` equals the last ordered ID. `duration_seconds` equals both the endpoint timestamp difference and the sum of adjacent gaps. `start_date` derives from the first timestamp under the verified upstream timestamp interpretation. No additional whole-chain 24-hour filter applies.

There is no generic chain-level `is_laundering` alias. Explicit label names prevent accidentally scoring targeted final transactions using the any-member label.

## Minimum Role 6 interface

| Task | Required sequence fields | Other input / counting rule |
|---|---|---|
| Outcome-blind HI frequency inspection | `dataset`, `sequence_length`, `method_sequence` | Count chain rows without labels |
| Descriptive sequence lift | Above plus `any_laundering` | Separate dataset/length baselines, including low-support patterns |
| Top-3/5/10 checks | `dataset`, `sequence_length`, `method_sequence`, `final_txn_id` | Frozen HI selections; deduplicate finals per length/support/top-N policy |
| Original target labels and denominators | `final_is_laundering` is a convenience check | Read `transactions_clean` for original labels and all cleaned transactions, including those outside chains |
| LI robustness | Pattern, dataset, length and label fields above | Apply frozen HI patterns unchanged; use LI's own sequence baseline |
| Traceability | `sequence_id`, ordered member arrays and timings | Join source on dataset plus transaction ID |
| Currency analysis | Currency arrays or readable strings | Secondary to the payment-method ranking |

Support sensitivity uses 50, 100 and 500. Do not filter these patterns out of Role 5 output or the baseline. Freeze HI selections separately by length and support policy before LI evaluation. Store the selected pattern lists in Role 6's analytical artifacts, not in this occurrence table.

Amounts, banks/accounts, `attempt_id` and `pattern_type` remain in the cleaned source and can be retrieved through lineage when needed. Their omission is intentional; they are not required for the agreed lift and final-transaction checks.

## Parquet partitions and catalog

Proposed format: Snappy-compressed Parquet, with partition keys in this order: `dataset`, `start_date`.

```text
s3://is459-g2t1-aml-processed-bucket/bq2_sequences/
  dataset=HI-Medium/start_date=2022-09-01/part-....snappy.parquet
  dataset=HI-Medium/start_date=2022-09-02/part-....snappy.parquet
  dataset=LI-Medium/start_date=2022-09-01/part-....snappy.parquet

s3://is459-g2t1-aml-processed-bucket/bq2_sequences_dev/
  dataset=sample/start_date=2022-09-01/part-....snappy.parquet
```

The table has 21 logical columns; standard partitioned writing stores partition values in the directory/catalog rather than requiring them in each Parquet file. Register the table and partitions explicitly through the job's catalog-writing process. Xiang Ying must filter dataset and length; start-date filters are appropriate for bounded checks but must not accidentally narrow full-dataset evaluation.

Do not partition by IDs or method patterns. Do not force one file for an entire dataset or one oversized file per date. File sizing and shuffle settings require profiling. Keep staging data and manifests outside both table roots so readers cannot mistake them for published rows.

## Development acceptance and safe reruns

Before full HI/LI processing, deliver a small development batch using the identical schema. Agree its size/window with Xiang Ying; retain complete chains and report counts by length, dataset and date. Include representative edge cases where available, without fabricating source labels. Clearly distinguish an exhaustive bounded batch from selected interface examples. No development prevalence, rankings or catch rates are final findings.

Acceptance checks include array lengths/alignment, distinct member IDs, source-ID uniqueness, unique ordered chains, exact account/time linkage, string reconstruction, label formulas, correct final fields, matching catalog types and Parquet readability. The source timestamp range and batch coverage must be supplied. Dataset boundaries can truncate future continuations; the full cleaned denominator still includes boundary transactions.

Reruns must replace the intended scope, preserve other datasets, and remove stale data/catalog partitions even when new output is empty. Stage and validate before publication, prohibit concurrent writers, and prevent Role 6 from reading a failed or partial refresh. Plain Parquet replacement is not atomic across partitions. The exact publication and recovery procedure remains to be verified.

A manifest outside table data folders should record source snapshot/coverage evidence, builder version, rule version, start-date coverage, counts (including zero-output dates) and successful completion. Do not mix rule versions or source refreshes in a supposedly comparable HI/LI analysis. Actual source uniqueness, timezone handling, growth limits and cost remain unverified.

## Xiang Ying schema-approval checklist

Ask Xiang Ying to approve these names and representations before implementation, then revise Role 6's own contract, SQL and validation consistently:

1. Keep `method_sequence` and the existing literal ` -> ` separator.
2. Replace generic sequence `is_laundering` references with `any_laundering` for descriptive lift.
3. Partition baseline calculations and rankings by dataset and sequence length; keep rare patterns in baselines; implement 50/100/500 sensitivity.
4. Replace constituent-transaction flagging with `final_txn_id`, deduplicated within each length/support/top-N policy. Use original final labels and full cleaned-dataset denominators.
5. Accept `txn_ids` arrays for ordered membership. There is no proposed physical membership table; an audit adapter is possible if needed.
6. Replace ambiguous `currency_sequence` with explicit payment and receiving sequence fields.
7. Freeze HI selections separately by length and support policy before LI evaluation, with deterministic tie handling.
8. Test against `bq2_sequences_dev` with the correct source dataset value, without treating development metrics as findings.

No Role 6 or other shared-role files are modified by this proposal. Sonia remains responsible for later Step Functions integration, including LI readiness.

Platform references: [Athena data types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) and [Athena array access](https://docs.aws.amazon.com/athena/latest/ug/accessing-array-elements.html).
