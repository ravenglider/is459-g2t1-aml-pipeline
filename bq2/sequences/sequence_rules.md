# BQ2 sequence-building rules

## Status and ownership

Contract version: `v1`.

The analytical requirements below were confirmed with Xiang Ying (Role 6), as reported by Ji Ning (Role 5), and approved for documentation. The proposed output interface is in [schema_bq2_sequences.md](schema_bq2_sequences.md); Xiang Ying still needs to approve its exact column names and representations.

These documents do not establish that a builder, table, development batch or deployment exists. No real sequence results or performance measurements are claimed. Role 5 owns this directory; Role 6 owns downstream SQL, and Sonia owns orchestration changes.

## Repository-confirmed input

- Table: `is459_g2t1_aml.transactions_clean`.
- Root: `s3://is459-g2t1-aml-processed-bucket/transactions_clean/`.
- Format: Parquet, partitioned by `dataset`, then `txn_date`.
- Dataset values: `sample`, `HI-Medium`, `LI-Medium`. Never link across datasets.
- IDs and banks are bigint; accounts, methods and currencies are strings; `txn_ts` is a timestamp; `is_laundering` is an integer label.
- Role 2 generates `txn_id` using `xxhash64` over transaction attributes. The hash excludes the laundering label and dataset. Its uniqueness is not enforced by the existing ETL.
- LI has transaction laundering labels but null attempt metadata. Attempt metadata is not a prerequisite for constructing or labeling chains.

Sources: [cleaned schema](../../etl/schema_transactions_clean.md), [ETL implementation](../../etl/etl_transactions_clean.py), [contribution conventions](../../CONTRIBUTING.md), and [ingestion handoff](../../docs/ingestion-handoff.md). These are repository facts, not a live AWS verification.

## Confirmed chain definition

A 2-hop chain contains two transactions; a 3-hop chain contains three. For each adjacent pair `(a, b)`, all conditions must hold:

```text
a.dataset = b.dataset
a.to_bank = b.from_bank
a.to_account = b.from_account
b.txn_ts > a.txn_ts
b.txn_ts <= a.txn_ts + 24 hours
```

Match both bank ID and account ID. Matching account text alone is insufficient. Do not coerce account strings into numbers or discard leading zeros.

The 24-hour bound is inclusive and applies to each link. Equal-timestamp links are excluded because minute-resolution timestamps do not establish order. There is no extra whole-chain 24-hour restriction: a triple can span up to 48 hours.

Retain all valid branches, same-bank transfers, repeated methods and overlapping chains. Do not require equal amounts, equal currencies or common attempt IDs. Keep valid pairs even when they extend into triples. Build and evaluate the two lengths separately; one physical table can hold both using `sequence_length`.

Do not repeat a transaction ID within a chain. Deduplicate identical ordered chains by `(dataset, ordered txn_ids)`, not by method pattern. Different chains with the same method pattern are separate occurrences. A sequence hash is a convenient reference, not a substitute for checking the logical ordered key.

A linked chain is a candidate path through accounts, not proof that the identical funds were forwarded.

### Explicit interpretation to include in schema review

Repeated accounts and cycles are not repeated transactions. The proposed implementation retains cycles and transfers from an account to itself when all stated rules pass; it adds no account-revisit exclusion. This makes the general eligibility rule explicit for Xiang Ying's review.

## Illustrative example (fictional, not project data)

All transactions below are in the same dataset. Each account is identified by bank/account together.

| ID | Timestamp | Sender | Receiver | Method | Original label |
|---|---|---|---|---|---:|
| 101 | 2022-09-01 09:00 | 10/A100 | 20/B200 | Wire | 0 |
| 104 | 2022-09-01 12:00 | 20/B200 | 50/E500 | Cash | 0 |
| 108 | 2022-09-02 12:00 | 50/E500 | 90/J900 | Wire | 1 |

`101 -> 104 -> 108` is valid: its gaps are 3 hours and exactly 24 hours, despite a total duration of 27 hours. Its method pattern is `Wire -> Cash -> Wire`. Its labels are `[0,0,1]`, so `laundering_count=1`, `any_laundering=1`, `all_laundering=0`, and `final_is_laundering=1`.

A different outgoing transfer from bank 20/B200 within the window creates another valid branch. An outgoing transfer from bank 99/B200 does not match. A link with an equal timestamp, or a gap of 24 hours and one minute, is invalid.

## Labels and currencies

Preserve each member's original `is_laundering` in chain order. Require binary, non-null labels; do not convert unknown labels into zero.

```text
laundering_count = sum(member labels)
any_laundering = 1 when laundering_count > 0, otherwise 0
all_laundering = 1 when laundering_count = sequence_length, otherwise 0
final_is_laundering = original label of the last member
```

`any_laundering` is the primary descriptive sequence-lift label. It is not the final transaction's label. For example, labels `[1,0]` give `any_laundering=1` but `final_is_laundering=0`. Preserve legitimate and mixed chains; do not build only positive chains.

Preserve payment and receiving currencies separately at every hop. Their aligned arrays express conversion within each transaction, as well as differences between one transaction's receiving currency and the next transaction's payment currency. Currency equality is not a linkage condition. The exact array and readable-string conventions are specified in the schema document.

## Confirmed Role 6 analytical contract

### Lift and support

Calculate each baseline and ranking separately for `dataset × sequence_length`:

```text
pattern positive rate = sum(any_laundering for the pattern) / pattern sequence count
baseline positive rate = sum(any_laundering for all sequences) / all sequence count
lift = pattern positive rate / baseline positive rate
```

Keep low-support patterns in the baseline even when excluding them from ranking. Do not substitute transaction-level prevalence for the sequence baseline. An empty population or zero positive baseline produces an undefined relevant rate/lift, not an invented zero.

Inspect HI pattern-frequency counts without laundering outcomes first. Assess support thresholds of 50, 100 and 500; do not select whichever threshold yields the highest lift. Role 5 publishes all valid chains without a minimum-support filter. Role 6 applies ranking eligibility and reports support, rates and lift.

Freeze a deterministic HI ranking and top-3/5/10 selection separately for each length and support policy before evaluating LI. Apply the same patterns unchanged on LI, including patterns absent or below the corresponding support threshold there. LI lift uses its own dataset/length baseline. Independently ranking LI is a separate descriptive comparison, not a replacement for frozen-rule evaluation. Deterministic tie-breaking and treatment of fewer than N eligible patterns must be documented by Role 6.

### Targeted checks

Flag only the final transaction of each matching chain. Count each distinct final transaction once within `(dataset, sequence_length, support cutoff, top-N policy)`, even if multiple matched chains or patterns end there. Use its original transaction-level label, not `any_laundering`.

```text
catch rate = distinct flagged positive final transactions / all positive cleaned transactions
legitimate review rate = distinct flagged negative final transactions / all negative cleaned transactions
```

Denominators include all cleaned transactions in the same dataset, including those outside valid chains. Obtain them from `transactions_clean`, not from sequence membership. Zero denominators produce undefined rates. A combined pair/triple policy would need a separately defined union and deduplication; do not add coverage percentages across lengths.

Sequence patterns are observable only when their final transaction occurs. Report these as checks at sequence completion, not proof of blocking an earlier transfer. Sequence occurrences can overlap and are not independent observations.

## Source ID validation and malformed inputs

The builder MUST validate `(dataset, txn_id)` uniqueness instead of assuming the hash is unique. This is required before using IDs to join, deduplicate chains or identify final transactions.

Proposed fail-closed policy:

1. Reject null required IDs, timestamps, linking bank/account fields, methods, currencies or labels; validate label values and string representations. Report failures before publishing.
2. If repeated IDs exist, distinguish identical repeated clean records from conflicting records, including conflicting labels.
3. Stop for conflicts. Do not select an arbitrary row, merge labels or generate replacement IDs.
4. Report identical repeated source records and stop production publication until their treatment is agreed with Role 2 and Xiang Ying. Do not silently deduplicate the source while Xiang Ying counts all source rows in denominators.
5. After validated inputs, exclude repeated IDs within chains and deduplicate generated ordered chains independently of source validation.

Resolving upstream defects is a coordinated handoff, not authorization to modify Role 2's code or data. Role 5 and Xiang Ying must use the same validated source snapshot.

## Boundaries and output growth

Proposed processing batches are defined by the first transaction's date. A start-day batch reads that day and the following two dates where available, then enforces the exact timestamp predicates. Only chains starting on the chosen day are emitted. This preserves valid cross-midnight pairs and up-to-48-hour triples without repeating chains across batches.

Actual dataset boundaries truncate observable continuations. Report input timestamp bounds, processed start dates and this limitation. Do not invent missing transactions or silently remove boundary transactions from the agreed full-dataset denominators. A row-limited sample is useful for interface tests but is not guaranteed to contain every transaction within its timestamp range.

Retaining all branches can create enormous output. Before materializing triples, measure predecessor and successor counts. For a middle transaction, the projected number of triples is their product. Aggregate projections and record hot accounts, edge counts and output counts. Any agreed resource/output guardrail must fail explicitly rather than silently truncate or sample production chains. Guardrail values, workers, shuffle sizing and full-data feasibility remain unmeasured.

## Development delivery and publication

Deliver a separate development table with the same schema before full HI/LI processing. Preserve source dataset identity, including `sample`; never relabel sample rows as full HI. Document whether the batch is exhaustive for a bounded start window or selected complete chains for interface testing. Report missing example types rather than manufacture positive labels. Keep transaction-level development data in team-controlled S3, not Git.

Production uses Snappy Parquet with proposed `dataset/start_date` partitions. See the schema document for exact locations. Development results are not final prevalence, lift or coverage findings.

Safe-rerun requirements:

- Stage and validate output before publishing; keep staging outside queryable table locations.
- Replace the intended dataset/batch scope instead of appending duplicate chains; preserve unrelated datasets.
- Remove obsolete files and catalog partitions, including dates whose new result is empty.
- Prevent concurrent writers to the same destination and keep downstream analysis behind successful publication and catalog updates.
- Record source coverage/version evidence, builder and rule version, counts and completion in a manifest outside table data folders. Record zero-output dates too.
- Plain Parquet does not provide an atomic multi-partition replacement. Define failure recovery and reader coordination before implementation; do not claim atomic publication.

## Decisions still requiring verification

- Xiang Ying approval of exact column names, arrays, readable strings and development mapping in the schema document.
- Upstream timezone interpretation: raw timestamps have no timezone and Role 2 does not explicitly set a Spark session timezone. Preserve upstream meaning; do not assume local Singapore time.
- Input uniqueness and required-field quality on actual data.
- Development batch bounds/size, branch-growth limits, worker settings and full-data runtime/cost.
- Exact staging, catalog update, stale-partition cleanup, manifest and failure-recovery mechanism.
- Source freshness coordination: existing orchestration refreshes HI only; LI readiness and later integration require Sonia's coordination.

This documentation does not authorize any AWS execution or shared-role changes.
