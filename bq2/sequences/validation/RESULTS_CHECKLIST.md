# Compact final acceptance results

Status: **NOT RUN**. Keep the filled copy and raw evidence private. Replace
pending fields with actual results/query IDs; never infer a pass from preparation.

## Dataset-level gates

| Check | HI | LI |
|---|---|---|
| Glue job/run + application run/attempt | pending | pending |
| Workers, Glue version, duration, DPU/cost evidence | pending | pending |
| Source/table/partition fingerprints match final job | pending | pending |
| Generation manifest + verified publication receipt | pending | pending |
| Table schema: all 21 names/types/order | pending | pending |
| Registered owner dates, including empty locations | pending / 28 | pending / 27 |
| Cleaned source transactions | pending / 31,898,218 | pending / 31,251,469 |
| Positive source labels | pending / 35,230 | pending / 16,041 |
| Source date scope | pending / Sep 1–28 | pending / Sep 1–27 |
| Source null/duplicate IDs, labels, dates, required fields, precision: all zero | pending | pending |
| Pair/triple totals match every selected receipt and run manifest | pending | pending |
| Sequence ID/hash and ordered-chain uniqueness | pending | pending |
| Array/string/label/timing/ownership errors: all zero | pending | pending |
| Label distributions retained, without filtering legitimate/mixed chains | pending | pending |
| Every member matches source attributes exactly once | pending | pending |
| Adjacent bank AND account continuity; 0 < gap <=24h | pending | pending |
| Member total = 2*pairs + 3*triples | pending | pending |
| Actual files/Catalog locations match one accepted run/attempt | pending | pending |
| Source and Catalog stable throughout validation | pending | pending |
| Boundary limits recorded; triples may span 48h | pending | pending |
| Owner pause confirmations and paid-query approval recorded | pending | pending |
| Exceptions resolved; final status | NOT ACCEPTED | NOT ACCEPTED |

Record each SQL query ID and link to its full results/`execution.json` separately.
For the final combined Catalog check expect **55 PASS rows**. No orphaned/mixed
version may be accepted; empty dates still require explicit Catalog/receipt proof.

## HI owner-date coverage

Fill pair/triple counts from `04-sequence-counts.sql`; confirm against that
date's selected receipt. Global schema/ID/value checks must pass before lineage.
If using the full lineage query, record that ID and aggregate reconciliation
once; use the daily rows to track count/receipt coverage without claiming separate
daily lineage execution. If using chunks, every daily lineage result is required.

| Owner date | Pairs / triples | Receipt counts match | Lineage query ID or full-scope reference | Members / errors |
|---|---|---|---|---|
| 2022-09-01 | pending | pending | pending | pending |
| 2022-09-02 | pending | pending | pending | pending |
| 2022-09-03 | pending | pending | pending | pending |
| 2022-09-04 | pending | pending | pending | pending |
| 2022-09-05 | pending | pending | pending | pending |
| 2022-09-06 | pending | pending | pending | pending |
| 2022-09-07 | pending | pending | pending | pending |
| 2022-09-08 | pending | pending | pending | pending |
| 2022-09-09 | pending | pending | pending | pending |
| 2022-09-10 | pending | pending | pending | pending |
| 2022-09-11 | pending | pending | pending | pending |
| 2022-09-12 | pending | pending | pending | pending |
| 2022-09-13 | pending | pending | pending | pending |
| 2022-09-14 | pending | pending | pending | pending |
| 2022-09-15 | pending | pending | pending | pending |
| 2022-09-16 | pending | pending | pending | pending |
| 2022-09-17 | pending | pending | pending | pending |
| 2022-09-18 | pending | pending | pending | pending |
| 2022-09-19 | pending | pending | pending | pending |
| 2022-09-20 | pending | pending | pending | pending |
| 2022-09-21 | pending | pending | pending | pending |
| 2022-09-22 | pending | pending | pending | pending |
| 2022-09-23 | pending | pending | pending | pending |
| 2022-09-24 | pending | pending | pending | pending |
| 2022-09-25 | pending | pending | pending | pending |
| 2022-09-26 | pending | pending | pending | pending |
| 2022-09-27 (boundary) | pending | pending | pending | pending |
| 2022-09-28 (boundary) | pending | pending | pending | pending |

## LI owner-date coverage

| Owner date | Pairs / triples | Receipt counts match | Lineage query ID or full-scope reference | Members / errors |
|---|---|---|---|---|
| 2022-09-01 | pending | pending | pending | pending |
| 2022-09-02 | pending | pending | pending | pending |
| 2022-09-03 | pending | pending | pending | pending |
| 2022-09-04 | pending | pending | pending | pending |
| 2022-09-05 | pending | pending | pending | pending |
| 2022-09-06 | pending | pending | pending | pending |
| 2022-09-07 | pending | pending | pending | pending |
| 2022-09-08 | pending | pending | pending | pending |
| 2022-09-09 | pending | pending | pending | pending |
| 2022-09-10 | pending | pending | pending | pending |
| 2022-09-11 | pending | pending | pending | pending |
| 2022-09-12 | pending | pending | pending | pending |
| 2022-09-13 | pending | pending | pending | pending |
| 2022-09-14 | pending | pending | pending | pending |
| 2022-09-15 | pending | pending | pending | pending |
| 2022-09-16 | pending | pending | pending | pending |
| 2022-09-17 | pending | pending | pending | pending |
| 2022-09-18 | pending | pending | pending | pending |
| 2022-09-19 | pending | pending | pending | pending |
| 2022-09-20 | pending | pending | pending | pending |
| 2022-09-21 | pending | pending | pending | pending |
| 2022-09-22 | pending | pending | pending | pending |
| 2022-09-23 | pending | pending | pending | pending |
| 2022-09-24 | pending | pending | pending | pending |
| 2022-09-25 | pending | pending | pending | pending |
| 2022-09-26 (boundary) | pending | pending | pending | pending |
| 2022-09-27 (boundary) | pending | pending | pending | pending |

Zero-output dates: write `0 / 0`, verify an explicit zero receipt and empty
published location; zero observed rows alone is insufficient. For chunked
lineage, retain the corresponding zero-member/zero-error query result too.

Final source baselines are all cleaned transactions, not members or sequence
occurrences. Do not apply BQ1's amount cutoff. Last-two-date boundary limitations
stay in the handoff; source/continuation coverage is not proof of unseen activity.

Use [READER_PAUSE_AND_HANDOFF.md](READER_PAUSE_AND_HANDOFF.md) for confirmation
records and the final message to Sonia/Xiang Ying. Reader release remains pending
until explicitly agreed after the applicable acceptance checks finish.
