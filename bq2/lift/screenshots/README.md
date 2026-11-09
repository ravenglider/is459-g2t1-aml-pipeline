# Role 6 screenshot evidence

Original PNGs copied from Xiang Ying's Desktop without editing. Desktop originals were retained.

| File | What it shows |
|---|---|
| [01-cleaned-schema.png](01-cleaned-schema.png) | Corrected schema screenshot: all 20 columns and types, matching the documented cleaned schema. |
| [02-hi-one-day-check.png](02-hi-one-day-check.png) | 4,465,985 transactions, 1,056 laundering rows, 0 missing-USD rows for the one-day HI query shared in chat. Runtime 754 ms, scanned 7.32 KB. |
| [03-hi-full-baseline.png](03-hi-full-baseline.png) | Full HI: 31,898,218 transactions, 35,230 laundering, 31,862,988 legitimate, 2022-09-01 through 2022-09-28. Runtime 1.384 sec, scanned 91.46 KB. |
| [04-li-full-baseline.png](04-li-full-baseline.png) | Full LI: 31,251,469 transactions, 16,041 laundering, 31,235,428 legitimate, 2022-09-01 through 2022-09-27. Runtime 1.075 sec, scanned 69.79 KB. |
| [05-bq2-input-table-check.png](05-bq2-input-table-check.png) | Only `transactions_clean` returned among the three queried table names. Runtime 460 ms, scanned 0.04 KB. |
| [06-dev-sequence-counts.png](06-dev-sequence-counts.png) | Own console count check: 184,449 pairs, 33,931 triples, total 218,380. Runtime 837 ms, scanned 32.72 KB, queue 100 ms; result reuse toggle off. |
| [07-dev-sequence-schema.png](07-dev-sequence-schema.png) | Own schema result: all 21 column names, types and ordinals match Role 5 v1, including bigint IDs, aligned arrays and both string partitions. Metrics/query ID not visible. |
| [08-dev-source-validation.png](08-dev-source-validation.png) | All cleaned sample rows: 500,000 transactions and unique IDs, 35 laundering labels, 0 invalid labels, 0 null IDs. Runtime 1.191 sec, scanned 3.82 MB, queue 99 ms; query ID not supplied. |
| [09-dev-pattern-frequency.png](09-dev-pattern-frequency.png) | Frequency query completed: 236 result rows; first six pair patterns visible. Runtime 691 ms, scanned 225.55 KB, queue 108 ms. Full output and query ID not supplied. |
| [09a-dev-output-table-check.png](09a-dev-output-table-check.png) | Destination-table lookup completed with 0 rows for the three proposed development table names. Runtime 412 ms, queue 104 ms; scanned displayed as a dash. Does not establish S3 prefix emptiness. |
| [10a-dev-lift-created.png](10a-dev-lift-created.png) | Development lift CTAS completed successfully. Runtime 1.454 sec, queue 108 ms, scanned 454.24 KB. SQL/table attribution from the accompanying walkthrough; query ID not supplied. |
| [11a-dev-tradeoff-created.png](11a-dev-tradeoff-created.png) | Development trade-off CTAS completed successfully. Runtime 2.369 sec, queue 99 ms, scanned 5.71 MB. Query ID not supplied; statement attribution from conversation. |
| [13a-dev-sankey-created.png](13a-dev-sankey-created.png) | Development Sankey CTAS completed. Runtime 961 ms, queue 80 ms; data-scanned unit cropped. Query ID not supplied; statement attribution from conversation. |

The corrected schema screenshot is saved as `01-cleaned-schema.png` and agrees with the pasted 20-column result.

The Desktop files for 06/07 used colon-separated filenames; repository copies use normal filenames inside this folder. Originals were retained.

The result crops do not display full SQL, account/region or query execution IDs. Query/dataset/date attribution comes from the accompanying conversation. Runtime and scan figures are transcribed as displayed, not independent benchmark reruns. The one-day query checks missing USD only; it does not establish full-dataset USD quality or transaction-ID uniqueness. Missing sequence names do not rule out a different Role 5 schema or permission-dependent visibility.


## Final-table schema evidence — 9 November 2026

[23-final-sequence-schema.png](23-final-sequence-schema.png): 21 columns in the agreed order, with compatible logical types. Notable inferred widths are rule_version varchar(2) and dataset varchar(9); other expected integer, bigint and array types are present. Runtime 634 ms, queue 99 ms, scanned 1.66 KB. Partition registration is not established by the column list.

[24-final-sequence-partitions.png](24-final-sequence-partitions.png): successful physical-table partition listing for bq2_sequences_a (published bq2_sequences is a view). HI/LI date entries are visible. Runtime 958 ms, queue 174 ms; scanned shown as a dash. Exact coverage summary and row-level acceptance are separate checks.
