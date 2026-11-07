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

The corrected schema screenshot is saved as `01-cleaned-schema.png` and agrees with the pasted 20-column result.

The Desktop files for 06/07 used colon-separated filenames; repository copies use normal filenames inside this folder. Originals were retained.

The result crops do not display full SQL, account/region or query execution IDs. Query/dataset/date attribution comes from the accompanying conversation. Runtime and scan figures are transcribed as displayed, not independent benchmark reruns. The one-day query checks missing USD only; it does not establish full-dataset USD quality or transaction-ID uniqueness. Missing sequence names do not rule out a different Role 5 schema or permission-dependent visibility.
