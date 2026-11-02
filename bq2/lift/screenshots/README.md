# Role 6 screenshot evidence

Original PNGs copied from Xiang Ying's Desktop without editing. Desktop originals were retained.

| File | What it shows |
|---|---|
| [00-personal-console-home.png](00-personal-console-home.png) | Personal-account console home in Singapore. Supplied as `01-cleaned-schema.png`, but does not show schema results. Retained for context only. |
| [02-hi-one-day-check.png](02-hi-one-day-check.png) | 4,465,985 transactions, 1,056 laundering rows, 0 missing-USD rows for the one-day HI query shared in chat. Runtime 754 ms, scanned 7.32 KB. |
| [03-hi-full-baseline.png](03-hi-full-baseline.png) | Full HI: 31,898,218 transactions, 35,230 laundering, 31,862,988 legitimate, 2022-09-01 through 2022-09-28. Runtime 1.384 sec, scanned 91.46 KB. |
| [04-li-full-baseline.png](04-li-full-baseline.png) | Full LI: 31,251,469 transactions, 16,041 laundering, 31,235,428 legitimate, 2022-09-01 through 2022-09-27. Runtime 1.075 sec, scanned 69.79 KB. |
| [05-bq2-input-table-check.png](05-bq2-input-table-check.png) | Only `transactions_clean` returned among the three queried table names. Runtime 460 ms, scanned 0.04 KB. |

Schema results were pasted in chat and match the documented 20 columns. The actual schema-result screenshot is still needed as `01-cleaned-schema.png`. Do not use the console-home image as schema evidence.

The result crops do not display full SQL, account/region or query execution IDs. Query/dataset/date attribution comes from the accompanying conversation. Runtime and scan figures are transcribed as displayed, not independent benchmark reruns. The one-day query checks missing USD only; it does not establish full-dataset USD quality or transaction-ID uniqueness. Missing sequence names do not rule out a different Role 5 schema or permission-dependent visibility.
