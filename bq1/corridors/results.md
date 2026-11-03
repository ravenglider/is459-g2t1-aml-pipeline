# BQ1 Results: Top Priority Corridors

Ranked by Extra Laundering Value, from `bq1_corridors_ranked` (HI-Medium dataset, outlier transactions ≥$20M excluded — see metrics.md).

| Rank | From Bank | To Bank | Direct Laundered (USD) | Total in Shared Path (USD) | Extra Laundering Value (USD) | Blindspot Gap |
|---|---|---|---|---|---|---|
| 1 | 260802 | 264284 | $0 | $97,006,377.05 | $97,006,377.05 | 1.00 |
| 2 | 264284 | 260802 | $0 | $97,006,377.05 | $97,006,377.05 | 1.00 |
| 3 | 26967 | 28219 | $0 | $96,840,838.84 | $96,840,838.84 | 1.00 |
| 4 | 28219 | 26967 | $0 | $96,840,838.84 | $96,840,838.84 | 1.00 |
| 5 | 18366 | 22602 | $0 | $96,840,838.84 | $96,840,838.84 | 1.00 |
| 6 | 22602 | 18366 | $0 | $96,840,838.84 | $96,840,838.84 | 1.00 |
| 7 | 121546 | 22602 | $0 | $96,840,838.84 | $96,840,838.84 | 1.00 |
| 8 | 260802 | 151989 | $7,758,401.24 | $96,840,838.84 | $89,082,437.60 | 0.92 |
| 9 | 26967 | 22602 | $9,255,317.22 | $97,006,099.28 | $87,750,782.06 | 0.90 |
| 10 | 1125022 | 121546 | $9,104,976.10 | $96,840,838.84 | $87,735,862.74 | 0.91 |

## Key finding
Several of the top pairs (rows 3–7, 10) share the same total shared-path value — that's because they're all part of the same single multi-bank laundering attempt (likely a FAN-OUT or GATHER-SCATTER pattern touching many banks at once). This is exactly the blind spot this metric is built to catch: no single bank pair in that chain can see the attempt's full value on their own, but every one of them is sitting on a Blindspot Gap near 1.0 — meaning almost all of that laundering activity is invisible unless they share records with each other.
