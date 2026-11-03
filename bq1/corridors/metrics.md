# BQ1 Metrics: Extra Laundering Value & Blindspot Gap

## Formulas
- **Extra Laundering Value** = Total laundering value in the path shared by a bank pair − the value that bank pair could already see directly on its own.
- **Blindspot Gap** = Extra Laundering Value / Total laundering value in the path.
  A gap near 1.0 means almost none of the laundering activity connecting two banks is visible without sharing records. A gap near 0 means the pair would already catch nearly everything on their own.

## How it's computed
1. Direct corridor value: sum of `amount_paid_usd` for cross-bank transactions directly between the two banks, where `is_laundering = 1`.
2. Shared attempt value: each laundering `attempt_id` (a multi-hop scheme from IBM's patterns file, e.g. FAN-OUT, GATHER-SCATTER) often touches more than two banks. Every pair of banks that co-occurs in the same attempt shares that attempt's full value — because only by comparing notes across the whole chain would any single bank pair see the full picture.
3. Extra Laundering Value = shared attempt value − direct value (floored at 0).
4. Blindspot Gap = Extra Laundering Value / shared attempt value.

## Data quality note
A small number of transactions in the IBM synthetic dataset have implausible values (one transaction to $15.9 billion). Checked the real distribution of laundering transaction sizes on `HI-Medium`:

| Percentile | Value (USD) |
|---|---|
| Median (p50) | $6,014 |
| p90 | $17,318 |
| p99 | $17,374,194 |
| p99.9 | $752,637,735 |
| Max | $15,938,012,093 |

Individual transactions of $20,000,000 or more (just above the real p99 ceiling) are excluded from all sums in this analysis — about 1% of laundering rows, all implausible synthetic artifacts, not real financial activity. Without this filter, the top "corridors" are dominated by repeats of the same broken values rather than real findings.

## Worked example (real numbers, HI-Medium, after filtering)
Bank pair **260802 → 151989**:
- Direct laundered value (seen without sharing): $7,758,401.24
- Total laundering value in the shared attempt: $96,840,838.84
- Extra Laundering Value: $96,840,838.84 − $7,758,401.24 = **$89,082,437.60**
- Blindspot Gap: $89,082,437.60 / $96,840,838.84 = **0.92**

Interpretation: if Bank 260802 and Bank 151989 only looked at their own direct transactions, they'd see about 8% of the real laundering value connecting them — sharing records would reveal the other 92%.
