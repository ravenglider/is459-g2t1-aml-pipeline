# transactions_clean: schema

- **Glue database / table:** `is459_g2t1_aml.transactions_clean`
- **S3 path:** `s3://is459-g2t1-aml-processed-bucket/transactions_clean/`
- **Format:** Parquet (snappy), partitioned by `dataset` then `txn_date`
- **Partitions:** `dataset` = `sample` (first 500,000 rows of HI-Medium), `HI-Medium`, `LI-Medium`

**Always filter on `dataset`**, otherwise you mix the sample, HI and LI rows.

| Column | Type | Meaning |
|---|---|---|
| txn_id | bigint | Unique ID per transaction (hash of its fields) |
| txn_ts | timestamp | Date and time of the transfer (from "2022/09/01 00:20") |
| from_bank | bigint | Sending bank ID |
| from_account | string | Sending account |
| to_bank | bigint | Receiving bank ID |
| to_account | string | Receiving account |
| amount_paid | double | Amount sent, in payment_currency |
| payment_currency | string | e.g. "US Dollar", "Bitcoin" |
| amount_paid_usd | double | amount_paid in USD |
| amount_received | double | Amount received, in receiving_currency |
| receiving_currency | string | Currency name |
| amount_received_usd | double | amount_received in USD |
| payment_format | string | Cash, Wire, ACH, Cheque, Credit Card, Bitcoin, Reinvestment |
| cross_bank | int | 1 if from_bank is different from to_bank, else 0 |
| is_laundering | int | 1 = laundering, 0 = legitimate |
| attempt_id | int | Laundering attempt number from IBM's patterns file. Empty for normal rows and for LI-Medium |
| pattern_type | string | Shape of that attempt: FAN-OUT, FAN-IN, CYCLE, GATHER-SCATTER, SCATTER-GATHER, STACK, BIPARTITE, RANDOM |
| usd_missing | int | 1 if the amount could not be converted to USD (flagged, never guessed) |
| dataset | string (partition) | sample, HI-Medium or LI-Medium |
| txn_date | string (partition) | "YYYY-MM-DD". Filter on it to read only the days you need |

Use the `*_usd` columns whenever you add up money.

Example:
```sql
SELECT payment_format, COUNT(*) AS n, SUM(amount_paid_usd) AS usd
FROM is459_g2t1_aml.transactions_clean
WHERE dataset = 'HI-Medium' AND is_laundering = 1
GROUP BY payment_format;
```
