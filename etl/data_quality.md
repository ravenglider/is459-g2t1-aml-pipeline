# Data quality

Every run prints a line starting with `DQ |` in the job's output log (Glue job > Runs > Output logs). The job **fails on purpose** if any timestamp cannot be read, or if more than 0.1% of rows cannot be converted to USD, so bad data never reaches the analysis.

## Results (from the `DQ |` log line of each run, 1 Nov 2026)

| Check | sample | HI-Medium | LI-Medium |
|---|---|---|---|
| Rows read (rows_raw) | 500,000 | 31,898,238 | 31,251,483 |
| Rows after cleaning (rows_clean) | 500,000 | 31,898,218 | 31,251,469 |
| Exact duplicates removed | 0 | 20 | 14 |
| Timestamps that could not be read | 0 | 0 | 0 |
| Rows with no USD rate (usd_missing) | 0 | 0 | 0 |
| Amounts of zero or less | 0 | 0 | 0 |
| Laundering rows | 35 | 35,230 (1 in 905) | 16,041 (1 in 1,948) |
| Rows linked to a laundering attempt | 18 | 22,743 (65%) | n/a (no LI patterns file used) |
| Date range | 2022-09-01 to 09-06 | 2022-09-01 to 09-28 | 2022-09-01 to 09-27 |

HI-Medium's 35,230 laundering rows match the figure IBM publishes for this dataset. An Athena query across all datasets returned the same counts as the logs (scanning only 283.61 KB).

## How the cleaning works
1. **Column names:** the original file has "Account" twice, so all 11 columns are renamed by position (from_account, to_account, ...).
2. **Types:** timestamp text "2022/09/01 00:20" becomes a real timestamp; banks become whole numbers; amounts become decimals.
3. **Duplicates:** rows identical in every column are removed.
4. **USD:** `amount_usd = amount / units_per_usd`, matched on currency name and date, using Sonia's daily rates (ECB via Frankfurter for 11 currencies, US Dollar = 1, Saudi Riyal fixed at 3.75, Bitcoin from Coinbase; weekends carry the last rate).
5. **Patterns file:** each transaction line inside a "BEGIN/END LAUNDERING ATTEMPT" block is matched to the transactions on time, banks, accounts, amount and payment format. Bank IDs in that file have leading zeros (e.g. 00952), so both sides are compared as numbers. If a transaction appears in two attempts, the first is kept so rows are not doubled.

## Ruble handling
ECB stopped publishing the Ruble in March 2022, so it is not in the rates file. The rate is derived from the dataset:
- Some transfers are paid in Ruble and received in another currency (or the reverse). Both sides are the same money, so `Ruble amount / (other amount / other currency's units_per_usd)` gives Rubles per USD.
- The job takes the **median** of these per day (the median ignores odd outliers). Days with fewer than 5 such transfers use the median across all days.
- Result (from the `ETL | Ruble implied rate` log line): **77.8040 RUB per USD**, from 17 transfers in the sample and 18,510 transfers in HI-Medium. The same value in both shows the IBM simulator uses one fixed Ruble rate, so the derived rate is consistent with the data.
- Anything still not convertible is flagged `usd_missing = 1`, never guessed.

## Tested before AWS
The script was tested locally on 15 hand-made rows in the exact raw formats. It removed the duplicate, converted Yen, Euro (on a Sunday), Bitcoin, Riyal and Ruble correctly (Ruble 60 per USD from 5 cross-currency rows), linked 3 laundering rows to their attempts (FAN-OUT, CYCLE) despite the leading zeros, and stopped with "DQ FAILED" when a currency had no rate.

## Screenshots
All in [screenshots/](screenshots/):

| File | Shows |
|---|---|
| 01-raw-sample.png | Raw sample rows in Athena before cleaning |
| 02-raw-count.png | Raw sample row count (500,000) |
| 03-create-script.png | Uploading the script to Glue |
| 04-job-details.png | Job settings (Glue 5.0, G.1X, 2 workers, Flex) |
| 05-job-parameters.png | `--DATASET` parameter |
| 06-run-succeeded.png | Sample run succeeded (2 m 18 s) |
| 07-dq-log.png | Sample DQ line |
| 07b-ruble-log.png | Sample Ruble rate line |
| 08-s3-date-folders.png | Output split into date folders in S3 |
| 09a-athena-counts.png | Athena check of the clean sample |
| 09b-athena-clean-rows.png | Clean rows with USD, attempt_id, pattern_type |
| 10-run-hi-medium.png | HI-Medium run succeeded (17 m 55 s) |
| 10b-dq-hi-medium.png | HI-Medium DQ line |
| 10c-ruble-hi-medium.png | HI-Medium Ruble rate line |
| 11-run-li-medium.png | LI-Medium run succeeded (9 m 40 s) |
| 11b-dq-li-medium.png | LI-Medium DQ line |
| 12-athena-all-datasets.png | Athena counts for all datasets |
| 13a-csv-scanned.png | Raw CSV query: data scanned |
| 13b-parquet-scanned.png | Parquet query: data scanned |
| 13c-parquet-one-day.png | Parquet one-day query: data scanned |
| 14a-size-csv.png | Raw CSV size in S3 |
| 14b-size-parquet.png | Parquet size in S3 |
