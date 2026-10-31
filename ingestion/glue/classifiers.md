# Glue custom classifiers

Both are CSV classifiers: comma delimiter, double-quote, **Has headings**.

## is459-g2t1-trans-csv

Used by `is459-g2t1-raw-crawler` for the IBM transaction files. Fixes the duplicate `Account` column and the spaces in the original headers.

```
timestamp,from_bank,from_account,to_bank,to_account,amount_received,receiving_currency,amount_paid,payment_currency,payment_format,is_laundering
```

## is459-g2t1-fxbtc-rates-csv

Used by `is459-g2t1-fxbtc-rates-crawler` for the FX and BTC files written by the Lambda. Kept in a separate crawler so the transaction classifier is never applied to the rates files.

```
date,currency,currency_code,units_per_usd,source
```
