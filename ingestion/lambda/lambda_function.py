"""
IS459 G2T1 AML project: external rates ingestion Lambda.

Fetches daily FX rates (ECB reference rates via the Frankfurter API) and daily
BTC/USD prices (Coinbase Exchange public API) for the dataset's date range and
writes them as CSV to the raw S3 bucket:

    s3://<BUCKET>/fx/fx_rates_daily.csv
    s3://<BUCKET>/btc/btc_usd_daily.csv

Both files use the same columns so Role 2 can union them if convenient:
    date, currency, currency_code, units_per_usd, source

units_per_usd = how many units of that currency equal 1 USD.
To convert an amount to USD: amount_usd = amount / units_per_usd

Notes:
- ECB publishes on business days only; weekends/holidays are forward-filled
  from the last published day so every calendar date has a rate.
- Saudi Riyal is not published by ECB; it is pegged at 3.75 SAR per USD.
- Russian Ruble is not published by ECB after March 2022, so it is omitted.
  Derive it from the dataset (amount_paid vs amount_received) in the ETL.
- Uses only the Python standard library (no layers or packages needed).

Environment variables (all optional):
    BUCKET      default: is459-g2t1-aml-bucket
    START_DATE  default: 2022-08-25  (a few days early so forward-fill has a seed)
    END_DATE    default: 2022-09-30
"""

import csv
import io
import json
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("BUCKET", "is459-g2t1-aml-bucket")
START_DATE = os.environ.get("START_DATE", "2022-08-25")
END_DATE = os.environ.get("END_DATE", "2022-09-30")

# Dataset currency name -> ISO code available from ECB/Frankfurter
FX_CURRENCIES = {
    "Euro": "EUR",
    "Yuan": "CNY",
    "Yen": "JPY",
    "Rupee": "INR",
    "UK Pound": "GBP",
    "Canadian Dollar": "CAD",
    "Australian Dollar": "AUD",
    "Swiss Franc": "CHF",
    "Mexican Peso": "MXN",
    "Brazil Real": "BRL",
    "Shekel": "ILS",
}
FIXED_PEGS = {"Saudi Riyal": ("SAR", 3.75)}

HEADERS = {"User-Agent": "is459-g2t1-aml-ingest/1.0", "Accept": "application/json"}
COLUMNS = ["date", "currency", "currency_code", "units_per_usd", "source"]

s3 = boto3.client("s3")


def http_get_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def date_range(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def fetch_fx(start, end):
    """Daily units-per-USD for each currency, forward-filled over all calendar days."""
    codes = ",".join(FX_CURRENCIES.values())
    url = (
        f"https://api.frankfurter.app/{start.isoformat()}..{end.isoformat()}"
        f"?from=USD&to={codes}"
    )
    data = http_get_json(url)
    published = data.get("rates", {})  # {"2022-09-01": {"EUR": 1.0, ...}, ...}

    rows, last = [], None
    for d in date_range(start, end):
        key = d.isoformat()
        if key in published:
            last = published[key]
        if last is None:
            continue  # no seed yet; START_DATE is set early to avoid gaps
        for name, code in FX_CURRENCIES.items():
            if code in last:
                rows.append([key, name, code, last[code], "ECB via Frankfurter"])
        rows.append([key, "US Dollar", "USD", 1.0, "base"])
        for name, (code, rate) in FIXED_PEGS.items():
            rows.append([key, name, code, rate, "fixed peg"])
    return rows


def fetch_btc(start, end):
    """Daily BTC close in USD from Coinbase, converted to BTC units per USD."""
    url = (
        "https://api.exchange.coinbase.com/products/BTC-USD/candles"
        f"?granularity=86400"
        f"&start={start.isoformat()}T00:00:00Z"
        f"&end={end.isoformat()}T23:59:59Z"
    )
    candles = http_get_json(url)  # [[time, low, high, open, close, volume], ...]
    rows = []
    for c in sorted(candles, key=lambda x: x[0]):
        day = datetime.fromtimestamp(c[0], tz=timezone.utc).date().isoformat()
        close_usd = float(c[4])
        # Fixed-point format: avoids scientific notation (e.g. 5.1e-05), which
        # makes the Glue crawler type the column as string instead of double.
        rows.append([day, "Bitcoin", "BTC", f"{1.0 / close_usd:.12f}", "Coinbase BTC-USD daily close"])
    return rows


def write_csv(rows, key):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(COLUMNS)
    writer.writerows(rows)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def lambda_handler(event, context):
    event = event or {}
    start = date.fromisoformat(event.get("start_date", START_DATE))
    end = date.fromisoformat(event.get("end_date", END_DATE))

    fx_rows = fetch_fx(start, end)
    btc_rows = fetch_btc(start, end)

    write_csv(fx_rows, "fx/fx_rates_daily.csv")
    write_csv(btc_rows, "btc/btc_usd_daily.csv")

    result = {
        "bucket": BUCKET,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "fx_rows": len(fx_rows),
        "btc_rows": len(btc_rows),
        "fx_key": "fx/fx_rates_daily.csv",
        "btc_key": "btc/btc_usd_daily.csv",
    }
    print(json.dumps(result))
    return result