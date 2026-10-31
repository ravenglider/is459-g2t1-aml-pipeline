# Ingestion and orchestration (Role 1)

| Folder | Contents |
|---|---|
| `lambda/` | `is459-g2t1-fetch-fxbtc-rates`: pulls ECB FX rates (via Frankfurter) and Coinbase BTC/USD into the raw bucket |
| `stepfunctions/` | `is459-g2t1-pipeline` state machine definition |
| `glue/` | Custom classifier headers for the two crawlers |

Full resource inventory, schemas and setup notes: [`docs/ingestion-handoff.md`](../docs/ingestion-handoff.md)

## Kaggle download (one-off, run on the EC2 ingest instance)

```bash
DS=ealtman2019/ibm-transactions-for-anti-money-laundering-aml
for f in HI-Medium_Trans.csv LI-Medium_Trans.csv HI-Medium_Patterns.txt HI-Medium_accounts.csv; do
  kaggle datasets download -d $DS -f $f
done
for z in *.zip; do unzip -o "$z" && rm "$z"; done
head -n 500001 HI-Medium_Trans.csv > hi_medium_sample.csv

B=s3://is459-g2t1-aml-bucket
aws s3 cp HI-Medium_Trans.csv     $B/ibm/hi_medium/
aws s3 cp LI-Medium_Trans.csv     $B/ibm/li_medium/
aws s3 cp hi_medium_sample.csv    $B/ibm/sample/
aws s3 cp HI-Medium_accounts.csv  $B/ibm/hi_medium_accounts/
aws s3 cp HI-Medium_Patterns.txt  $B/patterns/
```
