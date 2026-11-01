"""
Role 2: Glue Spark ETL job  ->  is459_g2t1_aml.transactions_clean

What it does, in order:
  1. Reads one raw IBM transactions file (sample, HI-Medium or LI-Medium).
  2. Cleans it: real timestamps, proper number types, exact duplicates removed.
  3. Links laundering rows to their attempt and shape from the patterns file
     (HI-Medium and the sample only; IBM's LI patterns file was not loaded).
  4. Converts every amount to USD with Sonia's daily rates (raw fx/ and btc/ files).
     Ruble is not in those files, so its rate is worked out from the dataset itself.
     Anything that still cannot be converted is flagged (usd_missing = 1), never guessed.
  5. Runs data quality checks and stops with an error if the data looks wrong.
  6. Writes Parquet, split by dataset and date, and registers/updates the table
     in the Glue Data Catalog so Athena and the other roles can query it.

Glue job parameter (Job details > Advanced properties > Job parameters):
  --DATASET   sample | HI-Medium | LI-Medium

Re-running a dataset replaces only that dataset's folder; other datasets are kept.
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ------------------------------------------------------------------ settings
RAW = "s3://is459-g2t1-aml-bucket"
OUTPUT_PATH = "s3://is459-g2t1-aml-processed-bucket/transactions_clean/"
DATABASE = "is459_g2t1_aml"
TABLE = "transactions_clean"

SOURCES = {
    "sample": f"{RAW}/ibm/sample/hi_medium_sample.csv",
    "HI-Medium": f"{RAW}/ibm/hi_medium/HI-Medium_Trans.csv",
    "LI-Medium": f"{RAW}/ibm/li_medium/LI-Medium_Trans.csv",
}
RATES_PATHS = [f"{RAW}/fx/fx_rates_daily.csv", f"{RAW}/btc/btc_usd_daily.csv"]
PATTERNS_PATH = f"{RAW}/patterns/HI-Medium_Patterns.txt"
DATASETS_WITH_PATTERNS = {"sample", "HI-Medium"}

MAX_USD_MISSING_SHARE = 0.001   # fail the run if more than 0.1% of rows cannot be converted
MIN_RUBLE_PAIRS_PER_DAY = 5      # fewer pairs than this on a day -> use the overall median

# The original Kaggle header has "Account" twice, so columns are renamed by position.
RAW_COLS = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account",
            "amount_received", "receiving_currency", "amount_paid",
            "payment_currency", "payment_format", "is_laundering"]
PATTERN_KEYS = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account",
                "amount_paid", "payment_format"]

# ------------------------------------------------------------------ start-up
try:  # running inside AWS Glue
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATASET"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
except ImportError:  # local test run: same logic, local folders instead of S3
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--DATASET", required=True)
    p.add_argument("--LOCAL_RAW", required=True)
    p.add_argument("--LOCAL_OUT", required=True)
    args = vars(p.parse_args())
    glue = None
    spark = SparkSession.builder.appName("etl-local-test").getOrCreate()
    RAW, OUTPUT_PATH = args["LOCAL_RAW"], args["LOCAL_OUT"]
    SOURCES = {k: v.replace("s3://is459-g2t1-aml-bucket", RAW) for k, v in SOURCES.items()}
    RATES_PATHS = [x.replace("s3://is459-g2t1-aml-bucket", RAW) for x in RATES_PATHS]
    PATTERNS_PATH = PATTERNS_PATH.replace("s3://is459-g2t1-aml-bucket", RAW)

DATASET = args["DATASET"]
if DATASET not in SOURCES:
    raise ValueError(f"--DATASET must be one of {list(SOURCES)}, got {DATASET!r}")
print(f"ETL | dataset={DATASET} source={SOURCES[DATASET]}")

# ------------------------------------------------------------------ 1. read
raw = spark.read.option("header", True).csv(SOURCES[DATASET]).toDF(*RAW_COLS)
rows_raw = raw.count()

# ------------------------------------------------------------------ 2. clean
txn = (raw
       .withColumn("txn_ts", F.to_timestamp("ts_raw", "yyyy/MM/dd HH:mm"))
       .withColumn("txn_date", F.date_format("txn_ts", "yyyy-MM-dd"))
       .withColumn("from_bank", F.col("from_bank").cast("bigint"))
       .withColumn("to_bank", F.col("to_bank").cast("bigint"))
       .withColumn("amount_received", F.col("amount_received").cast("double"))
       .withColumn("amount_paid", F.col("amount_paid").cast("double"))
       .withColumn("is_laundering", F.col("is_laundering").cast("int"))
       .dropDuplicates())

# ------------------------------------------------------------------ 3. patterns file
# Format: "BEGIN LAUNDERING ATTEMPT - FAN-OUT: ..." / transaction lines / "END LAUNDERING ATTEMPT - FAN-OUT"
if DATASET in DATASETS_WITH_PATTERNS:
    attempt, shape, prow = 0, None, []
    for (line,) in spark.read.text(PATTERNS_PATH).collect():  # small file (~2 MB)
        s = line.strip()
        if s.startswith("BEGIN LAUNDERING ATTEMPT"):
            attempt += 1
            shape = s.split(" - ", 1)[1].split(":")[0].strip()
        elif s.startswith("END LAUNDERING ATTEMPT"):
            shape = None
        elif shape and s:
            p = s.split(",")
            if len(p) >= 11:
                prow.append((attempt, shape, p[0], int(p[1]), p[2], int(p[3]), p[4],
                             float(p[7]), p[9]))
    patterns = spark.createDataFrame(
        prow, "attempt_id int, pattern_type string, ts_raw string, from_bank bigint, "
              "from_account string, to_bank bigint, to_account string, "
              "amount_paid double, payment_format string")
    # If a transaction is listed in two attempts, keep the first one so rows are not doubled.
    patterns = (patterns.groupBy(*PATTERN_KEYS)
                .agg(F.min(F.struct("attempt_id", "pattern_type")).alias("m"))
                .select(*PATTERN_KEYS, "m.attempt_id", "m.pattern_type"))
    print(f"ETL | patterns file: {attempt} attempts, {len(prow)} transaction lines")
    txn = txn.join(F.broadcast(patterns), PATTERN_KEYS, "left")
else:
    txn = (txn.withColumn("attempt_id", F.lit(None).cast("int"))
              .withColumn("pattern_type", F.lit(None).cast("string")))
txn = txn.drop("ts_raw").cache()

# ------------------------------------------------------------------ 4. USD conversion
rates = (spark.read.option("header", True).csv(RATES_PATHS)
         .select(F.col("date").alias("txn_date"), "currency",
                 F.col("units_per_usd").cast("double").alias("units_per_usd"))
         .where(F.col("units_per_usd").isNotNull() & (F.col("units_per_usd") > 0)))

# Ruble: no public rate after March 2022. Some transfers are paid in one currency and
# received in another, so both amounts are the same money: that ratio is an exchange rate.
pay_rub = (txn.where((F.col("payment_currency") == "Ruble") & (F.col("receiving_currency") != "Ruble"))
           .join(rates.withColumnRenamed("currency", "receiving_currency"), ["txn_date", "receiving_currency"])
           .select("txn_date", (F.col("amount_paid") / (F.col("amount_received") / F.col("units_per_usd"))).alias("rub_per_usd")))
rec_rub = (txn.where((F.col("receiving_currency") == "Ruble") & (F.col("payment_currency") != "Ruble"))
           .join(rates.withColumnRenamed("currency", "payment_currency"), ["txn_date", "payment_currency"])
           .select("txn_date", (F.col("amount_received") / (F.col("amount_paid") / F.col("units_per_usd"))).alias("rub_per_usd")))
rub_pairs = pay_rub.unionByName(rec_rub).where(F.col("rub_per_usd") > 0).cache()

overall = rub_pairs.agg(F.percentile_approx("rub_per_usd", 0.5).alias("m"), F.count("*").alias("n")).first()
if overall["m"] is not None:
    daily = (rub_pairs.groupBy("txn_date")
             .agg(F.percentile_approx("rub_per_usd", 0.5).alias("daily_m"), F.count("*").alias("n")))
    ruble_rates = (txn.select("txn_date").distinct()
                   .join(daily, "txn_date", "left")
                   .select("txn_date", F.lit("Ruble").alias("currency"),
                           F.when(F.col("n") >= MIN_RUBLE_PAIRS_PER_DAY, F.col("daily_m"))
                            .otherwise(F.lit(overall["m"])).alias("units_per_usd")))
    rates = rates.unionByName(ruble_rates)
    print(f"ETL | Ruble implied rate: median {overall['m']:.4f} RUB per USD from {overall['n']} cross-currency transfers")
else:
    print("ETL | Ruble implied rate: no cross-currency Ruble transfers found; Ruble rows will be flagged usd_missing")

pay_rate = rates.select("txn_date", F.col("currency").alias("payment_currency"), F.col("units_per_usd").alias("pay_units"))
rec_rate = rates.select("txn_date", F.col("currency").alias("receiving_currency"), F.col("units_per_usd").alias("rec_units"))

clean = (txn.join(F.broadcast(pay_rate), ["txn_date", "payment_currency"], "left")
            .join(F.broadcast(rec_rate), ["txn_date", "receiving_currency"], "left")
            .withColumn("amount_paid_usd", F.round(F.col("amount_paid") / F.col("pay_units"), 2))
            .withColumn("amount_received_usd", F.round(F.col("amount_received") / F.col("rec_units"), 2))
            .withColumn("usd_missing", (F.col("amount_paid_usd").isNull() | F.col("amount_received_usd").isNull()).cast("int"))
            .withColumn("cross_bank", (F.col("from_bank") != F.col("to_bank")).cast("int"))
            .withColumn("txn_id", F.xxhash64("txn_ts", "from_bank", "from_account", "to_bank", "to_account",
                                             "amount_paid", "payment_currency", "amount_received",
                                             "receiving_currency", "payment_format"))
            .withColumn("dataset", F.lit(DATASET))
            .select("txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account",
                    "amount_paid", "payment_currency", "amount_paid_usd",
                    "amount_received", "receiving_currency", "amount_received_usd",
                    "payment_format", "cross_bank", "is_laundering",
                    "attempt_id", "pattern_type", "usd_missing",
                    "dataset", "txn_date")
            .cache())

# ------------------------------------------------------------------ 5. data quality checks
dq = clean.agg(
    F.count("*").alias("rows_clean"),
    F.sum(F.col("txn_ts").isNull().cast("int")).alias("bad_timestamps"),
    F.sum("usd_missing").alias("usd_missing"),
    F.sum((F.col("amount_paid") <= 0).cast("int")).alias("non_positive_amounts"),
    F.sum("is_laundering").alias("laundering_rows"),
    F.sum(F.col("attempt_id").isNotNull().cast("int")).alias("rows_linked_to_attempt"),
    F.min("txn_date").alias("first_date"),
    F.max("txn_date").alias("last_date"),
).first().asDict()
dq["rows_raw"] = rows_raw
dq["duplicates_removed"] = rows_raw - dq["rows_clean"]
print("DQ | " + " | ".join(f"{k}={v}" for k, v in dq.items()))

missing = clean.where("usd_missing = 1").groupBy("payment_currency", "receiving_currency").count().collect()
if missing:
    print("DQ | rows with no USD rate by currency pair:", [tuple(r) for r in missing])

if dq["bad_timestamps"] > 0:
    raise RuntimeError(f"DQ FAILED: {dq['bad_timestamps']} timestamps could not be read")
if dq["usd_missing"] > MAX_USD_MISSING_SHARE * dq["rows_clean"]:
    raise RuntimeError(f"DQ FAILED: {dq['usd_missing']} rows could not be converted to USD")

# ------------------------------------------------------------------ 6. write + register table
out = clean.repartition("txn_date")  # about one file per day
if glue:
    # Remove this dataset's previous output first, so a re-run replaces it instead of doubling it.
    glue.purge_s3_path(f"{OUTPUT_PATH}dataset={DATASET}/", {"retentionPeriod": 0})
    sink = glue.getSink(connection_type="s3", path=OUTPUT_PATH, enableUpdateCatalog=True,
                        updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["dataset", "txn_date"])
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TABLE)
    sink.writeFrame(DynamicFrame.fromDF(out, glue, "transactions_clean"))
else:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out.write.mode("overwrite").partitionBy("dataset", "txn_date").parquet(OUTPUT_PATH)

print(f"ETL | done: wrote {dq['rows_clean']} rows to {OUTPUT_PATH}dataset={DATASET}/ and table {DATABASE}.{TABLE}")
