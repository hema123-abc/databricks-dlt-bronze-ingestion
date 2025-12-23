import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------- Orders ----------
@dlt.table(
    name="orders_bronze",
    comment="Bronze table for orders data"
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/raw/transactions/_schemas/orders")
        .load("/Volumes/raw/transactions/landing/orders")
        .withColumn("ingest_ts", current_timestamp())
    )


# ---------- Payments ----------
@dlt.table(
    name="payments_bronze",
    comment="Bronze table for payments data"
)
def payments_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/raw/transactions/_schemas/payments")
        .load("/Volumes/raw/transactions/landing/payments")
        .withColumn("ingest_ts", current_timestamp())
    )
