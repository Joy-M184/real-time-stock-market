import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# No dotenv needed — Docker injects env vars directly
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("StockMarketAnalytics") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── PostgreSQL connection ─────────────────────────────────────────────────────
pg_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
pg_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ── Read raw stock data ───────────────────────────────────────────────────────
print("📖 Reading stock data from PostgreSQL...")
df = spark.read.jdbc(url=pg_url, table="stock_data", properties=pg_properties)
df = df.withColumn("close", F.col("close").cast("double")) \
       .withColumn("open", F.col("open").cast("double")) \
       .withColumn("high", F.col("high").cast("double")) \
       .withColumn("low", F.col("low").cast("double")) \
       .withColumn("volume", F.col("volume").cast("long"))

# ── Window for moving average ─────────────────────────────────────────────────
window = Window.partitionBy("symbol").orderBy("date").rowsBetween(-4, 0)

# ── Calculations ──────────────────────────────────────────────────────────────
enriched = df \
    .withColumn("moving_avg_5", F.round(F.avg("close").over(window), 4)) \
    .withColumn("vwap", F.round(
        F.sum(F.col("close") * F.col("volume")).over(window) /
        F.sum("volume").over(window), 4)) \
    .withColumn("price_change_pct", F.round(
        (F.col("close") - F.col("open")) / F.col("open") * 100, 4))

# ── Write enriched data ───────────────────────────────────────────────────────
print("💾 Writing enriched data to PostgreSQL...")
enriched.select(
    "date", "symbol", "open", "high", "low", "close", "volume",
    "moving_avg_5", "vwap", "price_change_pct"
).write.jdbc(
    url=pg_url,
    table="stock_data_enriched",
    mode="overwrite",
    properties=pg_properties
)

print("✅ Spark job complete!")
spark.stop()