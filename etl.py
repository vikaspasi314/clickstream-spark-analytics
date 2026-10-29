"""Stage 1: ETL - read raw CSV, clean, enrich, write partitioned Parquet."""
from pyspark.sql import functions as F, types as T
from spark_session import get_spark
from config import RAW, CLEAN

SCHEMA = T.StructType([
    T.StructField("event_time", T.TimestampType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("session_hint", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("price", T.DoubleType()),
])

def run(spark):
    raw = spark.read.csv(RAW, header=True, schema=SCHEMA)
    total = raw.count()
    clean = (raw.drop("session_hint")
             .filter(F.col("user_id").isNotNull() & (F.col("user_id") != ""))
             .dropDuplicates()
             .withColumn("date", F.to_date("event_time"))
             .withColumn("hour", F.hour("event_time")))
    clean.write.mode("overwrite").partitionBy("date").parquet(CLEAN)
    kept = spark.read.parquet(CLEAN).count()
    print(f"[ETL] raw={total:,} clean={kept:,} dropped={total-kept:,}")

if __name__ == "__main__":
    run(get_spark("ETL"))
