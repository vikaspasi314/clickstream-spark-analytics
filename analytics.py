"""Stage 2: Batch analytics with DataFrames + Spark SQL."""
from pyspark.sql import functions as F, Window
from spark_session import get_spark
from config import CLEAN, OUT

def run(spark):
    df = spark.read.parquet(CLEAN).cache()
    df.createOrReplaceTempView("events")
    OUT.mkdir(exist_ok=True)

    # 1. Revenue by category
    rev = (df.filter("event_type='purchase'").groupBy("category")
           .agg(F.round(F.sum("price"), 2).alias("revenue"), F.count("*").alias("orders"))
           .orderBy(F.desc("revenue")))
    rev.show()

    # 2. Top 10 products by purchases (Spark SQL)
    top = spark.sql("""SELECT product_id, category, COUNT(*) purchases
                       FROM events WHERE event_type='purchase'
                       GROUP BY product_id, category ORDER BY purchases DESC LIMIT 10""")
    top.show()

    # 3. Traffic by hour of day
    hourly = df.groupBy("hour").count().orderBy("hour")

    # 4. Conversion funnel view -> cart -> purchase
    funnel = df.groupBy("event_type").agg(F.countDistinct("user_id").alias("users"))
    funnel.show()

    # 5. Sessionization: new session after 30 min inactivity
    w = Window.partitionBy("user_id").orderBy("event_time")
    s = (df.withColumn("prev", F.lag("event_time").over(w))
           .withColumn("gap", F.unix_timestamp("event_time") - F.unix_timestamp("prev"))
           .withColumn("new_sess", (F.col("gap").isNull() | (F.col("gap") > 1800)).cast("int"))
           .withColumn("session_id", F.sum("new_sess").over(w)))
    sess = s.groupBy("user_id", "session_id").agg(
        F.count("*").alias("events"),
        (F.max(F.unix_timestamp("event_time")) - F.min(F.unix_timestamp("event_time"))).alias("duration_s"))
    sess.agg(F.count("*").alias("sessions"), F.round(F.avg("events"), 2).alias("avg_events"),
             F.round(F.avg("duration_s"), 1).alias("avg_duration_s")).show()

    for name, d in [("revenue_by_category", rev), ("top_products", top),
                    ("hourly_traffic", hourly), ("funnel", funnel)]:
        d.toPandas().to_csv(OUT / f"{name}.csv", index=False)
    print(f"[Analytics] CSV results saved in {OUT}")

if __name__ == "__main__":
    run(get_spark("Analytics"))
