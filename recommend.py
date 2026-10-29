"""Stage 3: ML - product recommendations with ALS (collaborative filtering)."""
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from spark_session import get_spark
from config import CLEAN, OUT

def run(spark):
    df = spark.read.parquet(CLEAN)
    # implicit rating: view=1, cart=3, purchase=5
    r = (df.withColumn("rating", F.when(F.col("event_type") == "purchase", 5.0)
                        .when(F.col("event_type") == "cart", 3.0).otherwise(1.0))
           .groupBy("user_id", "product_id").agg(F.sum("rating").alias("rating")))
    r = StringIndexer(inputCol="user_id", outputCol="uid").fit(r).transform(r)
    r = StringIndexer(inputCol="product_id", outputCol="pid").fit(r).transform(r)
    train, test = r.randomSplit([0.8, 0.2], seed=1)
    als = ALS(userCol="uid", itemCol="pid", ratingCol="rating",
              rank=10, maxIter=8, regParam=0.1, coldStartStrategy="drop")
    model = als.fit(train)
    rmse = RegressionEvaluator(metricName="rmse", labelCol="rating",
                               predictionCol="prediction").evaluate(model.transform(test))
    print(f"[ALS] RMSE = {rmse:.3f}")
    model.recommendForAllUsers(5).limit(5).show(truncate=False)
    OUT.mkdir(exist_ok=True)
    (OUT / "als_metrics.txt").write_text(f"RMSE={rmse}\n")

if __name__ == "__main__":
    run(get_spark("ALS"))
