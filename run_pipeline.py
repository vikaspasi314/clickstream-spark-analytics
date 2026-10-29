import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from spark_session import get_spark
import etl, analytics, recommend

if __name__ == "__main__":
    spark = get_spark(); spark.sparkContext.setLogLevel("WARN")
    for name, mod in [("ETL", etl), ("Analytics", analytics), ("Recommender", recommend)]:
        t = time.time(); mod.run(spark); print(f"--- {name} done in {time.time()-t:.1f}s ---")
    spark.stop()
