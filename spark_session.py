from pyspark.sql import SparkSession

def get_spark(app="ClickstreamAnalytics"):
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "2g").getOrCreate())
