from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TLC Trip Record Analysis") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print("Spark session created successfully.")