from pyspark.sql import SparkSession
import random
import sys

if __name__ == "__main__" :
    spark = SparkSession.builder.appName("PiEstimation").getOrCreate()
    sc = spark.sparkContext

    num_samples = int(sys.argv[1]) if len(sys.argv) > 1 else 100000

    def inside(p) :
        x, y = random.random(), random.random()
        return x * x + y * y < 1
    
    count = sc.parallelize(range(0, num_samples)).filter(inside).count()
    pi = 4.0 * count / num_samples
    print(f"Pi is roughly {pi}")

    output_path = "/opt/spark-data/output"
    result = spark.createDataFrame([(pi,)], ["Estimated Pi"])
    result.write.csv(output_path, mode='overwrite', header = True)

    spark.stop()