from pyspark.sql import SparkSession
import sys
import random

def inside_circle(_):
    x, y = random.random(), random.random()
    return 1 if x**2 + y**2 <= 1 else 0

def main(output_path: str, num_samples: int):
    spark = SparkSession.builder.appName("PiEstimationRDD").getOrCreate()
    sc = spark.sparkContext

    samples = sc.parallelize(range(num_samples)).map(inside_circle)
    
    count_inside = samples.reduce(lambda a, b: a + b)
    pi_estimate = (count_inside / num_samples) * 4

    result_df = spark.createDataFrame([(pi_estimate, num_samples)], ["pi_estimate", "num_samples"])
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    print(f"Estimated π value: {pi_estimate} using {num_samples} samples")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit script.py <output_path> <num_samples>")
        sys.exit(1)

    output_path = sys.argv[1]
    num_samples = int(sys.argv[2])

    main(output_path, num_samples)
