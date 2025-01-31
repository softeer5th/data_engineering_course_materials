from pyspark.sql import SparkSession
import random

def inside_circle(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0

def main():
    spark = SparkSession.builder \
            .appName("PythonPi") \
            .getOrCreate()
    
    NUM_SAMPLES = 1000000
    num_partitions = 2

    count = spark.sparkContext.parallelize(range(0, NUM_SAMPLES), num_partitions) \
                .map(lambda _: inside_circle(_)) \
                .reduce(lambda a, b: a + b)
    
    pi = 4.0 * count / NUM_SAMPLES
    print("Pi is roughly", pi)
    spark.stop()

if __name__ == "__main__":
    main()