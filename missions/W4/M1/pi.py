import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi = 4.0 * count / n
    print("Pi is roughly %f" % pi)

    # 결과를 DataFrame으로 변환
    result = Row(num_samples=n, pi_estimate=pi)
    df = spark.createDataFrame([result])

    # CSV로 저장
    output_path = "/opt/spark/output/pi_estimate.csv"  # 저장할 경로
    df.write.mode("overwrite").csv(output_path, header=True)

    spark.stop()   
