import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType

if __name__ == "__main__":
    """
        Usage: pi [partitions] [output_path]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    output_path = sys.argv[2] if len(sys.argv) > 2 else "pi_result"
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi_value = 4.0 * count / n
    print("Pi is roughly %f" % pi_value)

    schema = StructType([
        StructField("pi_value", DoubleType(), False),
        StructField("sample_count", DoubleType(), False),
        StructField("partitions", DoubleType(), False)
    ])

    result_df = spark.createDataFrame([
        (pi_value, float(n), float(partitions))
    ], schema)

    result_df.write.mode("overwrite").parquet(output_path)

    spark.stop()

