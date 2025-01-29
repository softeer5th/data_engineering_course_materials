from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    # SparkSession 생성
    spark = SparkSession.builder.appName("PythonPiWithOutput").getOrCreate()

    # 파티션 수 설정 (기본값 2)
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    # Monte Carlo 작업 정의
    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    # RDD를 사용해 Pi 값 계산
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi = 4.0 * count / n
    print("Pi is roughly %f" % pi)

    # 결과를 DataFrame으로 변환
    result_df = spark.createDataFrame([(pi,)], ["pi_value"])

    # 결과 확인
    print("Checking result_df contents:")
    print(f"Number of rows in result_df : {result_df.count()}")
    result_df.show(truncate=False)

    # 결과를 CSV 파일로 저장
    HDFS_OUTPUT_PATH="hdfs://hadoop-namenode:9000/user/spark/output"
    result_df.coalesce(1).write.csv(HDFS_OUTPUT_PATH, mode="overwrite")
    print(f"Results successfully saved to {HDFS_OUTPUT_PATH}")

    # Spark 종료
    spark.stop()

