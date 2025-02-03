"""
이 코드는 Spark를 사용하여 원주율(π)을 추정하는 예제입니다.
100만 개의 샘플을 생성하고, 각 샘플이 원 안에 속하는지 여부를 확인하여 원주율을 추정합니다.
결과는 CSV 파일로 저장됩니다.
"""
from pyspark.sql import SparkSession
import os
import random

def inside(_):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pi Estimation").getOrCreate()

    n = 1000000  # 100만 개의 샘플
    count = spark.sparkContext.parallelize(range(n)).map(inside).reduce(lambda a, b: a + b)
    pi_estimate = 4.0 * count / n

    # 🔹 출력 디렉토리 생성
    output_dir = "/opt/spark/output"
    os.makedirs(output_dir, exist_ok=True)

    # 🔹 결과를 Spark DataFrame으로 변환
    df = spark.createDataFrame([(pi_estimate,)], ["Estimated Pi Value"])
    
    # # 🔹 단일 파일로 저장 (coalesce(1) 사용)
    # df.coalesce(1).write.mode("overwrite").csv(output_dir + "/pi_estimate.csv", header=True)

    # 🔹 CSV 파일로 저장
    csv_output_path = os.path.join(output_dir, "pi_estimate.csv")
    df.write.mode("overwrite").csv(csv_output_path, header=True)

    print(f"Estimated Pi Value: {pi_estimate}")
    print(f"CSV Result saved to {csv_output_path}")

    spark.stop()
    
    
    
"""
주석처리된 결과를 CSV 파일과 Parquet 파일로 저장됩니다.
"""
# from pyspark.sql import SparkSession
# import os
# import random

# def inside(_):
#     x, y = random.random(), random.random()
#     return 1 if x*x + y*y < 1 else 0

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("Pi Estimation").getOrCreate()

#     n = 1000000  # 100만 개의 샘플
#     count = spark.sparkContext.parallelize(range(n)).map(inside).reduce(lambda a, b: a + b)
#     pi_estimate = 4.0 * count / n

#     # 🔹 출력 디렉토리 생성
#     output_dir = "/opt/spark/output"
#     os.makedirs(output_dir, exist_ok=True)

#     # 🔹 결과를 Spark DataFrame으로 변환
#     df = spark.createDataFrame([(pi_estimate,)], ["Estimated Pi Value"])

#     # 🔹 CSV 파일로 저장
#     csv_output_path = os.path.join(output_dir, "pi_estimate.csv")
#     df.write.mode("overwrite").csv(csv_output_path, header=True)

#     # 🔹 Parquet 파일로 저장
#     parquet_output_path = os.path.join(output_dir, "pi_estimate.parquet")
#     df.write.mode("overwrite").parquet(parquet_output_path)

#     print(f"Estimated Pi Value: {pi_estimate}")
#     print(f"CSV Result saved to {csv_output_path}")
#     print(f"Parquet Result saved to {parquet_output_path}")

#     spark.stop()




"""
주석처리된 결과를 txt 파일로 저장됩니다.
"""
# from pyspark.sql import SparkSession
# import os
# import random

# def inside(_):
#     x, y = random.random(), random.random()
#     return 1 if x*x + y*y < 1 else 0

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("Pi Estimation").getOrCreate()

#     n = 1000000  # 100만 개의 샘플
#     count = spark.sparkContext.parallelize(range(n)).map(inside).reduce(lambda a, b: a + b)
#     pi_estimate = 4.0 * count / n

#     # 🔹 출력 디렉토리 생성 (없으면 생성)
#     output_dir = "/opt/spark/output"
#     os.makedirs(output_dir, exist_ok=True)

#     # 🔹 결과 파일 저장
#     output_path = os.path.join(output_dir, "pi_estimate.txt")
#     with open(output_path, "w") as f:
#         f.write(f"Estimated Pi Value: {pi_estimate}\n")

#     print(f"Estimated Pi Value: {pi_estimate}")
#     print(f"Result saved to {output_path}")

#     spark.stop()
