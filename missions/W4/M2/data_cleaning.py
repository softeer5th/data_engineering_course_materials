from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import functions as F

# Parquet 파일 읽기
df_parquet = spark.read.parquet('data/fhvhv_tripdata_2023-12.parquet')

# 결측값 처리 (예시: 결측값이 있는 행 삭제)
df_cleaned = df_csv.dropna()

# 시간 포맷 변환 (예: 'pickup_datetime'을 Timestamp로 변환)
df_cleaned = df_cleaned.withColumn("pickup_datetime", to_timestamp("pickup_datetime", "yyyy-MM-dd HH:mm:ss"))

# 비정상적인 데이터 필터링 (예: 음수의 여행 거리 및 시간)
df_cleaned = df_cleaned.filter((col("trip_duration") > 0) & (col("trip_distance") > 0))

print(df_cleaned)
