from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp, expr, hour
import time

#----------------데이터 수집-----------------#
# Spark 세션 생성 - Java Heap space 부족 방지
spark = SparkSession.builder \
    .appName("NYC_Taxi_Data_Analysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# 데이터 병합 - 2024년 1월~11월 yellow taxi 여행기록
# Spark는 경로에 여러 Parquet 파일이 존재할 경우 자동으로 병합한다. 
df = spark.read.parquet("./dataset/yellow_tripdata_2024-*.parquet")

# 데이터 개수 확인 - 총 37,501,349개
print(f"총 로드된 레코드 수: {df.count()}")

# 데이터 스키마 확인
df.printSchema()

# 데이터 샘플 조회
df.show(5)

#-----------------데이터 클렌징--------------------#
# [컬럼 타입 변환]
# 탑승시간과 하차시간을 timestamp형식으로 변환
# 데이터 전송여부를 이진값으로 변환(Y - 1, N - 0)
df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
       .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
       .withColumn("store_and_fwd_flag", when(col("store_and_fwd_flag") == "Y", 1).otherwise(0))

# [결측값 처리]
# 승차지점 지역ID, 하차지점 지역ID, 이동거리, 총요금에 결측치가 있는 행 삭제
df = df.dropna(subset=["PULocationID", "DOLocationID", "trip_distance", "total_amount"])

# [비정상 데이터 제거]
# 거리가 0 이하거나 200마일 이상인 데이터 삭제
# 총 요금이 0 이하인 데이터 삭제
# 기본 요금이 0보다 큰데 결제 유형이 무료 승차인 데이터 삭제
# 승객이 0명인 데이터 삭제
df = df.filter((col("trip_distance") > 0) & (col("trip_distance") < 200))  
df = df.filter((col("total_amount") > 0))  
df_cleaned = df.filter(~((col("fare_amount") > 0) & (col("payment_type") == 3)))
df = df.filter(col("passenger_count") > 0) 

# [새로운 컬럼 생성]
# 하차시간 - 탑승시간을 초 단위 계산: 평균 여행 시간 분석을 위해
# 위 값을 분단위로 변환: 분 단위 시각화로 가독성 향상을 위해
# 탑승시간에서 시간만 추출: peak hour분석용
df = df.withColumn("trip_duration_sec",
                   unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))
                  )
df = df.filter((col("trip_duration_sec") < 18000)) # 비정상 데이터 제거 - 이동이 5시간 이상인 데이터 제거
df = df.withColumn("trip_duration_min", col("trip_duration_sec") / 60.0)
df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))

#--------------데이터 저장----------------#
cleaned_parquet_path = "./dataset/cleaned_yellow_tripdata_2024.parquet"
cleaned_csv_path = "./dataset/cleaned_yellow_tripdata_2024.csv"

df = df.repartition(20)  # 20개의 파티션으로 나눠서 저장
df.write.mode("overwrite").parquet(cleaned_parquet_path)
df.write.mode("overwrite").csv(cleaned_csv_path, header=True)

print("데이터 클렌징 완료! 저장 경로:")
print(f" - Parquet: {cleaned_parquet_path}")
print(f" - CSV: {cleaned_csv_path}")

print("Spark UI를 유지하기 위해 대기 중...")
time.sleep(600)  # 10분 동안 대기 (Spark UI 유지)
spark.stop()



