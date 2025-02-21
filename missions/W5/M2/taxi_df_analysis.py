from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, substring, avg, col, sum, round

# SparkSession 생성
spark = SparkSession.builder.appName("NYC_Taxi_DF_Analysis").getOrCreate()

# HDFS에서 데이터 로드
taxi_data_path = "hdfs://hadoop-namenode:9000/user/root/dataset/"
weather_data_path = "hdfs://hadoop-namenode:9000/user/root/weather_data/"

# 1. 택시 데이터 로드
taxi_df = spark.read.parquet(taxi_data_path)

# 2. 데이터 클리닝 & 변환 (to_date 중복 제거)
taxi_df = taxi_df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
taxi_df = taxi_df.filter(
    (col("tpep_pickup_datetime").isNotNull()) & 
    (col("trip_distance").isNotNull()) & 
    (col("fare_amount").isNotNull()) & 
    (col("trip_distance") > 0) & 
    (col("fare_amount") > 0) & 
    (col("passenger_count") >= 2) &  # 승객이 2명 이상인 경우만 필터링
    (col("pickup_date") >= "2021-01-01") & 
    (col("pickup_date") <= "2021-12-31")
)

# 3. 날씨 데이터 로드
weather_df = spark.read.csv(weather_data_path, header=True, inferSchema=True)

# 4. 날씨 데이터 변환: 문자열 → 날짜 타입 변환
weather_df = weather_df.withColumn("date_only", to_date(substring(col("DATE"), 1, 10)))

# 5. 날짜별 평균 기온, 평균 강수량, 평균 풍속 계산 (캐싱 적용)
weather_agg_df = weather_df.groupBy("date_only").agg(
    round(avg("HourlyDryBulbTemperature"), 2).alias("avg_temp"),
    round(avg("HourlyPrecipitation"), 2).alias("avg_precipitation"),
    round(avg("HourlyWindSpeed"), 2).alias("avg_wind_speed")
).cache()  # 캐싱 적용

# 6. 날짜별 택시 데이터 요약 (캐싱 적용)
daily_metrics_df = taxi_df.groupBy("pickup_date").agg(
    round(sum("trip_distance"), 2).alias("total_distance"),
    round(sum("fare_amount"), 2).alias("total_revenue"),
    round(avg("trip_distance"), 2).alias("avg_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare")
).cache()  # 캐싱 적용

# 7. 택시 데이터와 날씨 데이터 조인 (날짜 타입 일치)
final_df = daily_metrics_df.join(
    weather_agg_df,
    daily_metrics_df.pickup_date == weather_agg_df.date_only,
    "left"
).drop("date_only")  # 중복 컬럼 제거

# 8. 결과 저장 (HDFS)
output_path = "hdfs://hadoop-namenode:9000/user/root/outputs/taxi_df_results"
final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# 9. 불필요한 캐시 해제 (메모리 관리)
weather_agg_df.unpersist()
daily_metrics_df.unpersist()

# 종료
spark.stop()
