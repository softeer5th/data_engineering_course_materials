from pyspark.sql import SparkSession
import os
from datetime import datetime

spark = (
    SparkSession.builder.appName("M01")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"{os.getcwd()}/spark-events")
    .config("spark.history.fs.logDirectory", f"{os.getcwd()}/spark-events")
    .getOrCreate()
)

df = spark.read.parquet("data/yellow_tripdata_2024-01.parquet")

# DataFrame을 RDD로 변환
df_rdd = df.rdd

# 결측값 제거
df_rdd = df_rdd.filter(
    lambda row: all(
        row[col] is not None
        for col in [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
        ]
    )
)

# 유효한 운임 및 거리 필터링
df_rdd = df_rdd.filter(lambda row: row.fare_amount > 0 and row.trip_distance > 0)


# 날짜 변환 및 필터링
def extract_date(row):
    pickup_date = row.tpep_pickup_datetime.date()  # 직접 date 객체 추출
    if datetime(2024, 1, 1).date() <= pickup_date < datetime(2024, 2, 1).date():
        return (pickup_date.strftime("%Y-%m-%d"), row)
    return None


df_rdd = df_rdd.map(extract_date).filter(lambda x: x is not None)

df_rdd.cache()

# 총 운행 횟수 계산
trip_count = df_rdd.count()
print("Total number of trips: ", trip_count)

# 총 수익 계산
total_revenue = df_rdd.map(lambda row: row[1].total_amount).sum()
print("Total revenue: ", total_revenue)

# 평균 거리 계산
total_distance, total_trips = df_rdd.map(lambda row: row[1].trip_distance).aggregate(
    (0.0, 0),
    lambda acc, value: (acc[0] + value, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]),
)
avg_trip_distance = total_distance / total_trips if total_trips > 0 else 0
print("Average trip distance: ", avg_trip_distance)

# 일별 운행 횟수 계산
trips_per_day = df_rdd.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a + b)
print("Trips per day:")
for date, count in trips_per_day.collect():
    print(f"{date}: {count}")

# 일별 총 수익 계산
total_revenue_per_day = df_rdd.map(
    lambda row: (row[0], row[1].total_amount)
).reduceByKey(lambda a, b: a + b)
print("Total revenue per day:")
for date, revenue in total_revenue_per_day.collect():
    print(f"{date}: {revenue}")