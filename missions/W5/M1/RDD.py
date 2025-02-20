# 필요한 패키지
from pyspark.sql import SparkSession
from datetime import datetime

# 스파크 세션 생성
spark = SparkSession.builder.appName("NYC_Taxi_Analysis_W5M1").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

# 파일 불러오기
def load_data(file_path, file_format) :
    if file_format == "parquet" :
        raw_df = spark.read.parquet(file_path)
        raw_rdd = raw_df.rdd
    elif file_format == "csv" :
        raw_df = spark.read.csv(file_path)
        raw_rdd = raw_df.rdd
    return raw_rdd

# 데이터 정제
def clean_data(rdd) :
    rdd = rdd.filter(lambda row: row.tpep_pickup_datetime is not None)
    rdd = rdd.filter(lambda row: row.total_amount is not None)
    rdd = rdd.filter(lambda row: row.trip_distance is not None)
    rdd = rdd.filter(lambda row: datetime(2022,1,1) <= row.tpep_pickup_datetime <= datetime(2022,1,31))
    return rdd

# 데이터 변환
def transform_data(rdd) :
    return rdd.map(lambda row: (row.tpep_pickup_datetime.strftime('%Y-%m-%d'), float(row.total_amount), float(row.trip_distance)))

# 데이터 합산
def aggreagate_data(rdd) :
    total_trips = float(rdd.count())
    total_revenue = rdd.map(lambda x : x[1]).sum()
    avg_trip_distance = rdd.map(lambda x : x[2]).mean()
    total_trips, total_revenue, total_trip_distance = rdd.aggregate(
        (0.0, 0.0, 0.0),
        lambda acc, x : (acc[0] + 1, acc[1] + x[1], acc[2] + x[2]),
        lambda acc1, acc2 : (acc1[0] + acc2[0], acc1[1] + acc2[1] , acc1[2] + acc2[2])
        )
    
    total_revenue = round(total_revenue, 2)
    avg_trip_distance = total_trip_distance / total_trips

    daily_trips = rdd.map(lambda x : (x[0],1)).reduceByKey(lambda a, b : a + b).collect()
    daily_revenue = rdd.map(lambda x : (x[0],x[1])).reduceByKey(lambda a, b : a + b).collect()
    return total_trips, total_revenue, avg_trip_distance, daily_trips, daily_revenue

# 결과 출력
def print_results(results) :
    total_trips, total_revenue, avg_trip_distance, daily_trips, daily_revenue = results
    print("Total trips :", total_trips)
    print("Total Revenue :", total_revenue)
    print("Average Trip Distance :", avg_trip_distance)

# 결과 저장
def save_results(results, output_path) :

    total_trips, total_revenue, avg_trip_distance, daily_trips, daily_revenue = results
    total_df = spark.createDataFrame([
        ("Total Trips", total_trips),
        ("Total Revenue", total_revenue),
        ("Average Trip Distance", avg_trip_distance)
    ], ["Metric", "Value"])

    trips_df = spark.createDataFrame(daily_trips, ["Date", "Trips"])
    revenue_df = spark.createDataFrame(daily_revenue, ["Date", "Revenue"])

    daily_df = trips_df.join(revenue_df, on="Date", how="inner")

    total_df.coalesce(1).write.csv(f"{output_path}/total_results.csv", mode="overwrite", header=True)
    daily_df.coalesce(1).write.csv(f"{output_path}/daily_results.csv", mode="overwrite", header=True)

# 메인 함수
if __name__ == "__main__" :
    raw_rdd = load_data("./NYC_TLC_Trip_Data/yellow_tripdata_2022-01.parquet", "parquet")
    cleaned_rdd = clean_data(raw_rdd)
    transformed_rdd = transform_data(cleaned_rdd)
    results = aggreagate_data(transformed_rdd)
    print_results(results)
    save_results(results, "./output")

    
