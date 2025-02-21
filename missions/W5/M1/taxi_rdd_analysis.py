from pyspark import SparkContext
from pyspark.sql import SparkSession  # RDD를 사용해도 Parquet을 읽으려면 SparkSession이 필요

# SparkSession 및 SparkContext 생성
spark = SparkSession.builder.appName("NYC_Taxi_RDD_Analysis").getOrCreate()
sc = spark.sparkContext 

# HDFS에서 Parquet 데이터 로드 후 RDD 변환
data_path = "hdfs://hadoop-namenode:9000/user/root/dataset/"
df = spark.read.parquet(data_path)
raw_rdd = df.rdd  # RDD 변환, 튜플 유지

# 1. 데이터 필터링: 누락된 값이 있는 행/ 거리 및 요금이 0이하인 경우/ 해당 년도 데이터가 아닌 경우 제거
filtered_rdd = raw_rdd.filter(lambda row: (
    row.tpep_pickup_datetime is not None and 
    row.trip_distance is not None and    
    row.fare_amount is not None and
    row.trip_distance > 0 and 
    row.fare_amount > 0 and
    row.tpep_pickup_datetime.year == 2021
)).cache()

# 2. 데이터 파싱: (날짜, (거리, 요금, 1)) 형태로 변환
def parse_row(row):
    try:
        date = row.tpep_pickup_datetime.date().isoformat()
        return (date, (row.trip_distance, row.fare_amount, 1))  # (날짜, (총 거리, 총 요금, 트립 수))
    except:
        return None

# 3. 변환 및 필터링 (캐싱 적용)
parsed_rdd = filtered_rdd.map(parse_row).filter(lambda x: x is not None).cache()

# 4. 집계 연산: 날짜별 총 이동 거리, 총 수익, 총 택시 이용 횟수 계산
daily_metrics_rdd = parsed_rdd.reduceByKey(lambda a, b: (
    a[0] + b[0],  # 총 이동 거리
    a[1] + b[1],  # 총 수익
    a[2] + b[2]   # 총 택시 이용 횟수
)).mapValues(lambda v: (v[2], v[1], v[0] / v[2], v[1] / v[2]))  # 바로 평균 계산

# 5. 저장 전에 파티션 조정하여 셔플 최소화
final_rdd = daily_metrics_rdd.coalesce(1)  # 스테이지 줄이기 위한 최적화

# 6. 결과 저장 (HDFS)
output_path = "hdfs://hadoop-namenode:9000/user/root/outputs/taxi_rdd_results"
final_rdd.saveAsTextFile(output_path)

# 7. 불필요한 캐시 해제
filtered_rdd.unpersist()

# 종료
sc.stop()
