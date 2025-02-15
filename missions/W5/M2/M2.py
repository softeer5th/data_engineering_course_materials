# 필요한 패키지
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import datetime

# 스파크 세션 생성
spark = SparkSession.builder.appName("NYC_Taxi_Analysis_W5M2").config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.adaptive.enabled", "false").getOrCreate()

# 파일 불러오기
def load_data(file_path, file_format) :
    if file_format == 'parquet' :
        taxi_df = spark.read.parquet(file_path)
    elif file_format == 'csv' :
        taxi_df = spark.read.csv(file_path)
    return taxi_df

# 데이터 정제
def clean_data(taxi_df) :
    # 데이터셋의 관측 시점에 벗어난 데이터 제한
    cleaned_df = taxi_df.filter(f.col('tpep_pickup_datetime').isNotNull())\
        .filter(f.col('tpep_pickup_datetime').isNotNull())\
        .filter(f.col('trip_distance').isNotNull())\
        .filter(datetime(2022,1,1) <= f.col('tpep_pickup_datetime')) \
        .filter(f.col('tpep_pickup_datetime')<= datetime(2022,1,31))
    return cleaned_df

# 데이터 변환
def transform_data(cleaned_df) :

    # 1명 이상 승객을 대상으로 분석 진행
    df = cleaned_df.filter(f.col('passenger_count') > 1).cache()

    # 날짜 형식 변환
    df = df.withColumn('pickup_date', f.to_date(f.col('tpep_pickup_datetime')))

    # Daily 집계 구하기
    df = df.groupBy('pickup_date').agg(
        f.count('*').alias("Daily_trip_count"),
        f.sum('total_amount').alias("Daily_revenue"),
        f.mean('trip_distance').alias("Daily_average_trip_distance")
    )

    # 날짜 순으로 정렬
    transformed_df = df.orderBy('pickup_date')

    return transformed_df

# 데이터 출력 및 저장
def action(transformed_df) :

    # 변환된 데이터프레임 출력
    transformed_df.show(5)

    # 데이터프레임 저장
    transformed_df.coalesce(1).write.csv(path = './output/daily_results_plural.csv', mode = 'overwrite', header = True)

# 메인 함수
if __name__ == '__main__' :
    taxi_df = load_data('./NYC_TLC_Trip_Data/yellow_tripdata_2022-01.parquet', 'parquet')
    cleaned_df = clean_data(taxi_df)
    cleaned_df.explain()
    input('Cleaning이 완료되었습니다. 이후 단계를 실행하려면 엔터를 눌러주세요.')

    transformed_df = transform_data(cleaned_df)
    transformed_df.explain()
    input('Transforming이 완료되었습니다. 이후 단계를 실행하려면 엔터를 눌러주세요.')

    action(transformed_df)
    input('Action이 완료되었습니다. 스파크 세션을 종료하려면 엔터를 눌러주세요..')