# submit-pi-and-check.sh (컨테이너 내부용)
#!/bin/bash

# 저장 경로에 쓰기 권한 부여
echo "Setting write permissions for /opt/spark-data..."
chmod -R 777 /opt/spark-data

# HDFS 내 Spark 이벤트 로그 디렉토리 설정
echo "Ensuring Spark event log directory exists on HDFS..."
hdfs dfsadmin -safemode leave
hdfs dfs -mkdir -p /user/root/spark-logs
hdfs dfs -chmod -R 777 /user/root/spark-logs

# 택시, 날씨 데이터를 hdfs에 적재
echo "Loading dataset on HDFS..."
hdfs dfsadmin -safemode leave
hdfs dfs -rm -r /user/root/dataset
hdfs dfs -mkdir /user/root/dataset
hdfs dfs -put /opt/spark-data/dataset/*.parquet /user/root/dataset

hdfs dfs -rm -r /user/root/weather_data
hdfs dfs -mkdir /user/root/weather_data
hdfs dfs -put /opt/spark-data/dataset/NYC_climate_hourly_data.csv /user/root/weather_data


# HDFS 결과 디렉토리 삭제 (기존 결과 제거)
echo "Removing old results from HDFS..."
hdfs dfs -rm -r /user/root/outputs/taxi_df_results

# Spark 작업 제출
echo "Submitting Spark job..."
if spark-submit --master spark://spark-master:7077 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hadoop-namenode:9000/user/root/spark-logs \
    /opt/spark-data/taxi_df_analysis.py; then
    echo "Spark job completed successfully."
else
    echo "Spark job failed."
    exit 1
fi

# 작업 결과 확인
echo "Checking output..."
if hdfs dfs -test -e /user/root/outputs/taxi_df_results; then
    echo "Result file exists. Showing content:"
    # 정확한 파일명 가져오기 (coalesce(1)로 저장된 단일 파일)
    result_file=$(hdfs dfs -ls /user/root/outputs/taxi_df_results | grep -E "part-.*\.csv" | awk '{print $8}')
    # 헤더 포함하여 상위 11개 줄 출력 (헤더 + 10개 데이터)
    hdfs dfs -cat "$result_file" | head -n 11
else
    echo "Result file not found in /user/root/outputs/taxi_df_results."
    exit 1
fi

echo "Starting History Server for checking DAG..."
start-history-server.sh