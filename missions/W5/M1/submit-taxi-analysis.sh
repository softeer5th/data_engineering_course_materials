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

# 데이터를 hdfs에 적재
echo "Loading dataset on HDFS..."
hdfs dfsadmin -safemode leave
hdfs dfs -rm -r /user/root/dataset
hdfs dfs -mkdir /user/root/dataset
hdfs dfs -put /opt/spark-data/dataset/* /user/root/dataset


# HDFS 결과 디렉토리 삭제 (기존 결과 제거)
echo "Removing old results from HDFS..."
hdfs dfs -rm -r /user/root/outputs/taxi_rdd_results

# Spark 작업 제출
echo "Submitting Spark job..."
if spark-submit --master spark://spark-master:7077 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hadoop-namenode:9000/user/root/spark-logs \
    /opt/spark-data/taxi_rdd_analysis.py; then
    echo "Spark job completed successfully."
else
    echo "Spark job failed."
    exit 1
fi

# 작업 결과 확인
echo "Checking output..."
if hdfs dfs -test -e /user/root/outputs/taxi_rdd_results/part-00000; then
    echo "Result file exists. Showing content:"
    hdfs dfs -cat /user/root/outputs/taxi_rdd_results/part-00000 | head -n 20
else
    echo "Result file not found in /user/root/outputs/taxi_rdd_results."
    exit 1
fi

echo "Starting History Server for checking DAG..."
start-history-server.sh