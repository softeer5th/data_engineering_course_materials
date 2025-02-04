# submit-pi-and-check.sh (컨테이너 내부용)
#!/bin/bash

# 저장 경로에 쓰기 권한 부여
echo "Setting write permissions for /opt/spark-data..."
chmod -R 777 /opt/spark-data

# Spark 작업 제출
echo "Submitting Spark job..."
if spark-submit --master spark://spark-master:7077 /opt/spark-data/pi_with_output.py; then
    echo "Spark job completed successfully."
else
    echo "Spark job failed."
    exit 1
fi

# 작업 결과 확인
RESULT_DIR="/user/spark/output"
echo "Checking output..."
if hdfs dfs -test -e "$RESULT_DIR/part-00000*"; then
    echo "Result file exists. Showing content:"
    hdfs dfs -cat "$RESULT_DIR/part-00000*"
else
    echo "Result file not found in $RESULT_DIR."
    exit 1
fi