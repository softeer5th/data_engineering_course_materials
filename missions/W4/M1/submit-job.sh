#!/bin/bash

# Variables
MASTER_URL="spark://spark-master:7077"
APP_NAME="Pi Estimation"
SCRIPT_PATH="/opt/spark/jobs/pi.py"
#NUM_SAMPLES=100000
OUTPUT_PATH="/opt/spark/output/pi_estimate.csv"  # 볼륨 연결된 경로로 저장

# Submit Spark job
docker exec -it spark-master \
  bin/spark-submit \
  --master $MASTER_URL \
  --name "$APP_NAME" \
  $SCRIPT_PATH 1000 

# 결과 파일 확인 및 출력
echo "Pi estimation result saved to ./output/pi_estimate.csv"
ls ./output/pi_estimate.csv  # 출력 경로 확인
cat ./output/pi_estimate.csv/* 2>/dev/null || echo "Result file not found!"