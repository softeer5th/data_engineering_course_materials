#!/bin/bash

# SPARK_MODE에 따라 마스터 또는 워커 실행
if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    $SPARK_HOME/sbin/start-master.sh -h 0.0.0.0
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    $SPARK_HOME/sbin/start-worker.sh spark://$SPARK_MASTER_HOST:7077
else
    echo "Invalid SPARK_MODE. Use 'master' or 'worker'."
    exit 1
fi

# Keep container running
tail -f /dev/null