#!/bin/bash

# service ssh start

if [[ "$HOSTNAME" == "spark-master" ]]; then
    echo "[INFO] Starting Spark Master..."
    $SPARK_HOME/sbin/start-master.sh

elif [[ "$HOSTNAME" == spark-worker-* ]]; then
    echo "[INFO] Starting WorkerNode..."
    $SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077

fi

tail -f /dev/null