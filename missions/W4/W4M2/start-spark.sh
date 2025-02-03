#!/bin/bash

# Spark Master 실행
if [[ $SPARK_MODE == "master" ]]; then
    echo "Starting Spark Master..."
    ${SPARK_HOME}/sbin/start-master.sh
    tail -f ${SPARK_HOME}/logs/spark--org.apache.spark.deploy.master.Master-1-$(hostname).out

# Spark Worker 실행
elif [[ $SPARK_MODE == "worker" ]]; then
    echo "Starting Spark Worker..."
    ${SPARK_HOME}/sbin/start-worker.sh ${SPARK_MASTER_URL}
    tail -f ${SPARK_HOME}/logs/spark--org.apache.spark.deploy.worker.Worker-1-$(hostname).out

# Jupyter Notebook 실행
elif [[ $SPARK_MODE == "jupyter" ]]; then
    echo "Starting Jupyter Notebook..."
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=''

    # Jupyter 서버가 계속 실행되도록 유지
    tail -f /dev/null
fi