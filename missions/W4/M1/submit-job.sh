#!/bin/bash

cp -f pi.py ${HOME}/docker/volumes/spark/data/pi.py

if docker exec spark-master bash -c 'spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  $SPARK_HOME/data/pi.py 10 $SPARK_HOME/data/pi_result' > /dev/null 2>&1; then
    echo "Success"
else
    echo "Failed"
fi