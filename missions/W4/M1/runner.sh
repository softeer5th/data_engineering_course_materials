#!/bin/bash
docker exec -i spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/examples/src/main/python/pi.py > ./db/master/output.log 2>/dev/null
