#!/bin/bash

spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.eventLog.enabled=true \
    main.py