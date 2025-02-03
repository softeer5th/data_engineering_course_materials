
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.eventLog.enabled=true \
    main.py