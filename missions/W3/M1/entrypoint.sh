#!/bin/bash

# Format HDFS if namenode
if [ ! -f /usr/local/hadoop/tmp/dfs/name/current/VERSION ]; then
    echo "[INFO] Formatting HDFS NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
    echo "[INFO] HDFS NameNode formatted successfully."
else
    echo "[INFO] HDFS NameNode already formatted. Skipping."
fi


# Start SSH service
echo "[INFO] Starting SSH service..."
service ssh start

# Start Hadoop services based on hostname
echo "[INFO] Starting Hadoop services for $HOSTNAME..."
$HADOOP_HOME/sbin/start-dfs.sh

# Keep the container running
echo "[INFO] Container setup complete. Tailing logs to keep container running..."
tail -f /dev/null