#!/bin/bash

service ssh start

sudo -u hadoop bash << EOF

# Format HDFS if NameNode
if [ "$HOSTNAME" == "masternode" ]; then
    if [ ! -f $$HADOOP_HOME/tmp/dfs/name/current/VERSION ]; then
        echo "[INFO] Formatting HDFS NameNode..."
        $HADOOP_HOME/bin/hdfs namenode -format -force
        echo "[INFO] HDFS NameNode formatted successfully."
    else
        echo "[INFO] HDFS NameNode already formatted. Skipping."
    fi
fi

# Start Hadoop services based on hostname
if [ "$HOSTNAME" == "masternode" ]; then
    echo "[INFO] Starting HDFS..."
    $HADOOP_HOME/sbin/start-dfs.sh

    echo "[INFO] Starting YARN..."
    $HADOOP_HOME/sbin/start-yarn.sh
fi

EOF

tail -f /dev/null