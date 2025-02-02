#!/bin/bash

# Start SSH service
service ssh start

# Initialize HDFS directories if not already done
if [ ! -f /usr/local/hadoop/tmp/dfs/name/current/VERSION ]; then
    echo "Formatting HDFS NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
fi

# Start HDFS services
echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# Keep the container running
echo "Hadoop is up and running!"
tail -f /dev/null