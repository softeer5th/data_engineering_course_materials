#!/bin/bash

# Start SSH service
sudo service ssh start

# Check if HDFS NameNode is already formatted
if [ ! -f /hadoopdata/hdfs/namenode/current/VERSION ]; then
    echo "Formatting HDFS NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
else
    echo "HDFS NameNode already formatted. Skipping format."
fi

# Start HDFS services
echo "Starting HDFS services..."
$HADOOP_HOME/sbin/start-dfs.sh

# Start YARN services
echo "Starting YARN services..."
$HADOOP_HOME/sbin/start-yarn.sh

# Keep the container running
echo "Hadoop is up and running!"
tail -f /dev/null