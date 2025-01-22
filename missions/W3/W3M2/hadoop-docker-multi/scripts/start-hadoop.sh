#!/bin/bash

# Start SSH service
sudo service ssh start

export HADOOP_VERSION=3.4.0
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}

# Check if HDFS NameNode is already formatted
if [ ! -f /hadoopdata/hdfs/namenode/current/VERSION ]; then
    echo "Formatting HDFS NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
else
    echo "HDFS NameNode already formatted. Skipping format."
fi

# Start Hadoop services
if [ "$HOSTNAME" = "hadoop-master" ]; then
  su - hdfs -c "$HADOOP_HOME/bin/hdfs --daemon start namenode"
  su - hdfs -c "$HADOOP_HOME/bin/hdfs --daemon start secondarynamenode"
  su - yarn -c "$HADOOP_HOME/bin/yarn --daemon start resourcemanager"

  su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/"
  su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/root/"
  su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -chown root:root /user/root"
  su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -chown root:root /"
else
  su - hdfs -c "$HADOOP_HOME/bin/hdfs --daemon start datanode"
  su - yarn -c "$HADOOP_HOME/bin/yarn --daemon start nodemanager"
fi

# Keep the container running
tail -f /dev/null