#!/bin/bash
set -e

echo "Stopping YARN ${HADOOP_YARN_ROLE}..."
yarn --daemon stop ${HADOOP_YARN_ROLE}

echo "Stopping HDFS ${HADOOP_HDFS_ROLE}..."
hdfs --daemon stop ${HADOOP_HDFS_ROLE}

if [ "${HADOOP_YARN_ROLE}" = "resourcemanager" ]; then
    echo "Stopping MapReduce Job History Server..."
    mapred --daemon stop historyserver
fi

echo "Hadoop services stopped."