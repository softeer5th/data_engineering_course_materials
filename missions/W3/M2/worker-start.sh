#!/bin/bash
set -e

service ssh start

sed -i "s#\${HADOOP_DATA}#$HADOOP_DATA#g" $HADOOP_HOME/etc/hadoop/hdfs-site.xml

hdfs --daemon start datanode
yarn --daemon start nodemanager

tail -f /dev/null