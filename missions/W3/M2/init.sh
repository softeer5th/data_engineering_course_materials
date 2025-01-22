#!/bin/bash

service ssh start

if [ "$NODE_TYPE" == "master" ]; then
    echo "Starting Hadoop master service"
    hdfs namenode -format -force
    start-dfs.sh
    start-yarn.sh
elif [ "$NODE_TYPE" == "worker" ]; then
    echo "Starting Hadoop worker services"
    start-dfs.sh
fi

tail -f /dev/null