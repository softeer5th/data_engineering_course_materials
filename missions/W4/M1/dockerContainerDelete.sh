#!/bin/sh
SLAVE_CNT=4

for ((num=0; num<SLAVE_CNT; num++))
do
docker container rm spark-slave${num}
done

docker container rm spark-master
docker network rm hadoop-net