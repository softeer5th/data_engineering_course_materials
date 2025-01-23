#!/bin/sh
SLAVE_CNT=4

for ((num=0; num<SLAVE_CNT; num++))
do
docker container rm hadoop-slave${num}
done

docker container rm hadoop-master
docker network rm hadoop