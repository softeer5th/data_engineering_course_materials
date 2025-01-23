#! /bin/sh
SLAVE_CNT=4

for ((num=0; num<SLAVE_CNT; num++))
do
docker container start hadoop-slave${num}
done

docker container start hadoop-master
