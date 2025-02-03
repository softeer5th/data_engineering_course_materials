#! /bin/sh
SLAVE_CNT=4

for ((num=0; num<SLAVE_CNT; num++))
do
docker container stop spark-slave${num} &
done

docker container stop spark-master &
