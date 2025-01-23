#! /bin/sh
SLAVE_CNT=4

for ((num=0; num<SLAVE_CNT; num++))
do
docker container create --name hadoop-slave${num} --hostname slave${num} --network hadoop-net slave_img
done

docker container create --name hadoop-master --hostname master --network hadoop-net -p 8088:8088 -p 9870:9870 master_img
