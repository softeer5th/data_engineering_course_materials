#!/bin/sh

SLAVE_CNT=4

# set workers file
touch workers
for ((num=0; num<SLAVE_CNT; num++))
do
echo slave${num} >> workers
done

# make ssh key first
ssh-keygen -t rsa -P "" -f ./id_rsa

# make network group
docker network rm hadoop-net
docker network create --driver=bridge hadoop-net
# run slaves
docker build -t slave_img -f ./Dockerfile_slave .
for ((num=0; num<SLAVE_CNT; num++))
do
docker run -d --name hadoop-slave${num} --hostname slave${num} --network hadoop-net slave_img
done

# make master image
docker build -t master_img -f ./Dockerfile_master .

# delete hard links of common file
rm workers
rm id_rsa.pub
rm id_rsa

# run the master
#docker run -itd --name hadoop-master --hostname master --network hadoop-net -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 9866:9866 -p 9867:9867 master_img bash
docker run -dit --name hadoop-master --hostname master --network hadoop-net -p 8088:8088 -p 9870:9870 master_img bash