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

# make slave image
docker build -t slave_spark -f ./spark_slave.dockerfile .

# make master image
docker build -t master_spark -f ./spark_master.dockerfile .

# delete hard links of common file
rm workers
rm id_rsa.pub
rm id_rsa