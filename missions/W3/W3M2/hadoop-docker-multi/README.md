# W3M2a - Hadoop Multi-Node Cluster on Docker

## Docker 빌드, 실행 (최초 실행시)
```bash
docker compose up -d --build
```

## Docker 빌드, 실행
```bash
docker compose up -d
```

## Container Access
```bash
docker exec -it hadoop-master /bin/bash
```

## Component Check
```bash
jps
```

## Check Connection (On hadoop master)
```bash
ssh hadoop-worker1
ssh hadoop-worker2
ssh hadoop-worker3
```

## Testfile Create
```bash
cd /home/hdfs
echo "Hadoop is a framework" > input.txt
echo "Hadoop is used for big data processing" >> input.txt
echo "Hadoop uses MapReduce programming model" >> input.txt
```


## HDFS File Put
```bash
hdfs dfs -mkdir -p /user/hdfs/input
hdfs dfs -put input.txt /user/hdfs/input/
hdfs dfs -ls /user/hdfs/input/
```

## MapReduce 실행
```bash
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount /user/hdfs/input /user/hdfs/output
```

## MapReduce 결과 확인
```bash
hdfs dfs -ls /user/hdfs/output
hdfs dfs -cat /user/hdfs/output/part-r-00000
```

## MapReduce 결과 Get
```bash
hdfs dfs -get /user/hdfs/output/part-r-00000 [user directory]
```

