## Docker image build

```bash
# In project root
docker buildx build --load -t hadoop-base -f missions/W3/M2/Dockerfile .
```

## Docker compose

```bash
docker compose -f missions/W3/M2/docker-compose.yaml up
```

### HDFS 확인

```bash
docker exec -it namenode /bin/bash
hdfs dfs -ls /
```

```output
Found 3 items
drwxr-xr-x   - hduser supergroup          0 2025-01-22 07:30 /jars
drwxrwxrwx   - hduser supergroup          0 2025-01-22 07:38 /tmp
drwxrwxrwx   - hduser supergroup          0 2025-01-22 07:45 /users
```

### datanode 확인

```bash
docker exec -it namenode /bin/bash
hdfs dfsadmin -report
```

```output
...
-------------------------------------------------
Live datanodes (2):
...
Wed Jan 22 07:58:17 UTC 2025
Num of Blocks: 13
```

### HDFS 파일 생성

```bash
docker exec -it namenode /bin/bash
hdfs dfs -mkdir /user/input
hdfs dfs -put $HADOOP_HOME/README.txt /user/input
```

### MapReduce 실행

```bash
docker exec -it namenode /bin/bash
hadoop jar hadoop-mapreduce-examples-*.jar wordcount /users/input /users/output
```

### Web UI
- HDFS: http://localhost:9870
- YARN: http://localhost:8088