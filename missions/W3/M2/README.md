# Hadoop Multi Node Cluster on Docker

### Build Images
```
docker compose build
```

### Run Containers
```
docker compose up
```

### Format HDFS
If you want to format HDFS, run the following command:
```
hdfs namenode -format
```

### HDFS Test Commands
```
echo "hello world" > test.txt
hdfs dfs -put test.txt /test.txt
hdfs dfs -cat /test.txt
```

### MapReduce Test Commands
```
yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 16 1000
```

### Client
```xml
<!-- core-site.xml -->
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
</property>
```
```xml
<!-- hdfs-site.xml -->
<property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
</property>
```
```bash
# /etc/hosts
127.0.0.1       hadoop-worker1
127.0.0.1       hadoop-worker2
127.0.0.1       hadoop-worker3
```
```bash
hdfs dfs -ls /
```