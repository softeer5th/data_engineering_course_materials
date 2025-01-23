# Hadoop Single Node Cluster on Docker


### Build Image
```
docker build -t hadoop-single-node .
```

### Run Container
```
docker run -it -p 9870:9870 -p 8088:8088 -v $(pwd)/data:/usr/local/hadoop/data hadoop-single-node
```

### Format HDFS
By default, the format is set to run when the container is executed.
If additional formatting is needed, run the following command:
```
hdfs namenode -format
```

### HDFS Test Commands
```
echo "hello world" > test.txt
hdfs dfs -put test.txt /test/test.txt
hdfs dfs -cat /test/test.txt
```

