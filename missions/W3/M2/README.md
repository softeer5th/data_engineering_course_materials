# Hadoop Multi-Node Cluster with Docker
This guide explains how to set up a fully functional Hadoop multi-node cluster using Docker and Docker Compose. It includes steps to configure Hadoop, start services, and perform basic HDFS operations.

```
.
├─ Dockerfile
├─ README.md
├─ .env
├─ config
│  ├─ core-site.xml
│  ├─ hdfs-site.xml
│  ├─ mapred-site.xml
│  └─ yarn-site.xml
├─ data
│  ├─ namenode
│  ├─ datanode1
│  └─ datanode2
├─ docker-compose.yml
└─ entrypoint.sh
```

## Prerequisites
- Docker and Docker Compose installed on your machine.
- Basic knowledge of the Hadoop ecosystem.

## Step 1: Clone the Repository
``` bash
git clone https://github.com/yjy323/data_engineering_course_materials/tree/yjy323-W3
cd missions/W3/M2
```

## Step 2: Build and Start the Hadoop Cluster

### Build the Docker Image
Use Docker Compose to build the image and start the containers:
``` bash
docker-compose up -d
```

### Verify Services
- HDFS Web UI: Visit [http://localhost:9870](http://localhost:9870) to access the HDFS NameNode interface.
- YARN ResourceManager Web UI: Visit [http://localhost:8088](http://localhost:8088) to access the YARN ResourceManager interface.

### List Running Containers
To verify that all containers are running:
``` bash
docker ps
```

## Step 3: Configure Hadoop (Within the Containers)
If additional configuration is required, you can access the containers and edit Hadoop’s configuration files.

### Access a Container
To access a specific container (e.g., namenode):
``` bash
docker exec -it namenode /bin/bash
```

### Edit Configuration Files
Hadoop configuration files are located in `/usr/local/hadoop/etc/hadoop/`. You can modify them using `vim` or any text editor:
- `core-site.xml`
- `hdfs-site.xml`
- `mapred-site.xml`
- `yarn-site.xml`

### Restart Hadoop Services
After making changes, restart the services to apply the new configuration:
``` bash
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/start-dfs.sh
```

## Step 4: Perform HDFS and MapReduce Operations
Once the cluster is running, you can interact with HDFS and run MapReduce jobs to validate distributed processing.

### HDFS Operations
#### 1. Upload Input Data to HDFS
``` bash
echo -e "Hello World\nWelcome to Hadoop\nHadoop MapReduce Example" > input.txt
hdfs dfs -mkdir /input
hdfs dfs -put input.txt /input
```

#### 2. Verify Input Data
``` bash
hdfs dfs -ls /input
```

#### 3. View Uploaded Data
``` bash
hdfs dfs -cat /input/input.txt
```

### MapReduce Operations
#### 1. Run WordCount Job
``` bash
yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /input /output
```

#### 2. Monitor Job Progress
- Visit the YARN ResourceManager Web UI: [http://localhost:8088](http://localhost:8088).
- Check the job progress and logs.

#### 3. Verify Job Output
``` bash
hdfs dfs -ls /output
hdfs dfs -cat /output/part-r-00000
```

#### Example Output:
```
Hadoop    2
Hello     1
MapReduce 1
World     1
Welcome   1
to        1
Example   1
```

## Step 5: Stop the Cluster
To stop the running containers and remove all associated resources:
``` bash
docker-compose down
```

## Cluster Architecture
This setup consists of the following services:
- **NameNode**: Manages HDFS metadata and oversees file system operations.
- **DataNodes**: Stores actual data blocks.
- **ResourceManager**: Manages YARN resource allocation.
- **NodeManagers**: Executes tasks as instructed by the ResourceManager.

## Summary
With this setup, you now have a fully functional Hadoop multi-node cluster running in Docker containers. You can explore Hadoop’s features, perform HDFS operations, and develop applications in a controlled local environment.

For any issues, consult the Hadoop logs by accessing the container and navigating to the `$HADOOP_HOME/logs` directory.

## Next Steps
- Extend this setup to include more DataNodes or NodeManagers.
- Integrate Hadoop with tools like Hive or Spark for advanced data processing.