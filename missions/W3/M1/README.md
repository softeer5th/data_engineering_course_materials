# Hadoop Single Node Cluster with Docker
This guide explains how to set up a fully functional Hadoop single-node cluster using Docker and Docker Compose. It includes steps to configure Hadoop, start services, and perform basic HDFS operations.
```
.
├─ Dockerfile
├─ README.md
├─ config
│  ├─ core-site.xml
│  ├─ hdfs-site.xml
│  └─ mapred-site.xml
├─ data
│  ├─ data
│  └─ name
├─ docker-compose.yml
└─ entrypoint.sh
```

## Prerequisites
- Docker and Docker Compose installed on your machine.
- Basic knowledge of the Hadoop ecosystem.

## Step 1: Clone the Repository
```bash
git clone https://github.com/cleonno3o/softeer_de_repo.git
git checkout joosumin-w3
cd missions/W3/M1
```

## Step 2: Build and Start the Hadoop Cluster
### Build the Docker Image
Use Docker Compose to build the image and start the container:
``` bash
docker-compose up -d
```

### Start the Services
After the container is running, Hadoop services are automatically configured and started via the entrypoint.sh script.

### Verify Services
HDFS Web UI: Visit http://localhost:9870 to access the HDFS NameNode interface.

## Step 3: Configure Hadoop (Within the Container)
If additional configuration is required, you can access the container and edit Hadoop's configuration files.

### 1. Access the container:
``` bash
docker exec -it hadoop-single-node-container /bin/bash
```
### 2. Edit configuration files:
   Hadoop configuration files are located in /usr/local/hadoop/etc/hadoop/. You can modify them using vim or any text editor:
- core-site.xml
- hdfs-site.xml
- mapred-site.xml

### 3. Restart Hadoop services:
After making changes, restart the services to apply the new configuration:
``` bash
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/start-dfs.sh
```

## Step 4: Basic HDFS Operations
Once the cluster is running, you can interact with HDFS using the hdfs dfs command.

### 1. Create Directories in HDFS
``` bash
docker exec -it hadoop-container /bin/bash
hdfs dfs -mkdir /user
```

### 2. Upload Files to HDFS
To upload a file from the local filesystem to HDFS:
1. Place a file in a shared/mounted directory or directly within the container.
``` bash
echo "Hello Hadoop" > /tmp/hello.txt
```

2. Upload the file to HDFS:
``` bash
hdfs dfs -put /tmp/hello.txt /user/
```

### 3. List Files in HDFS
``` bash
hdfs dfs -ls /user/
```

### 4. Read a File from HDFS
``` bash
hdfs dfs -cat /user/hello.txt
```

### 5. Remove a File or Directory from HDFS
``` bash
hdfs dfs -rm /user/hello.txt
hdfs dfs -rm -r /user/
```

## Step 5: Stop the Cluster
To stop the running container and remove all associated resources:
``` bash
docker-compose down
```