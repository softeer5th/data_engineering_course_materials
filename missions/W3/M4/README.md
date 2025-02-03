# Multi-node Hadoop Cluster with Docker: Running a Word Count MapReduce Job
## Overview
This guide explains how to set up a multi-node Hadoop cluster using Docker and demonstrates running a Word Count MapReduce job. The guide is tailored for users who are new to Hadoop and provides a step-by-step approach to configure, deploy, and execute a simple yet effective MapReduce task in a containerized environment.


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
├─ scripts
│  ├─ mapper.py
│  └─ reducer.py
├─ docker-compose.yml
└─ entrypoint.sh
```

## Prerequisites
Before starting, ensure you have the following:

A system with Docker and Docker Compose installed.
Basic knowledge of Linux commands.
Internet connectivity to download required Docker images and files.

## Cluster Setup
### Step 1: Prepare the Environment
Verify Docker and Docker Compose installation.
Clone the repository containing the required configurations:
``` bash
git clone https://github.com/yjy323/data_engineering_course_materials/tree/yjy323-W3
cd missions/W3/M3
```

### Step 2: Build and Deploy Docker Containers
1. Use the provided Dockerfile to build the Hadoop image:
``` bash
docker build -t ubuntu/hadoop:latest .
```

2. Start the Hadoop cluster using Docker Compose:
``` bash
docker-compose up -d
```

## Running the Word Count Job
### Input Preparation
1. Download a sample text file inside the NameNode container:
``` bash
curl -L -o sentiment140.zip https://www.kaggle.com/api/v1/datasets/download/kazanova/sentiment140
unzip sentiment140.zip
rm sentiment140.zip
docker cp training.1600000.processed.noemoticon.csv namenode:sentiment140.csv
```

2. Create an input directory in HDFS and upload the text file:
``` bash
hadoop fs -mkdir -p /user/$(whoami)/input
hadoop fs -put sentiment140.csv /user/$(whoami)/input/
```

### Executing the Job
1. Copy the mapper.py and reducer.py scripts into the NameNode container:
``` bash
docker cp ./scripts/mapper.py namenode:/mapper.py
docker cp ./scripts/reducer.py namenode:/reducer.py
```

2. Run the Word Count MapReduce job:
``` bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input /user/$(whoami)/input/sentiment140.csv \
-output /user/$(whoami)/output \
-mapper mapper.py \
-reducer reducer.py \
-file /home/$(whoami)/mapper.py \
-file /home/$(whoami)/reducer.py
```

## Output Verification
1. Check the output directory:
``` bash
hadoop fs -ls /user/$(whoami)/output
```

2. Retrieve and display the results:
``` bash
hadoop fs -cat /user/$(whoami)/output/part-00000
```

## Monitoring the Cluster
Access the Hadoop Web UI to monitor job progress and resource utilization:
- NameNode: http://localhost:9870
- ResourceManager: http://localhost:8088