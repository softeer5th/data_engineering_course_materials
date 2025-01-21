# Hadoop Single-Node Cluster using Docker

This project provides a Dockerized single-node Hadoop cluster to simplify Hadoop deployment and learning. Follow this guide to build, run, and interact with the Hadoop cluster.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Build and Run](#build-and-run)
3. [Hadoop Configuration & Startup](#hadoop-configuration--startup)
4. [HDFS Operations](#hdfs-operations)
5. [Web UI Access](#web-ui-access)
6. [Persistent Data Storage](#persistent-data-storage)
7. [Cluster Management](#cluster-management)
8. [Troubleshooting](#troubleshooting)
9. [Cleanup](#cleanup)
10. [Command Summary](#command-summary)

## Prerequisites

Ensure you have the following installed on your system:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Build and Run

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### Step 2: Build the Docker Image

```bash
docker-compose build
```

### Step 3: Run the Hadoop Cluster

```bash
docker-compose up -d
```

- The container runs in the background (\`-d\`).
- The following services are accessible:

  - **HDFS Web UI:** [http://localhost:9870](http://localhost:9870)
  - **YARN Resource Manager:** [http://localhost:8088](http://localhost:8088)
  - **DataNode Web UI:** [http://localhost:9864](http://localhost:9864)

## Hadoop Configuration & Startup

### Step 1: Access the Running Container

```bash
docker exec -it hadoop-cluster bash
```

### Step 2: Format the HDFS Namenode (Only Once)

```bash
hdfs namenode -format
```

### Step 3: Start Hadoop Services

```bash
start-dfs.sh
start-yarn.sh
```

### Step 4: Verify Running Processes

```bash
jps
```

Expected output:

```
1234 NameNode
1235 DataNode
1236 SecondaryNameNode
1237 ResourceManager
1238 NodeManager
```

## HDFS Operations

### Step 1: Create a Directory in HDFS

```bash
hdfs dfs -mkdir /user/test
hdfs dfs -ls /user
```

### Step 2: Upload a File to HDFS

```bash
echo "Hello Hadoop!" > /tmp/testfile.txt
hdfs dfs -put /tmp/testfile.txt /user/test/
```

Verify the upload:

```bash
hdfs dfs -ls /user/test/
```

### Step 3: Read a File from HDFS

```bash
hdfs dfs -cat /user/test/testfile.txt
```

### Step 4: Download a File from HDFS to Local System

```bash
hdfs dfs -get /user/test/testfile.txt /tmp/downloaded_testfile.txt
```

### Step 5: Remove Files and Directories from HDFS

```bash
hdfs dfs -rm /user/test/testfile.txt
hdfs dfs -rm -r /user/test
```

## Web UI Access

Once the cluster is running, the following interfaces are accessible:

- **HDFS Web UI (Namenode):** [http://localhost:9870](http://localhost:9870)
- **YARN Resource Manager:** [http://localhost:8088](http://localhost:8088)
- **DataNode Web UI:** [http://localhost:9864](http://localhost:9864)

## Persistent Data Storage

To ensure data persists across container restarts, Docker volumes are used:

```yaml
volumes:
  hadoop_namenode:
  hadoop_datanode:
```

## Cluster Management

### Stop the Cluster

```bash
docker-compose down
```

### Restart the Cluster

```bash
docker-compose up -d
```

## Troubleshooting

### 1. Check Running Containers

```bash
docker-compose ps
```

### 2. View Logs

```bash
docker-compose logs -f
```

## Cleanup

To remove unused Docker resources:

```bash
docker-compose down -v
docker volume rm hadoop_namenode hadoop_datanode
docker system prune -af
```

## Command Summary

| Operation                | Command                                   |
|--------------------------|-------------------------------------------|
| Build Docker Image       | `docker-compose build`                    |
| Start Hadoop Cluster     | `docker-compose up -d`                     |
| Stop Hadoop Cluster      | `docker-compose down`                      |
| Access Hadoop Container  | `docker exec -it hadoop-cluster bash`      |
| Format HDFS Namenode     | `hdfs namenode -format`                     |
| Start HDFS Services      | `start-dfs.sh`                             |
| Start YARN Services      | `start-yarn.sh`                            |
