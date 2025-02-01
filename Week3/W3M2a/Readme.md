# Setting Up and Running an Apache Hadoop Multi-Node Cluster Using Docker

## Prerequisites

1. **Download Hadoop Binary**
   - Visit the [Apache Hadoop Release Page](https://hadoop.apache.org/releases.html) and download the 3.3.6 version binary file along with its checksum signature.
   - Place the downloaded files in the same directory as this repository before proceeding.

2. **Install Docker and Docker Compose**
   - Ensure Docker and Docker Compose are installed on your system.
   - Verify installation with:
     ```bash
     docker --version
     docker-compose --version
     ```

## Steps to Build and Run the Cluster

### 1. Build Docker Images and Start Containers

- Use Docker Compose to build the images and start the containers.

```bash
docker-compose up --build -d
```

This command will:
- Build the Docker image specified in the `Dockerfile`.
- Start the Hadoop master and worker nodes as defined in the `docker-compose.yml` file.

### 2. Format the Namenode

- Access the Hadoop master container:

```bash
docker exec -it hadoop-master bash
```

- Format the Namenode:

```bash
hdfs namenode -format
```

### 3. Access Hadoop Web Interfaces

- HDFS Namenode UI: [http://localhost:9870](http://localhost:9870)
- YARN Resource Manager UI: [http://localhost:8088](http://localhost:8088)

## Performing HDFS Operations

### 1. Create a Folder in HDFS

```bash
hdfs dfs -mkdir /<folder_name>
```

### 2. Upload a File to HDFS

- Create a sample file on your local machine:

```bash
echo "hello world" > test.txt
```

- Upload the file to the previously created folder in HDFS:

```bash
hdfs dfs -put test.txt /<folder_name>/test.txt
```

### 3. Verify the Uploaded File

- View the contents of the uploaded file in HDFS:

```bash
hdfs dfs -cat /<folder_name>/test.txt
```

## Running a MapReduce Job

Execute a sample MapReduce job to calculate the value of Pi:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 16 1000
```

This command uses 16 mappers and performs 1,000 iterations to estimate the value of Pi.


## File Descriptions

- **Dockerfile**: Defines the custom Hadoop Docker image.
- **docker-compose.yml**: Specifies the multi-node cluster configuration with one master and multiple workers.
- **core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml**: Hadoop configuration files to set up HDFS, MapReduce, and YARN.

## Notes

- Ensure all configuration files (e.g., `core-site.xml`, `hdfs-site.xml`) are correctly placed and customized for your setup before starting the cluster.
- Use `docker logs <container_name>` to debug any issues.
