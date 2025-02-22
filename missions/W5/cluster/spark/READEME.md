# Apache Spark Cluster Setup with Docker

## Overview
This project sets up an Apache Spark cluster using Docker and Docker Compose. It includes a Spark master node and a worker node, allowing you to run distributed computing tasks.

## Prerequisites
Before you begin, ensure you have the following installed:
- Docker
- Docker Compose

## Project Structure
```
.
├─ Dockerfile
├─ READEME.md
├─ conf
│  └─ spark-defaults.conf
├─ data
│  ├─ input
│  │  └─ monte_carlo_pi.py
│  └─ output
├─ docker-compose.yml
├─ entrypoint.sh
├─ submission.sh
├─ logs
└─ requirements
   └─ requirements.txt
```

## Build the Docker Image
To build the Spark Docker image, run the following command:
```sh
docker build -t ubuntu/spark:latest .
```

## Start the Spark Cluster
Use Docker Compose to start the Spark master and worker nodes:
```sh
docker-compose up --scale spark-worker=2 -d
```
This will start the following services:
- Spark Master (accessible at `http://localhost:8080`)
- Spark Worker

## Submit a Spark Job
To submit a Spark job, use the provided `submission.sh` script:
```sh
./submission.sh
```
Ensure the script has execution permissions:
```sh
chmod +x submission.sh
```

## Verify the Results
After the job completes, check the output directory:
```sh
ls data/output
```
You can also monitor the Spark job progress via the Spark Web UI:
- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
- **Spark Worker UI:** [http://localhost:8081](http://localhost:8081) *(accessible from within the container)*

## Stopping the Cluster
To stop the Spark cluster, run:
```sh
docker-compose down
```

## Logs and Debugging
- Logs are stored in the `logs/` directory.
- Use `docker logs <container_id>` to view container logs.
- To access a running container's shell:
  ```sh
  docker exec -it spark-master /bin/bash
  ```

## Notes
- The Spark Master and Worker nodes communicate using the `spark-net` Docker network.
- The `entrypoint.sh` script configures Spark roles dynamically.