# 🚀 Apache Spark Standalone Cluster with Docker

This project sets up an **Apache Spark Standalone Cluster** using Docker and Docker Compose. The cluster includes:

- **1 Spark Master**
- **2 Spark Workers**
- **Pre-installed Java, Python, and Spark**

With this setup, you can submit Spark jobs and process distributed data using Spark's powerful capabilities.

---

## 🛠 **Prerequisites**

Ensure you have the following installed:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

You can verify your installation by running:

```bash
docker --version
# Example output: Docker version 24.0.5

docker-compose --version
# Example output: Docker Compose version 2.19.1
```

---

## 📂 **Project Structure**

```
spark-cluster/
│── docker-compose.yml  # Docker Compose configuration
│── Dockerfile          # Custom Docker image with Spark, Java, Python
│── scripts/
│   ├── start-master.sh # Spark Master startup script
│   ├── start-worker.sh # Spark Worker startup script
│── jobs/
│   ├── pi.py           # Sample Spark Job (Monte Carlo Pi Estimation)
│── output/             # Directory for storing Spark job results
```

---

## 🚀 **How to Set Up and Run the Cluster**

### 1️⃣ **Build and Start the Cluster**

```bash
docker-compose build --no-cache
docker-compose up -d
```

After starting, check the running containers:

```bash
docker ps
```

You should see containers for **Spark Master** and **Spark Workers**.

### 2️⃣ **Access the Spark Web UI**

Once the cluster is running, open **[http://localhost:8080](http://localhost:8080)** to access the Spark Master Web UI.

### 3️⃣ **Submit a Spark Job**

Submit a sample job (`pi.py`) to estimate π using Monte Carlo method:

```bash
bash scripts/submit-job.sh
```

After execution, the results will be saved in the `output/` directory.

### 54️⃣ **Check Job Output**

```bash
cat output/pi_estimate.csv/part-00000-*.csv
```

OR

```bash
docker exec -it spark-master cat /opt/spark/output/pi_estimate.csv/part-00000-*.csv
```

---

## 🛠 **Configuration Details**

### 🔹 **Dockerfile Overview**

- **Base Image**: OpenJDK 11
- **Installs**: Python, Apache Spark 3.3.2
- **Sets Up**: Spark Master and Workers
- **Creates Output Directory**: `/opt/spark/output`

### 🔹 **docker-compose.yml Overview**

- Defines services for **Spark Master** and **Spark Workers**
- Mounts **jobs/** directory for Spark scripts
- Mounts **output/** directory for saving results

---

## ❌ **Stopping and Cleaning Up**

To stop and remove the containers:

```bash
docker-compose down
```

To remove all images:

```bash
docker rmi $(docker images -q) -f
```

---
