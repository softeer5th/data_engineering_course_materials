# Base image with OpenJDK and Python
FROM openjdk:8-jdk-slim

# Install Python and other required dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_VERSION=3.4.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Download and extract Apache Spark
RUN curl -L https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Set working directory
WORKDIR /opt/spark

# Copy Spark job script into the container
COPY pi.py /opt/spark/jobs/pi.py

# Expose Spark master and Web UI ports
EXPOSE 7077 8080 8081

# Entry point
CMD ["bash"]
