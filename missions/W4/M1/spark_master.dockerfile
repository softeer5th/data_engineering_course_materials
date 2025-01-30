# Base Image
FROM ubuntu:20.04

# Set environment variables
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

ENV SPARK_VERSION=3.5.4
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# Install necessary packages
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk ssh rsync && \
    apt-get clean

# Copy the SSH private and public keys from host to container
# This assumes the keys are on the host at ~/.ssh/hadoop_id_rsa and ~/.ssh/hadoop_id_rsa.pub
COPY id_rsa /root/.ssh/id_rsa
COPY id_rsa.pub /root/.ssh/id_rsa.pub

# Setup SSH authorized_keys for password-less SSH access
RUN mkdir -p ~/.ssh && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/id_rsa && \
    chmod 0600 ~/.ssh/id_rsa.pub

# Download and install Hadoop
COPY hadoop-$HADOOP_VERSION.tar.gz /hadoop-$HADOOP_VERSION.tar.gz
RUN tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Configure Hadoop
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site_master.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml
COPY workers $HADOOP_HOME/etc/hadoop/workers

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    $HADOOP_HOME/bin/hdfs namenode -format -y

# Spark 설치
COPY spark-${SPARK_VERSION}-bin-hadoop3.tar /spark-${SPARK_VERSION}-bin-hadoop3.tar
RUN tar -xf /spark-${SPARK_VERSION}-bin-hadoop3.tar && \
    mv spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tar

# Expose Hadoop ports
EXPOSE 8088 9870 9864 9866 9867

# Start Hadoop services
CMD service ssh start && \
    $HADOOP_HOME/sbin/start-dfs.sh && \ 
    $HADOOP_HOME/sbin/start-yarn.sh && \
    $SPARK_HOME/sbin/start-master.sh && \
    tail -f $SPARK_HOME/logs/spark--org.apache.spark.deploy.master.Master-*.out && \
    tail -f /dev/null