# Step 1: Base image (Ubuntu 20.04 LTS)
FROM ubuntu:20.04

# Step 2: Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk wget ssh rsync curl vim sudo && \
    apt-get clean

# Step 3: Set environment variables for Hadoop
ENV HADOOP_VERSION=3.4.0
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64

# Step 4: Copy Hadoop binaries and extract
COPY hadoop-${HADOOP_VERSION}-aarch64.tar.gz /opt/
RUN tar -xvzf /opt/hadoop-${HADOOP_VERSION}-aarch64.tar.gz -C /opt/ && \
    rm /opt/hadoop-${HADOOP_VERSION}-aarch64.tar.gz

# Step 5: Copy Hadoop configuration files
COPY core-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY mapred-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY yarn-site.xml ${HADOOP_HOME}/etc/hadoop/

# Step 6: Set up SSH (required for Hadoop services)
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -N "" && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys

# Step 7: Format HDFS namenode
RUN ${HADOOP_HOME}/bin/hdfs namenode -format -force

# Step 8: Copy start script and make executable
COPY start-hadoop.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-hadoop.sh

RUN echo "export JAVA_HOME=$JAVA_HOME" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# Step 9: Create HDFS storage directory
RUN mkdir -p /hadoopdata/hdfs/namenode && \
    mkdir -p /hadoopdata/hdfs/datanode && \
    chown -R root:root /hadoopdata

# Step 10: Expose necessary ports (for web interface)
EXPOSE 9870 8088 9000

# Step 11: Run Hadoop services on container startup
CMD ["start-hadoop.sh"]