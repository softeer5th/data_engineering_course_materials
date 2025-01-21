# Step 1: Base image (Ubuntu 20.04 LTS)
FROM ubuntu:20.04

# Step 2: Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk wget ssh rsync curl vim sudo && \
    apt-get clean

ENV HDFS_NAMENODE_USER=hdfs
ENV HDFS_DATANODE_USER=hdfs
ENV HDFS_SECONDARYNAMENODE_USER=hdfs
ENV YARN_RESOURCEMANAGER_USER=yarn
ENV YARN_NODEMANAGER_USER=yarn

# Create Hadoop users and give them sudo privileges
RUN useradd -ms /bin/bash hdfs && \
    useradd -ms /bin/bash yarn  && \
    passwd -d hdfs && passwd -d yarn && \
    echo "hdfs ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    echo "yarn ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

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
COPY configs/* ${HADOOP_HOME}/etc/hadoop/
COPY scripts/* /usr/local/bin/


USER hdfs
RUN ssh-keygen -t rsa -P '' -f /home/hdfs/.ssh/id_rsa && \
    cat /home/hdfs/.ssh/id_rsa.pub >> /home/hdfs/.ssh/authorized_keys && \
    chmod 0600 /home/hdfs/.ssh/authorized_keys

USER yarn
RUN ssh-keygen -t rsa -P '' -f /home/yarn/.ssh/id_rsa && \
    cat /home/yarn/.ssh/id_rsa.pub >> /home/yarn/.ssh/authorized_keys && \
    chmod 0600 /home/yarn/.ssh/authorized_keys


USER root

# Set ownership and permissions for Hadoop directories
RUN mkdir -p /hadoopdata/hdfs/namenode && \
    mkdir -p /hadoopdata/hdfs/datanode && \
    mkdir -p ${HADOOP_HOME}/logs && \
    chown -R hdfs:hdfs /opt/hadoop-${HADOOP_VERSION} /hadoopdata && \
    chmod -R 755 /opt/hadoop-${HADOOP_VERSION} /hadoopdata && \
    sudo chown -R hdfs:hdfs /opt/hadoop-3.4.0/logs && \
    chown -R hdfs:hdfs /hadoopdata/hdfs && \
    chown -R yarn:yarn /home/yarn/.ssh

RUN echo "export JAVA_HOME=$JAVA_HOME" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# Step 7: Format HDFS namenode
RUN ${HADOOP_HOME}/bin/hdfs namenode -format -force
    

# Step 8: Copy start script and make executable
USER hdfs
COPY --chown=hdfs:hdfs scripts/start-hadoop.sh /usr/local/bin/

USER root
RUN chmod +x /usr/local/bin/*.sh


# Step 10: Expose necessary ports (for web interface)
EXPOSE 9870 8088 9000 9864 8020 8042

# Set Hadoop user environment variables
# ENV HDFS_NAMENODE_USER=root
# ENV HDFS_DATANODE_USER=root
# ENV HDFS_SECONDARYNAMENODE_USER=root
# ENV YARN_RESOURCEMANAGER_USER=root
# ENV YARN_NODEMANAGER_USER=root

# Step 11: Run Hadoop services on container startup

CMD ["/usr/local/bin/start-hadoop.sh"]