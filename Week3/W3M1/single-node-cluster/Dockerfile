# Base Image
FROM ubuntu:20.04

# Set environment variables
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

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

# Download and install Hadoop
COPY ./hadoop-$HADOOP_VERSION.tar.gz /hadoop-$HADOOP_VERSION.tar.gz
RUN tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Configure Hadoop
# Hadoop 설정 복사
COPY config/* /usr/local/hadoop/etc/hadoop/

# Configure SSH for Hadoop
RUN ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Expose ports
EXPOSE 8088 9870 9864 9866 9867

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Copy init script
COPY init.sh /init.sh

# Make init.sh executable
RUN chmod +x /init.sh

# Run init script
CMD ["sh", "/init.sh"]