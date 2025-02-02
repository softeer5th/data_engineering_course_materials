# Base Image: Use an appropriate Linux distribution
FROM ubuntu:24.04
# Set environment variables for Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/usr/local/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    ssh \
    rsync \
    vim \
    && apt-get clean
# Download and install Hadoop
RUN wget https://mirrors.sonic.net/apache/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar xvzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz
# Configure SSH for passwordless communication
RUN ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys
# Hadoop configuration files
COPY config/*.xml /usr/local/hadoop/etc/hadoop/
# Add JAVA_HOME to hadoop-env.sh
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# Add entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
# Expose HDFS
EXPOSE 9870 9000
# Set entrypoint script
ENTRYPOINT ["entrypoint.sh"]