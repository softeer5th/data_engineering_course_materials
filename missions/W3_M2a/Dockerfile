# Use a base image with OpenJDK (required for Hadoop)
FROM ubuntu:20.04

# Set environment variables
ENV HADOOP_VERSION=3.4.0
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    ssh \
    rsync \
    wget \
    vim \
    sudo \
    && apt-get clean

# Add a non-root user for Hadoop
RUN useradd -m hadoop && echo "hadoop:hadoop" | chpasswd

# Grant sudo privileges to hadoop user (optional, for debugging purposes)
RUN echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Configure SSH for passwordless access
RUN mkdir -p /home/hadoop/.ssh \
    && ssh-keygen -t rsa -P "" -f /home/hadoop/.ssh/id_rsa \
    && cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys \
    && chmod 600 /home/hadoop/.ssh/authorized_keys \
    && chown -R hadoop:hadoop /home/hadoop/.ssh \
    && echo "StrictHostKeyChecking=no" >> /etc/ssh/ssh_config

# Set the working directory
WORKDIR /home/hadoop

# Download and install Hadoop
COPY hadoop-3.4.0.tar.gz /tmp/
RUN tar -xvzf /tmp/hadoop-3.4.0.tar.gz -C /usr/local/ \
    && mv /usr/local/hadoop-3.4.0 $HADOOP_HOME \
    && rm /tmp/hadoop-3.4.0.tar.gz

# Copy configuration files
COPY ./config/core-site.xml $HADOOP_HOME/etc/hadoop/
COPY ./config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY ./config/mapred-site.xml $HADOOP_HOME/etc/hadoop/
COPY ./config/yarn-site.xml $HADOOP_HOME/etc/hadoop/

# Set permissions for Hadoop directory
RUN chown -R hadoop:hadoop $HADOOP_HOME

# Configure Hadoop environment
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo "export HADOOP_HOME=$HADOOP_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo "export PATH=$PATH" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Add Hadoop SSH Configurations (to communicate between nodes)
COPY start-hadoop.sh /home/hadoop/start-hadoop.sh
RUN chmod +x /home/hadoop/start-hadoop.sh && chown hadoop:hadoop /home/hadoop/start-hadoop.sh

# Switch to hadoop user to run Hadoop commands
USER hadoop

# Expose ports for Hadoop services
EXPOSE 9870 9864 9000 8088 8042

# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start; /home/hadoop/start-hadoop.sh; tail -f /dev/null"]

