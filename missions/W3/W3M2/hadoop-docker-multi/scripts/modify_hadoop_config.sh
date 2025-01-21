
#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <config_dir>"
    exit 1
fi

CONFIG_DIR=$1

create_directories() {
    local directories=("/hadoop/tmp" "/hadoop/dfs/name" "/hadoop/dfs/data")
    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p $dir
            chown -R hdfs:hdfs $dir
        fi
    done
}

backup_file() {
    local file_path=$1
    local backup_path="${file_path}.bak"
    cp "$file_path" "$backup_path"
    echo "Backing up $(basename "$file_path")..."
}

modify_xml() {
    local file_path=$1
    local property=$2
    local value=$3

    if grep -q "<name>$property</name>" "$file_path"; then
        # Update existing property value
        sed -i "/<name>$property<\/name>/{n;s|<value>.*</value>|<value>$value</value>|;}" "$file_path"
    else
        # Add new property with proper indentation
        sed -i "/<\/configuration>/i\    <property>\n        <name>$property</name>\n        <value>$value</value>\n    </property>" "$file_path"
    fi

    echo "Modifying $(basename "$file_path")..."
}

restart_hadoop_services() {
    if [[ "$HOSTNAME" == "hadoop-master" ]]; then
        echo "Stopping Hadoop DFS..."
        $HADOOP_HOME/bin/hdfs --daemon stop namenode
        $HADOOP_HOME/bin/hdfs --daemon stop secondarynamenode
        echo "Stopping YARN..."
        $HADOOP_HOME/bin/yarn --daemon stop resourcemanager
        $HADOOP_HOME/bin/yarn --daemon stop nodemanager
        jps
        echo "Starting Hadoop DFS..."
        $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
        $HADOOP_HOME/bin/hdfs --daemon start namenode
        $HADOOP_HOME/bin/hdfs --daemon start secondarynamenode
        echo "Starting YARN..."
        $HADOOP_HOME/bin/yarn --daemon start resourcemanager
        $HADOOP_HOME/bin/yarn --daemon start nodemanager
        jps
        su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/"
        su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/root/"
        su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -chown root:root /user/root"
        su - hdfs -c "$HADOOP_HOME/bin/hdfs dfs -chown root:root /"
    else
        echo "Stopping Hadoop DFS..."
        $HADOOP_HOME/bin/hdfs --daemon stop datanode
        echo "Stopping YARN..."
        $HADOOP_HOME/bin/yarn --daemon stop nodemanager
        echo "Starting Hadoop DFS..."
        $HADOOP_HOME/bin/hdfs --daemon start datanode
        echo "Starting YARN..."
        $HADOOP_HOME/bin/yarn --daemon start nodemanager
    fi
    echo "Configuration changes applied and services restarted."
}

main() {
    backup_file "$CONFIG_DIR/core-site.xml"
    modify_xml "$CONFIG_DIR/core-site.xml" "fs.defaultFS" "hdfs://hadoop-master:9000"
    modify_xml "$CONFIG_DIR/core-site.xml" "hadoop.tmp.dir" "/hadoop/tmp"
    modify_xml "$CONFIG_DIR/core-site.xml" "io.file.buffer.size" "131072"

    backup_file "$CONFIG_DIR/hdfs-site.xml"
    modify_xml "$CONFIG_DIR/hdfs-site.xml" "dfs.replication" "2"
    modify_xml "$CONFIG_DIR/hdfs-site.xml" "dfs.blocksize" "134217728"
    modify_xml "$CONFIG_DIR/hdfs-site.xml" "dfs.namenode.name.dir" "/hadoop/dfs/name"
    modify_xml "$CONFIG_DIR/hdfs-site.xml" "dfs.datanode.data.dir" "/hadoop/dfs/data"

    backup_file "$CONFIG_DIR/mapred-site.xml"
    modify_xml "$CONFIG_DIR/mapred-site.xml" "mapreduce.framework.name" "yarn"
    modify_xml "$CONFIG_DIR/mapred-site.xml" "mapreduce.jobhistory.address" "hadoop-master:10020"
    modify_xml "$CONFIG_DIR/mapred-site.xml" "mapreduce.task.io.sort.mb" "256"

    backup_file "$CONFIG_DIR/yarn-site.xml"
    modify_xml "$CONFIG_DIR/yarn-site.xml" "yarn.resourcemanager.address" "hadoop-master:8032"
    modify_xml "$CONFIG_DIR/yarn-site.xml" "yarn.nodemanager.resource.memory-mb" "8192"
    modify_xml "$CONFIG_DIR/yarn-site.xml" "yarn.scheduler.minimum-allocation-mb" "1024"
    
    create_directories
    restart_hadoop_services
}

main
