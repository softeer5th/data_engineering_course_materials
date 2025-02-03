
#!/bin/bash

execute_command() {
    local command=("$@")
    "${command[@]}"
}

check_value() {
    local command=("${@:1:${#@}-1}")
    local expected_value=${@: -1}
    local actual_value=$(execute_command "${command[@]}")

    local command_str="["
    for item in "${command[@]}"; do
        command_str+="'$item', "
    done
    command_str=${command_str%, }
    command_str+="]"

    if [[ "$actual_value" == "$expected_value" ]]; then
        echo "PASS: $command_str -> $actual_value"
    else
        echo "FAIL: $command_str -> $actual_value (expected $expected_value)"
    fi
}

# Verify Hadoop configurations
verify_hadoop_config() {
    local configs=(
        "hdfs getconf -confKey fs.defaultFS|hdfs://hadoop-master:9000"
        "hdfs getconf -confKey hadoop.tmp.dir|/hadoop/tmp"
        "hdfs getconf -confKey io.file.buffer.size|131072"
        "hdfs getconf -confKey dfs.replication|2"
        "hdfs getconf -confKey dfs.blocksize|134217728"
        "hdfs getconf -confKey dfs.namenode.name.dir|/hadoop/dfs/name"
        "hdfs getconf -confKey mapreduce.framework.name|yarn"
        "hdfs getconf -confKey mapreduce.jobhistory.address|hadoop-master:10020"
        "hdfs getconf -confKey mapreduce.task.io.sort.mb|256"
        "hdfs getconf -confKey yarn.resourcemanager.hostname|resourcemanager"
        "hdfs getconf -confKey yarn.nodemanager.resource.memory-mb|8192"
        "hdfs getconf -confKey yarn.scheduler.minimum-allocation-mb|1024"
    )

    for config in "${configs[@]}"; do
        local key=${config%%|*}
        local expected_value=${config##*|}
        IFS=' ' read -r -a command <<< "$key"
        check_value "${command[@]}" "$expected_value"
    done
}

verify_hdfs_replication() {
    local test_file="/tmp/testfile"
    echo "Hello Hadoop" > "$test_file"
    hdfs dfs -rm -f "$test_file"
    hdfs dfs -mkdir -p /tmp
    hdfs dfs -put "$test_file" "$test_file"
    
    local actual_replication=$(hdfs fsck "$test_file" -files -blocks -racks | grep -o 'Default replication factor:[0-9]*' | cut -d':' -f2 | xargs)
    local expected_replication="2"
    
    if [[ "$actual_replication" == "$expected_replication" ]]; then
        echo "PASS: Replication factor is $actual_replication"
    else
        echo "FAIL: Replication factor is $actual_replication (expected $expected_replication)"
    fi
}

# Run MapReduce job
verify_mapreduce_job() {
    hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 2 2
    if [ $? -eq 0 ]; then
        echo "PASS: MapReduce job ran successfully."
    else
        echo "FAIL: MapReduce job failed."
    fi
}

# Verify YARN ResourceManager total available memory
verify_yarn_memory() {
    local expected_memory="8192"
    local total_memory=0

    # Get the list of all nodes and pick the first one
    local node=$(yarn node -list | grep -o '^[^[:space:]]\+:[0-9]\+' | head -n 1)

    if [[ -z "$node" ]]; then
        echo "FAIL: No nodes found"
        return
    fi
    
    node_status=$(yarn node -status $node)
    memory_capacity=$(echo "$node_status" | grep 'Memory-Capacity' | awk '{print $NF}' | grep -o '[0-9]\+')

    if [[ -z "$memory_capacity" ]]; then
        echo "FAIL: Could not retrieve memory capacity"
        return
    fi

    total_memory=$((total_memory + memory_capacity))

    if [[ "$total_memory" == "$expected_memory" ]]; then
        echo "PASS: Total available memory is $total_memory MB for node $node"
    else
        echo "FAIL: Total available memory is $total_memory MB for node $node (expected $expected_memory MB)"
    fi
}

main() {
    echo "Verifying Hadoop configuration and settings..."

    verify_hadoop_config
    verify_hdfs_replication
    verify_mapreduce_job
    verify_yarn_memory

    echo "Verification completed."
}

main
