import os
import shutil
from xml.etree import ElementTree as ET

def backup_file(file_path) :
    backup_path = f"{file_path}.bak"
    shutil.copy(file_path, backup_path)
    print(f"Backup created: {backup_path}")

def modify_xml(file_path, changes) :
    tree = ET.parse(file_path)
    root = tree.getroot()
    for key, value in changes.items() :
        for element in root.findall(".//property") :
            if element.find("name").text == key :
                element.find("value").text = value
                print(f"Modified {key} to {value}")
        tree.write(file_path)
        print(f"File updated: {file_path}")

def restart_services() :
    os.system("stop-dfs.sh && stop-yarn.sh")
    os.system("start-dfs.sh && start-yarn.sh")
    print("Hadoop services restarted")

if __name__ == "__main__":
    config_dir = "/path/to/hadoop/etc/hadoop"
    files_and_changes = {
        "core-site.xml": {
            "fs.defaultFS": "hdfs://namenode:9000",
            "hadoop.tmp.dir": "/hadoop/tmp",
            "io.file.buffer.size": "131072"
        },
        "hdfs-site.xml": {
            "dfs.replication": "2",
            "dfs.blocksize": "134217728",
            "dfs.namenode.name.dir": "/hadoop/dfs/name"
        },
        "mapred-site.xml": {
            "mapreduce.framework.name": "yarn",
            "mapreduce.jobhistory.address": "namenode:10020",
            "mapreduce.task.io.sort.mb": "256"
        },
        "yarn-site.xml": {
            "yarn.resourcemanager.address": "namenode:8032",
            "yarn.nodemanager.resource.memory-mb": "8192",
            "yarn.scheduler.minimum-allocation-mb": "1024"
        }
    }
    for file_name, changes in files_and_changes.items():
        file_path = os.path.join(config_dir, file_name)
        backup_file(file_path)
        modify_xml(file_path, changes)
    restart_services()