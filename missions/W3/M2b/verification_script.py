import subprocess

def run_command(command) :
    try :
        result = subprocess.check_output(command, shell=True, text = True)
        return result.strip()
    except subprocess.CalledProcessError as e :
        print(f"Command failed: {command}")
        return None
    
def verify_setting(key, expected_value, command) :
    result = run_command(command)
    if result == expected_value :
        print(f"PASS: {key} -> {result}")
    else :
        print(f"FAIL: {key} -> {result} (expected {expected_value})")
    
if __name__ == "__main__":
    settings_to_verify = {
        "fs.defaultFS": ("hdfs://namenode:9000", "hdfs getconf -confKey fs.defaultFS"),
        "hadoop.tmp.dir": ("/hadoop/tmp", "hdfs getconf -confKey hadoop.tmp.dir"),
        "dfs.replication": ("2", "hdfs getconf -confKey dfs.replication"),
        "yarn.resourcemanager.address": ("namenode:8032", "yarn getconf -confKey yarn.resourcemanager.address"),
        "yarn.nodemanager.resource.memory-mb": ("8192", "yarn getconf -confKey yarn.nodemanager.resource.memory-mb")
    }

    for key, (expected_value, command) in settings_to_verify.items():
        verify_setting(key, expected_value, command)