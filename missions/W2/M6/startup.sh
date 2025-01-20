#! /bin/bash
sudo yum update -y
# 스왑 메모리 설정
sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# 스왑 메모리 설정 영구화
echo "/swapfile swap swap defaults 0 0" | sudo tee -a /etc/fstab
# /etc/fstab 파일 내용 예시: fstab은 파일 시스템 테이블을 나타내는 파일로, 부팅 시 자동으로 마운트할 디스크를 설정할 수 있습니다.
# <file system>  <mount point>  <type>  <options>  <dump>  <pass>

# 스왑 메모리 사용 빈도 조정
sudo sysctl vm.swappiness=10
echo "vm.swappiness = 10" | sudo tee -a /etc/sysctl.conf

# RAM, 스왑 메모리, DISK IO 확인


# 패키지 업데이트 및 필요한 도구 설치
sudo yum install -y wget

# CloudWatch 에이전트 설치를 위한 사전 작업
sudo yum install -y amazon-ssm-agent
sudo systemctl enable amazon-ssm-agent
sudo systemctl start amazon-ssm-agent

# CloudWatch Agent 설치
cd /var
sudo wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm

# cwagent 사용자 생성 (자동생성됨)
# sudo useradd -rs /sbin/nologin cwagent

# 임시 디렉토리에 설정 파일 먼저 생성
sudo cat << 'EOF' > /tmp/amazon-cloudwatch-agent.json
{
    "agent": {
        "metrics_collection_interval": 10,
        "run_as_user": "cwagent"
    },
    "metrics": {
        "append_dimensions": {
            "AutoScalingGroupName": "${aws:AutoScalingGroupName}",
            "ImageId": "${aws:ImageId}",
            "InstanceId": "${aws:InstanceId}",
            "InstanceType": "${aws:InstanceType}"
        },
        "metrics_collected": {
            "cpu": {
                "measurement": [
                    "cpu_usage_idle",
                    "cpu_usage_iowait",
                    "cpu_usage_user",
                    "cpu_usage_system"
                ],
                "metrics_collection_interval": 10,
                "resources": [
                    "*"
                ],
                "totalcpu": false
            },
            "disk": {
                "measurement": [
                    "used_percent",
                    "inodes_free"
                ],
                "metrics_collection_interval": 10,
                "resources": [
                    "*"
                ]
            },
            "diskio": {
                "measurement": [
                    "io_time",
                    "write_bytes",
                    "read_bytes",
                    "writes",
                    "reads"
                ],
                "metrics_collection_interval": 10,
                "resources": [
                    "*"
                ]
            },
            "mem": {
                "measurement": [
                    "mem_used_percent"
                ],
                "metrics_collection_interval": 10
            },
            "netstat": {
                "measurement": [
                    "tcp_established",
                    "tcp_time_wait"
                ],
                "metrics_collection_interval": 10
            },
            "swap": {
                "measurement": [
                    "swap_used_percent"
                ],
                "metrics_collection_interval": 10
            }
        }
    }
}
EOF

# 디렉토리 생성 및 권한 설정
sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
sudo cp /tmp/amazon-cloudwatch-agent.json /opt/aws/amazon-cloudwatch-agent/etc/
sudo chown cwagent:cwagent /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
sudo chmod 640 /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# 설정 파일 적용 및 에이전트 시작
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -m ec2 -a start

# 서비스 활성화
sudo systemctl enable amazon-cloudwatch-agent

# 한국 시간대 설정
sudo rm -f /etc/localtime
sudo ln -s /usr/share/zoneinfo/Asia/Seoul /etc/localtime

# 로그 확인
# sleep 30
# sudo cat /opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log

# Docker 설치

sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
sudo chkconfig docker on

# ECR 로그인을 위한 변수 설정
export AWS_REGION="ap-northeast-2"  # 사용하는 리전으로 변경
export ECR_REGISTRY="public.ecr.aws/s0p0f0l3"  # AWS 계정 ID로 변경
export REPOSITORY_NAME="ket0825/jupyterlab"  # ECR 레포지토리 이름으로 변경
export IMAGE_TAG="latest"  # 사용할 이미지 태그로 변경

# ECR 로그인
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

export IMAGE_URL=${ECR_REGISTRY}/${REPOSITORY_NAME}:${IMAGE_TAG}
# Docker 이미지 풀
docker pull ${IMAGE_URL}

# Docker 컨테이너 실행 
docker run -d \
   -p 10000:8888 \
   -e JUPYTER_PASSWORD="0543" \
   ${IMAGE_URL}

<persist>true</persist>

cat << 'EOF' > /tmp/amazon-cloudwatch-agent.json
{
    "agent": {
        "metrics_collection_interval": 10,
        "run_as_user": "cwagent"
    },
    "metrics": {
        "append_dimensions": {
            "AutoScalingGroupName": "${aws:AutoScalingGroupName}",
            "ImageId": "${aws:ImageId}",
            "InstanceId": "${aws:InstanceId}",
            "InstanceType": "${aws:InstanceType}"
        },
        "metrics_collected": {
            "disk": {
                "measurement": [
                    "used_percent",
                    "inodes_free"
                ],
                "metrics_collection_interval": 10,
                "ignore_file_system_types": [
                    "overlay", "overlay2", "tmpfs", "devtmpfs", "none"
                ],
                "drop_device": true,
                "mount_points": [
                    "/",
                    "/home"
                ]
            },
            "mem": {
                "measurement": [
                    "mem_used_percent",
                    "mem_total",
                    "mem_used",
                    "mem_available"
                ],
                "metrics_collection_interval": 10
            }
        }
    }
}
EOF



# 파일 복사 및 권한 설정
sudo cp /tmp/amazon-cloudwatch-agent.json /opt/aws/amazon-cloudwatch-agent/etc/
sudo chown cwagent:cwagent /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
sudo chmod 640 /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# 에이전트 재시작
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -m ec2 -a stop
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -m ec2 -a start