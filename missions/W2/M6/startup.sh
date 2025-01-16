#! /bin/bash
sudo yum update -y
# 스왑 메모리 설정 추가.
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
sudo chkconfig docker on

# ECR 로그인을 위한 변수 설정
AWS_REGION="ap-northeast-2"  # 사용하는 리전으로 변경
ECR_REGISTRY="public.ecr.aws/s0p0f0l3"  # AWS 계정 ID로 변경
REPOSITORY_NAME="ket0825/jupyterlab"  # ECR 레포지토리 이름으로 변경
IMAGE_TAG="latest"  # 사용할 이미지 태그로 변경

# ECR 로그인
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

# Docker 이미지 풀
docker pull ${ECR_REGISTRY}/${REPOSITORY_NAME}:${IMAGE_TAG}

docker run -d \
   -p 10000:8888 \
   -e JUPYTER_PASSWORD="0543" \
   public.ecr.aws/s0p0f0l3/ket0825/jupyterlab:latest

# (선택사항) Docker 로그 확인
# docker logs my-container

<persist>true</persist>