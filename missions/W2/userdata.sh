#!/bin/bash
sudo yum update -y
sudo yum install -y docker

sudo service docker start
sudo systemctl enable docker

sudo usermod -aG docker ec2-user

docker pull public.ecr.aws/d9y2a5z0/jupyter:latest
docker run -p 80:8888 public.ecr.aws/d9y2a5z0/jupyter