docker build -f Dockerfile --platform linux/amd64 -t public.ecr.aws/d9y2a5z0/jupyter:latest ..
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/d9y2a5z0
docker push public.ecr.aws/d9y2a5z0/jupyter:latest