# 개요
hadoop을 single-node로 실행해보는 파트입니다.

# 실행방법
0. M1 폴더로 이동합니다
1. 하둡 파일을 다운로드 받습니다.
```sh
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```
2. 배치 파일을 이용하여 이미지를 빌드합니다.
```sh
./dockerBuild.sh
```
3. 배치 파일을 이용하여 컨테이너를 만듧니다.
```sh
./dockerContainerCreate.sh
```
4. 배치 파일을 이용하여 각 컨테이너를 실행합니다.
```sh
./dockerContainerStart.sh
```
5. 브라우저를 localhost:8088 혹은 localhost:9870을 접속합니다.

# Master에 접속 및 작업
다음 명령어로 Master에 접속 가능합니다:
```sh
docker container exec -it hadoop-master bash
```

# 종료
1. 도커 컨테이너를 배치파일을 이용해 종료합니다.
```sh
./dockerContainerStop.sh
```
2. 도커 컨테이너를 배치파일을 이용해 제거합니다.
