# 개요
hadoop을 single-node로 실행해보는 파트입니다.

# 실행방법
0. M1 폴더로 이동합니다
1. 하둡 파일을 다운로드 받습니다.
```sh
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```
2. 이미지를 빌드합니다.
```sh
docker image build -t hd-single .
```
3. 컨테이너를 만들고 실행합니다.
```sh
docker run -itd --name hadoop -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 9866:9866 -p 9867:9867 hd-single bash
```
4. 3번의 코드를 실행하면 명령어를 입력중이던 터미널이 docker 컨테이너에 접속이 되고, 하둡에서 필요한 작업은 계속해서 터미널에서 하면 됩니다.
5. 브라우저를 localhost:8088혹은 localhost:9870을 접속합니다.