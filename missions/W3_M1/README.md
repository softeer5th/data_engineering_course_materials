# Week03 Mission1 실행 instruction

1. 로컬에 하둡 다운로드 받기
- https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/ 에서
- hadoop-3.4.0.tar.gz 파일 다운로드 받기

+) 맥 설정 추가하기 </br>
https://velog.io/@boyunj0226/M1-맥에서-Hadoop-Spark-설치하기</br>


2. 도커 이미지 빌드
- docker build -t hadoop-single .

3. 컨테이너 실행
- docker run -it --name single-cluster -p 9870:9870 -p 9000:9000 hadoop-single

4. 터미널에서 컨테이너 내부로 들어가기
- docker exec -it single-cluster bash

5. 컨테이너 내부에서 hdfs 초기화(처음에만 하기)
- hdfs namenode -format

#ssh 연결 확인 및 연결하기
- service ssh status로 ssh 연결되어 있는지 확인하기
- sudo service ssh start

6. 하둡 서비스 시작
- start-dfs.sh
#서비스 상태 확인
- jps 

7. 하둡 operations
#폴더 생성
- hdfs dfs -mkdir /user 
- hdfs dfs -ls /
#파일 만들기
- echo "Hello Hadoop" > sample.txt 
#HDFS로 파일 업로드   
- hdfs dfs -put sample.txt /user/kga    
#파일 확인
- hdfs dfs -ls /user/kga 
#파일 다운로드
- hdfs dfs -get /user/kga/sample.txt downloaded_sample.txt 

- http://localhost:9870 실행해보기