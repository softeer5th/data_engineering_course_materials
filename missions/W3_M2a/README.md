## 도커 컨테이너 시작

- Docker 이미지 빌드
docker-compose build

- 컨테이너 실행
docker-compose up -d

- 마스터 노드에 접속
docker exec -it hadoop-master bash

- ssh 연결되어 있는지 확인하고, 연결되어 있지 않다면 연결하기
service ssh status</br>
sudo service ssh start</br>

## 하둡 서비스 시작
- start-dfs.sh
#서비스 상태 확인
- jps 
(NameNode, SecondaryNameNode, DataNode, 그리고 ResourceManager 나오는지 확인)

- 설정된 NameNode 저장소 디렉토리가 없거나 잘못되었을 수 있다. 디렉토리를 수동으로 생성하고 올바른 권한을 설정하기

sudo mkdir -p /usr/local/hadoop/data/namenode</br>
sudo chown -R hadoop:hadoop /usr/local/hadoop/data/namenode</br>
sudo mkdir -p /usr/local/hadoop/data/datanode</br>
sudo chown -R hadoop:hadoop /usr/local/hadoop/data/datanode</br>

#처음에 네임노드 포맷 확인(기존에 저장된 데이터 있으면 삭제됨)</br>
$HADOOP_HOME/bin/hdfs namenode -format </br>

## 하둡 master에서

#HDFS 상태 테스트
- NameNode 상태 확인</br>
NameNode의 웹 UI로 접속해 상태를 확인한다.</br>
http://<master-ip>:9870

- DataNode 상태 확인</br>
docker exec -it master bash -c "hdfs dfsadmin -report"

- 예상 결과: </br>
Configured Capacity와 DFS Remaining 값을 확인한다.</br>
적어도 하나의 DataNode가 Live nodes 섹션에 표시되어야 한다.</br>

- NameNode가 시작되지 않은 상태이면 직접 실행하여 오류를 확인한다.</br>
$HADOOP_HOME/bin/hdfs namenode</br>


## 하둡 worker에서
- $HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager</br>
#DataNode를 수동으로 다시 시작하고 싶다면
- $HADOOP_HOME/bin/hdfs --daemon start datanode </br>

- jps 로 SecondaryNameNode, NodeManager 확인하기


#live datanode 한 개 이상 있는지 확인하기</br>
hdfs dfsadmin -report 


## HDFS operation 확인
- HDFS 디렉토리 확인</br>
hdfs dfs -ls /

- HDFS 폴더 생성</br>
hdfs dfs -mkdir /user/test

- 파일 생성</br>
echo 'Hello, Hadoop!' > /tmp/testfile.txt

- HDFS에 파일 업로드</br>
hdfs dfs -put /tmp/testfile.txt /user/test

- 업로드된 파일 확인</br>
hdfs dfs -ls /user/test

- HDFS에 저장된 파일 다운로드</br>
hdfs dfs -get /user/test/testfile.txt /tmp/test_file_downloaded.txt

- 다운로드된 파일 확인</br>
cat /tmp/test_file_downloaded.txt


## yarn 작동하는지 테스트 코드 실행(마스터에서, worker도 실행해야 함)
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 16 1000 </br>

결과: 아래처럼 나온다</br>
Estimated value of Pi is 3.14250000000000000000</br>

### 컨테이너 종료
docker-compose down


## M3 이어서 하기
- 로컬에서 컨테이너로 파일을 복사:</br>
docker cp Harry_Potter_1.txt hadoop-master:/tmp/</br>

텍스트 파일 출처: https://github.com/amephraim/nlp/blob/master/texts/J.%20K.%20Rowling%20-%20Harry%20Potter%201%20-%20Sorcerer's%20Stone.txt

- 컨테이너 내부로 접속</br>
docker exec -it hadoop-master bash</br>

- HDFS로 파일 업로드</br>
hdfs dfs -put /tmp/Harry_Potter_1.txt /tmp/</br>

- HDFS에 업로드된 파일 확인</br>
hdfs dfs -ls /tmp/</br>

- mapper과 reducer 파일 컨테이너로 복사</br>
docker cp mapper.py hadoop-master:/tmp/</br>
docker cp reducer.py hadoop-master:/tmp/</br>

- mapper 함수 잘 실행되는지 확인</br>
echo "Harry Potter and the Sorcerer's Stone" | /usr/bin/python3 /tmp/mapper.py</br>

- reducer 함수 잘 실행되는지 확인</br>
echo -e "apple\t1\nbanana\t1\napple\t1\norange\t1\nbanana\t1" | /usr/bin/python3 /tmp/reducer.py</br>

- mapper, reducer 파일 사용 권한 주기
sudo chmod +x /tmp/mapper.py</br>
sudo chmod +x /tmp/reducer.py</br>

-- 여기까지 실행됨</br>----------

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper '/usr/bin/python3 /tmp/mapper.py' \
    -reducer '/usr/bin/python3 /tmp/reducer.py' \
    -input /tmp/Harry_Potter_1.txt \
    -output /tmp/output</br>
(-input과 -output 경로는 HDFS 경로여야 한다.)</br>



