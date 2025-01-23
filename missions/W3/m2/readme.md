# building the Docker image

1. 베이스 이미지로 우분투 선택

2. 환경변수 하둡, 자바 설정

3. apt-get update, java 8, ssh , vi, 설치 후 캐시 클린

4. 하둡 설치(용량 문제로 로컬에서 복사 후 압축 해제하고 압축 파일 삭제)

5. 자동 실행을 위해 ssh 암호키 제거

6. 하둡 환경 변수에 유저(루트) 추가

7. Configuration 카피

# Hadoop configurations
1. core-site.xml
- fs.defaultFS
하둡에서 사용할 기본 파일시스템을 지정하고 네임노드와 사용할 포트 지정
hdfs://namenode:9000
  - hdfs를 사용하고 네임노드 호스트네임은 namenode, 포트는 9000을 사용하겠다.

2. hdfs-site.xml
- dfs.replication
데이터 노드에 복제해놓을 갯수

- dfs.hosts
호스트 리스트를 담은 파일 위치

- dfs.namenode.name.dir
네임노드가 dfs를 구성하면서 사용할 디렉토리

- dfs.datanode.name.dir
데이터노드가 dfs를 구성하면서 사용할 디렉토리

3. yarn-site.xml
- yarn.resourcemanager.address
yarn 레이어에서 리소스 매니저의 위치
namenode:8032
호스트 이름에 포트 지정해줌

- yarn.resourcemanager.hostname
호스트 네임 지정
start-yarn.sh 에서 쓰는 것 같다.

- yarn.resourcemanager.scheduler.address
- yarn.resourcemanager.webapp.address
스케쥴러랑 웹앱 어드레스인데 다 네임노드에 박아놓고 포트 지정해줌

4. mapred-site.xml
- mapreduce.framework.name
맵 리듀스 어플리케이션 레이어에서 어떤 자원 관리 프레임워크를 선택할 것인가?
yarn으로 설정

- mapreduce.jobhistory.address
- mapreduce.jobhistory.webapp.address
잡히스토리 서버 위치, 웹앱 위치. 호스트네임하고 포트

# running the containers

1. docker-compose.yaml로 컨테이너 구동

2. 4개의 서비스(네임노드 겸 마스터 노드 1, 데이터 노드 겸 슬레이브 노드 3)

3. 네임 노드 
3-1. 네임노드 포트 바인딩

3-2. 슬레이브로 쓸 워커 파일 삭제 후 재생성, 데이터 노드 호스트 네임 추가

3-3. ssh start

3-4 네임노드 포맷

3-5 start dfs, start yarn

4. datanodes

4-1. ssh 열어주고 대기

# performing HDFS and MapReduce operations
1. Hdfs -> m1과 동일하게 저장하면 데이터 노드에 configuration에서  
hdfs-user 폴더 만들고 거기에 cat.jpg 저장함.
-> datanode-3에 blk_1073741826과 메타데이터가 생성되었따.
리트라이브하여 도커 컨테이너 내부 리눅스 파일시스템에 다시 가져올 수도 있었다.

2. Mapreduce 일단 localhost:8088 yarn webui는 들어왔는데 아직 어떤 job을 실행시켜야 할지 모르겠다. m3에서 word count를 하자.