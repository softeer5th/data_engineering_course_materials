# building the Docker image

1. 베이스 이미지로 우분투 선택

2. 환경변수 하둡, 자바 설정

3. apt-get update, java 8, ssh , vi, 설치 후 캐시 클린

4. 하둡 설치(용량 문제로 로컬에서 복사 후 압축 해제하고 압축 파일 삭제)

5. 자동 실행을 위해 ssh 암호키 제거

6. 하둡 환경 변수에 유저(루트) 추가

7. Configuration 카피

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
hdfs-user 폴더 만들고 거기에 cat.jpg 저장함

2. Mapreduce 일단 localhost:8088 yarn webui는 들어왔는데 아직 어떤 job을 실행시켜야 할지 모르겠다. m3에서 word count를 하자.

