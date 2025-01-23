# Multiple Node Cluster Hadoop

해당 Dockerfile은 hadoop을 도커로 실행시켜준다. <br>
이때, root user로 hadoop을 실행하면 보안상의 문제가 생길 수 있기에,<br>
```hadoop```이라는 유저를 만들어서 해당 유저로 실행하게 된다. <br>
hadoop 유저의 패스워드를 설정하기 위해, .env파일이 요구된다.

```.env``` 파일은 이와 같은 구조로, ```docker-compose.yml```과 같은 위치의 디렉토리에 있어야 된다.

> **.env**
>```
>HADOOP_USER_PWD=<password>
>```

## Docker compose & container 실행 명령어

### Hadoop service 시작하기
```docker compose up -d --build```
### Hadoop service 종료하기
```docker compose down```

### Hadoop service volume 삭제와 함께 종료하기
```docker compose down --volumes```

### Hadoop service 이미지 삭제와 함께 종료하기
```docker compose down --rmi local```

### Docker container로 접속
```docker exec -it hadoop-container /bin/bash```

### Docker로 파일 넣기
* 현재 docker compose가 있는 디렉토리에 ```shared-dir``` 폴더를 만들도록 하였다.
<br>이 폴더는 도커 내부의 ```/hadoop/shared```과 연동되어있다.
<br>해당 폴더에 파일을 추가하거나 가져올 수 있다.

## Docker 내부에서 hdfs 명령
### hdfs에 디렉토리 만들기 (mkdir)
```hdfs dfs -mkdir <your_directory_name>```
### hdfs에 파일 추가하기 (put)
```hdfs dfs -put <your_file_name> <dest_path_name>```
### hdfs에 file list 확인하기 (ls)
```hdfs dfs -ls <target_path_name>```
### hdfs에서 file 가져오기. (get)
```hdfs dfs -get <hdfs_path> <local_path>```

## Docker 내부에서 yarn & mapreduce 테스트하기
1. shared-dir 폴더에 ```mapper.py```, ```reducer.py```, ```inputfile```을 넣어준다.
2. input file을 ```hdfs dfs -put```명령어를 통해 hdfs 파일 시스템에 업로드한다.
3. ```
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /hadoop/mapper.py,/hadoop/reducer.py \
    -input <hdfs상 inputfile path> \
    -output <hdfs상 output path> \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"
   ```
   위와 같은 방식으로 reducer와 mapper의 위치를 files로 지정해주고, <br>
   input 파일의 위치 output 파일의 위치를 지정한다.
4. ```hdfs dfs -cat <output file> | head -10``` 과 같은 명령어를 통해 결과값의 상위 10개만 확인 가능하다.
5. ```hdfs dfs -get```을 통해서 hdfs 상에 올라가있는 결과 파일을 다운 받을 수 있다.

## 웹 UI로 접근하기
* http://127.0.0.1:9870/explorer.html#/ 를 통해서 hdfs에 접근 가능하다.
* http://127.0.0.1:8088 를 통해서 resource manager에 접근 가능하다.