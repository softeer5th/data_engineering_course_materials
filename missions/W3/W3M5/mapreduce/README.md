# W3M5 - Average Rating of Movies using MapReduce

## Download Data
[MovieLens 20M Dataset](https://grouplens.org/datasets/movielens/20m/)
- ratings.csv만 사용

## File Copy to Container (W3M2/hadoop-docker-multi 기준)
```bash
docker cp ../../W3M5/mapreduce/mapper.py hadoop-master:opt/hadoop-3.4.0/mapreduce/movie/
docker cp ../../W3M5/mapreduce/reducer.py hadoop-master:opt/hadoop-3.4.0/mapreduce/movie/
docker cp ../../W3M5/mapreduce/ratings.csv hadoop-master:opt/hadoop-3.4.0/mapreduce/movie/
```

## mapper, reducer 권한 설정
```bash
chmod 777 /opt/hadoop-3.4.0/mapreduce/movie/mapper.py
chmod 777 /opt/hadoop-3.4.0/mapreduce/movie/reducer.py
```

## File Copy to HDFS
```bash
hdfs dfs -put /opt/hadoop-3.4.0/mapreduce/movie/ratings.csv /user/hdfs/input
```

## MapReduce 실행 (movie_output 디렉토리 없는 상태여야 함)
```bash
hadoop jar /opt/hadoop-3.4.0/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar \
    -input /user/hdfs/input/ratings.csv \
    -output /user/hdfs/movie_output/ \
    -mapper /opt/hadoop-3.4.0/mapreduce/movie/mapper.py \
    -reducer /opt/hadoop-3.4.0/mapreduce/movie/reducer.py \
    -file /opt/hadoop-3.4.0/mapreduce/movie/mapper.py \
    -file /opt/hadoop-3.4.0/mapreduce/movie/reducer.py
```

## MapReduce 결과 확인
```bash
hdfs dfs -cat /user/hdfs/movie_output/part-00000
```

## File Download to Container
```bash
hdfs dfs -get /user/hdfs/movie_output/part-00000 /opt/hadoop-3.4.0/mapreduce/movie/output/
```