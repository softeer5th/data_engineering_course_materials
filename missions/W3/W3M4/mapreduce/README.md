# W3M4 - Twitter Sentiment Analysis using MapReduce

## Download Data
[Twitter Data](https://www.kaggle.com/datasets/kazanova/sentiment140)

## File Copy to Container (W3M2/hadoop-docker-multi 기준)
```bash
docker cp ../../W3M4/mapreduce/mapper.py hadoop-master:opt/hadoop-3.4.0/mapreduce/twitter/
docker cp ../../W3M4/mapreduce/reducer.py hadoop-master:opt/hadoop-3.4.0/mapreduce/twitter/
docker cp ../../W3M4/mapreduce/twitter_data.csv hadoop-master:opt/hadoop-3.4.0/mapreduce/twitter/
```

## mapper, reducer 권한 설정
```bash
chmod 777 /opt/hadoop-3.4.0/mapreduce/twitter/mapper.py
chmod 777 /opt/hadoop-3.4.0/mapreduce/twitter/reducer.py
```

## File Copy to HDFS
```bash
hdfs dfs -put /opt/hadoop-3.4.0/mapreduce/twitter/twitter_data.csv /user/hdfs/input
```

## MapReduce 실행 (twitter_output 디렉토리 없는 상태여야 함)
```bash
hadoop jar /opt/hadoop-3.4.0/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar \
    -input /user/hdfs/input/twitter_data.csv \
    -output /user/hdfs/twitter_output/ \
    -mapper /opt/hadoop-3.4.0/mapreduce/twitter/mapper.py \
    -reducer /opt/hadoop-3.4.0/mapreduce/twitter/reducer.py \
    -file /opt/hadoop-3.4.0/mapreduce/twitter/mapper.py \
    -file /opt/hadoop-3.4.0/mapreduce/twitter/reducer.py
```

## MapReduce 결과 확인
```bash
hdfs dfs -cat /user/hdfs/twitter_output/part-00000
```

## File Download to Container
```bash
hdfs dfs -get /user/hdfs/twitter_output/part-00000 /opt/hadoop-3.4.0/mapreduce/twitter/output/
```