# W3M3 - Word Count using MapReduce

## E-Book Download
[Enchanted April](https://www.gutenberg.org/ebooks/16389)

## File Copy to Container (W3M2/hadoop-docker-multi 기준)
```bash
docker cp ../../W3M3/mapreduce/mapper.py hadoop-master:opt/hadoop-3.4.0/mapreduce/wordcount
docker cp ../../W3M3/mapreduce/reducer.py hadoop-master:opt/hadoop-3.4.0/mapreduce/wordcount
docker cp ../../W3M3/mapreduce/Enchanted_April.txt  hadoop-master:opt/hadoop-3.4.0/mapreduce/wordcount
```

## mapper, reducer 권한 설정
```bash
chmod 777 /opt/hadoop-3.4.0/mapreduce/wordcount/mapper.py
chmod 777 /opt/hadoop-3.4.0/mapreduce/wordcount/reducer.py
```

## File Copy to HDFS
```bash
hdfs dfs -put /opt/hadoop-3.4.0/mapreduce/wordcount/input/Enchanted_April.txt /user/hdfs/input
```

## MapReduce 실행 (wcoutput 디렉토리 없는 상태여야 함)
```bash
hadoop jar /opt/hadoop-3.4.0/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar \
    -input /user/hdfs/input/Enchanted_April.txt \
    -output /user/hdfs/wcoutput/ \
    -mapper /opt/hadoop-3.4.0/mapreduce/wordcount/mapper.py \
    -reducer /opt/hadoop-3.4.0/mapreduce/wordcount/reducer.py \
    -file /opt/hadoop-3.4.0/mapreduce/wordcount/mapper.py \
    -file /opt/hadoop-3.4.0/mapreduce/wordcount/reducer.py
```

## MapReduce 결과 확인
```bash
hdfs dfs -cat /user/hdfs/wcoutput/part-00000
```

## File Download to Container
```bash
hdfs dfs -get /user/hdfs/wcoutput/part-00000 /opt/hadoop-3.4.0/mapreduce/wordcount/output/
```