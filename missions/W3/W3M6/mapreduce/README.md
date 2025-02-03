# W3M6 - Amazon Product Review using MapReduce

## Download Data
[Amazon Reviews'23](https://amazon-reviews-2023.github.io/)
- Clothing_Shoes_and_Jewelry.jsonl만 사용

## File Copy to Container (W3M2/hadoop-docker-multi 기준)
```bash
docker cp ../../W3M6/mapreduce/mapper.py hadoop-master:opt/hadoop-3.4.0/mapreduce/amazon/
docker cp ../../W3M6/mapreduce/reducer.py hadoop-master:opt/hadoop-3.4.0/mapreduce/amazon/
docker cp ../../W3M6/mapreduce/Clothing_Shoes_and_Jewelry.jsonl hadoop-master:opt/hadoop-3.4.0/mapreduce/amazon/
```

## mapper, reducer 권한 설정
```bash
chmod 777 /opt/hadoop-3.4.0/mapreduce/amazon/mapper.py
chmod 777 /opt/hadoop-3.4.0/mapreduce/amazon/reducer.py
```

## File Copy to HDFS
```bash
hdfs dfs -put /opt/hadoop-3.4.0/mapreduce/amazon/Clothing_Shoes_and_Jewelry.jsonl /user/hdfs/input
```

## MapReduce 실행 (movie_output 디렉토리 없는 상태여야 함)
```bash
hadoop jar /opt/hadoop-3.4.0/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar \
    -input /user/hdfs/input/Clothing_Shoes_and_Jewelry.jsonl \
    -output /user/hdfs/amazon_output/ \
    -mapper /opt/hadoop-3.4.0/mapreduce/amazon/mapper.py \
    -reducer /opt/hadoop-3.4.0/mapreduce/amazon/reducer.py \
    -file /opt/hadoop-3.4.0/mapreduce/amazon/mapper.py \
    -file /opt/hadoop-3.4.0/mapreduce/amazon/reducer.py
```

## MapReduce 결과 확인
```bash
hdfs dfs -cat /user/hdfs/amazon_output/part-00000
```

## File Download to Container
```bash
hdfs dfs -get /user/hdfs/amazon_output/part-00000 /opt/hadoop-3.4.0/mapreduce/amazon/output/
```