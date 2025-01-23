#!bin/bash
cd "$(dirname "$0")" || exit
docker exec master mkdir -p /code
docker cp code/. master:/code/

# 필요한 라이브러리 설치
docker exec master pip install -r /code/require.txt

# 데이터 가져오기
docker exec master python /code/get_data.py

# 데이터 hdfs에 저장
docker exec master hdfs dfs -mkdir /data
docker exec master hdfs dfs -put /code/ml-20m/ratings.csv /data

# mapreduce 수행
docker exec master chmod +x /code/mapper.py
docker exec master chmod +x /code/reducer.py

docker exec master hdfs dfs -rm -r /output
docker exec master hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -input /data \
    -output /output \
    -mapper mapper.py \
    -reducer reducer.py \
    -file /code/mapper.py \
    -file /code/reducer.py

# 결과 가져오기
docker exec master mkdir -p /res
docker exec master rm -r /res/output
docker exec master hdfs dfs -get /output /res
docker cp master:/res/output/. res/