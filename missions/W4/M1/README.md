1. Docker 이미지를 빌드
docker-compose build

2. 클러스터 시작
docker-compose up -d

3. master node와 workernode 잘 실행되는지 확인하기
- docker exec -it spark-master bash
- docker exec -it spark-worker-1 bash
- docker exec -it spark-worker-2 bash
- jps

3. Spark 작업 제출
chmod +x submit-job.sh
./submit-job.sh

4. Spark Web UI 확인
http://localhost:8080를 열어 작업 상태를 모니터링