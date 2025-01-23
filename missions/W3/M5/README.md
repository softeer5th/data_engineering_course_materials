# M6
## 파일 구조
```
.
├─ code
│  ├─ get_data.py
│  ├─ mapper.py
│  ├─ reducer.py
│  └─ require.txt
├─ config
│  ├─ core-site.xml
│  ├─ hdfs-site.xml
│  ├─ mapred-site.xml
│  └─ yarn-site.xml
├─ db
│  ├─ data-1
│  ├─ data-2
│  ├─ data-3
│  └─ name
├─ res
├─ docker-compose.yml
├─ Dockerfile
├─ README.md
└─ runner.sh
```

## 사전 준비
- Docker 설치

## 1단계: 저장소 클론
```bash
git clone https://github.com/cleonno3o/softeer_de_repo.git
git checkout joosumin-w3
cd missions/W3/M5
```

## 2단계: Hadoop 클러스터 빌드 및 시작
### Docker 이미지 빌드
```bash
docker build -t hadoop .
```
### 컨테이너 시작
```bash
docker-compose up -d
```

### 서비스 확인
- **HDFS 웹 UI**: [http://localhost:9870](http://localhost:9870)
- **YARN 웹 UI**: [http://localhost:8088](http://localhost:8088)

## 3단계: 클러스터 중지
```bash
docker-compose down
```
## 예제 수행
아래를 실행하면 예제 데이터에 대해 mapreduce과정을 수행
```bash
sh runner.sh
```
### HDFS에 입력 데이터 업로드
code 폴더에 저장한 파일은 runner.sh 수행 시 업로드 진행
### 출력 데이터 검색
출력 결과는 /res 폴더에 복사되어 저장

### 데이터셋 출처
MovieLens: https://grouplens.org/datasets/movielens/20m/