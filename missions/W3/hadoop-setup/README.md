# 하둡 클러스터 설치 및 배포

## 파일 설명

* `.env`: docker compose 빌드 및 배포 시 사용되는 환경 변수를 정의 (docker-compose.yml과 같은 경로에 있으면 자동으로 적용됨.)

## 추가 예정 사항

* python 패키지 사용을 위해 base.Dockerfile에서 pip 설치 필요.

## 빌드 명령어

hadoop-base, hadoop-image 빌드

```bash
docker compose -f docker-compose.build.yml build
```

hadoop-image만 빌드 (hadoop-base에 대한 빌드 캐시가 있는 경우 위 명령어만 실행해도 상관 없음.)

```bash
docker compose -f docker-compose.build.yml build hadoop-image
```

하둡 서비스 실행

```bash
docker compose -f docker-compose.yml up -d
```