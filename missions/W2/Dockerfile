FROM quay.io/jupyter/base-notebook:latest

# 작업 디렉터리 설정
WORKDIR /app

# root 권한으로 전환
USER root

# 시스템 패키지 설치
RUN apt update && \
    apt install -y --no-install-recommends \
    g++ \
    default-jre-headless \
    curl \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*


# 사용자 권한으로 전환
USER ${NB_UID}

# Python 패키지 목록 복사
COPY requirements.txt .

# Python 패키지 설치
RUN pip install -r requirements.txt

# 프로젝트 파일 복사
COPY . .

# juptyer notebook 실행
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]