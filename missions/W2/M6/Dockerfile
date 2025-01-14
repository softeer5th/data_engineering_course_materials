# FROM quay.io/jupyter/base-notebook

# WORKDIR /app

# COPY missions/W2 /app/

# RUN pip install -r requirements.txt

# # 879381276318.dkr.ecr.ap-northeast-2.amazonaws.com/ket0825/jupyterlab

FROM quay.io/jupyter/base-notebook:python-3.11
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libzmq3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

WORKDIR /app

COPY . /app

COPY missions/W2/M6/jupyter_server_config.py ~/.jupyter

RUN pip install --upgrade pip && \
    pip install pyzmq && \
    pip install -r missions/W2/requirements.txt

EXPOSE 10000

ENV PYTHONPATH="${PYTHONPATH}:/app" JUPYTER_PASSWORD=
ENV JUPYTER_CONFIG_DIR=/app/.jupyter
ENV JUPYTER_DATA_DIR=/app/.local/share/jupyter
ENV JUPYTER_RUNTIME_DIR=/app/.local/share/jupyter/runtime


CMD jupyter server \
    --ip=0.0.0.0 \
    --NotebookApp.password="$(python -c 'from jupyter_server.auth import passwd; print(passwd("'$JUPYTER_PASSWORD'"))')" \
    --NotebookApp.root_dir=/app \
    --ServerApp.root_dir=/app \
    --NotebookApp.notebook_dir=/app


# docker run -p 10000:8888 -e JUPYTER_PASSWORD="0543" -e JUPYTER_PATH=/app ket0825/myjupyter