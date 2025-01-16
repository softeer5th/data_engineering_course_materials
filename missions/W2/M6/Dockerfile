FROM quay.io/jupyter/base-notebook:python-3.11
USER root
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     build-essential \
#     gcc \
#     g++ \
#     libzmq3-dev \
#     pkg-config \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER root
COPY missions/W2/M6/jupyter_server_config.py ~/.jupyter
RUN chown jovyan:users ~/.jupyter/jupyter_server_config.py


RUN pip install pandas matplotlib numpy jupyter-server requests nltk wordcloud

# RUN pip install --upgrade pip && \
#     pip install pyzmq && \
#     pip install -r missions/W2/requirements.txt
USER ${NB_UID}
EXPOSE 10000

ENV PYTHONPATH="${PYTHONPATH}:/app" JUPYTER_PASSWORD=


CMD PYTHONPATH=/app:$PYTHONPATH jupyter server \
    --ip=0.0.0.0 \
    --ServerApp.password="$(python -c 'from jupyter_server.auth import passwd; print(passwd("'$JUPYTER_PASSWORD'"))')" \
    --ServerApp.root_dir=/app \
    --ServerApp.notebook_dir=/app


# docker run -p 10000:8888 -e JUPYTER_PASSWORD="0543" -e JUPYTER_PATH=/app ket0825/myjupyter