FROM ubuntu:20.04
RUN apt update && apt install -y python3 python3-pip

RUN pip3 install jupyterlab pandas matplotlib BeautifulSoup4 requests datetime pycountry-convert wordcloud google_play_scraper

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
