import requests
import datetime as dt
import json


def get_raw_data(url):
    response = requests.get(url)
    now = dt.datetime.now().strftime("%Y-%b-%d-%H-%M-%S")
    # fmt: skip
    js = {
        "url": url,
        "text": response.text,
        "date": now,
    }
    return js


def save_raw_data(js, path_raw_data):
    try:
        with open(path_raw_data, "w") as f:
            json.dump(js, f)
    except Exception as e:
        return e
