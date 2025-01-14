import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime as dt

# Const block
LIST_START = 'https://entertain.daum.net/ranking/popular?date=20250114'
AUTHORIZATION = 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI5MmE4ZmUxMi1hMDdkLTRlODAtOTA0YS04MTA3MGRmNzZjY2UiLCJjbGllbnRfaWQiOiIyNkJYQXZLbnk1V0Y1WjA5bHI1azc3WTgiLCJmb3J1bV9rZXkiOiJuZXdzIiwiZm9ydW1faWQiOi05OSwiZ3JhbnRfdHlwZSI6ImFsZXhfY3JlZGVudGlhbHMiLCJhdXRob3JpdGllcyI6WyJST0xFX0NMSUVOVCJdLCJzY29wZSI6W10sImV4cCI6MTczNjg2MTc0M30.v2r_nkmeFpPQFmVKpZ7ZNUrWR-l-XI8CFzkdpwjh88Q'
POPULAR_PATH = 'data/'


def get_popular(js, date):
    url = 'https://entertain.daum.net/ranking/popular?date={}'.format(date)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    li = soup.select('a.link_txt')
    for tag in li:
        js['title'].append(tag.text)
        js['id'].append(tag.get('data-tiara-id'))
    return js
        

def get_sentiment(news_id, authorization):
    url = 'https://action.daum.net/apis/v1/reactions/home?itemKey={}'.format(news_id)
    header = {
        "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
        "referer": url,
        'Authorization' : authorization
    }
    raw = requests.get(url, headers=header)

    s_jsonData = json.loads(raw.text)
    s_jsonData

    sentiment = {"좋아요" : 0, "감동이에요" : 0, "슬퍼요" : 0, "화나요" : 0, "추천해요" : 0}

    sentiment['좋아요'] = s_jsonData['item']['stats']['LIKE']
    sentiment['감동이에요'] = s_jsonData['item']['stats']['IMPRESS']
    sentiment['슬퍼요'] = s_jsonData['item']['stats']['SAD']
    sentiment['화나요'] = s_jsonData['item']['stats']['ANGRY']
    sentiment['추천해요'] = s_jsonData['item']['stats']['RECOMMEND']
    
    return sentiment

# main
js = {'title':[], 'id':[]}
print(get_popular(js, 20250114))
