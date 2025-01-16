from typing import List
import requests
import json
from pathlib import Path


def fetch_json(url: str):
    r = requests.get(url)
    fetched_data = r.json()
    return fetched_data

def dump_jsonl(data, fp:str | Path):
    if not fp.endswith(".jsonl"):
        raise ValueError("fp must be a jsonl extension")
    for d in data:
        with open(fp, "a", encoding="utf-8") as f:
            l = json.dumps(d, ensure_ascii=False)
            f.write(l +"\n")

def extract_reviews():
    total_reviews = []
    rt_program_ids = [
        "c156dad3-ec06-3b36-8dd6-9368b41284e4",
        # "3f0a0478-c57a-3fc6-8ec1-e4dd8f1e41fb" # 오겜2
    ]
    rt_cursors = [
        "eyJyZWFsbV91c2VySWQiOiJSVF8xZGVhYmYwMi01MzlkLTQ2MTEtYmFhNS1kY2NlYzJjNmRhNTYiLCJlbXNJZCI6ImMxNTZkYWQzLWVjMDYtM2IzNi04ZGQ2LTkzNjhiNDEyODRlNCIsImVtc0lkX2hhc1Jldmlld0lzVmlzaWJsZSI6ImMxNTZkYWQzLWVjMDYtM2IzNi04ZGQ2LTkzNjhiNDEyODRlNF9UIiwiY3JlYXRlRGF0ZSI6IjIwMjQtMDItMTdUMjI6NDk6MzQuNzU2WiJ9",
    ]
    total_reviews.extend(extract_reviews_from_rt(rt_program_ids, rt_cursors, max_page=200))

    ### IMDB 추가 예정
    ### 리뷰 포맷팅 과정 필수 (json 형식 통일시켜야 함)
    json_path = "missions/W2/M5/reviews.jsonl"
    dump_jsonl(total_reviews, json_path)


def extract_reviews_from_rt(program_ids, cursors, max_page=200) -> List:
    reviews = []
    for program_id, cursor in zip(program_ids, cursors):
        baseurl = "https://www.rottentomatoes.com/napi/season"

        # cursor 기준 다음 리뷰 가져오기
        full_url = f"{baseurl}/{program_id}/reviews/user?after={cursor}&pageCount={max_page}"
        # "pageInfo": {
        #     "hasNextPage": false,
        #     "hasPreviousPage": true,
        #     "startCursor": "eyJyZWFsbV91c2VySWQiOiJSVF82MjlkYjQ0Yy1jMjIwLTQzOTUtOGE5My1mMGRjMTJhNDAxNjMiLCJlbXNJZCI6ImMxNTZkYWQzLWVjMDYtM2IzNi04ZGQ2LTkzNjhiNDEyODRlNCIsImVtc0lkX2hhc1Jldmlld0lzVmlzaWJsZSI6ImMxNTZkYWQzLWVjMDYtM2IzNi04ZGQ2LTkzNjhiNDEyODRlNF9UIiwiY3JlYXRlRGF0ZSI6IjIwMjQtMDItMTBUMTk6Mzc6MDMuNzM2WiJ9"
        # }
        while True:
            fetched_data = fetch_json(full_url)        
            reviews.extend(fetched_data['reviews'])
            print("받은 리뷰 수:",  len(fetched_data['reviews']))
            print("총 리뷰 수:", len(reviews))

            if fetched_data['pageInfo']['hasNextPage']:
                cur_cursor = fetched_data['pageInfo']['endCursor']
                full_url = f"{baseurl}/{program_id}/reviews/user?before={cur_cursor}&pageCount={max_page}"
            else:
                break

        # cursor 기준 이전 리뷰 가져오기        
        full_url = f"{baseurl}/{program_id}/reviews/user?before={cursor}&pageCount={max_page}"
        while True:
            fetched_data = fetch_json(full_url)        
            reviews.extend(fetched_data['reviews'])
            print("받은 리뷰 수:",  len(fetched_data['reviews']))
            print("총 리뷰 수:", len(reviews))

            if fetched_data['pageInfo']['hasPreviousPage']:
                cur_cursor = fetched_data['pageInfo']['startCursor']
                full_url = f"{baseurl}/{program_id}/reviews/user?before={cur_cursor}&pageCount={max_page}"
            else:
                break
    
    return reviews