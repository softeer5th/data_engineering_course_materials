import pandas as pd
from konlpy.tag import Okt

stopwords = [
    '이', '나오', '있', '가지', '하', '씨', '것', '시키', '들', '만들', 
    '그', '지금', '되', '생각하', '수', '그러', '이', '속', '보', '하나', 
    '않', '집', '없', '살', '나', '모르', '사람', '적', '주', '월', '아니', 
    '데', '등', '자신', '같', '안', '우리', '어떤', '때', '내', '년', '내', 
    '가', '경우', '한', '명', '지', '생각', '대하', '시간', '오', '그녀', 
    '말', '다시', '일', '이런', '그렇', '앞', '위하', '보이', '때문', '번', 
    '그것', '나', '두', '다른', '말하', '어떻', '알', '여자', '그러나', '개', 
    '받', '전', '못하', '들', '일', '사실', '그런', '이렇', '또', '점', 
    '문제', '싶', '더', '말', '사회', '정도', '많', '좀', '그리고', '원', 
    '좋', '잘', '크', '통하', '따르', '소리', '중', '놓'
    ]
types = ['Adjective', 'Adverb', 'Exclamation', 'Noun', 'Number', 'Verb']

def mySplit(text:str)->list:
    res = [w[0] for w in Okt().pos(text) if (w[0] not in stopwords) and (w[1] in types)]
    return res

def splitDataFrame(df:pd.DataFrame, cnt:int) -> list:
    res = []
    idx = 0
    split_size = df.shape[0] // cnt
    for i in range(1, cnt):
        res.append(
            df[idx:idx + split_size + 1]
        )
        idx += split_size + 1
    res.append(df[idx:])
    
    return res

def countWord(df:pd.DataFrame, target:str)->dict:
    # split
    word_df = df[df['teacher_name'] == target].loc[:,'text'].apply(mySplit)
    # 소문자
    word_exp_df = word_df.explode().str.lower()
    # to dict
    word_cnt_dict = word_exp_df.value_counts().to_dict()
    return word_cnt_dict