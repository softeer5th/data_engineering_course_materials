import pandas as pd
import multiprocessing as mp
from wordcloud import WordCloud, STOPWORDS

def myFilter(s:str)-> str:
    if s[0] == '@':
        return ''
    elif len(s) == 1:
        return ''
    else:
        return s.strip("!@#$%^&*()-=;'\",.")

def mySplit(text:str)->list:
    res = []
    res = list(filter(lambda x: x != '', map(myFilter, (text.split()))))
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

def countWord(df:pd.DataFrame, target:int)->dict:
    # split
    word_df = df[df['target'] == target].loc[:,'text'].apply(mySplit)
    # 소문자
    word_exp_df = word_df.explode().str.lower()
    # to dict
    word_cnt_dict = word_exp_df.value_counts().to_dict()
    # 쓸데없는 단어 제거
    for s_word in STOPWORDS:
        word_cnt_dict.pop(s_word, None)
    if word_cnt_dict.get('&amp;', 0) != 0:
        print("있따")
    return word_cnt_dict
