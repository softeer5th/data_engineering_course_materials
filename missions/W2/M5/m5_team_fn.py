import pandas as pd
from konlpy.tag import Kkma
import nltk
from nltk.corpus import stopwords
def myFilter(s:str)-> str:
    if s[0] == '@':
        return ''
    elif len(s) == 1:
        return ''
    else:
        return s.strip("!@#$%^&*()-=;'\",.")

def mySplit(text:str)->list:
    nltk.download('stopwords')
    stopwords_kr = stopwords.words('korean')
    res = [word[0] for word in Kkma().pos(text) if word[1] not in filter_type]
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
    # word_df = df[df['teacher_name'] == target].loc[:,'text'].str.split()
    # 소문자
    word_exp_df = word_df.explode().str.lower()
    # to dict
    word_cnt_dict = word_exp_df.value_counts().to_dict()
    # 쓸데없는 단어 제거
    # for s_word in STOPWORDS:
    #     word_cnt_dict.pop(s_word, None)
    if word_cnt_dict.get('&amp;', 0) != 0:
        print("있따")
    return word_cnt_dict